package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

// writeBinary responds with v in the Sia binary encoding.
func writeBinary(w http.ResponseWriter, v types.EncoderTo) {
	w.Header().Set("Content-Type", applicationOctetStream)
	e := types.NewEncoder(w)
	v.EncodeTo(e)
	e.Flush()
}

func TestDoRequestHTTPError(t *testing.T) {
	do := func(h http.HandlerFunc) error {
		t.Helper()

		srv := httptest.NewServer(h)
		defer srv.Close()

		u, err := url.Parse(srv.URL)
		if err != nil {
			t.Fatal(err)
		}

		_, err = doRequest(context.Background(), http.MethodGet, u, nil, applicationJSON)
		return err
	}

	// empty body falls back to status text
	err := do(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})

	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatal("expected HTTPError")
	} else if httpErr.StatusCode != http.StatusBadGateway {
		t.Fatal("unexpected", httpErr.StatusCode)
	} else if httpErr.Body != "" {
		t.Fatal("unexpected", httpErr.Body)
	} else if httpErr.Error() != "HTTP 502: Bad Gateway" {
		t.Fatal("unexpected", httpErr.Error())
	}

	// body is trimmed and used as the message
	err = do(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("  database is down\n"))
	})

	if !errors.As(err, &httpErr) {
		t.Fatal("expected HTTPError")
	} else if httpErr.StatusCode != http.StatusInternalServerError {
		t.Fatal("unexpected", httpErr.StatusCode)
	} else if httpErr.Body != "database is down" {
		t.Fatal("unexpected", httpErr.Body)
	} else if httpErr.Error() != "HTTP 500: database is down" {
		t.Fatal("unexpected", httpErr.Error())
	}

	// non-standard status with empty body omits the trailing colon
	err = do(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(599)
	})

	if !errors.As(err, &httpErr) {
		t.Fatal("expected HTTPError")
	} else if httpErr.StatusCode != 599 {
		t.Fatal("unexpected", httpErr.StatusCode)
	} else if httpErr.Body != "" {
		t.Fatal("unexpected", httpErr.Body)
	} else if httpErr.Error() != "HTTP 599" {
		t.Fatal("unexpected", httpErr.Error())
	}
}

func TestListObjectsBatched(t *testing.T) {
	const numSlabs = 2*api.MaxLimit + 1
	pinned := make(map[slabs.SlabID]slabs.PinnedSlab, numSlabs+1)
	newSlab := func() slabs.PinnedSlab {
		slab := slabs.PinnedSlab{
			EncryptionKey: frand.Entropy256(),
			MinShards:     1,
			Sectors: []slabs.PinnedSector{{
				Root:    frand.Entropy256(),
				HostKey: types.PublicKey{1},
			}},
		}
		slab.ID = slab.Slice(0, 1).Digest()
		pinned[slab.ID] = slab
		return slab
	}
	refs := make([]slabs.ObjectSlab, numSlabs)
	for i := range refs {
		refs[i] = slabs.ObjectSlab{
			ID:     newSlab().ID,
			Offset: uint32(i),
			Length: uint32(i + 1),
		}
	}
	// unpinned between listing the references and fetching the slabs
	unpinned := slabs.ObjectSlab{ID: slabs.SlabID(frand.Entropy256()), Length: 1}
	// replaces the deleted object when the page is fetched again
	extra := slabs.ObjectSlab{ID: newSlab().ID, Length: 1}

	var listings, requests, requested, inflight atomic.Int64
	var concurrent atomic.Bool
	overlap := make(chan struct{}) // closed once two slab requests are in flight at the same time
	var closeOverlap sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/objects":
			if got := r.URL.Query().Get("expandslabs"); got != "false" {
				t.Errorf("expected expandslabs=false, got %q", got)
			}
			events := []slabs.ObjectEventReference{
				{Key: types.Hash256{1}, Object: &slabs.SealedObjectReference{Slabs: refs}},
				{Key: types.Hash256{2}, Object: &slabs.SealedObjectReference{Slabs: []slabs.ObjectSlab{unpinned}}},
				{Key: types.Hash256{3}, Deleted: true},
			}
			if listings.Add(1) > 1 {
				events = []slabs.ObjectEventReference{
					events[0],
					events[2],
					{Key: types.Hash256{4}, Object: &slabs.SealedObjectReference{Slabs: []slabs.ObjectSlab{extra}}},
				}
			}
			writeBinary(w, slabs.ObjectEventReferences(events))
		case "/slabs/batch":
			var ids []slabs.SlabID
			if err := json.NewDecoder(r.Body).Decode(&ids); err != nil {
				t.Errorf("failed to decode slab IDs: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			} else if len(ids) > api.MaxLimit {
				t.Errorf("too many slab IDs: %d", len(ids))
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			requests.Add(1)
			requested.Add(int64(len(ids)))
			if inflight.Add(1) > 1 {
				closeOverlap.Do(func() {
					concurrent.Store(true)
					close(overlap)
				})
			}
			// hold the response until another slab request overlaps this one
			select {
			case <-overlap:
			case <-time.After(time.Second):
			}
			inflight.Add(-1)

			var resp slabs.PinnedSlabs
			for _, id := range ids {
				if slab, ok := pinned[id]; ok {
					if listings.Load() > 1 {
						// migrated after the first page was listed
						slab.Sectors = []slabs.PinnedSector{{Root: slab.Sectors[0].Root, HostKey: types.PublicKey{2}}}
					}
					resp = append(resp, slab)
				}
			}
			writeBinary(w, resp)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	events, err := client.ListObjectsBatched(t.Context(), types.GeneratePrivateKey(), slabs.Cursor{}, 3)
	if err != nil {
		t.Fatal(err)
	} else if listings.Load() != 2 {
		t.Fatalf("expected 2 listings, got %d", listings.Load())
	} else if len(events) != 3 {
		t.Fatalf("expected a full page, got %+v", events)
	} else if events[0].Key != (types.Hash256{1}) || events[0].Object == nil {
		t.Fatalf("unexpected first event: %+v", events[0])
	} else if events[1].Key != (types.Hash256{3}) || !events[1].Deleted || events[1].Object != nil {
		t.Fatalf("unexpected second event: %+v", events[1])
	} else if events[2].Key != (types.Hash256{4}) || events[2].Object == nil || len(events[2].Object.Slabs) != 1 || events[2].Object.Slabs[0].Digest() != extra.ID {
		t.Fatalf("unexpected third event: %+v", events[2])
	} else if len(events[0].Object.Slabs) != numSlabs {
		t.Fatalf("expected %d slabs, got %d", numSlabs, len(events[0].Object.Slabs))
	} else if requests.Load() != 6 || requested.Load() != 2*numSlabs+2 {
		t.Fatalf("expected 6 requests fetching %d slabs, got %d requests fetching %d", 2*numSlabs+2, requests.Load(), requested.Load())
	} else if !concurrent.Load() {
		t.Fatal("expected slabs to be fetched concurrently")
	}
	for i, slab := range events[0].Object.Slabs {
		if slab.Digest() != refs[i].ID || slab.Offset != refs[i].Offset || slab.Length != refs[i].Length {
			t.Fatalf("unexpected slab %d: %+v", i, slab)
		} else if slab.Sectors[0].HostKey != (types.PublicKey{2}) {
			t.Fatalf("expected slab %d to reflect the migration, got host %v", i, slab.Sectors[0].HostKey)
		}
	}
}

func TestListObjectsBatchedConcurrentDeletes(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var listings atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/objects":
			if listings.Add(1) == 2 {
				cancel()
			}
			writeBinary(w, slabs.ObjectEventReferences{{
				Key:    types.Hash256{1},
				Object: &slabs.SealedObjectReference{Slabs: []slabs.ObjectSlab{{ID: slabs.SlabID(frand.Entropy256()), Length: 1}}},
			}})
		case "/slabs/batch":
			writeBinary(w, slabs.PinnedSlabs(nil))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL).ListObjectsBatched(ctx, types.GeneratePrivateKey(), slabs.Cursor{}, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	} else if listings.Load() != 2 {
		t.Fatalf("expected 2 listing attempts, got %d", listings.Load())
	}
}
