package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

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

// objectSlabsResponse describes a response from the object slab-slice endpoint.
type objectSlabsResponse struct {
	key    types.Hash256
	slabs  []slabs.SlabSlice
	status int // served instead of the slabs when non-zero
}

// newObjectSlabsResponse builds n slab slices and computes their object ID.
func newObjectSlabsResponse(n int) *objectSlabsResponse {
	obj := new(objectSlabsResponse)
	for i := range n {
		obj.slabs = append(obj.slabs, newTestSlabSlice(i))
	}
	obj.key = slabs.ObjectID(obj.slabs)
	return obj
}

func newTestSlabSlice(i int) slabs.SlabSlice {
	return slabs.SlabSlice{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []slabs.PinnedSector{{
			Root:    frand.Entropy256(),
			HostKey: types.PublicKey{1},
		}},
		Offset: uint32(i),
		Length: uint32(i + 1),
	}
}

// slabsRoute is the route the client uses to page through an object's slabs.
func slabsRoute(key types.Hash256) string {
	return fmt.Sprintf("/objects/%s/slabs", key)
}

// writeCBOR responds with v encoded as CBOR, like the handlers do.
func writeCBOR(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	buf, err := encodeCBOR(v)
	if err != nil {
		t.Errorf("failed to encode response: %v", err)
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
	w.Header().Set(contentTypeHeader, applicationCBOR)
	w.Write(buf)
}

// serveObjectEvents serves a page of object events without their slabs.
func serveObjectEvents(t *testing.T, w http.ResponseWriter, events ...slabs.ObjectEventWithoutSlabs) {
	t.Helper()
	writeCBOR(t, w, events)
}

// newObjectEvent builds a listed event whose object's slabs are fetched
// separately.
func newObjectEvent(key types.Hash256) slabs.ObjectEventWithoutSlabs {
	return slabs.ObjectEventWithoutSlabs{Key: key, Object: new(slabs.SealedObjectWithoutSlabs)}
}

// serveObjectSlabs serves a page of the object's slabs, mirroring the handler.
func serveObjectSlabs(t *testing.T, w http.ResponseWriter, r *http.Request, obj *objectSlabsResponse) {
	t.Helper()
	if obj == nil {
		http.Error(w, "object not found", http.StatusNotFound)
		return
	} else if obj.status != 0 {
		http.Error(w, http.StatusText(obj.status), obj.status)
		return
	}
	cursor, err := strconv.Atoi(r.URL.Query().Get("cursor"))
	if err != nil || cursor < 0 {
		t.Errorf("invalid cursor %q: %v", r.URL.Query().Get("cursor"), err)
		http.Error(w, "invalid cursor", http.StatusBadRequest)
		return
	}
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit < 1 || limit > api.MaxLimit {
		t.Errorf("unexpected limit %q: %v", r.URL.Query().Get("limit"), err)
		http.Error(w, "bad limit", http.StatusBadRequest)
		return
	}
	page := []slabs.SlabSlice{}
	if cursor < len(obj.slabs) {
		page = obj.slabs[cursor:min(cursor+limit, len(obj.slabs))]
	}
	writeCBOR(t, w, page)
}

func TestListObjectsWithSlabPagination(t *testing.T) {
	// the large object's slab slices span three pages
	large := newObjectSlabsResponse(2*api.MaxLimit + 1)
	// deleted between listing the events and fetching its slab slices
	deleted := &objectSlabsResponse{key: types.Hash256{2}, status: http.StatusNotFound}
	// replaces the deleted object when the page is listed again
	extra := newObjectSlabsResponse(1)

	// keys[2] is a deletion event, so its slabs are never fetched
	keys := []types.Hash256{large.key, deleted.key, {3}, extra.key}
	objects := map[string]*objectSlabsResponse{
		slabsRoute(large.key):   large,
		slabsRoute(deleted.key): deleted,
		slabsRoute(extra.key):   extra,
	}

	var listings atomic.Int64
	// hold the not-found response until the large object's last page to
	// verify that object slab slices are fetched concurrently
	largeDone := make(chan struct{})
	var closeLargeDone sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/objects":
			if got := r.URL.Query().Get("includeslabs"); got != "false" {
				t.Errorf("expected includeslabs=false, got %q", got)
			}
			events := []slabs.ObjectEventWithoutSlabs{
				newObjectEvent(keys[0]),
				newObjectEvent(keys[1]),
				{Key: keys[2], Deleted: true},
			}
			if listings.Add(1) > 1 {
				events = []slabs.ObjectEventWithoutSlabs{events[0], events[2], newObjectEvent(keys[3])}
			}
			serveObjectEvents(t, w, events...)
		case strings.HasSuffix(r.URL.Path, "/slabs"):
			obj := objects[r.URL.Path]
			if obj == deleted {
				// hold the deletion back until the large object has been
				// paged so the two objects are known to overlap
				select {
				case <-largeDone:
				case <-time.After(time.Second):
					t.Error("expected objects to be fetched concurrently")
				}
			}
			serveObjectSlabs(t, w, r, obj)
			if obj == large && r.URL.Query().Get("cursor") == strconv.Itoa(2*api.MaxLimit) {
				closeLargeDone.Do(func() { close(largeDone) })
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	events, err := NewClient(srv.URL).ListObjectsWithSlabPagination(t.Context(), types.GeneratePrivateKey(), slabs.Cursor{}, 3)
	if err != nil {
		t.Fatal(err)
	} else if listings.Load() != 2 {
		t.Fatalf("expected 2 listings, got %d", listings.Load())
	} else if len(events) != 3 {
		t.Fatalf("expected a full page, got %+v", events)
	} else if events[0].Key != keys[0] || events[0].Object == nil {
		t.Fatalf("unexpected first event: %+v", events[0])
	} else if events[1].Key != keys[2] || !events[1].Deleted || events[1].Object != nil {
		t.Fatalf("unexpected second event: %+v", events[1])
	} else if events[2].Key != keys[3] || events[2].Object == nil {
		t.Fatalf("unexpected third event: %+v", events[2])
	} else if !reflect.DeepEqual(events[0].Object.Slabs, large.slabs) {
		t.Fatalf("expected %d slabs, got %d", len(large.slabs), len(events[0].Object.Slabs))
	} else if !reflect.DeepEqual(events[2].Object.Slabs, extra.slabs) {
		t.Fatalf("unexpected slabs for the third event: %+v", events[2].Object.Slabs)
	}
}

// handlerTransport serves requests in memory so retry tests can use fake time.
type handlerTransport struct {
	http.Handler
}

func (tr handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	w := httptest.NewRecorder()
	tr.ServeHTTP(w, r)
	return w.Result(), nil
}

func TestListObjectsWithSlabPaginationObjectUnavailable(t *testing.T) {
	statuses := map[string]int{
		"deleted": http.StatusNotFound,
		"blocked": http.StatusUnavailableForLegalReasons,
	}
	for name, status := range statuses {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				key := types.Hash256{1}
				var listings []time.Time
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/objects":
						listings = append(listings, time.Now())
						serveObjectEvents(t, w, newObjectEvent(key))
					case slabsRoute(key):
						serveObjectSlabs(t, w, r, &objectSlabsResponse{status: status})
					default:
						http.NotFound(w, r)
					}
				})
				previous := http.DefaultClient
				http.DefaultClient = &http.Client{Transport: handlerTransport{handler}}
				defer func() { http.DefaultClient = previous }()

				_, err := NewClient("http://indexer.test").ListObjectsWithSlabPagination(t.Context(), types.GeneratePrivateKey(), slabs.Cursor{}, 1)
				if !errors.Is(err, errObjectUnavailable) {
					t.Fatalf("expected errObjectUnavailable, got %v", err)
				} else if len(listings) != maxListRetries+1 {
					t.Fatalf("expected %d listing attempts, got %d", maxListRetries+1, len(listings))
				}
				for i := 1; i < len(listings); i++ {
					expected := listRetryDelay << (i - 1)
					if elapsed := listings[i].Sub(listings[i-1]); elapsed != expected {
						t.Fatalf("expected retry delay %v, got %v", expected, elapsed)
					}
				}
			})
		})
	}
}

// TestListObjectsWithSlabPaginationSlabMismatch checks that incomplete or incorrect
// slab slices are rejected when they do not match the listed object ID.
func TestListObjectsWithSlabPaginationSlabMismatch(t *testing.T) {
	obj := newObjectSlabsResponse(3)
	served := map[string]*objectSlabsResponse{
		// a dropped slab must not silently shorten the object
		"truncated": {key: obj.key, slabs: obj.slabs[:len(obj.slabs)-1]},
		// another object's slabs must not be served under this key
		"wrong object": {key: obj.key, slabs: newObjectSlabsResponse(3).slabs},
	}
	for name, served := range served {
		t.Run(name, func(t *testing.T) {
			var listings atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/objects":
					listings.Add(1)
					serveObjectEvents(t, w, newObjectEvent(obj.key))
				case slabsRoute(obj.key):
					serveObjectSlabs(t, w, r, served)
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()

			_, err := NewClient(srv.URL).ListObjectsWithSlabPagination(t.Context(), types.GeneratePrivateKey(), slabs.Cursor{}, 1)
			if !errors.Is(err, errObjectSlabsMismatch) {
				t.Fatalf("expected errObjectSlabsMismatch, got %v", err)
			} else if listings.Load() != 1 {
				t.Fatalf("expected the mismatch not to be retried, got %d listings", listings.Load())
			}
		})
	}
}

func TestListObjectsWithSlabPaginationConcurrentDeletes(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	key := types.Hash256{1}
	var listings atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/objects":
			if listings.Add(1) == 2 {
				cancel()
			}
			serveObjectEvents(t, w, newObjectEvent(key))
		case slabsRoute(key):
			serveObjectSlabs(t, w, r, &objectSlabsResponse{status: http.StatusNotFound})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL).ListObjectsWithSlabPagination(ctx, types.GeneratePrivateKey(), slabs.Cursor{}, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	} else if listings.Load() != 2 {
		t.Fatalf("expected 2 listing attempts, got %d", listings.Load())
	}
}
