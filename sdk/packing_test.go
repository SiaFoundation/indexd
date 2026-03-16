package sdk

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func TestUploadPacked(t *testing.T) {
	// create SDK
	appKey := types.GeneratePrivateKey()
	dialer := newMockDialer(50)
	s := newTestSDK(t, appKey, newMockAppClient(), dialer)
	defer s.Close()

	// create packed upload
	u, err := s.UploadPacked()
	if err != nil {
		t.Fatalf("failed to create packed upload: %v", err)
	}
	defer u.Close()

	// assert remaining length
	remaining := u.Remaining()
	if remaining != u.slabSize() {
		t.Fatalf("expected remaining %d, got %d", u.slabSize(), remaining)
	}

	// prepare 3 objects
	data1 := frand.Bytes(1024)
	data2 := frand.Bytes(2048)
	data3 := frand.Bytes(512)
	datas := [][]byte{data1, data2, data3}

	// add those objects
	var total int
	for _, data := range datas {
		n, err := u.Add(t.Context(), bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		} else if n != int64(len(data)) {
			t.Fatalf("unexpected number of bytes returned, %d != %d", len(data), n)
		}
		total += len(data)
	}

	// assert total and remaining length
	if u.Length() != int64(total) {
		t.Fatalf("expected total length %d, got %d", total, u.Length())
	} else if rem := u.Remaining(); rem != u.slabSize()-int64(total) {
		t.Fatalf("expected remaining %d, got %d", u.slabSize()-int64(total), rem)
	}

	// finalize the upload
	objects, err := u.Finalize(t.Context())
	if err != nil {
		t.Fatalf("failed to finalize: %v", err)
	} else if len(objects) != 3 {
		t.Fatalf("expected 3 objects, got %d", len(objects))
	}

	// assert every object
	var offset int
	seen := make(map[types.Hash256]bool)
	for i, obj := range objects {
		// assert offset and size
		if obj.Size() != uint64(len(datas[i])) {
			t.Fatalf("unexpected size %d, expected %d", obj.Size(), len(datas[i]))
		} else if slabs := objects[i].Slabs(); len(slabs) != 1 {
			t.Fatalf("expected 1 slab, got %d", len(obj.Slabs()))
		} else if slabs[0].Offset != uint32(offset) {
			t.Fatalf("unexpected offset: %d != %d", slabs[0].Offset, offset)
		} else {
			offset += len(datas[i])
		}

		// assert unique ID
		if seen[objects[i].ID()] {
			t.Fatalf("object %d has duplicate ID", i)
		}
		seen[objects[i].ID()] = true

		// assert download works
		buf := bytes.NewBuffer(nil)
		if err := s.Download(context.Background(), buf, obj); err != nil {
			t.Fatalf("object %d: failed to download: %v", i, err)
		}
		if !bytes.Equal(buf.Bytes(), datas[i]) {
			t.Fatalf("object %d: data mismatch", i)
		}
	}

	// create a new packed upload
	u, err = s.UploadPacked()
	if err != nil {
		t.Fatalf("failed to create packed upload: %v", err)
	}
	defer u.Close()

	// add objects that span multiple slabs
	dataL := frand.Bytes(int(u.slabSize()) + 1024)
	dataS := frand.Bytes(512)
	if _, err := u.Add(context.Background(), bytes.NewReader(dataL)); err != nil {
		t.Fatalf("failed to add large object: %v", err)
	}
	if _, err := u.Add(context.Background(), bytes.NewReader(dataS)); err != nil {
		t.Fatalf("failed to add small object: %v", err)
	}

	// finalize the upload
	objects, err = u.Finalize(t.Context())
	if err != nil {
		t.Fatalf("failed to finalize: %v", err)
	} else if len(objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(objects))
	}

	// assert first object has two slabs
	if slabs := objects[0].Slabs(); len(slabs) != 2 {
		t.Fatalf("expected 2 slabs, got %d", len(objects[0].Slabs()))
	} else if objects[0].Size() != uint64(len(dataL)) {
		t.Fatalf("expected size %d, got %d", len(dataL), objects[0].Size())
	}

	// assert second object has one slab with offset
	if slabs := objects[1].Slabs(); len(slabs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(objects[1].Slabs()))
	} else if expectedOffset := uint32(len(dataL) % int(u.slabSize())); slabs[0].Offset != expectedOffset {
		t.Fatalf("expected offset %d, got %d", expectedOffset, slabs[0].Offset)
	}

	// assert downloads work
	buf := bytes.NewBuffer(nil)
	if err := s.Download(context.Background(), buf, objects[0]); err != nil {
		t.Fatalf("large object: failed to download: %v", err)
	} else if !bytes.Equal(buf.Bytes(), dataL) {
		t.Fatal("large object: data mismatch")
	}
	buf.Reset()
	if err := s.Download(context.Background(), buf, objects[1]); err != nil {
		t.Fatalf("small object: failed to download: %v", err)
	} else if !bytes.Equal(buf.Bytes(), dataS) {
		t.Fatal("small object: data mismatch")
	}

	// create a new packed upload
	u, err = s.UploadPacked()
	if err != nil {
		t.Fatalf("failed to create packed upload: %v", err)
	}
	defer u.Close()

	// now add objects in reverse order
	if _, err := u.Add(context.Background(), bytes.NewReader(dataS)); err != nil {
		t.Fatalf("failed to add small object: %v", err)
	}
	if _, err := u.Add(context.Background(), bytes.NewReader(dataL)); err != nil {
		t.Fatalf("failed to add large object: %v", err)
	}

	// finalize the upload
	objects, err = u.Finalize(t.Context())
	if err != nil {
		t.Fatalf("failed to finalize: %v", err)
	} else if len(objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(objects))
	}

	// assert first object has one slab
	if slabs := objects[0].Slabs(); len(slabs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(objects[0].Slabs()))
	} else if objects[0].Size() != uint64(len(dataS)) {
		t.Fatalf("expected size %d, got %d", len(dataS), objects[0].Size())
	}

	// assert second object has two slabs
	if slabs := objects[1].Slabs(); len(slabs) != 2 {
		t.Fatalf("expected 2 slabs, got %d", len(objects[1].Slabs()))
	} else if objects[1].Size() != uint64(len(dataL)) {
		t.Fatalf("expected size %d, got %d", len(dataL), objects[1].Size())
	} else if expectedOffset := uint32(len(dataS)); slabs[0].Offset != expectedOffset {
		t.Fatalf("expected offset %d, got %d", expectedOffset, slabs[0].Offset)
	}

	// assert downloads work
	buf.Reset()
	if err := s.Download(context.Background(), buf, objects[0]); err != nil {
		t.Fatalf("small object: failed to download: %v", err)
	} else if !bytes.Equal(buf.Bytes(), dataS) {
		t.Fatal("small object: data mismatch")
	}
	buf.Reset()
	if err := s.Download(context.Background(), buf, objects[1]); err != nil {
		t.Fatalf("large object: failed to download: %v", err)
	} else if !bytes.Equal(buf.Bytes(), dataL) {
		t.Fatal("large object: data mismatch")
	}

	// create a new packed upload
	u, err = s.UploadPacked()
	if err != nil {
		t.Fatalf("failed to create packed upload: %v", err)
	}
	defer u.Close()

	// finalize it and assert Add returns ErrUploadFinalized
	if _, err := u.Add(t.Context(), bytes.NewReader(data1)); err != nil {
		t.Fatalf("failed to add object: %v", err)
	} else if _, err := u.Finalize(t.Context()); err != nil {
		t.Fatalf("failed to finalize: %v", err)
	} else if _, err := u.Add(t.Context(), bytes.NewReader(data1)); !errors.Is(err, ErrUploadFinalized) {
		t.Fatalf("expected ErrUploadFinalized, got %v", err)
	}

	// create a new packed upload
	u, err = s.UploadPacked()
	if err != nil {
		t.Fatalf("failed to create packed upload: %v", err)
	}
	defer u.Close()

	// add data in goroutine
	ctx, cancel := context.WithCancelCause(t.Context())
	errCh := make(chan error, 1)
	go func() {
		_, err := u.Add(ctx, frand.Reader)
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond) // ensure Add reads some data

	// cancel the context
	cause := errors.New(t.Name())
	cancel(cause)

	// assert Add returned the context error
	select {
	case err := <-errCh:
		if !errors.Is(err, cause) {
			t.Fatalf("expected error %v, got %v", cause, err)
		}
	case <-time.After(time.Second):
		t.Fatal("Add did not return after context cancellation")
	}

	// create a new packed upload
	u, err = s.UploadPacked()
	if err != nil {
		t.Fatalf("failed to create packed upload: %v", err)
	}
	defer u.Close()

	// assert immediate finalize without adding data works
	objects, err = u.Finalize(t.Context())
	if err != nil {
		t.Fatalf("failed to finalize: %v", err)
	} else if len(objects) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(objects))
	}

	// create a new packed upload
	u, err = s.UploadPacked()
	if err != nil {
		t.Fatalf("failed to create packed upload: %v", err)
	}

	// add something to kickoff the upload process
	go func() { u.Add(t.Context(), frand.Reader) }()

	// close the upload - the race detector will catch any resource leaks here
	// should there be any
	u.Close()
}
