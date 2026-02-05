package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

var (
	// ErrEmptyObject is returned when trying to add an empty object.
	ErrEmptyObject = errors.New("empty object")
	// ErrUploadClosed is returned when trying to add an object to a closed upload.
	ErrUploadClosed = errors.New("upload is closed")
	// ErrUploadFinalized is returned when trying to add an object to an
	// already finalized upload.
	ErrUploadFinalized = errors.New("upload already finalized")
)

type (
	// A PackedUpload allows multiple objects to be uploaded together in a single
	// upload. This can be more efficient than uploading each object separately
	// if the size of the objects is less than the minimum slab size.
	PackedUpload struct {
		// static upload options
		dataShards   uint8
		parityShards uint8

		// upload state
		reader  *io.PipeReader
		writer  *io.PipeWriter
		objects []packedObject

		// orchestration
		result        packedResult
		resultAvailCh chan struct{}
		once          sync.Once

		tg *threadgroup.ThreadGroup
	}

	packedObject struct {
		offset   int64
		length   int64
		dataKey  [32]byte
		packedAt time.Time
	}

	packedResult struct {
		slabs []slabs.SlabSlice
		err   error
	}
)

// Add adds a new object to the upload. The data will be read until EOF and
// packed into the upload. The caller must call Finalize to get the resulting
// objects after all objects have been added.
func (u *PackedUpload) Add(ctx context.Context, r io.Reader) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-u.resultAvailCh:
		if u.result.err != nil {
			return 0, u.result.err
		}
		return 0, ErrUploadFinalized
	default:
	}

	// create encrypted reader
	dataKey := frand.Entropy256()
	r = encrypt(&dataKey, r, 0)

	// spawn goroutine to interrupt io.Copy if context is cancelled
	done := make(chan struct{})
	defer close(done)
	addCtx, cancel, err := u.tg.AddContext(ctx)
	if err != nil {
		return 0, err
	}
	go func() {
		defer cancel()
		select {
		case <-done:
		case <-addCtx.Done():
			u.reader.CloseWithError(context.Cause(addCtx))
		}
	}()

	// pipe data into writer
	n, err := io.Copy(u.writer, r)
	if err != nil {
		// overwrite the error on cancellation to provide more context
		if ctx.Err() != nil {
			err = context.Cause(addCtx)
		}
		// stream is corrupted, finish with error
		u.finish(nil, err)
		return 0, fmt.Errorf("failed to add object: %w", err)
	} else if n == 0 {
		return 0, ErrEmptyObject
	}

	// create packed object
	u.objects = append(u.objects, packedObject{
		offset:   u.Length(),
		length:   n,
		dataKey:  dataKey,
		packedAt: time.Now(),
	})
	return n, nil
}

// Close closes the packed upload and releases any resources. The caller must
// always call Close to ensure proper cleanup.
func (u *PackedUpload) Close() error {
	_ = u.reader.Close()
	_ = u.writer.Close()
	u.tg.Stop()
	u.finish(nil, ErrUploadClosed)
	return nil
}

// Finalize finalizes the upload and returns the resulting objects. This will
// wait for all slabs to be uploaded before returning. The resulting objects
// will contain the metadata needed to download the objects.
//
// The caller must pin the resulting objects to the indexer when ready. Finalize
// is idempotent and can be called multiple times.
func (u *PackedUpload) Finalize(ctx context.Context) ([]Object, error) {
	// close the writer to signal EOF to the uploader
	_ = u.writer.Close()

	// wait for upload to complete
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-u.resultAvailCh:
		if u.result.err != nil {
			return nil, u.result.err
		}
	}

	// build objects
	slabSize := u.slabSize()
	objects := make([]Object, len(u.objects))
	for i, o := range u.objects {
		// sanity check slab range to avoid panics
		slabsStart, slabsEnd := o.slabRange(slabSize)
		if slabsEnd > int64(len(u.result.slabs)) {
			return nil, fmt.Errorf("packed upload incomplete: object %d references slab %d but only %d slabs uploaded", i, slabsEnd-1, len(u.result.slabs))
		}

		// create object with relevant slabs
		objects[i] = Object{
			dataKey:   o.dataKey[:],
			createdAt: o.packedAt,
			updatedAt: o.packedAt,
			slabs:     slices.Clone(u.result.slabs[slabsStart:slabsEnd]),
		}

		// adjust offset and length
		objects[i].slabs[0].Offset = uint32(o.offset % slabSize)
		if len(objects[i].slabs) > 1 {
			// if spanning multiple slabs, adjust first slab's length
			objects[i].slabs[0].Length = uint32(slabSize - int64(objects[i].slabs[0].Offset))
		}
		lastSlabIdx := len(objects[i].slabs) - 1
		lastSlabOffset := int64(objects[i].slabs[lastSlabIdx].Offset)
		objects[i].slabs[lastSlabIdx].Length = uint32(o.offset + o.length - (slabsEnd-1)*slabSize - lastSlabOffset)
	}

	return objects, nil
}

// Length returns the cumulative length of all objects currently in the upload.
func (u *PackedUpload) Length() int64 {
	if len(u.objects) == 0 {
		return 0
	}
	last := u.objects[len(u.objects)-1]
	return last.offset + last.length
}

// Remaining returns the number of bytes remaining until reaching the optimal
// packed size. Adding objects larger than this will span multiple slabs. To
// minimize padding, prioritize objects that fit within the remaining size.
func (u *PackedUpload) Remaining() int64 {
	slabSize := u.slabSize()
	length := u.Length()
	if length == 0 {
		return slabSize
	}
	return (slabSize - (length % slabSize)) % slabSize
}

// finish sets the result of the upload exactly once
func (u *PackedUpload) finish(slabs []slabs.SlabSlice, err error) {
	_ = u.writer.Close()
	u.once.Do(func() {
		u.result = packedResult{slabs: slabs, err: err}
		close(u.resultAvailCh)
	})
}

// slabRange returns the start and end slab indices for this object.
func (o *packedObject) slabRange(slabSize int64) (start, end int64) {
	start = o.offset / slabSize
	end = (o.offset + o.length + slabSize - 1) / slabSize
	return
}

// slabSize returns the size of a slab based on the number of data shards.
func (u *PackedUpload) slabSize() int64 {
	return int64(u.dataShards) * proto4.SectorSize
}

// UploadPacked creates a new packed upload. This allows multiple objects to be
// packed together for more efficient uploads. The returned PackedUpload can be
// used to add objects and then finalized to get the resulting objects. A packed
// upload is not thread-safe.
func (s *SDK) UploadPacked(ctx context.Context, opts ...UploadOption) (*PackedUpload, error) {
	uo := uploadOption{
		dataShards:   10,
		parityShards: 20,
		hostTimeout:  4 * time.Second, // ~10 Mbps
		maxInflight:  30,
	}
	for _, opt := range opts {
		opt(&uo)
	}

	// validate erasure coding params
	totalShards := int(uo.dataShards) + int(uo.parityShards)
	if err := slabs.ValidateECParams(int(uo.dataShards), totalShards); err != nil {
		return nil, err
	}

	// create RS encoder
	enc, err := reedsolomon.New(int(uo.dataShards), int(uo.parityShards))
	if err != nil {
		return nil, fmt.Errorf("failed to create erasure coder: %w", err)
	}

	// create packed upload
	reader, writer := io.Pipe()
	u := &PackedUpload{
		dataShards:   uo.dataShards,
		parityShards: uo.parityShards,

		reader: reader,
		writer: writer,

		resultAvailCh: make(chan struct{}),
		tg:            threadgroup.New(),
	}

	// register with threadgroup to ensure proper cleanup of goroutines on Close
	bgCtx, bgCancel, err := u.tg.AddContext(ctx)
	if err != nil {
		return nil, err
	}

	// upload slabs in background
	slabCh := make(chan slabUpload, concurrentSlabUploads)
	go func() {
		s.uploadSlabs(bgCtx, slabCh, reader, enc, int(u.dataShards), int(u.parityShards), uo.maxInflight, uo.hostTimeout)
		close(slabCh)
	}()

	// collect uploaded slabs in the background, we have to do this to avoid a
	// deadlock in PackedUpload.Add since it writes to an unbuffered io.Pipe and
	// uploadSlabs reads from that pipe
	go func() {
		defer bgCancel()

		var uploaded []slabs.SlabSlice
		var uploadErr error

	outer:
		for slab := range slabCh {
			if errors.Is(slab.err, io.EOF) {
				break
			} else if slab.err != nil {
				uploadErr = slab.err
				break
			}

			// collect shards
			totalShards := u.dataShards + u.parityShards
			sectors := make([]slabs.PinnedSector, totalShards)
			for n := totalShards; n > 0; n-- {
				select {
				case <-bgCtx.Done():
					uploadErr = bgCtx.Err()
					break outer
				case shard := <-slab.uploadsCh:
					if shard.err != nil {
						uploadErr = fmt.Errorf("failed to upload slab: shard upload failed: %w", shard.err)
						break outer
					}
					sectors[shard.index] = slabs.PinnedSector{
						HostKey: shard.host,
						Root:    shard.root,
					}
				}
			}

			// append uploaded slab
			uploaded = append(uploaded, slabs.SlabSlice{
				EncryptionKey: slab.encryptionKey,
				MinShards:     uint(u.dataShards),
				Sectors:       sectors,
				Offset:        0,
				Length:        slab.length,
			})
		}

		// ensure context is taken into account
		if uploadErr == nil && bgCtx.Err() != nil {
			uploadErr = bgCtx.Err()
		}

		u.finish(uploaded, uploadErr)
	}()

	return u, nil
}
