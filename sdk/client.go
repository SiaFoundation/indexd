package sdk

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"golang.org/x/crypto/chacha20"
)

type (
	hostClient interface {
		WriteSector(ctx context.Context, accountKey types.PrivateKey, hostKey types.PublicKey, data []byte) (rhp.RPCWriteSectorResult, error)
		ReadSector(ctx context.Context, accountKey types.PrivateKey, hostKey types.PublicKey, root types.Hash256, w io.Writer, offset, length uint64) (rhp.RPCReadSectorResult, error)

		UploadQueue() (*client.HostQueue, error)
		Prioritize(hosts []types.PublicKey) []types.PublicKey
		Close() error
	}

	// An appClient is an interface for the application API of the indexer.
	appClient interface {
		Account(ctx context.Context, appKey types.PrivateKey) (resp app.AccountResponse, err error)

		Hosts(context.Context, types.PrivateKey, ...api.URLQueryParameterOption) ([]hosts.HostInfo, error)

		CreateSharedObjectURL(ctx context.Context, appKey types.PrivateKey, objectID types.Hash256, encryptionKey []byte, validUntil time.Time) (string, error)
		SharedObject(ctx context.Context, sharedURL string) (slabs.SharedObject, []byte, error)

		ListObjects(ctx context.Context, appKey types.PrivateKey, cursor slabs.Cursor, limit int) ([]slabs.ObjectEvent, error)
		Object(ctx context.Context, appKey types.PrivateKey, key types.Hash256) (slabs.SealedObject, error)
		PinObject(ctx context.Context, appKey types.PrivateKey, obj slabs.SealedObject) error
		DeleteObject(ctx context.Context, appKey types.PrivateKey, key types.Hash256) error

		Slab(context.Context, types.PrivateKey, slabs.SlabID) (slabs.PinnedSlab, error)
		PinSlabs(context.Context, types.PrivateKey, ...slabs.SlabPinParams) ([]slabs.SlabID, error)
		UnpinSlab(context.Context, types.PrivateKey, slabs.SlabID) error
		PruneSlabs(context.Context, types.PrivateKey) error
	}

	downloadOption struct {
		hostTimeout time.Duration
		maxInflight int
		offset      uint64
		length      uint64
	}

	// An UploadOption configures the upload behavior
	UploadOption func(*uploadOption)

	// A DownloadOption configures the download behavior
	DownloadOption func(*downloadOption)

	// An SDK is a client for the indexd service.
	SDK struct {
		log    *zap.Logger
		appKey types.PrivateKey
		client appClient
		hosts  hostClient
	}
)

var (
	// ErrNotEnoughShards is returned when not enough shards were
	// uploaded or downloaded to satisfy the minimum required shards.
	ErrNotEnoughShards = errors.New("not enough shards")

	// ErrNoMoreHosts is returned when there are no more hosts
	// available to attempt to upload a shard
	ErrNoMoreHosts = errors.New("no more hosts available")
)

type sectorDownload struct {
	index  int
	sector slabs.PinnedSector
}

func (s *SDK) downloadSlab(ctx context.Context, slab slabs.SlabSlice, timeout time.Duration) ([][]byte, error) {
	if slab.MinShards == 0 {
		return nil, errors.New("invalid slab: min shards cannot be 0")
	} else if int(slab.MinShards) > len(slab.Sectors) {
		return nil, fmt.Errorf("invalid slab: min shards %d exceeds sector count %d", slab.MinShards, len(slab.Sectors))
	}

	slabSectors := make(map[types.PublicKey]sectorDownload)
	slabHosts := make([]types.PublicKey, 0, len(slab.Sectors))
	for i, sector := range slab.Sectors {
		slabSectors[sector.HostKey] = sectorDownload{
			index:  i,
			sector: sector,
		}
		slabHosts = append(slabHosts, sector.HostKey)
	}
	if len(slabHosts) < int(slab.MinShards) {
		return nil, fmt.Errorf("slab has %d sectors with hosts, minimum required: %d: %w", len(slabHosts), slab.MinShards, ErrNotEnoughShards)
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		wg.Wait()
	}()

	// calculate offset and length that's required from each sector to recover
	// the data referenced by the slab slice
	offset, length := sectorRegion(slab)

	// prioritize hosts
	slabHosts = s.hosts.Prioritize(slabHosts)

	type result struct {
		index int
		buf   []byte
		err   error
	}
	responseCh := make(chan result, len(slab.Sectors))
	var outstanding int
	// helper to launch download
	tryDownloadSector := func(ctx context.Context, d sectorDownload) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		outstanding++
		wg.Go(func() {
			buf := bytes.NewBuffer(make([]byte, 0, length))
			err := downloadShard(ctx, s.hosts, s.appKey, d.sector.HostKey, buf, d.sector.Root, offset, length, timeout)
			select {
			case <-ctx.Done():
				return
			case responseCh <- result{index: d.index, buf: buf.Bytes(), err: err}:
			}
		})
		return true
	}

	// launch minShards downloads right away
	for range slab.MinShards {
		if !tryDownloadSector(ctx, slabSectors[slabHosts[0]]) {
			return nil, ctx.Err()
		}
		slabHosts = slabHosts[1:]
	}

	// launch more downloads as results come in or periodically to race
	// slow hosts
	var successful int
	shards := make([][]byte, len(slab.Sectors))
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case res := <-responseCh:
			outstanding--
			if res.err == nil {
				// successful download
				shards[res.index] = res.buf
				successful++
				if successful >= int(slab.MinShards) {
					// enough shards downloaded
					return shards, nil
				}
			}
			// check if enough potential successes remain
			rem := int(slab.MinShards) - successful
			if outstanding+len(slabHosts) < rem {
				return nil, ErrNotEnoughShards
			}
			if res.err != nil && len(slabHosts) > 0 {
				if tryDownloadSector(ctx, slabSectors[slabHosts[0]]) {
					slabHosts = slabHosts[1:]
				}
			}
		case <-timer.C:
			// periodically launch an extra download to race slow hosts
			if len(slabHosts) > 0 {
				if tryDownloadSector(ctx, slabSectors[slabHosts[0]]) {
					slabHosts = slabHosts[1:]
				}
			}
		case <-ctx.Done():
			// download got interrupted before it could finish
			return nil, ctx.Err()
		}
		timer.Reset(500 * time.Millisecond)
	}
}

// AppKey returns the app key used by the SDK.
//
// It should be kept secret. Applications
// should store it securely to authenticate with
// the indexer.
func (s *SDK) AppKey() types.PrivateKey {
	return s.appKey
}

// Account retrieves account information for the current app key.
func (s *SDK) Account(ctx context.Context) (app.AccountResponse, error) {
	return s.client.Account(ctx, s.appKey)
}

// PruneSlabs removes all slabs on the account that are not associated with
// an object.
func (s *SDK) PruneSlabs(ctx context.Context) error {
	return s.client.PruneSlabs(ctx, s.appKey)
}

// DeleteObject deletes the object with the given key from the indexer.
func (s *SDK) DeleteObject(ctx context.Context, key types.Hash256) error {
	return s.client.DeleteObject(ctx, s.appKey, key)
}

// Upload uploads the data to hosts.
//
// Appends the metadata of the slabs that were uploaded to the given object.
// After uploading the object, the caller must call PinObject to pin the
// slabs and save the object metadata to the indexer.
func (s *SDK) Upload(ctx context.Context, obj *Object, r io.Reader, opts ...UploadOption) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	uo := uploadOption{
		dataShards:   10,
		parityShards: 20,
		maxInflight:  30,
	}
	for _, opt := range opts {
		opt(&uo)
	}

	totalShards := int(uo.dataShards) + int(uo.parityShards)
	if err := slabs.ValidateECParams(int(uo.dataShards), totalShards); err != nil {
		return err
	}

	r = encrypt((*[32]byte)(obj.dataKey), r, obj.Size())

	// create erasure coder
	enc, err := reedsolomon.New(int(uo.dataShards), int(uo.parityShards))
	if err != nil {
		return fmt.Errorf("failed to create erasure coder: %w", err)
	}

	// start uploading slabs
	slabsCh := make(chan slabUpload, concurrentSlabUploads)
	go s.uploadSlabs(ctx, slabsCh, r, enc, int(uo.dataShards), int(uo.parityShards), uo.maxInflight)

	// collect uploaded slabs in a temporary variable to avoid modifying the
	// object on error and to sort the slabs by index
	var uploaded []slabs.SlabSlice
	var uploadedIndices []int

top:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case slab := <-slabsCh:
			if errors.Is(slab.err, io.EOF) {
				break top
			} else if slab.err != nil {
				return slab.err
			}

			totalShards := uo.dataShards + uo.parityShards
			sectors := make([]slabs.PinnedSector, totalShards)

			// collect all shards
			for n := totalShards; n > 0; n-- {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case shard := <-slab.uploadsCh:
					if shard.err != nil {
						return fmt.Errorf("failed to upload slab: shard upload failed: %w", shard.err)
					}
					sectors[shard.index] = slabs.PinnedSector{
						HostKey: shard.host,
						Root:    shard.root,
					}
				}
			}

			uploaded = append(uploaded, slabs.SlabSlice{
				EncryptionKey: slab.encryptionKey,
				MinShards:     uint(uo.dataShards),
				Sectors:       sectors,
				Offset:        0,
				Length:        slab.length,
			})
			uploadedIndices = append(uploadedIndices, slab.slabIndex)
		}
	}
	sort.Slice(uploaded, func(i, j int) bool {
		return uploadedIndices[i] < uploadedIndices[j] //nolint:gocritic
	})
	obj.slabs = append(obj.slabs, uploaded...)
	return nil
}

// Download downloads object metadata
func (s *SDK) Download(ctx context.Context, w io.Writer, obj Object, opts ...DownloadOption) error {
	do := defaultDownloadOption(obj.Size())
	for _, opt := range opts {
		opt(&do)
	}

	if !do.normalizeRange(obj.Size()) {
		return nil
	}

	// decrypt stream using the object's master key
	if len(obj.dataKey) != 32 {
		return fmt.Errorf("invalid data key length: %d", len(obj.dataKey))
	}
	w = decrypt((*[32]byte)(obj.dataKey), w, uint64(do.offset))

	return s.downloadSlabs(ctx, w, do.maxInflight, do.hostTimeout, obj.slabs, do.offset, do.length)
}

// DownloadSharedObject downloads a shared object from a shared URL
func (s *SDK) DownloadSharedObject(ctx context.Context, w io.Writer, sharedURL string, opts ...DownloadOption) error {
	// retrieve shared object metadata
	obj, encryptionKey, err := s.client.SharedObject(ctx, sharedURL)
	if err != nil {
		return err
	}

	do := defaultDownloadOption(obj.Size())
	for _, opt := range opts {
		opt(&do)
	}

	if !do.normalizeRange(obj.Size()) {
		return nil
	}

	// decrypt stream using the object's master key
	w = decrypt((*[32]byte)(encryptionKey), w, uint64(do.offset))

	return s.downloadSlabs(ctx, w, do.maxInflight, do.hostTimeout, obj.Slabs, do.offset, do.length)
}

func defaultDownloadOption(maxLength uint64) downloadOption {
	return downloadOption{
		hostTimeout: 60 * time.Second, // long to handle slow hosts, racing will ensure we don't waste time unnecessarily
		maxInflight: 80,               // ~20 MiB in memory
		offset:      0,
		length:      maxLength,
	}
}

// normalizeRange clamps the download range to the object size. Returns
// false if the range is empty (nothing to download).
func (do *downloadOption) normalizeRange(maxLength uint64) bool {
	if do.offset >= maxLength || do.length == 0 {
		return false
	}
	do.length = min(do.length, maxLength-do.offset)
	return true
}

// Close closes the SDK and releases all resources.
func (s *SDK) Close() error {
	return s.hosts.Close()
}

// PinObject pins the object's slabs and saves the object metadata to the
// indexer.
func (s *SDK) PinObject(ctx context.Context, obj Object) error {
	params := make([]slabs.SlabPinParams, len(obj.slabs))
	for i, slab := range obj.slabs {
		params[i] = slabs.SlabPinParams{
			EncryptionKey: slab.EncryptionKey,
			MinShards:     slab.MinShards,
			Sectors:       slab.Sectors,
		}
	}

	slabIDs, err := s.client.PinSlabs(ctx, s.appKey, params...)
	if err != nil {
		return fmt.Errorf("failed to pin slabs: %w", err)
	}

	for i, slab := range obj.slabs {
		if expected := slab.Digest(); slabIDs[i] != expected {
			return fmt.Errorf("slab %d: pinned id %s does not match expected id %s", i, slabIDs[i], expected)
		}
	}

	return s.client.PinObject(ctx, s.appKey, obj.Seal(s.appKey).SealedObject)
}

const chunkSize = 1 << 18 // 256 KiB

// chunkIter splits slabs into fixed-size chunks for parallel recovery.
// It handles byte-range selection internally: offset is a byte offset into
// the logical stream of all slabs and length limits total output.
type chunkIter struct {
	slabs     []slabs.SlabSlice
	slabIdx   int
	offset    uint64 // position within current slab
	remaining uint64 // total bytes left to yield
}

func newChunkIter(ss []slabs.SlabSlice, offset, length uint64) *chunkIter {
	ci := &chunkIter{
		slabs:     ss,
		remaining: length,
	}
	for ci.slabIdx < len(ci.slabs) {
		slabLength := uint64(ci.slabs[ci.slabIdx].Length)
		if offset < slabLength {
			break
		}
		offset -= slabLength
		ci.slabIdx++
	}
	ci.offset = offset
	return ci
}

func (ci *chunkIter) next() (slabs.SlabSlice, bool) {
	for ci.remaining > 0 && ci.slabIdx < len(ci.slabs) {
		slab := ci.slabs[ci.slabIdx]
		available := uint64(slab.Length) - ci.offset
		if available == 0 {
			ci.slabIdx++
			ci.offset = 0
			continue
		}
		chunkLen := min(available, ci.remaining, chunkSize)
		chunk := slab
		chunk.Offset = slab.Offset + uint32(ci.offset)
		chunk.Length = uint32(chunkLen)
		ci.offset += chunkLen
		if ci.offset >= uint64(slab.Length) {
			ci.offset = 0
			ci.slabIdx++
		}
		ci.remaining -= chunkLen
		return chunk, true
	}
	return slabs.SlabSlice{}, false
}

type recoveredChunk struct {
	shards   [][]byte
	skip     int
	writeLen int
}

func (s *SDK) recoverChunk(ctx context.Context, chunk slabs.SlabSlice, hostTimeout time.Duration) (recoveredChunk, error) {
	shards, err := s.downloadSlab(ctx, chunk, hostTimeout)
	if err != nil {
		return recoveredChunk{}, fmt.Errorf("failed to download slab: %w", err)
	}

	// decrypt shards
	counter := chunk.Offset / (proto4.LeafSize * uint32(chunk.MinShards))
	var nonce [24]byte
	for i, shard := range shards {
		if shard == nil {
			continue
		}
		nonce[0] = byte(i)
		c, _ := chacha20.NewUnauthenticatedCipher(chunk.EncryptionKey[:], nonce[:])
		c.SetCounter(counter)
		c.XORKeyStream(shard, shard)
	}

	// reconstruct data shards
	enc, err := reedsolomon.New(int(chunk.MinShards), len(shards)-int(chunk.MinShards))
	if err != nil {
		return recoveredChunk{}, fmt.Errorf("failed to create reedsolomon coder: %w", err)
	}
	if err := enc.ReconstructData(shards); err != nil {
		return recoveredChunk{}, fmt.Errorf("failed to reconstruct data shards: %w", err)
	}

	return recoveredChunk{
		shards:   shards[:int(chunk.MinShards)],
		skip:     int(chunk.Offset) % (proto4.LeafSize * int(chunk.MinShards)),
		writeLen: int(chunk.Length),
	}, nil
}

func (s *SDK) downloadSlabs(ctx context.Context, w io.Writer, maxInflight int, hostTimeout time.Duration, ss []slabs.SlabSlice, offset, length uint64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if maxInflight <= 0 {
		return errors.New("download inflight must be greater than 0")
	}

	chunks := newChunkIter(ss, offset, length)
	bw := bufio.NewWriterSize(w, 1<<16)

	// recover first chunk synchronously for fast TTFB
	chunk, ok := chunks.next()
	if !ok {
		return bw.Flush()
	}
	rc, err := s.recoverChunk(ctx, chunk, hostTimeout)
	if err != nil {
		return err
	}
	if err := stripedJoin(bw, rc.shards, rc.skip, rc.writeLen); err != nil {
		return err
	}

	type chunkWork struct {
		index int
		chunk slabs.SlabSlice
	}
	type chunkResult struct {
		index int
		recoveredChunk
		err error
	}

	workCh := make(chan chunkWork, maxInflight)
	resultCh := make(chan chunkResult, maxInflight)

	// start worker pool
	var wg sync.WaitGroup
	for range maxInflight {
		wg.Go(func() {
			for w := range workCh {
				rc, err := s.recoverChunk(ctx, w.chunk, hostTimeout)
				select {
				case resultCh <- chunkResult{index: w.index, recoveredChunk: rc, err: err}:
				case <-ctx.Done():
					return
				}
			}
		})
	}

	// feed chunks to workers
	go func() {
		defer func() {
			close(workCh)
			wg.Wait()
			close(resultCh)
		}()
		for chunkIdx := 0; ; chunkIdx++ {
			chunk, ok := chunks.next()
			if !ok {
				return
			}
			select {
			case workCh <- chunkWork{index: chunkIdx, chunk: chunk}:
			case <-ctx.Done():
				return
			}
		}
	}()

	completed := make(map[int]chunkResult)
	nextWrite := 0
	for res := range resultCh {
		if res.err != nil {
			return res.err
		}
		if res.index == nextWrite {
			if err := stripedJoin(bw, res.shards, res.skip, res.writeLen); err != nil {
				return err
			}
			nextWrite++
			for {
				r, ok := completed[nextWrite]
				if !ok {
					break
				}
				delete(completed, nextWrite)
				if err := stripedJoin(bw, r.shards, r.skip, r.writeLen); err != nil {
					return err
				}
				nextWrite++
			}
		} else {
			completed[res.index] = res
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	if len(completed) > 0 {
		panic(fmt.Sprintf("%d chunks remaining but no tasks in flight", len(completed)))
	}
	return bw.Flush()
}

// stripedSplit splits data into striped data shards, which must have sufficient
// capacity.
func stripedSplit(data []byte, dataShards [][]byte) {
	buf := bytes.NewBuffer(data)
	for off := 0; buf.Len() > 0; off += proto4.LeafSize {
		for _, shard := range dataShards {
			copy(shard[off:], buf.Next(proto4.LeafSize))
		}
	}
}

// stripedJoin joins the striped data shards, writing them to dst. The first 'skip'
// bytes of the recovered data are skipped, and 'writeLen' bytes are written in
// total.
func stripedJoin(dst io.Writer, dataShards [][]byte, skip, writeLen int) error {
	for off := 0; writeLen > 0; off += proto4.LeafSize {
		for _, shard := range dataShards {
			if len(shard[off:]) < proto4.LeafSize {
				return reedsolomon.ErrShortData
			}
			shard = shard[off:][:proto4.LeafSize]
			if skip >= len(shard) {
				skip -= len(shard)
				continue
			} else if skip > 0 {
				shard = shard[skip:]
				skip = 0
			}
			if writeLen < len(shard) {
				shard = shard[:writeLen]
			}
			n, err := dst.Write(shard)
			if err != nil {
				return err
			}
			writeLen -= n
		}
	}
	return nil
}

// sectorRegion returns the offset and length of the sector region that must be
// downloaded in order to recover the data referenced by the slice.
func sectorRegion(ss slabs.SlabSlice) (offset, length uint64) {
	minChunkSize := proto4.LeafSize * uint32(ss.MinShards)
	start := (ss.Offset / minChunkSize) * proto4.LeafSize
	end := ((ss.Offset + ss.Length) / minChunkSize) * proto4.LeafSize
	if (ss.Offset+ss.Length)%minChunkSize != 0 {
		end += proto4.LeafSize
	}
	return uint64(start), uint64(end - start)
}

// downloadShard reads a sector from a host
func downloadShard(ctx context.Context, client hostClient, accountKey types.PrivateKey, hostKey types.PublicKey, w io.Writer, root types.Hash256, offset, length uint64, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := client.ReadSector(ctx, accountKey, hostKey, root, w, offset, length)
	return err
}

// uploadShard uploads a shard to a host
func uploadShard(ctx context.Context, client hostClient, accountKey types.PrivateKey, hostKey types.PublicKey, data []byte, timeout time.Duration) (types.Hash256, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := client.WriteSector(ctx, accountKey, hostKey, data)
	return result.Root, err
}

// readAtMost reads from the reader until the buffer is filled,
// no data is read, an error is returned, or EOF is reached.
//
// It is different from io.ReadFull, which returns [io.ErrUnexpectedEOF]
// if the reader returns less data than requested. This is so EOF can be
// used as a signal to gracefully close the slab loop in Upload.
func readAtMost(r io.Reader, buf []byte) (int, error) {
	var n int
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		} else if m == 0 {
			return n, io.EOF
		}
	}
	return n, nil
}

// WithRedundancy sets the number of data and parity shards for the upload.
// The number of shards must be at least 2x redundancy:
// `(dataShards + parityShards) / dataShards >= 2`.
func WithRedundancy(dataShards, parityShards uint8) UploadOption {
	return func(uo *uploadOption) {
		uo.dataShards = dataShards
		uo.parityShards = parityShards
	}
}

// WithUploadInflight sets the maximum number of concurrent shard uploads.
// This is useful to reduce bandwidth consumption, but will decrease
// performance.
func WithUploadInflight(maxInflight int) UploadOption {
	return func(uo *uploadOption) {
		uo.maxInflight = maxInflight
	}
}

// WithDownloadHostTimeout sets the timeout for reading sectors
// from individual hosts. The default is 60 seconds.
func WithDownloadHostTimeout(timeout time.Duration) DownloadOption {
	return func(do *downloadOption) {
		do.hostTimeout = timeout
	}
}

// WithDownloadInflight sets the maximum number of concurrent chunk
// downloads. The default is 80.
func WithDownloadInflight(maxInflight int) DownloadOption {
	return func(do *downloadOption) {
		do.maxInflight = maxInflight
	}
}

// WithDownloadRange sets the byte range to download from the object.
func WithDownloadRange(offset, length uint64) DownloadOption {
	return func(do *downloadOption) {
		do.offset = offset
		do.length = length
	}
}

// An Option configures the SDK.
type Option func(*SDK)

// WithLogger sets the logger for the SDK. The default behavior is to not log
// anything.
func WithLogger(log *zap.Logger) Option {
	return func(s *SDK) {
		s.log = log
	}
}

func initSDK(appKey types.PrivateKey, app appClient, provider *client.Provider, opts ...Option) *SDK {
	sdk := &SDK{
		appKey: appKey,

		log:    zap.NewNop(), // no logging by default
		client: app,
	}
	for _, opt := range opts {
		opt(sdk)
	}
	sdk.hosts = client.New(provider, sdk.log.Named("client"))
	return sdk
}
