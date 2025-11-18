package sdk

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
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
	"lukechampine.com/frand"
)

type (
	hostClient interface {
		WriteSector(ctx context.Context, accountKey types.PrivateKey, hostKey types.PublicKey, data []byte) (rhp.RPCWriteSectorResult, error)
		ReadSector(ctx context.Context, accountKey types.PrivateKey, hostKey types.PublicKey, root types.Hash256, w io.Writer, offset, length uint64) (rhp.RPCReadSectorResult, error)

		Candidates() (*client.Candidates, error)
		Prioritize(hosts []types.PublicKey) []types.PublicKey
		Close() error
	}

	// An appClient is an interface for the application API of the indexer.
	appClient interface {
		Hosts(context.Context, ...api.URLQueryParameterOption) ([]hosts.HostInfo, error)

		CreateSharedObjectURL(ctx context.Context, objectID types.Hash256, encryptionKey []byte, validUntil time.Time) (string, error)
		SharedObject(ctx context.Context, sharedURL string) (slabs.SharedObject, []byte, error)

		ListObjects(ctx context.Context, cursor slabs.Cursor, limit int) ([]slabs.ObjectEvent, error)
		Object(ctx context.Context, key types.Hash256) (slabs.SealedObject, error)
		SaveObject(ctx context.Context, obj slabs.SealedObject) error

		Slab(context.Context, slabs.SlabID) (slabs.PinnedSlab, error)
		PinSlabs(context.Context, ...slabs.SlabPinParams) ([]slabs.SlabID, error)
		UnpinSlab(context.Context, slabs.SlabID) error
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

func (s *SDK) downloadSlab(ctx context.Context, slab slabs.PinnedSlabSlice, maxInflight int, timeout time.Duration) ([][]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	slabSectors := make(map[types.PublicKey]sectorDownload)
	slabHosts := make([]types.PublicKey, 0, len(slab.Sectors))
	for i, sector := range slab.Sectors {
		slabSectors[sector.HostKey] = sectorDownload{
			index:  i,
			sector: sector,
		}
		slabHosts = append(slabHosts, sector.HostKey)
	}

	// calculate offset and length that's required from each sector to recover
	// the data referenced by the slab slice
	offset, length := slab.SectorRegion()

	// prioritize hosts
	slabHosts = s.hosts.Prioritize(slabHosts)

	var successful atomic.Uint32
	var wg sync.WaitGroup
	sectors := make([][]byte, len(slab.Sectors))
	sema := make(chan struct{}, maxInflight)
top:
	for _, hostKey := range slabHosts {
		select {
		case <-ctx.Done():
			break top
		case sema <- struct{}{}:
			// limit number of concurrent requests
		}
		wg.Add(1)
		sector, ok := slabSectors[hostKey]
		if !ok {
			panic("missing slab for host") // developer error
		}
		go func(ctx context.Context, sector slabs.PinnedSector, i int) {
			defer func() {
				<-sema
				wg.Done()
			}()
			buf := bytes.NewBuffer(make([]byte, 0, length))
			err := downloadShard(ctx, s.hosts, s.appKey, sector.HostKey, buf, sector.Root, offset, length, timeout)
			if err != nil {
				return
			}
			sectors[i] = buf.Bytes()
			if v := successful.Add(1); v >= uint32(slab.MinShards) {
				// got enough pieces to recover
				cancel()
			}
		}(ctx, sector.sector, sector.index)
	}

	wg.Wait()
	if n := successful.Load(); n < uint32(slab.MinShards) {
		return nil, fmt.Errorf("retrieved %d sectors, minimum required: %d: %w", n, slab.MinShards, ErrNotEnoughShards)
	}
	return sectors, nil
}

// Upload uploads the data to hosts and pins it to the indexer.
//
// Returns the metadata of the slabs that were pinned
func (s *SDK) Upload(ctx context.Context, r io.Reader, opts ...UploadOption) (Object, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	uo := uploadOption{
		dataShards:   10,
		parityShards: 20,
		hostTimeout:  4 * time.Second, // ~10 Mbps
		maxInflight:  30,
	}
	for _, opt := range opts {
		opt(&uo)
	}

	totalShards := int(uo.dataShards) + int(uo.parityShards)
	if err := slabs.ValidateECParams(int(uo.dataShards), totalShards); err != nil {
		return Object{}, err
	}

	obj := Object{masterKey: frand.Bytes(32)}
	r = encrypt((*[32]byte)(obj.masterKey), r, 0)

	// create erasure coder
	enc, err := reedsolomon.New(int(uo.dataShards), int(uo.parityShards))
	if err != nil {
		return Object{}, fmt.Errorf("failed to create erasure coder: %w", err)
	}

	// start uploading slabs
	slabsCh := make(chan slabUpload, concurrentSlabUploads)
	go s.uploadSlabs(ctx, slabsCh, r, enc, int(uo.dataShards), int(uo.parityShards), uo.maxInflight, uo.hostTimeout)

	// TODO: cleanup on failure
top:
	for {
		select {
		case <-ctx.Done():
			return Object{}, ctx.Err()
		case slab := <-slabsCh:
			err := slab.err
			if errors.Is(err, io.EOF) {
				// all slabs complete
				break top
			} else if slab.err != nil {
				return Object{}, slab.err
			}

			slabIndex := len(obj.slabs)
			totalShards := uo.dataShards + uo.parityShards
			params := slabs.SlabPinParams{
				EncryptionKey: slab.encryptionKey,
				MinShards:     uint(uo.dataShards),
				Sectors:       make([]slabs.PinnedSector, totalShards),
			}

			// collect all shards
			for n := totalShards; n > 0; n-- {
				select {
				case <-ctx.Done():
					return Object{}, ctx.Err()
				case shard := <-slab.uploadsCh:
					if shard.err != nil {
						return Object{}, fmt.Errorf("failed to upload slab: shard upload failed: %w", shard.err)
					}
					params.Sectors[shard.index] = slabs.PinnedSector{
						HostKey: shard.host,
						Root:    shard.root,
					}
				}
			}

			expectedSlabID, err := params.Digest()
			if err != nil {
				return Object{}, fmt.Errorf("failed to compute slab id for slab %d: %w", slabIndex, err)
			}

			slabIDs, err := s.client.PinSlabs(ctx, params)
			if err != nil {
				return Object{}, fmt.Errorf("failed to pin slab %d: %w", slabIndex, err)
			}
			slabID := slabIDs[0]

			if slabID != expectedSlabID {
				return Object{}, fmt.Errorf("pinned slab %d id %s does not match expected id %s", slabIndex, slabID.String(), expectedSlabID.String())
			}
			obj.slabs = append(obj.slabs, slabs.SlabSlice{
				SlabID: slabID,
				Offset: 0,
				Length: slab.length,
			})
		}
	}
	// pin the object
	return obj, s.client.SaveObject(ctx, obj.Seal(s.appKey))
}

// Download downloads object metadata
func (s *SDK) Download(ctx context.Context, w io.Writer, obj Object, opts ...DownloadOption) error {
	// parse options
	do := downloadOption{
		hostTimeout: 4 * time.Second, // ~10 Mbps
		maxInflight: 10,
		offset:      0,
		length:      obj.Size(),
	}
	for _, opt := range opts {
		opt(&do)
	}

	// validate range
	if do.offset+do.length > obj.Size() {
		return errors.New("requested range exceeds object size")
	}

	// determine which slabs are required for the requested range
	required := obj.SlabsForRange(do.offset, do.length)
	if len(required) == 0 {
		return errors.New("no slabs that cover the requested range")
	}

	// decrypt stream using the object's master key
	if len(obj.masterKey) != 32 {
		return fmt.Errorf("invalid master key length: %d", len(obj.masterKey))
	}
	w = decrypt((*[32]byte)(obj.masterKey), w, uint64(do.offset))

	var curr int
	return s.downloadSlabs(ctx, w, do.maxInflight, do.hostTimeout, func() (slabs.PinnedSlabSlice, error) {
		if curr >= len(required) {
			return slabs.PinnedSlabSlice{}, nil
		}
		slab := required[curr]
		curr++

		pinned, err := s.client.Slab(ctx, slab.SlabID)
		if err != nil {
			return slabs.PinnedSlabSlice{}, fmt.Errorf("failed to get slab %d metadata: %w", curr, err)
		}
		return slabs.PinnedSlabSlice{
			PinnedSlab: pinned,
			Offset:     slab.Offset,
			Length:     slab.Length,
		}, nil
	})
}

// DownloadSharedObject downloads a shared object from a shared URL
func (s *SDK) DownloadSharedObject(ctx context.Context, w io.Writer, sharedURL string, opts ...DownloadOption) error {
	// retrieve shared object metadata
	obj, encryptionKey, err := s.client.SharedObject(ctx, sharedURL)
	if err != nil {
		return err
	}

	// parse options
	do := downloadOption{
		hostTimeout: 4 * time.Second, // ~10 Mbps
		maxInflight: 10,
		offset:      0,
		length:      obj.Size(),
	}
	for _, opt := range opts {
		opt(&do)
	}

	// validate range
	if do.offset+do.length > obj.Size() {
		return errors.New("requested range exceeds object size")
	}

	// determine which slabs are required for the requested range
	required := obj.SlabsForRange(do.offset, do.length)
	if len(required) == 0 {
		return errors.New("no slabs that cover the requested range")
	}

	// decrypt stream using the object's master key
	w = decrypt((*[32]byte)(encryptionKey), w, uint64(do.offset))

	var curr int
	return s.downloadSlabs(ctx, w, do.maxInflight, do.hostTimeout, func() (slabs.PinnedSlabSlice, error) {
		if curr >= len(required) {
			return slabs.PinnedSlabSlice{}, nil
		}
		slab := required[curr]
		curr++
		return slab, nil
	})
}

// Close closes the SDK and releases all resources.
func (s *SDK) Close() error {
	return s.hosts.Close()
}

type slabIterFn func() (slabs.PinnedSlabSlice, error)

func (s *SDK) downloadSlabs(ctx context.Context, w io.Writer, maxInflight int, hostTimeout time.Duration, next slabIterFn) error {
	type work struct {
		skip     int
		writeLen int
		shards   [][]byte
		err      error
	}
	workCh := make(chan work, 1)

	sendErr := func(err error) {
		select {
		case workCh <- work{err: err}:
		case <-ctx.Done():
		}
	}

	go func() {
		for {
			slab, err := next()
			if err != nil {
				sendErr(fmt.Errorf("failed to get next slab: %w", err))
				return
			} else if slab.Length == 0 {
				break
			}

			shards, err := s.downloadSlab(ctx, slab, maxInflight, hostTimeout)
			if err != nil {
				sendErr(fmt.Errorf("failed to download slab: %w", err))
				return
			}

			offset := slab.Offset / (proto4.LeafSize * uint32(slab.MinShards))
			var wg sync.WaitGroup
			for i := range shards {
				wg.Add(1)
				go func(i int) {
					nonce := make([]byte, 24)
					nonce[0] = byte(i)
					c, _ := chacha20.NewUnauthenticatedCipher(slab.EncryptionKey[:], nonce)
					c.SetCounter(offset)
					c.XORKeyStream(shards[i], shards[i])
					wg.Done()
				}(i)
			}
			wg.Wait()

			enc, err := reedsolomon.New(int(slab.MinShards), len(shards)-int(slab.MinShards))
			if err != nil {
				sendErr(fmt.Errorf("failed to create reedsolomon coder: %w", err))
				return
			} else if err := enc.ReconstructData(shards); err != nil {
				sendErr(fmt.Errorf("failed to reconstruct data shards: %w", err))
				return
			}

			workCh <- work{
				skip:     int(slab.Offset) % (proto4.LeafSize * int(slab.MinShards)),
				writeLen: int(slab.Length),
				shards:   shards[:int(slab.MinShards)],
			}
		}
		workCh <- work{err: io.EOF}
	}()

	bw := bufio.NewWriterSize(w, 1<<16)
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case work := <-workCh:
			err := work.err
			if errors.Is(err, io.EOF) {
				// EOF signals completion
				if err := bw.Flush(); err != nil {
					return fmt.Errorf("failed to flush write: %w", err)
				}
				return nil
			} else if err != nil {
				return err
			}

			if err := stripedJoin(bw, work.shards, work.skip, work.writeLen); err != nil {
				return fmt.Errorf("failed to write slab %d: %w", i, err)
			}
		}
	}
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

// WithUploadHostTimeout sets the timeout for writing sectors to individual
// hosts. This avoids long hangs when a host is unresponsive or slow.
// The default timeout is 4 seconds, worst case around 300Mbps.
func WithUploadHostTimeout(timeout time.Duration) UploadOption {
	return func(uo *uploadOption) {
		uo.hostTimeout = timeout
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
// from individual hosts. This avoids long hangs when a host is unresponsive
// or slow. The default is 4 seconds, worst case around 300Mbps.
func WithDownloadHostTimeout(timeout time.Duration) DownloadOption {
	return func(do *downloadOption) {
		do.hostTimeout = timeout
	}
}

// WithDownloadInflight sets the maximum number of concurrent shard
// downloads. This is useful to reduce bandwidth waste, but may
// decrease performance.
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

func initSDK(appKey types.PrivateKey, app appClient, hosts hostClient, opts ...Option) *SDK {
	sdk := &SDK{
		appKey: appKey,

		log:    zap.NewNop(), // no logging by default
		hosts:  hosts,
		client: app,
	}
	for _, opt := range opts {
		opt(sdk)
	}
	return sdk
}

// NewSDK creates a new indexd client with the given app key and base URL.
func NewSDK(baseURL string, appKey types.PrivateKey, opts ...Option) (*SDK, error) {
	app := app.NewClient(baseURL, appKey)
	hostStore, err := newCachedHostStore(app)
	if err != nil {
		return nil, fmt.Errorf("failed to create host store: %w", err)
	}
	return initSDK(appKey, app, client.New(client.NewProvider(hostStore)), opts...), nil
}
