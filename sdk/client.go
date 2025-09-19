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
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

type (
	uploadOption struct {
		dataShards   uint8
		parityShards uint8
		hostTimeout  time.Duration
		maxInflight  int

		customKey         *[32]byte
		disableEncryption bool
	}

	downloadOption struct {
		hostTimeout time.Duration
		maxInflight int
	}

	// An AppClient is an interface for the application API of the indexer.
	AppClient interface {
		Hosts(context.Context, ...api.URLQueryParameterOption) ([]hosts.HostInfo, error)

		CreateSharedObjectURL(ctx context.Context, objectKey types.Hash256, encryptionKey [32]byte, validUntil time.Time) (string, error)
		SharedObject(ctx context.Context, sharedURL string) (slabs.SharedObject, *[32]byte, error)
		SaveObject(ctx context.Context, obj slabs.Object) (err error)

		Slab(context.Context, slabs.SlabID) (slabs.PinnedSlab, error)
		PinSlab(context.Context, slabs.SlabPinParams) (slabs.SlabID, error)
		UnpinSlab(context.Context, slabs.SlabID) error
	}

	// A HostDialer is an interface for writing and reading sectors to/from hosts.
	HostDialer interface {
		// Hosts returns the public keys of all hosts that are available for
		// upload or download.
		Hosts() []types.PublicKey

		// WriteSector writes a sector to the host identified by the public key.
		WriteSector(context.Context, types.PublicKey, *[proto4.SectorSize]byte) (types.Hash256, error)
		// ReadSector reads a sector from the host identified by the public key.
		ReadSector(context.Context, types.PublicKey, types.Hash256) (*[proto4.SectorSize]byte, error)
	}

	// An UploadOption configures the upload behavior
	UploadOption func(*uploadOption)

	// A DownloadOption configures the download behavior
	DownloadOption func(*downloadOption)

	// A Slab represents a collection of erasure-coded sectors
	Slab struct {
		ID     slabs.SlabID `json:"id"`
		Offset uint32       `json:"offset"`
		Length uint32       `json:"length"`
	}

	// An Object represents a collection of slabs that are associated with a
	// specific key.
	Object struct {
		Key   *[32]byte
		Slabs []Slab
	}

	// An SDK is a client for the indexd service.
	SDK struct {
		appKey types.PrivateKey
		client AppClient

		dialer HostDialer
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

func (s *SDK) uploadSlab(ctx context.Context, encryptionKey [32]byte, shards [][]byte, dataShards uint8, maxInFlight int, timeout time.Duration) (slabs.SlabPinParams, error) {
	if len(shards) == 0 {
		return slabs.SlabPinParams{}, errors.New("no shards to upload")
	} else if len(shards) < int(dataShards) {
		return slabs.SlabPinParams{}, fmt.Errorf("not enough shards to upload: %d, required: %d", len(shards), dataShards)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	slab := slabs.SlabPinParams{
		EncryptionKey: encryptionKey,
		MinShards:     uint(dataShards),
		Sectors:       make([]slabs.PinnedSector, len(shards)),
	}

	var hostsMu sync.Mutex
	hosts := s.dialer.Hosts()
	if len(hosts) < len(shards) {
		return slabs.SlabPinParams{}, fmt.Errorf("not enough hosts available: %d, required: %d", len(hosts), len(shards))
	}

	errCh := make(chan error, len(shards))
	nonce := make([]byte, 24)
	sema := make(chan struct{}, maxInFlight)
	for i := range shards {
		// encrypt the shard before upload
		nonce[0] = byte(i)
		c, _ := chacha20.NewUnauthenticatedCipher(encryptionKey[:], nonce)
		c.XORKeyStream(shards[i], shards[i])

		select {
		case <-ctx.Done():
			return slabs.SlabPinParams{}, ctx.Err()
		case sema <- struct{}{}:
			// limit number of concurrent requests
		}

		go func(ctx context.Context, shard []byte, index int) {
			defer func() { <-sema }() // release semaphore
			sector := (*[proto4.SectorSize]byte)(shard)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				hostsMu.Lock()
				if len(hosts) == 0 {
					errCh <- ErrNoMoreHosts
					hostsMu.Unlock()
					return
				}
				hostKey := hosts[0]
				hosts = hosts[1:]
				hostsMu.Unlock()

				root, err := uploadShard(ctx, sector, hostKey, s.dialer, timeout) // error can be ignored, hosts will be retried until none are left and the upload fails.
				if err == nil {
					slab.Sectors[index] = slabs.PinnedSector{
						HostKey: hostKey,
						Root:    root,
					}
					errCh <- nil
					break
				}
			}
		}(ctx, shards[i], i)
	}
	for range len(shards) {
		select {
		case <-ctx.Done():
			return slabs.SlabPinParams{}, ctx.Err()
		case err := <-errCh:
			if err != nil {
				return slabs.SlabPinParams{}, fmt.Errorf("failed to upload shard: %w", err)
			}
		}
	}
	return slab, nil
}

func (s *SDK) downloadSlab(ctx context.Context, slab slabs.PinnedSlab, maxInflight int, timeout time.Duration) ([][]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var successful atomic.Uint32
	var wg sync.WaitGroup
	sectors := make([][]byte, len(slab.Sectors))
	sema := make(chan struct{}, maxInflight)
top:
	for i, sector := range slab.Sectors {
		select {
		case <-ctx.Done():
			break top
		case sema <- struct{}{}:
			// limit number of concurrent requests
		}
		wg.Add(1)
		go func(ctx context.Context, sector slabs.PinnedSector, i int) {
			defer func() { <-sema }() // release semaphore
			defer wg.Done()
			data, err := downloadShard(ctx, sector.Root, sector.HostKey, s.dialer, timeout)
			if err != nil {
				return
			}
			sectors[i] = data[:]
			if v := successful.Add(1); v >= uint32(slab.MinShards) {
				// got enough pieces to recover
				cancel()
			}
		}(ctx, sector, i)
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
	uo := uploadOption{
		dataShards:   10,
		parityShards: 20,
		hostTimeout:  4 * time.Second, // ~10 Mbps
		maxInflight:  30,
	}
	for _, opt := range opts {
		opt(&uo)
	}

	if (uo.parityShards+uo.dataShards)/uo.dataShards < 2 {
		return Object{}, errors.New("redundancy must be at least 2x")
	} else if uo.disableEncryption && uo.customKey != nil {
		return Object{}, errors.New("custom key provided but encryption disabled ")
	}

	var obj Object
	if !uo.disableEncryption {
		if uo.customKey != nil {
			obj.Key = uo.customKey
		} else {
			obj.Key = new([32]byte)
			frand.Read(obj.Key[:])
		}

		r = encrypt(obj.Key, r, 0)
	}

	type work struct {
		length        int
		shards        [][]byte
		encryptionKey [32]byte
		err           error
	}
	workCh := make(chan work, 1)
	go func() {
		enc, err := reedsolomon.New(int(uo.dataShards), int(uo.parityShards))
		if err != nil {
			workCh <- work{err: fmt.Errorf("failed to create erasure coder: %w", err)}
			return
		}
		slabBuf := make([]byte, proto4.SectorSize*int(uo.dataShards))
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			n, err := readAtMost(r, slabBuf)
			if n == 0 && errors.Is(err, io.EOF) {
				workCh <- work{err: io.EOF} // signal done with EOF
				break
			} else if err != nil && !errors.Is(err, io.EOF) {
				workCh <- work{err: fmt.Errorf("failed to read slab %d: %w", i, err)}
				return
			}
			shards := make([][]byte, uo.dataShards+uo.parityShards)
			for i := range shards {
				shards[i] = make([]byte, proto4.SectorSize)
			}
			stripedSplit(slabBuf, shards[:uo.dataShards])
			if err := enc.Encode(shards); err != nil {
				workCh <- work{err: fmt.Errorf("failed to encode slab %d shards: %w", i, err)}
				return
			}
			encryptionKey := types.HashBytes(append(s.appKey[:], slabBuf[:n]...))
			workCh <- work{length: n, encryptionKey: encryptionKey, shards: shards}
		}
	}()

	// TODO: cleanup on failure
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return Object{}, ctx.Err()
		case work := <-workCh:
			err := work.err
			shards := work.shards
			shardKey := work.encryptionKey

			if errors.Is(err, io.EOF) {
				// no more slabs to upload, return the pinned slabs
				return obj, nil
			} else if work.err != nil {
				return Object{}, work.err
			}
			params, err := s.uploadSlab(ctx, shardKey, shards, uo.dataShards, uo.maxInflight, uo.hostTimeout)
			if err != nil {
				return Object{}, fmt.Errorf("failed to upload slab %d: %w", i, err)
			}
			expectedSlabID, err := params.Digest()
			if err != nil {
				return Object{}, fmt.Errorf("failed to compute slab id for slab %d: %w", i, err)
			}

			slabID, err := s.client.PinSlab(ctx, params)
			if err != nil {
				return Object{}, fmt.Errorf("failed to pin slab %d: %w", i, err)
			} else if slabID != expectedSlabID {
				return Object{}, fmt.Errorf("pinned slab %d id %s does not match expected id %s", i, slabID.String(), expectedSlabID.String())
			}
			obj.Slabs = append(obj.Slabs, Slab{
				ID:     slabID,
				Offset: 0,
				Length: uint32(work.length),
			})
		}
	}
}

// Download downloads object metadata
//
// TODO: support seeks
func (s *SDK) Download(ctx context.Context, w io.Writer, obj Object, opts ...DownloadOption) error {
	if len(obj.Slabs) == 0 {
		return errors.New("no slabs to download")
	} else if obj.Key != nil {
		w = decrypt(obj.Key, w, 0)
	}

	do := downloadOption{
		hostTimeout: 4 * time.Second, // ~10 Mbps
		maxInflight: 10,
	}
	for _, opt := range opts {
		opt(&do)
	}

	var curr int
	return s.downloadSlabs(ctx, w, do.maxInflight, do.hostTimeout, func() (slabs.SharedObjectSlab, error) {
		if curr >= len(obj.Slabs) {
			return slabs.SharedObjectSlab{}, nil
		}
		slab := obj.Slabs[curr]
		curr++

		pinned, err := s.client.Slab(ctx, slab.ID)
		if err != nil {
			return slabs.SharedObjectSlab{}, fmt.Errorf("failed to get slab %d metadata: %w", curr, err)
		}
		return slabs.SharedObjectSlab{
			PinnedSlab: pinned,
			Offset:     slab.Offset,
			Length:     slab.Length,
		}, nil
	})
}

// DownloadSharedObject downloads a shared object from a shared URL
func (s *SDK) DownloadSharedObject(ctx context.Context, w io.Writer, url string, opts ...DownloadOption) error {
	obj, encryptionKey, err := s.client.SharedObject(ctx, url)
	if err != nil {
		return err
	} else if len(obj.Slabs) == 0 {
		return errors.New("no slabs to download")
	} else {
		w = decrypt(encryptionKey, w, 0)
	}

	do := downloadOption{
		hostTimeout: 4 * time.Second, // ~10 Mbps
		maxInflight: 10,
	}
	for _, opt := range opts {
		opt(&do)
	}

	var curr int
	return s.downloadSlabs(ctx, w, do.maxInflight, do.hostTimeout, func() (slabs.SharedObjectSlab, error) {
		if curr >= len(obj.Slabs) {
			return slabs.SharedObjectSlab{}, nil
		}
		slab := obj.Slabs[curr]
		curr++
		return slab, nil
	})
}

type slabIterFn func() (slabs.SharedObjectSlab, error)

func (s *SDK) downloadSlabs(ctx context.Context, w io.Writer, maxInflight int, hostTimeout time.Duration, next slabIterFn) error {
	type work struct {
		shards [][]byte
		length int
		err    error
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

			enc, err := reedsolomon.New(int(slab.MinShards), len(slab.Sectors)-int(slab.MinShards))
			if err != nil {
				sendErr(fmt.Errorf("failed to create erasure coder: %w", err))
				return
			}
			shards, err := s.downloadSlab(ctx, slab.PinnedSlab, maxInflight, hostTimeout)
			if err != nil {
				sendErr(fmt.Errorf("failed to download slab: %w", err))
				return
			} else if err := enc.ReconstructData(shards); err != nil {
				sendErr(fmt.Errorf("failed to reconstruct slab data: %w", err))
				return
			}
			nonce := make([]byte, 24)
			for i := range shards {
				nonce[0] = byte(i)
				c, _ := chacha20.NewUnauthenticatedCipher(slab.EncryptionKey[:], nonce)
				c.XORKeyStream(shards[i], shards[i]) // decrypt shard in place
			}
			workCh <- work{
				shards: shards[:slab.MinShards],
				length: int(slab.Length),
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
			if err := stripedJoin(bw, work.shards, work.length); err != nil {
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
func stripedJoin(dst io.Writer, dataShards [][]byte, writeLen int) error {
	for off := 0; writeLen > 0; off += proto4.LeafSize {
		for _, shard := range dataShards {
			if len(shard[off:]) < proto4.LeafSize {
				return reedsolomon.ErrShortData
			}
			shard = shard[off:][:proto4.LeafSize]
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
func downloadShard(ctx context.Context, root types.Hash256, hostKey types.PublicKey, dialer HostDialer, timeout time.Duration) (*[proto4.SectorSize]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return dialer.ReadSector(ctx, hostKey, root)
}

// uploadShard uploads a shard to a host
func uploadShard(ctx context.Context, sector *[proto4.SectorSize]byte, hostKey types.PublicKey, dialer HostDialer, timeout time.Duration) (types.Hash256, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rootCh := make(chan types.Hash256, 1)
	go func() {
		root := proto4.SectorRoot(sector)
		rootCh <- root
	}()

	uploaded, err := dialer.WriteSector(ctx, hostKey, sector)
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to upload shard to host %s: %w", hostKey.String(), err)
	} else if root := <-rootCh; uploaded != root {
		return types.Hash256{}, fmt.Errorf("uploaded shard root %s does not match expected root %s", uploaded.String(), root.String())
	}
	return uploaded, nil
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

// WithDisableEncryption disables client side encryption for uploads.  By
// default client side encryption is enabled.
func WithDisableEncryption() UploadOption {
	return func(uo *uploadOption) {
		uo.disableEncryption = true
	}
}

// WithXChaCha20Secret sets a custom key for client side encryption.  By
// default a randomly generated key is used.  In both cases, the key will be
// returned alongside the slabs in the Object.
func WithXChaCha20Secret(key [32]byte) UploadOption {
	return func(uo *uploadOption) {
		uo.customKey = &key
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

func initSDK(client AppClient, dialer HostDialer, appKey types.PrivateKey) (*SDK, error) {
	if client == nil {
		return nil, errors.New("app client is required")
	} else if dialer == nil {
		return nil, errors.New("host dialer is required")
	}
	return &SDK{
		appKey: appKey,
		client: client,
		dialer: dialer,
	}, nil
}

type (
	option struct {
		logger *zap.Logger
	}

	// Option is a functional option for configuring the SDK.
	Option func(*option)
)

// WithLogger sets the logger for the SDK. The default behavior is to not log
// anything.
func WithLogger(logger *zap.Logger) Option {
	return func(o *option) {
		o.logger = logger
	}
}

// NewSDK creates a new indexd client with the given app key and base URL.
func NewSDK(baseURL string, appKey types.PrivateKey, opts ...Option) (*SDK, error) {
	options := option{
		logger: zap.NewNop(), // no logging by default
	}
	for _, opt := range opts {
		opt(&options)
	}

	options.logger = options.logger.Named("sdk") // decorate logger

	c := app.NewClient(baseURL, appKey)
	dialer, err := NewDialer(c, appKey, options.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create host dialer: %w", err)
	}
	return initSDK(c, dialer, appKey)
}
