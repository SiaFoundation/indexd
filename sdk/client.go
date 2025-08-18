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
	}

	downloadOption struct {
		hostTimeout time.Duration
		maxInflight int
	}

	// An AppClient is an interface for the application API of the indexer.
	AppClient interface {
		PinSlab(context.Context, slabs.SlabPinParams) (slabs.SlabID, error)
		UnpinSlab(context.Context, slabs.SlabID) error

		Hosts(context.Context, ...api.URLQueryParameterOption) ([]hosts.HostInfo, error)
		Slab(context.Context, slabs.SlabID) (slabs.PinnedSlab, error)
	}

	// A HostDialer is an interface for writing and reading sectors to/from hosts.
	HostDialer interface {
		// Hosts returns the public keys of all hosts that are available for
		// upload or download.
		Hosts() []types.PublicKey

		// ActiveHosts returns hosts that there is an active connection with.
		ActiveHosts() []types.PublicKey

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
		Key   string
		Slabs []Slab
	}

	// An SDK is a client for the indexd service.
	SDK struct {
		log    *zap.Logger
		client AppClient
		dialer HostDialer

		appKey types.PrivateKey
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
		Sectors:       make([]slabs.SectorPinParams, len(shards)),
	}

	hostCh := make(chan types.PublicKey, maxInFlight)
	go func() {
		defer close(hostCh)

		seen := make(map[types.PublicKey]struct{})
		for _, host := range shuffle(s.dialer.ActiveHosts()) {
			select {
			case <-ctx.Done():
				return
			case hostCh <- host:
				seen[host] = struct{}{}
			}
		}
		for _, host := range shuffle(s.dialer.Hosts()) {
			if _, ok := seen[host]; ok {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case hostCh <- host:
				seen[host] = struct{}{}
			}
		}
	}()

	errCh := make(chan error, len(shards))
	sema := make(chan struct{}, maxInFlight)
	for i := range shards {
		select {
		case <-ctx.Done():
			return slabs.SlabPinParams{}, ctx.Err()
		case sema <- struct{}{}:
			// limit number of concurrent requests
		}

		go func(ctx context.Context, shard []byte, index int) {
			nonce := [24]byte{byte(index)}
			c, _ := chacha20.NewUnauthenticatedCipher(encryptionKey[:], nonce[:])
			c.XORKeyStream(shard, shard)

			defer func() { <-sema }() // release semaphore
			sector := (*[proto4.SectorSize]byte)(shard)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				hostKey, ok := <-hostCh
				if !ok {
					errCh <- ErrNoMoreHosts
					return
				}

				start := time.Now()
				root, err := uploadShard(ctx, sector, hostKey, s.dialer, timeout) // error can be ignored, hosts will be retried until none are left and the upload fails.
				if err == nil {
					slab.Sectors[index] = slabs.SectorPinParams{
						HostKey: hostKey,
						Root:    root,
					}
					errCh <- nil
					s.log.Debug("shard uploaded", zap.Stringer("host", hostKey), zap.Stringer("root", root), zap.Duration("elapsed", time.Since(start)))
					break
				}
				s.log.Debug("failed to upload shard", zap.Stringer("host", hostKey), zap.Error(err))
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
				s.log.Debug("failed to download sector", zap.Stringer("host", sector.HostKey), zap.Error(err))
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
func (s *SDK) Upload(ctx context.Context, r io.Reader, opts ...UploadOption) ([]Slab, error) {
	uo := uploadOption{
		dataShards:   10,
		parityShards: 20,
		hostTimeout:  4 * time.Second,
		maxInflight:  30,
	}
	for _, opt := range opts {
		opt(&uo)
	}

	if (uo.parityShards+uo.dataShards)/uo.dataShards < 2 {
		return nil, errors.New("redundancy must be at least 2x")
	}

	type result struct {
		slab  Slab
		err   error
		index int
	}
	resultCh := make(chan result, 1)
	go func() {
		defer close(resultCh) // signals done
		enc, err := reedsolomon.New(int(uo.dataShards), int(uo.parityShards))
		if err != nil {
			resultCh <- result{err: fmt.Errorf("failed to create erasure coder: %w", err)}
			return
		}
		slabBuf := make([]byte, proto4.SectorSize*int(uo.dataShards))
		sema := make(chan struct{}, 2)
		var wg sync.WaitGroup
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			n, err := readAtMost(r, slabBuf)
			if n == 0 && errors.Is(err, io.EOF) {
				break
			} else if err != nil && !errors.Is(err, io.EOF) {
				resultCh <- result{err: fmt.Errorf("failed to read slab %d: %w", i, err)}
				return
			}
			shards := make([][]byte, uo.dataShards+uo.parityShards)
			for i := range shards {
				shards[i] = make([]byte, proto4.SectorSize)
			}
			stripedSplit(slabBuf, shards[:uo.dataShards])
			if err := enc.Encode(shards); err != nil {
				resultCh <- result{err: fmt.Errorf("failed to encode slab %d shards: %w", i, err)}
				return
			}
			encryptionKey := types.HashBytes(append(s.appKey[:], slabBuf[:n]...))
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				select {
				case <-ctx.Done():
					return
				case sema <- struct{}{}:
					// acquire
				}
				defer func() { <-sema }() // release
				params, err := s.uploadSlab(ctx, encryptionKey, shards, uo.dataShards, uo.maxInflight, uo.hostTimeout)
				if err != nil {
					resultCh <- result{err: fmt.Errorf("failed to upload slab %d: %w", i, err)}
					return
				}

				slabID, err := s.client.PinSlab(ctx, params)
				if err != nil {
					resultCh <- result{err: fmt.Errorf("failed to pin slab %d: %w", i, err)}
					return
				}
				resultCh <- result{slab: Slab{
					ID:     slabID,
					Offset: 0,
					Length: uint32(n),
				}, err: err, index: i}
			}(i)
		}

		wg.Wait()
	}()

	// TODO: cleanup on failure
	var pinned []Slab
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result, ok := <-resultCh:
			if !ok {
				return pinned, nil
			} else if result.err != nil {
				return nil, result.err
			}

			if result.index == len(pinned) {
				pinned = append(pinned, result.slab)
			} else {
				for len(pinned) <= result.index {
					pinned = append(pinned, Slab{})
				}
				pinned[result.index] = result.slab
			}
		}
	}
}

// Download downloads object metadata
//
// TODO: support seeks
func (s *SDK) Download(ctx context.Context, w io.Writer, metadata []Slab, opts ...DownloadOption) error {
	if len(metadata) == 0 {
		return errors.New("no slabs to download")
	}

	do := downloadOption{
		hostTimeout: 30 * time.Second,
		maxInflight: 10,
	}
	for _, opt := range opts {
		opt(&do)
	}

	type work struct {
		shards [][]byte
		err    error
	}
	workCh := make(chan work, 1)
	go func() {
		for i, meta := range metadata {
			pinned, err := s.client.Slab(ctx, meta.ID)
			if err != nil {
				workCh <- work{err: fmt.Errorf("failed to get slab %d metadata: %w", i, err)}
				return
			}
			enc, err := reedsolomon.New(int(pinned.MinShards), len(pinned.Sectors)-int(pinned.MinShards))
			if err != nil {
				workCh <- work{err: fmt.Errorf("failed to create erasure coder for slab %d: %w", i, err)}
				return
			}
			shards, err := s.downloadSlab(ctx, pinned, do.maxInflight, do.hostTimeout)
			if err != nil {
				workCh <- work{err: fmt.Errorf("failed to download slab %d: %w", i, err)}
				return
			} else if err := enc.ReconstructData(shards); err != nil {
				workCh <- work{err: fmt.Errorf("failed to reconstruct slab %d data: %w", i, err)}
				return
			}
			nonce := make([]byte, 24)
			for i := range shards {
				nonce[0] = byte(i)
				c, _ := chacha20.NewUnauthenticatedCipher(pinned.EncryptionKey[:], nonce)
				c.XORKeyStream(shards[i], shards[i]) // decrypt shard in place
			}
			workCh <- work{shards: shards[:pinned.MinShards]}
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
			slab := metadata[i]
			if err := stripedJoin(bw, work.shards, int(slab.Length)); err != nil {
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

	uploaded, err := dialer.WriteSector(ctx, hostKey, sector)
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to upload shard to host %s: %w", hostKey.String(), err)
	}
	return uploaded, nil
}

// shuffle shuffles the elements of a slice in place and returns it.
func shuffle[T any, S ~[]T](s S) S {
	frand.Shuffle(len(s), func(i, j int) {
		s[i], s[j] = s[j], s[i]
	})
	return s
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

func initSDK(client AppClient, dialer HostDialer, appKey types.PrivateKey, log *zap.Logger) (*SDK, error) {
	if client == nil {
		return nil, errors.New("app client is required")
	} else if dialer == nil {
		return nil, errors.New("host dialer is required")
	}
	return &SDK{
		appKey: appKey,
		client: client,
		dialer: dialer,
		log:    log,
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

	c, err := app.NewClient(baseURL, appKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create app client: %w", err)
	}
	dialer, err := NewDialer(c, appKey, options.logger.Named("dialer"))
	if err != nil {
		return nil, fmt.Errorf("failed to create host dialer: %w", err)
	}
	return initSDK(c, dialer, appKey, options.logger)
}
