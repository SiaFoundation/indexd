package indexd

import (
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
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

type (
	uploadOption struct {
		dataShards   uint8
		parityShards uint8
		hostTimeout  time.Duration
	}

	downloadOption struct {
		hostTimeout time.Duration
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

	SlabSector struct {
		Root    types.Hash256
		HostKey types.PublicKey
	}

	Slab struct {
		SectorKey  [32]byte
		MinSectors uint8
		Offset     uint32
		Length     uint32
		Sectors    []SlabSector
	}

	Object struct {
		Key   string
		Slabs []Slab
	}

	// An SDK is a client for the indexd service.
	SDK struct {
		appKey types.PrivateKey
		c      api.Client

		dialer HostDialer
	}
)

var (
	// ErrNotEnoughShards is returned when not enough shards were
	// uploaded or downloaded to satisfy the minimum required shards.
	ErrNotEnoughShards = errors.New("not enough shards")
)

func (s *SDK) uploadSlab(ctx context.Context, encryptionKey [32]byte, shards [][]byte, dataShards, parityShards uint8, timeout time.Duration) (Slab, error) {
	if len(shards) == 0 {
		return Slab{}, errors.New("no shards to upload")
	}

	slab := Slab{
		SectorKey:  encryptionKey,
		MinSectors: dataShards,
		Sectors:    make([]SlabSector, len(shards)),
	}

	var hostsMu sync.Mutex
	hosts := shuffle(s.dialer.Hosts())
	if len(hosts) < int(dataShards+parityShards) {
		return Slab{}, fmt.Errorf("not enough hosts available: %d, required: %d", len(hosts), dataShards+parityShards)
	}

	errCh := make(chan error, len(shards))
	nonce := make([]byte, 24)
	for i := range shards {
		// encrypt the shard before upload
		nonce[0] = byte(i)
		c, _ := chacha20.NewUnauthenticatedCipher(encryptionKey[:], nonce[:])
		c.XORKeyStream(shards[i], shards[i])
		go func(ctx context.Context, shard []byte, index int) {
			sector := (*[proto4.SectorSize]byte)(shard)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				hostsMu.Lock()
				if len(hosts) == 0 {
					errCh <- errors.New("no hosts available")
					hostsMu.Unlock()
					return
				}
				hostKey := hosts[0]
				hosts = hosts[1:]
				hostsMu.Unlock()

				root, err := uploadShard(ctx, sector, hostKey, s.dialer, timeout) // error can be ignored, hosts will be retried until none are left and the upload fails.
				if err == nil {
					slab.Sectors[index] = SlabSector{
						HostKey: hostKey,
						Root:    root,
					}
					errCh <- nil
					break
				}
			}
		}(ctx, shards[i], i)
	}

	var completed uint8
	for range len(shards) {
		select {
		case <-ctx.Done():
			return Slab{}, ctx.Err()
		case err := <-errCh:
			if err == nil {
				completed++
			}
		}
	}

	// redundancy is guaranteed to be at least 2x; 80% of shards
	// being present is more than enough to repair.
	minShards := uint8(float64(dataShards+parityShards) * 0.8)
	if completed < minShards {
		return Slab{}, fmt.Errorf("upload failed %d successful, %d required: %w", completed, minShards, ErrNotEnoughShards)
	}
	return slab, nil
}

func (s *SDK) downloadSlab(ctx context.Context, slab Slab, timeout time.Duration) ([][]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var successful atomic.Uint32
	var wg sync.WaitGroup
	shards := make([][]byte, len(slab.Sectors))
	for i, sector := range slab.Sectors {
		wg.Add(1)
		go func(ctx context.Context, sector SlabSector, i int) {
			defer wg.Done()

			data, err := downloadShard(ctx, sector.Root, sector.HostKey, s.dialer, timeout)
			if err == nil {
				shards[i] = data[:]
				if new := successful.Add(1); new >= uint32(slab.MinSectors) {
					// got enough pieces to recover
					cancel()
				}
			}
		}(ctx, sector, i)
	}

	wg.Wait()
	if successful.Load() < uint32(slab.MinSectors) {
		return nil, ErrNotEnoughShards
	}
	return shards, nil
}

// Upload uploads the data to hosts and pins it to the indexer.
//
// Returns the metadata of the slabs that were pinned
func (s *SDK) Upload(ctx context.Context, r io.Reader, opts ...UploadOption) ([]Slab, error) {
	uo := uploadOption{
		dataShards:   10,
		parityShards: 20,
		hostTimeout:  4 * time.Second, // ~10 Mbps
	}
	for _, opt := range opts {
		opt(&uo)
	}

	if (uo.parityShards+uo.dataShards)/uo.dataShards < 2 {
		return nil, errors.New("redundancy must be at least 2x")
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

	var pinned []Slab
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case work := <-workCh:
			err := work.err
			shards := work.shards
			shardKey := work.encryptionKey

			if errors.Is(err, io.EOF) {
				// no more slabs to upload, return the pinned slabs
				return pinned, nil
			} else if work.err != nil {
				return nil, work.err
			}
			slab, err := s.uploadSlab(ctx, shardKey, shards, uo.dataShards, uo.parityShards, uo.hostTimeout)
			if err != nil {
				return nil, fmt.Errorf("failed to upload slab %d: %w", i, err)
			}
			slab.Length = uint32(work.length)
			// TODO: pin slab
			pinned = append(pinned, slab)
		}
	}
}

// Download downloads object metadata
//
// TODO: support seeks
func (s *SDK) Download(ctx context.Context, w io.Writer, metadata []Slab, opts ...DownloadOption) error {
	do := downloadOption{
		hostTimeout: 4 * time.Second, // ~10 Mbps
	}
	for _, opt := range opts {
		opt(&do)
	}

	if len(metadata) == 0 {
		return errors.New("no slabs to download")
	}

	dataShards := int(metadata[0].MinSectors)
	parityShards := len(metadata[0].Sectors) - dataShards

	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %w", err)
	}

	type work struct {
		shards [][]byte
		err    error
	}
	workCh := make(chan work, 1)
	go func(ctx context.Context, slabs []Slab) {
		for i, slab := range metadata {
			shards, err := s.downloadSlab(ctx, slab, do.hostTimeout)
			if err != nil {
				workCh <- work{err: fmt.Errorf("failed to download slab %d: %w", i, err)}
				return
			}
			workCh <- work{shards: shards}
		}
		workCh <- work{err: io.EOF}
	}(ctx, metadata)

	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case work := <-workCh:
			err := work.err
			if errors.Is(err, io.EOF) {
				// EOF signals completion
				return nil
			} else if err != nil {
				return err
			}
			slab := metadata[i]
			encryptionKey := slab.SectorKey
			shards := work.shards
			nonce := make([]byte, 24)
			for i := range shards {
				nonce[0] = byte(i)
				c, _ := chacha20.NewUnauthenticatedCipher(encryptionKey[:], nonce[:])
				c.XORKeyStream(shards[i], shards[i]) // decrypt shard in place
			}
			enc.ReconstructData(shards)
			if err := stripedJoin(w, shards[:slab.MinSectors], int(slab.Length)); err != nil {
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

// WithDownloadHostTimeout sets the timeout for reading sectors
// from individual hosts. This avoids long hangs when a host is unresponsive
// or slow. The default is 4 seconds, worst case around 300Mbps.
func WithDownloadHostTimeout(timeout time.Duration) DownloadOption {
	return func(do *downloadOption) {
		do.hostTimeout = timeout
	}
}

// NewSDK creates a new indexd client with the given app key and base URL.
func NewSDK(baseURL string, appKey types.PrivateKey, dialer HostDialer) *SDK {
	return &SDK{
		appKey: appKey,
		dialer: dialer,
		c:      *api.NewClient(baseURL, ""),
	}
}
