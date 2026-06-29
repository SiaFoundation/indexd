package slabs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/reedsolomon"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/mux/v3"
	"go.uber.org/zap"
	"golang.org/x/crypto/chacha20"
)

var errNotEnoughShards = errors.New("not enough shards")

// defaultRecoveryChunkSize is the default size of the segment-aligned byte
// range requested from each host during recovery. It must be a multiple of
// proto.LeafSize.
const defaultRecoveryChunkSize = 1 << 20 // 1 MiB

const (
	// raceFactor scales the chunk read estimate to decide when to start a
	// parallel download against a slow host. It matches the Rust SDK.
	raceFactor = 1.5

	// minRaceInterval floors the adaptive race interval so a fast network
	// (where the estimate dips below typical RPC latency) doesn't dogpile
	// hosts with redundant reads.
	minRaceInterval = 200 * time.Millisecond
)

type slabDownload struct {
	root  types.Hash256
	index int
}

// slabRecovery coordinates the chunked recovery of a single slab. Rather than
// downloading whole sectors from MinShards hosts, the sector is split into
// segment-aligned byte-range chunks that are recovered concurrently and spread
// across all available hosts. Allowing a slab download that requires 10 shards
// out of 30 to actually leverage 30 hosts.
type slabRecovery struct {
	m        *SlabManager
	slab     Slab
	required []bool
	rs       reedsolomon.Encoder
	log      *zap.Logger

	// out holds the decrypted, reconstructed plaintext for every required
	// shard index; all other indices are nil. Each chunk writes into the
	// [off, off+len) window of every required shard.
	out [][]byte

	// excluded tracks hosts that reported a lost sector mid-recovery so that
	// later chunks skip them.
	mu       sync.Mutex
	excluded map[types.PublicKey]struct{}
}

// recoverShards downloads enough segment-aligned chunks of the slab's sectors,
// spread across all available hosts, to reconstruct the shards marked in
// required. It returns a slice of length len(slab.Sectors) where each required
// index holds the decrypted, reconstructed plaintext shard and all other
// indices are nil.
func (m *SlabManager) recoverShards(ctx context.Context, slab Slab, required []bool, log *zap.Logger) ([][]byte, error) {
	if len(required) != len(slab.Sectors) {
		panic(fmt.Sprintf("slab %s has %d sectors but %d required flags", slab.ID, len(slab.Sectors), len(required))) // developer error
	}

	rs, err := reedsolomon.New(int(slab.MinShards), len(slab.Sectors)-int(slab.MinShards))
	if err != nil {
		// New only errors on invalid parameters, which originate from the
		// database and should always be valid.
		log.Panic("failed to create reedsolomon encoder", zap.Error(err))
	}

	out := make([][]byte, len(slab.Sectors))
	for i, req := range required {
		if req {
			out[i] = make([]byte, proto.SectorSize)
		}
	}

	r := &slabRecovery{
		m:        m,
		slab:     slab,
		required: required,
		rs:       rs,
		log:      log,
		out:      out,
		excluded: make(map[types.PublicKey]struct{}),
	}

	// determine the segment-aligned chunk size
	chunkSize := m.recoveryChunkSize
	if chunkSize <= 0 || chunkSize > proto.SectorSize {
		chunkSize = proto.SectorSize
	}
	chunkSize -= chunkSize % proto.LeafSize
	if chunkSize == 0 {
		chunkSize = proto.LeafSize
	}

	numChunks := (proto.SectorSize + chunkSize - 1) / chunkSize

	// run multiple chunks concurrently so we engage more than MinShards hosts
	// at once. spread is the number of disjoint MinShards host groups that fit
	// across the available shards; with one chunk per group we light up close
	// to every host without heavily oversubscribing any single one.
	spread := max(len(slab.Sectors)/int(slab.MinShards), 1)
	concurrency := min(spread, numChunks)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sema := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	fail := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

chunkLoop:
	for off := 0; off < proto.SectorSize; off += chunkSize {
		length := chunkSize
		if off+length > proto.SectorSize {
			length = proto.SectorSize - off
		}

		select {
		case <-ctx.Done():
			break chunkLoop
		case sema <- struct{}{}:
		}

		off, length := off, length
		wg.Go(func() {
			defer func() { <-sema }()
			if err := r.recoverChunk(ctx, uint64(off), uint64(length)); err != nil {
				fail(err)
			}
		})
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	} else if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return out, nil
}

// raceInterval returns how long recoverChunk waits without progress before
// hedging a chunk read against an additional host. It is derived from the
// network-wide read-throughput estimate (scaled by raceFactor), floored by
// minRaceInterval and capped by the hard per-RPC shardTimeout.
func (m *SlabManager) raceInterval(length uint64) time.Duration {
	d := time.Duration(float64(m.hosts.ReadEstimate(length)) * raceFactor)
	if d < minRaceInterval {
		d = minRaceInterval
	}
	if d > m.shardTimeout {
		d = m.shardTimeout
	}
	return d
}

// recoverChunk downloads the [offset, offset+length) byte range of MinShards of
// the slab's sectors, spread across the available hosts, then decrypts and
// reconstructs that range for every required shard.
func (r *slabRecovery) recoverChunk(ctx context.Context, offset, length uint64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	m := r.m
	cols := make([][]byte, len(r.slab.Sectors))
	var downloaded atomic.Uint32

	// build the candidate set: sectors that still have a host, deduplicated
	// and minus any host excluded by a concurrent chunk.
	slabHosts := make(map[types.PublicKey]slabDownload)
	candidates := make([]types.PublicKey, 0, len(r.slab.Sectors))
	r.mu.Lock()
	for i, sector := range r.slab.Sectors {
		if sector.HostKey == nil {
			continue
		} else if _, excluded := r.excluded[*sector.HostKey]; excluded {
			continue
		} else if _, exists := slabHosts[*sector.HostKey]; exists {
			continue // prevent duplicates
		}
		candidates = append(candidates, *sector.HostKey)
		slabHosts[*sector.HostKey] = slabDownload{
			root:  sector.Root,
			index: i,
		}
	}
	r.mu.Unlock()

	// helper to download a chunk of a shard from a host
	sema := make(chan struct{}, r.slab.MinShards)
	downloadShard := func(ctx context.Context, hostKey types.PublicKey, sector slabDownload, log *zap.Logger) error {
		defer func() {
			<-sema
		}()
		if ctx.Err() != nil {
			return ctx.Err()
		}

		prices, err := m.hosts.Prices(ctx, hostKey)
		if err != nil {
			return fmt.Errorf("failed to fetch host prices: %w", err)
		}

		// debit the service account for the read since the host may charge for it
		// even if it is cancelled quickly. This is best effort, it's fine to
		// log the error and continue on failure.
		cost := prices.RPCReadSectorCost(length).RenterCost()
		if err = m.am.DebitServiceAccount(hostKey, m.migrationAccount, cost); err != nil {
			log.Warn("failed to debit service account for read sector", zap.Error(err))
		}

		start := time.Now()
		buf := bytes.NewBuffer(make([]byte, 0, length))
		if _, err := m.hosts.ReadSector(ctx, m.migrationAccountKey, hostKey, sector.root, buf, offset, length); err != nil {
			if isErrLostSector(err) {
				log.Debug("host reports sector lost", zap.Duration("elapsed", time.Since(start)))
				// exclude the host from subsequent chunks and mark the sector lost
				r.exclude(hostKey)
				if err := m.store.MarkSectorsLost(hostKey, []types.Hash256{sector.root}); err != nil {
					log.Error("failed to mark sector as lost", zap.Error(err))
				}
			} else if !errors.Is(err, mux.ErrClosedStream) && !errors.Is(err, ctx.Err()) {
				log.Debug("failed to download shard", zap.Duration("elapsed", time.Since(start)), zap.Error(err))
			}
			return err
		}

		cols[sector.index] = buf.Bytes()
		if n := downloaded.Add(1); n >= uint32(r.slab.MinShards) {
			cancel()
		}
		return nil
	}

	var wg sync.WaitGroup
	failedCh := make(chan struct{}, r.slab.MinShards)
	spawnDownload := func(hostKey types.PublicKey, sector slabDownload, release func(), initial bool) {
		log := r.log.With(zap.Stringer("hostKey", hostKey), zap.Stringer("sectorRoot", sector.root), zap.Uint64("offset", offset))
		wg.Go(func() {
			defer release()
			timeoutCtx, timeoutCancel := context.WithTimeout(ctx, m.shardTimeout)
			defer timeoutCancel()
			if err := downloadShard(timeoutCtx, hostKey, sector, log); err != nil {
				log.Debug("shard download failed", zap.Error(err))
				// non-blocking send to indicate a failure
				select {
				case failedCh <- struct{}{}:
				default:
				}
				// a host gets demoted if either
				// 1. it hit the shard timeout
				// 2. it was part of the initial batch of hosts and was interrupted
				if (timeoutCtx.Err() != nil && ctx.Err() == nil) || (initial && ctx.Err() != nil) {
					log.Debug("demoting host for failed download", zap.Error(err))
					m.hosts.AddFailedRPC(hostKey)
				}
			}
		})
	}

	initialHosts, releases, remaining := m.hosts.PickReads(candidates, int(r.slab.MinShards))
	if len(initialHosts) == 0 {
		return fmt.Errorf("only %d available sectors, minimum required: %d: %w", len(remaining), r.slab.MinShards, errNotEnoughShards)
	}

initialLoop:
	for i, hostKey := range initialHosts {
		select {
		case <-ctx.Done():
			for _, r := range releases[i:] {
				r()
			}
			break initialLoop
		case sema <- struct{}{}:
		}
		spawnDownload(hostKey, slabHosts[hostKey], releases[i], true)
	}

	// hedge against slow shards on an adaptive interval sized to the expected
	// time to read this chunk, decoupled from the hard per-RPC shardTimeout.
	raceInterval := m.raceInterval(length)
	timer := time.NewTimer(raceInterval)
	defer timer.Stop()
raceLoop:
	for downloaded.Load() < uint32(r.slab.MinShards) && len(remaining) > 0 {
		select {
		case <-ctx.Done():
			break raceLoop
		case <-failedCh:
			// a download has failed - hedge immediately
		case <-timer.C:
			// no progress within the race interval - hedge against slow shards
			r.log.Debug("racing slow shards", zap.Uint32("downloaded", downloaded.Load()), zap.Uint32("required", uint32(r.slab.MinShards)), zap.Duration("raceInterval", raceInterval))
		}

		// reset the race interval before attempting the next hedge
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(raceInterval)

		select {
		case sema <- struct{}{}:
			// re-pick the best remaining candidate atomically so we see
			// fresh inflight state from other concurrent downloads.
			picked, pickReleases, rem := m.hosts.PickReads(remaining, 1)
			if len(picked) == 0 {
				break raceLoop
			}
			remaining = rem
			hostKey, release := picked[0], pickReleases[0]

			spawnDownload(hostKey, slabHosts[hostKey], release, false)
		case <-ctx.Done():
			break raceLoop
		}
	}

	wg.Wait()

	if downloaded.Load() < uint32(r.slab.MinShards) {
		return fmt.Errorf("downloaded %d sectors, minimum required: %d: %w", downloaded.Load(), r.slab.MinShards, errNotEnoughShards)
	}

	return r.reconstructChunk(offset, length, cols)
}

// reconstructChunk decrypts the downloaded columns in place, reconstructs the
// required shards for the [offset, offset+length) range and writes them into
// the recovery's output buffers.
func (r *slabRecovery) reconstructChunk(offset, length uint64, cols [][]byte) error {
	// the chacha20 block counter is the leaf offset into the sector; offset is
	// segment (leaf) aligned so this is exact.
	counter := uint32(offset / proto.LeafSize)
	nonce := make([]byte, 24)
	for i, col := range cols {
		if col == nil {
			continue
		}
		nonce[0] = byte(i)
		c, _ := chacha20.NewUnauthenticatedCipher(r.slab.EncryptionKey[:], nonce)
		c.SetCounter(counter)
		c.XORKeyStream(col, col)
	}

	if err := r.rs.ReconstructSome(cols, r.required); err != nil {
		return fmt.Errorf("failed to reconstruct chunk at offset %d: %w", offset, err)
	}

	for i, req := range r.required {
		if !req {
			continue
		}
		copy(r.out[i][offset:offset+length], cols[i])
	}
	return nil
}

// exclude marks a host so that subsequent chunks skip it.
func (r *slabRecovery) exclude(hostKey types.PublicKey) {
	r.mu.Lock()
	r.excluded[hostKey] = struct{}{}
	r.mu.Unlock()
}

func isErrLostSector(err error) bool {
	return err != nil && strings.Contains(err.Error(), proto.ErrSectorNotFound.Error())
}

func isErrNotEnoughFunds(err error) bool {
	return err != nil && strings.Contains(err.Error(), proto.ErrNotEnoughFunds.Error())
}
