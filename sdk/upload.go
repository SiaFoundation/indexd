package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/klauspost/reedsolomon"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/client/v2"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

const (
	// concurrentSlabUploads is the number of slabs that can be uploading at the
	// same time. If one slow host is holding up the upload of a slab, we read
	// the next slab and start uploading that set of shards.
	concurrentSlabUploads = 4
)

type (
	shard struct {
		host types.PublicKey
		root types.Hash256

		index int
		err   error
	}

	shardUpload struct {
		hosts  *client.Candidates
		shards chan shard

		encryptionKey [32]byte
		index         int
		sector        []byte
	}

	slabUpload struct {
		encryptionKey [32]byte
		length        uint32

		uploadsCh chan shard
		err       error
	}

	uploadOption struct {
		dataShards   uint8
		parityShards uint8
		hostTimeout  time.Duration
		maxInflight  int
	}
)

func (s *SDK) uploadSlabs(ctx context.Context, slabsCh chan slabUpload, r io.Reader, enc reedsolomon.Encoder, dataShards, parityShards, maxInflight int, hostTimeout time.Duration) {
	shardsCh := make(chan shardUpload)
	defer close(shardsCh)

	// run 'maxInflight' upload workers that pull shards from the queue
	go runUploadWorkers(ctx, s.hosts, s.appKey, shardsCh, maxInflight, hostTimeout)

	// convenience variables
	slabSize := dataShards * proto4.SectorSize
	totalShards := dataShards + parityShards

	sendErr := func(err error) {
		select {
		case <-ctx.Done():
		case slabsCh <- slabUpload{err: err}:
		}
	}

	// read slabs in a loop
	buffer := make([]byte, slabSize)
	for i := 0; ctx.Err() == nil; i++ {
		// prepare upload candidates, every shard upload holds a reference
		// to the upload candidates to ensure every shard is uploaded to a
		// unique host
		candidates, err := s.hosts.Candidates()
		if err != nil {
			sendErr(fmt.Errorf("failed to get upload candidates for slab %d: %w", i, err))
			return
		} else if candidates.Available() < totalShards {
			sendErr(fmt.Errorf("not enough hosts available to upload slab %d: %d < %d", i, candidates.Available(), totalShards))
			return
		}

		// read next slab
		n, err := readAtMost(r, buffer)
		if n == 0 && errors.Is(err, io.EOF) {
			sendErr(io.EOF) // signal upload is done
			return
		} else if err != nil && !errors.Is(err, io.EOF) {
			sendErr(fmt.Errorf("failed to read slab %d: %w", i, err))
			return
		}

		// prepare shards
		shards := make([][]byte, totalShards)
		for i := range shards {
			shards[i] = make([]byte, proto4.SectorSize)
		}
		stripedSplit(buffer, shards[:dataShards])

		// generate a random encryption key
		encryptionKey := frand.Entropy256()

		// since data shards do not change during encoding, we can launch
		// these ahead of time and not wait for encoding to finish first
		uploadsCh := make(chan shard, totalShards)
		for i, data := range shards[:dataShards] {
			sector := make([]byte, proto4.SectorSize)
			copy(sector, data)
			shardsCh <- shardUpload{
				hosts:  candidates,
				shards: uploadsCh,

				encryptionKey: encryptionKey,
				index:         i,
				sector:        sector,
			}
		}

		// encode the shards
		if err := enc.Encode(shards); err != nil {
			sendErr(fmt.Errorf("failed to encode slab %d shards: %w", i, err))
			return
		}

		// launch uploads for parity shards
		for i, data := range shards[dataShards:] {
			shardsCh <- shardUpload{
				hosts:  candidates,
				shards: uploadsCh,

				encryptionKey: encryptionKey,
				index:         dataShards + i,
				sector:        data,
			}
		}

		// send slab off for pinning
		select {
		case <-ctx.Done():
			return
		case slabsCh <- slabUpload{
			encryptionKey: encryptionKey,
			length:        uint32(n),
			uploadsCh:     uploadsCh,
		}:
		}
	}
}

func runUploadWorkers(ctx context.Context, client hostClient, accountKey types.PrivateKey, queue chan shardUpload, maxInflight int, hostTimeout time.Duration) {
	sema := make(chan struct{}, maxInflight)
	for job := range queue {
		sema <- struct{}{}
		go func() {
			defer func() { <-sema }()

			// encrypt the sector
			nonce := make([]byte, 24)
			nonce[0] = byte(job.index)
			c, _ := chacha20.NewUnauthenticatedCipher(job.encryptionKey[:], nonce)
			c.XORKeyStream(job.sector, job.sector)

			// try hosts until one works
			for host := range job.hosts.Iter() {
				if ctx.Err() != nil {
					return
				}

				root, err := uploadShard(ctx, client, accountKey, host, job.sector, hostTimeout)
				if err == nil {
					log.Println("uploaded shard", root, "to host", host)
					job.shards <- shard{
						index: job.index,
						host:  host,
						root:  root,
					}
					return
				}
			}
		}()
	}
}
