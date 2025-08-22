package main

import (
	"context"
	"crypto/pbkdf2"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/api/app"
	"go.sia.tech/indexd/sdk"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"lukechampine.com/frand"
)

const (
	dataShards        = 2
	parityShards      = 4
	slabSize          = dataShards * proto.SectorSize
	redundantSlabSize = (dataShards + parityShards) * proto.SectorSize
	redundancy        = (dataShards + parityShards) / dataShards
)

var (
	appSecret  string
	indexerURL string

	logLevel zap.AtomicLevel
	logPath  string

	threads int

	elapsedMu sync.Mutex
	elapsed   []time.Duration
)

func init() {
	flag.StringVar(&indexerURL, "indexer.url", "http://localhost:9982", "the URL of the indexer API")
	flag.StringVar(&appSecret, "app.secret", "", "a secret used to derive the application key")

	flag.TextVar(&logLevel, "log.level", zap.NewAtomicLevelAt(zap.InfoLevel), "the log level to use")
	flag.StringVar(&logPath, "log.path", "", "the path to write the log to")

	flag.IntVar(&threads, "threads", 1, "the number of upload threads")

	flag.Parse()
}

func main() {
	log := newLogger()

	sk, err := loadPrivateKey()
	if err != nil {
		log.Fatal("failed to load private key", zap.Error(err))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	resp, connected, err := sdk.Connect(ctx, indexerURL, sk, app.RegisterAppRequest{
		Name:        "junkd Uploader",
		Description: "A tool to upload junk data to the indexer",
		LogoURL:     "https://example.com/logo.png",
		ServiceURL:  "https://example.com/service",
	})
	if err != nil {
		log.Fatal("failed to connect app", zap.Error(err))
	} else if !connected {
		log.Info("please approve app connection", zap.String("url", resp.ResponseURL))
		if connected, err := resp.WaitForApproval(ctx); err != nil {
			log.Fatal("failed to wait for app approval", zap.Error(err))
		} else if !connected {
			log.Fatal("user denied app connection")
		}
	}
	log.Info("junkd connected")

	sdkClient, err := sdk.NewSDK(indexerURL, sk, sdk.WithLogger(log.Named("sdk")))
	if err != nil {
		log.Fatal("failed to create SDK client", zap.Error(err))
	}

	var wg sync.WaitGroup
	for n := 1; n <= threads; n++ {
		wg.Add(1)
		go func(log *zap.Logger) {
			defer wg.Done()
			log.Debug("starting upload thread")

		loop:
			for {
				// upload slab
				start := time.Now()
				obj, err := sdkClient.Upload(ctx, io.LimitReader(frand.Reader, slabSize), sdk.WithRedundancy(dataShards, parityShards))
				if err != nil {
					log.Error("failed to upload slab, timing out for 5 minutes", zap.Error(err), zap.Duration("duration", time.Since(start)))
					if ok := <-waitFor(ctx, 5*time.Minute); ok {
						continue loop
					}
					break loop
				} else if len(obj.Slabs) != 1 {
					log.Error(fmt.Sprintf("expected 1 slab, got %d", len(obj.Slabs)))
					break loop
				}

				elapsedMu.Lock()
				elapsed = append(elapsed, time.Since(start))
				elapsedMu.Unlock()

				log.Info("upload completed", zap.Stringer("SlabID", obj.Slabs[0].ID), zap.Duration("duration", time.Since(start)), zap.String("speed", formatBpsString(redundantSlabSize, time.Since(start))))
			}
		}(log.Named(fmt.Sprintf("upload-thread-%d", n)))
	}
	go printUploadSpeeds(ctx, log)
	wg.Wait()

	log.Info("all upload threads finished, exiting")
}

func waitFor(ctx context.Context, d time.Duration) <-chan bool {
	c := make(chan bool, 1)
	go func() {
		select {
		case <-ctx.Done():
			c <- false
		case <-time.After(d):
			c <- true
		}
	}()
	return c
}

func loadPrivateKey() (types.PrivateKey, error) {
	if appSecret == "" {
		return types.PrivateKey{}, fmt.Errorf("app secret is required")
	}

	derived, err := pbkdf2.Key(sha256.New, appSecret, []byte("junkd-pk-salt"), 4096, 32)
	if err != nil {
		return types.PrivateKey{}, fmt.Errorf("failed to derive key: %w", err)
	}

	var seed [32]byte
	copy(seed[:], derived)
	return wallet.KeyFromSeed(&seed, 0), nil
}

func newLogger() *zap.Logger {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	cfg.EncodeDuration = zapcore.MillisDurationEncoder
	return zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(cfg), zapcore.Lock(os.Stdout), logLevel))
}

func formatBpsString(b int64, t time.Duration) string {
	const units = "KMGTPE"
	const factor = 1000

	time := t.Truncate(time.Second).Seconds()
	if time <= 0 {
		return "0.00 bps"
	}

	// calculate bps
	speed := float64(b*8) / time

	// short-circuit for < 1000 bits/s
	if speed < factor {
		return fmt.Sprintf("%.2f bps", speed)
	}

	var i = -1
	for ; speed >= factor; i++ {
		speed /= factor
	}
	return fmt.Sprintf("%.2f %cbps", speed, units[i])
}

func printUploadSpeeds(ctx context.Context, log *zap.Logger) {
	t := time.NewTicker(2 * time.Minute)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			elapsedMu.Lock()
			if len(elapsed) > 1000 {
				elapsed = elapsed[len(elapsed)-1000:]
			}
			times := elapsed
			elapsedMu.Unlock()

			var avg time.Duration
			if len(times) == 0 {
				avg = time.Second
			} else {
				for _, t := range times {
					avg += t
				}
				avg /= time.Duration(len(times))
			}
			log.Info("average upload time", zap.String("averageSpeed", formatBpsString(int64(redundantSlabSize), avg)))
		}
	}
}
