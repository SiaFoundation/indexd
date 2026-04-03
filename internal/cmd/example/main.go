package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sdk"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	appID := types.Hash256(bytes.Repeat([]byte{0x01}, 32))
	builder := sdk.NewBuilder("https://app.sia.storage", sdk.AppMetadata{
		ID:          appID,
		Name:        "go example",
		Description: "an example app",
		ServiceURL:  "https://example.com",
	})

	responseURL, err := builder.RequestConnection(ctx)
	if err != nil {
		return fmt.Errorf("request connection: %w", err)
	}

	fmt.Printf("Please approve connection %s\n", responseURL)

	approved, err := builder.WaitForApproval(ctx)
	if err != nil {
		return fmt.Errorf("wait for approval: %w", err)
	} else if !approved {
		return fmt.Errorf("connection request denied")
	}

	fmt.Println("Enter mnemonic (or leave empty to generate new):")
	mnemonic, err := readLine()
	if err != nil {
		return fmt.Errorf("read mnemonic: %w", err)
	} else if mnemonic == "" {
		mnemonic = sdk.NewSeedPhrase()
		fmt.Println("mnemonic:", mnemonic)
	}

	client, err := builder.Register(ctx, mnemonic)
	if err != nil {
		return fmt.Errorf("register app: %w", err)
	}
	defer client.Close()

	appKey := client.AppKey()
	fmt.Println("App registered:", base64.StdEncoding.EncodeToString(appKey))
	fmt.Println("Connected to indexer")

	obj := sdk.NewEmptyObject()
	start := time.Now()
	if err := client.Upload(ctx, &obj, bytes.NewReader([]byte("hello from upload()!"))); err != nil {
		return fmt.Errorf("upload object: %w", err)
	} else if err := client.PinObject(ctx, obj); err != nil {
		return fmt.Errorf("pin object: %w", err)
	}
	fmt.Printf("Uploaded and pinned %d bytes with upload() in %s\n", obj.Size(), time.Since(start))

	var downloaded bytes.Buffer
	start = time.Now()
	if err := client.Download(ctx, &downloaded, obj); err != nil {
		return fmt.Errorf("download object: %w", err)
	}
	fmt.Printf("Downloaded with download(): %q in %s\n", downloaded.String(), time.Since(start))

	fmt.Println("\nUpload Packing Example...")

	start = time.Now()
	upload, err := client.UploadPacked()
	if err != nil {
		return fmt.Errorf("create packed upload: %w", err)
	}
	defer upload.Close()

	for i := range 10 {
		data := fmt.Sprintf("hello, world %d!", i)
		size, err := upload.Add(ctx, bytes.NewReader([]byte(data)))
		if err != nil {
			return fmt.Errorf("add packed object %d: %w", i, err)
		}
		fmt.Printf("upload %d added %d bytes (%d remaining)\n", i, size, upload.Remaining())
	}

	objects, err := upload.Finalize(ctx)
	if err != nil {
		return fmt.Errorf("finalize packed upload: %w", err)
	}
	fmt.Printf("Upload finished %d objects in %s\n", len(objects), time.Since(start))

	for _, obj := range objects {
		if err := client.PinObject(ctx, obj); err != nil {
			return fmt.Errorf("pin packed object %s: %w", obj.ID(), err)
		}
		fmt.Printf("Pinned object %s\n", obj.ID())
	}

	last := objects[len(objects)-1]
	var packedDownload bytes.Buffer
	start = time.Now()
	fmt.Printf("Downloading object %s %d bytes\n", last.ID(), last.Size())
	if err := client.Download(ctx, &packedDownload, last); err != nil {
		return fmt.Errorf("download packed object: %w", err)
	}
	fmt.Printf("Downloaded object %s with %d bytes in %s\n", last.ID(), packedDownload.Len(), time.Since(start))

	return nil
}

func readLine() (string, error) {
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && line == "" {
		return "", err
	}
	return strings.TrimSpace(line), nil
}
