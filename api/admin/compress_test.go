package admin_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"testing"

	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap"
)

func TestCompressedResponses(t *testing.T) {
	c := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, c, zap.NewNop())

	// add enough keys for the listing to exceed the minimum size for compression
	const nKeys = 10
	for i := 0; i < nKeys; i++ {
		if _, err := indexer.Admin.AddAppConnectKey(t.Context(), accounts.AppConnectKeyRequest{
			Description: "a connect key used to test response compression",
			Quota:       "default",
		}); err != nil {
			t.Fatal("failed to add app connect key:", err)
		}
	}

	expected, err := indexer.Admin.AppConnectKeys(t.Context(), 0, nKeys)
	if err != nil {
		t.Fatal("failed to get app connect keys:", err)
	}

	// DisableCompression stops the transport from advertising gzip on its own
	// and transparently decompressing the response before the test sees it.
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}

	tests := []struct {
		name           string
		acceptEncoding string
		compressed     bool
	}{
		{"gzip", "gzip", true},
		{"no accept-encoding", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, indexer.AdminURL+"/apps/connect/keys?offset=0&limit=10", nil)
			if err != nil {
				t.Fatal("failed to create request:", err)
			}
			req.SetBasicAuth("", indexer.AdminPassword)
			if tt.acceptEncoding != "" {
				req.Header.Set("Accept-Encoding", tt.acceptEncoding)
			}

			resp, err := client.Do(req)
			if err != nil {
				t.Fatal("failed to send request:", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
			} else if vary := resp.Header.Get("Vary"); vary != "Accept-Encoding" {
				t.Fatalf("expected Vary %q, got %q", "Accept-Encoding", vary)
			}

			enc := resp.Header.Get("Content-Encoding")
			if tt.compressed != (enc == "gzip") {
				t.Fatalf("expected compressed=%t, got Content-Encoding %q", tt.compressed, enc)
			}

			sent, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal("failed to read response:", err)
			}

			body := sent
			if tt.compressed {
				zr, err := gzip.NewReader(bytes.NewReader(sent))
				if err != nil {
					t.Fatal("failed to create gzip reader:", err)
				}
				defer zr.Close()

				if body, err = io.ReadAll(zr); err != nil {
					t.Fatal("failed to decompress response:", err)
				} else if len(sent) >= len(body) {
					t.Fatalf("expected compressed response to be smaller, got %d >= %d", len(sent), len(body))
				}
			}

			var keys []accounts.ConnectKey
			if err := json.Unmarshal(body, &keys); err != nil {
				t.Fatal("failed to unmarshal app connect keys:", err)
			} else if !reflect.DeepEqual(keys, expected) {
				t.Fatal("response does not match the app connect keys")
			}
		})
	}
}
