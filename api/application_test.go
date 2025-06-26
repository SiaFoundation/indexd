package api_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/internal/testutils"
	"go.uber.org/zap"
)

func TestApplicationAPI(t *testing.T) {
	cs := testutils.NewConsensusNode(t, zap.NewNop())
	indexer := testutils.NewIndexer(t, cs, zap.NewNop())

	// prepare account
	sk := types.GeneratePrivateKey()
	if err := indexer.AccountsAdd(context.Background(), sk.PublicKey()); err != nil {
		t.Fatal(err)
	}

	// prepare request
	req, err := http.NewRequest("GET", indexer.ApplicationAPIAddress()+"/foo", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	// prepare request hash
	validUntil := time.Now().Add(time.Hour)
	h := types.NewHasher()
	h.E.Write([]byte(testutils.DefaultHostname))
	h.E.WriteUint64(uint64(validUntil.Unix()))
	requestHash := h.Sum()

	// prepare query parameters
	val := url.Values{}
	val.Set("SiaIdx-ValidUntil", fmt.Sprint(validUntil.Unix()))
	val.Set("SiaIdx-Credential", sk.PublicKey().String())
	val.Set("SiaIdx-Signature", sk.SignHash(requestHash).String())
	req.URL.RawQuery = val.Encode()

	// execute the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// assert we successfully authenticated
	if resp.StatusCode != http.StatusOK {
		t.Fatal("unexpected status code:", resp.StatusCode)
	}
}
