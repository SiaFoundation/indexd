package testutils

import (
	"context"
	"testing"

	"go.sia.tech/indexd/api"
	"go.uber.org/zap"
)

func TestIndexer(t *testing.T) {
	c := NewConsensusNode(t, zap.NewNop())
	indexer := c.NewIndexer(t, zap.NewNop())

	state, err := indexer.State(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if state.BuildState == (api.BuildState{}) {
		t.Fatal("expected build state")
	} else if state.StartTime.IsZero() {
		t.Fatal("expected start time")
	}
}
