package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"go.sia.tech/jape"
)

type repackableSlabs struct {
	Slabs
	called   bool
	minWaste int
}

func (s *repackableSlabs) RepackableObjects(_ context.Context, _ proto.Account, _ time.Time, _, _, minWaste int) ([]slabs.RepackableObject, error) {
	s.called = true
	s.minWaste = minWaste
	return []slabs.RepackableObject{}, nil
}

func TestRepackableObjectsMinWaste(t *testing.T) {
	for _, tc := range []struct {
		value  string
		status int
		want   int
	}{
		{"", http.StatusBadRequest, 0},
		{"-1", http.StatusBadRequest, 0},
		{"101", http.StatusBadRequest, 0},
		{"abc", http.StatusBadRequest, 0},
		{"0.5", http.StatusBadRequest, 0},
		{"0", http.StatusOK, 0},
		{"25", http.StatusOK, 25},
		{"50", http.StatusOK, 50},
		{"100", http.StatusOK, 100},
	} {
		t.Run(tc.value, func(t *testing.T) {
			s := new(repackableSlabs)
			a := &app{slabs: s}
			h := jape.Mux(map[string]jape.Handler{"GET /repackable/objects": func(jc jape.Context) {
				a.handleGETRepackableObjects(jc, types.PublicKey{})
			}})
			req := httptest.NewRequest(http.MethodGet, "/repackable/objects?datashards=10&parityshards=20&minwaste="+tc.value, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("expected status %d, got %d: %s", tc.status, rec.Code, rec.Body.String())
			} else if s.called != (tc.status == http.StatusOK) || s.minWaste != tc.want {
				t.Fatalf("unexpected manager call: called=%v minWaste=%d", s.called, s.minWaste)
			}
		})
	}
}

func TestRepackableObjectsClient(t *testing.T) {
	s := new(repackableSlabs)
	a := &app{slabs: s}
	server := httptest.NewServer(jape.Mux(map[string]jape.Handler{
		"GET /repackable/objects": func(jc jape.Context) {
			a.handleGETRepackableObjects(jc, types.PublicKey{})
		},
	}))
	defer server.Close()
	if _, err := NewClient(server.URL).RepackableObjects(t.Context(), types.GeneratePrivateKey(), 10, 20, 25); err != nil {
		t.Fatal(err)
	} else if !s.called || s.minWaste != 25 {
		t.Fatalf("unexpected manager call: called=%v minWaste=%d", s.called, s.minWaste)
	}
}
