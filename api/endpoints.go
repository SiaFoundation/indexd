package api

import (
	"runtime"
	"time"

	"go.sia.tech/indexd/build"
	"go.sia.tech/jape"
)

var startTime = time.Now()

func (a *api) handleGETState(jc jape.Context) {
	jc.Encode(State{
		StartTime: startTime,
		BuildState: BuildState{
			Version:   build.Version(),
			Commit:    build.Commit(),
			OS:        runtime.GOOS,
			BuildTime: build.Time(),
		},
	})
}
