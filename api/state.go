package api

import "time"

type (
	// BuildState contains static information about the build.
	BuildState struct {
		Version   string    `json:"version"`
		Commit    string    `json:"commit"`
		OS        string    `json:"os"`
		BuildTime time.Time `json:"buildTime"`
	}

	// State is the response body for the [GET] /state endpoint.
	State struct {
		BuildState

		ScanHeight uint64    `json:"scanHeight"`
		StartTime  time.Time `json:"startTime"`
	}
)
