package admin

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

type (
	// BuildState contains static information about the build.
	BuildState struct {
		Version   string    `json:"version"`
		Commit    string    `json:"commit"`
		OS        string    `json:"os"`
		BuildTime time.Time `json:"buildTime"`
	}

	// ContractsStatsResponse is the response body for the [GET] /stats/contracts
	ContractsStatsResponse struct {
		Contracts    uint64 `json:"contracts"`
		BadContracts uint64 `json:"badContracts"`
		Renewing     uint64 `json:"renewing"`

		TotalCapacity uint64 `json:"totalCapacity"`
		TotalSize     uint64 `json:"totalSize"`
	}

	// ExplorerState contains static information about explorer data sources.
	ExplorerState struct {
		Enabled bool   `json:"enabled"`
		URL     string `json:"url"`
	}

	// HostsBlocklistRequest is the request body for the [POST] /hosts/blocklist.
	HostsBlocklistRequest struct {
		HostKeys []types.PublicKey `json:"hostKeys"`
		Reason   string            `json:"reason"`
	}

	// SectorsStatsResponse is the response body for the [GET] /stats/sectors
	SectorsStatsResponse struct {
		Slabs      int64 `json:"slabs"`
		Migrated   int64 `json:"migrated"`
		Pinned     int64 `json:"pinned"`
		Unpinnable int64 `json:"unpinnable"`
		Unpinned   int64 `json:"unpinned"`
	}

	// AccountStatsResponse is the response body for the [GET] /stats/accounts.
	AccountStatsResponse struct {
		Registered uint64 `json:"registered"`
		Active     uint64 `json:"active"`
	}

	// State is the response body for the [GET] /state endpoint.
	State struct {
		BuildState

		Explorer   ExplorerState `json:"explorer"`
		Network    string        `json:"network"`
		ScanHeight uint64        `json:"scanHeight"`
		StartTime  time.Time     `json:"startTime"`
		SyncHeight uint64        `json:"syncHeight"`
		Synced     bool          `json:"synced"`
	}

	// WalletResponse is the response body for the [GET] /wallet endpoint.
	WalletResponse struct {
		wallet.Balance

		Address types.Address `json:"address"`
	}

	// WalletSendSiacoinsRequest is the request body for the [POST] /wallet/send
	// endpoint.
	WalletSendSiacoinsRequest struct {
		Address          types.Address  `json:"address"`
		Amount           types.Currency `json:"amount"`
		SubtractMinerFee bool           `json:"subtractMinerFee"`
		UseUnconfirmed   bool           `json:"useUnconfirmed"`
	}
)
