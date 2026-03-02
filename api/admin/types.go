package admin

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/hosts"
)

type (
	// BuildState contains static information about the build.
	BuildState struct {
		Version   string    `json:"version"`
		Commit    string    `json:"commit"`
		OS        string    `json:"os"`
		BuildTime time.Time `json:"buildTime"`
	}

	// AggregatedHostStatsResponse is the response body for the [GET] /stats/hosts
	AggregatedHostStatsResponse struct {
		Active        uint64 `json:"active"`
		GoodForUpload uint64 `json:"goodForUpload"`
		TotalScans    int64  `json:"totalScans"`
		FailedScans   int64  `json:"failedScans"`
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
		Reasons  []string          `json:"reasons"`
	}

	// SectorsStatsResponse is the response body for the [GET] /stats/sectors
	SectorsStatsResponse struct {
		Slabs       int64 `json:"slabs"`
		Migrated    int64 `json:"migrated"`
		Pinned      int64 `json:"pinned"`
		Unpinnable  int64 `json:"unpinnable"`
		Unpinned    int64 `json:"unpinned"`
		Lost        int64 `json:"lost"`
		Checked     int64 `json:"checked"`
		CheckFailed int64 `json:"checkFailed"`
	}

	// AccountStatsResponse is the response body for the [GET] /stats/accounts.
	AccountStatsResponse struct {
		Registered uint64 `json:"registered"`
		Active     uint64 `json:"active"`
	}

	// AppStatsResponse is the response body for the [GET] /stats/apps.
	AppStatsResponse []AppStats

	// AppStats contains per-app statistics.
	AppStats struct {
		AppID      types.Hash256 `json:"appID"`
		Accounts   uint64        `json:"accounts"`
		Active     uint64        `json:"active"`
		PinnedData uint64        `json:"pinnedData"`
	}

	// HostStatsResponse is the response body for the [GET] /stats/hosts/detailed.
	HostStatsResponse []HostStats

	// HostStats wraps hosts.HostStats to provide a custom PrometheusMetric method.
	HostStats hosts.HostStats

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

	// RegisterAppKeyRequest is the request body for the [POST] /apps/register
	// endpoint.
	RegisterAppKeyRequest struct {
		ConnectKey string           `json:"connectKey"`
		AppKey     types.PublicKey  `json:"appKey"`
		Meta       accounts.AppMeta `json:"meta"`
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
