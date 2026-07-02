package admin

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.sia.tech/indexd/slabs"
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
	AggregatedHostStatsResponse hosts.AggregatedHostStats

	// ContractsStatsResponse is the response body for the [GET] /stats/contracts
	ContractsStatsResponse contracts.Stats

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
	SectorsStatsResponse slabs.SectorsStats

	// AccountStatsResponse is the response body for the [GET] /stats/accounts.
	AccountStatsResponse accounts.AccountStats

	// ConnectKeyStatsResponse is the response body for the [GET] /stats/connectkeys.
	ConnectKeyStatsResponse accounts.ConnectKeyStats

	// AppStatsResponse is the response body for the [GET] /stats/apps.
	AppStatsResponse []accounts.AppStats

	// HostStatsResponse is the response body for the [GET] /stats/hosts/detailed.
	HostStatsResponse []hosts.HostStats

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
