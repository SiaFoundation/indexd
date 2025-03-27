package contracts

import (
	"context"
	"fmt"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const sectorsPerGiB = uint64(1<<30) / proto.SectorSize

// minAllowance is a sane minimum for the allowance we put into a contract to
// make sure forming the contract is worthwhile and we don't spend more on fees
// than on actual usage.
var minAllowance = types.Siacoins(10)

type formContractSigner struct {
	renterKey types.PrivateKey
	w         rhp.Wallet
}

// NewFormContractSigner implements the rhp.FormContractSigner interface by
// wrapping a wallet.
func NewFormContractSigner(w rhp.Wallet, renterKey types.PrivateKey) rhp.FormContractSigner {
	return &formContractSigner{
		renterKey: renterKey,
		w:         w,
	}
}

func (s *formContractSigner) FundV2Transaction(txn *types.V2Transaction, amount types.Currency) (types.ChainIndex, []int, error) {
	return s.w.FundV2Transaction(txn, amount, true)
}

func (s *formContractSigner) ReleaseInputs(txns []types.V2Transaction) {
	s.w.ReleaseInputs(nil, txns)
}

func (s *formContractSigner) SignHash(h types.Hash256) types.Signature {
	return s.renterKey.SignHash(h)
}

func (s *formContractSigner) SignV2Inputs(txn *types.V2Transaction, toSign []int) {
	s.w.SignV2Inputs(txn, toSign)
}

type contractor struct {
	cm     *chain.Manager
	signer *formContractSigner
}

// NewContractor creates a production Contractor that forms, refreshes and renews contracts by
// dialing up hosts using the SiaMux protocol and fetching fresh settings right
// before the RPC call.
func NewContractor(cm *chain.Manager, w rhp.Wallet, renterKey types.PrivateKey) Contractor {
	return &contractor{
		cm: cm,
		signer: &formContractSigner{
			renterKey: renterKey,
			w:         w,
		},
	}
}

func (cf *contractor) FormContract(ctx context.Context, hk types.PublicKey, addr string, settings proto.HostSettings, params proto.RPCFormContractParams) (rhp.RPCFormContractResult, error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	t, err := siamux.Dial(dialCtx, addr, hk)
	if err != nil {
		return rhp.RPCFormContractResult{}, fmt.Errorf("failed to dial host: %w", err)
	}
	defer t.Close()

	res, err := rhp.RPCFormContract(ctx, t, cf.cm, cf.signer, cf.cm.TipState(), settings.Prices, hk, settings.WalletAddress, params)
	if err != nil {
		return rhp.RPCFormContractResult{}, fmt.Errorf("failed to form contract: %w", err)
	}

	return res, nil
}

// performContractFormation makes sure that we have at least 'wanted' good
// contracts with good hosts in unique CIDRs.
func (cm *ContractManager) performContractFormation(ctx context.Context, period uint64, wanted uint64, log *zap.Logger) error {
	formationLog := log.Named("formation")
	activeContracts, err := cm.store.Contracts(ctx, WithRevisable(true))
	if err != nil {
		return fmt.Errorf("failed to fetch active contracts: %w", err)
	}

	// helpers for CIDR check
	usedCidrs := make(map[string]types.PublicKey)
	addHost := func(host hosts.Host) {
		for _, cidr := range host.Networks {
			usedCidrs[cidr.IP.String()] = host.PublicKey
		}
		wanted--
	}
	hasCidrConflict := func(host hosts.Host) (types.PublicKey, bool) {
		for _, cidr := range host.Networks {
			if hk, known := usedCidrs[cidr.IP.String()]; known {
				return hk, true
			}
		}
		return types.PublicKey{}, false
	}

	// helper to check if a host is good to form a contract with
	isGood := func(host hosts.Host, log *zap.Logger) bool {
		hostLog := log.With(zap.Stringer("hostKey", host.PublicKey))
		if good := host.Usability.Usable(); !good {
			// host should be good
			hostLog.Debug("host is not usable due to bad usability")
			return false
		} else if usedBy, used := hasCidrConflict(host); used {
			// host should be on a unique cidr
			hostLog.Debug("host is not usable cidr is already in use", zap.Stringer("usedBy", usedBy))
			return false
		} else if host.Settings.RemainingStorage < minRemainingStorage {
			// host should at least have 10GB of storage left
			hostLog.Debug("host is not usable since host has less than 10GiB of storage left", zap.Uint64("remainingStorage", host.Settings.RemainingStorage))
			return false
		}
		return true
	}

	// determine how many contracts we need to form
	for _, contract := range activeContracts {
		contractLog := formationLog.Named(contract.ID.String()).With(zap.Stringer("hostKey", contract.HostKey))

		// host checks
		host, err := cm.store.Host(ctx, contract.HostKey)
		if err != nil {
			contractLog.Error("failed to fetch host for contract", zap.Error(err))
			continue
		} else if !isGood(host, contractLog) {
			continue
		}

		// contract is good if we can upload to it
		if !contract.GoodForUpload(host.Settings.MaxCollateral) {
			log.Debug("skipping contract since it's not good for uploading",
				zap.Bool("good", contract.Good),
				zap.Bool("maxSizeReached", contract.Size >= maxContractSize),
				zap.Bool("maxCollateralReached", contract.UsedCollateral.Cmp(host.Settings.MaxCollateral) > 0),
			)
			continue
		}

		// contract is good
		addHost(host)
	}

	// fetch all hosts
	var hosts []hosts.Host
	const batchSize = 50
	for offset := 0; ; offset += batchSize {
		batch, err := cm.store.Hosts(ctx, offset, batchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch hosts to form contracts with: %w", err)
		}
		hosts = append(hosts, batch...)
		if len(batch) < batchSize {
			break
		}
	}

	// randomize their order to avoid preferring any host
	cm.shuffle(len(hosts), func(i, j int) { hosts[i], hosts[j] = hosts[j], hosts[i] })

	for i := range hosts {
		hostLog := formationLog.With(zap.Stringer("hostKey", hosts[i].PublicKey))

		// filter out bad hosts
		if !isGood(hosts[i], hostLog) {
			continue
		}

		// scan host for valid price settings
		scanCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		host, err := cm.scanner.ScanHost(scanCtx, hosts[i].PublicKey)
		cancel()
		if err != nil {
			hostLog.Error("failed to scan host", zap.Error(err))
			continue
		}

		// make sure the host is still good
		if !isGood(host, hostLog) {
			continue // ignore bad host
		}
		hostAddr := host.SiamuxAddr()

		allowance, collateral := initialContractFunding(host.Settings.Prices, period)
		formationCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		res, err := cm.contractor.FormContract(formationCtx, host.PublicKey, hostAddr, host.Settings, proto.RPCFormContractParams{
			RenterPublicKey: cm.renterKey,
			RenterAddress:   cm.w.Address(),
			Allowance:       allowance,
			Collateral:      collateral,
			ProofHeight:     cm.cm.TipState().Index.Height + period,
		})
		cancel()
		if err != nil {
			hostLog.Error("failed to form contract", zap.Error(err))
			continue
		}
		contract := res.Contract
		minerFee := res.FormationSet.Transactions[len(res.FormationSet.Transactions)-1].MinerFee

		err = cm.store.AddFormedContract(ctx, contract.ID, host.PublicKey, contract.Revision.ProofHeight, contract.Revision.ExpirationHeight, host.Settings.Prices.ContractPrice, allowance, minerFee, contract.Revision.TotalCollateral)
		if err != nil {
			formationLog.Error("failed to add formed contract", zap.Error(err))
			continue
		}

		// contract formed successfully
		addHost(host)
	}

	return nil
}

func initialContractFunding(prices proto.HostPrices, period uint64) (allowance, collateral types.Currency) {
	// each 10GB of upload + download + storage
	basePrice := prices.ContractPrice
	writeUsage := prices.RPCWriteSectorCost(proto.SectorSize).Mul(10 * sectorsPerGiB)
	readUsage := prices.RPCReadSectorCost(proto.SectorSize).Mul(10 * sectorsPerGiB)
	storageUsage := prices.RPCAppendSectorsCost(10*sectorsPerGiB, period)
	total := writeUsage.Add(readUsage).Add(storageUsage)
	allowance, collateral = total.RenterCost().Add(basePrice), total.HostRiskedCollateral()

	// don't go below a sane minimum to make sure we can fill an account without
	// immediately draining the contract and requiring a refresh.
	if allowance.Cmp(minAllowance) < 0 {
		allowance = minAllowance
	}
	return allowance, collateral
}
