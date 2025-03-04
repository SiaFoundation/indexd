package hosts

import (
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.uber.org/zap"
)

const (
	// maxAddrsPerProtocol is the maximum number of announced addresses we will
	// track per host, per protocol
	maxAddrsPerProtocol = 2
)

type (
	// UpdateTx defines what the host manager needs to atomically process a
	// chain update in the database.
	UpdateTx interface {
		AddHostAnnouncement(hk types.PublicKey, ha chain.V2HostAnnouncement, ts time.Time) error
	}

	// HostManager manages the host announcements.
	HostManager struct {
		announcementMaxAge time.Duration

		log *zap.Logger
	}
)

// NewManager creates a new host manager.
func NewManager(opts ...Option) (*HostManager, error) {
	m := &HostManager{
		announcementMaxAge: time.Hour * 24 * 365,
		log:                zap.NewNop(),
	}
	for _, opt := range opts {
		opt(m)
	}

	// sanity check options
	if m.announcementMaxAge == 0 {
		return nil, fmt.Errorf("announcementMaxAge can not be zero")
	}

	return m, nil
}

// Close closes the manager.
func (m *HostManager) Close() error {
	return nil
}

// UpdateChainState updates the host announcements in the database.
func (m *HostManager) UpdateChainState(tx UpdateTx, applied []chain.ApplyUpdate) error {
	for _, update := range applied {
		// ignore announcements that are too old
		if time.Since(update.Block.Timestamp) > time.Duration(m.announcementMaxAge) {
			continue
		}

		has := make(map[types.PublicKey]chain.V2HostAnnouncement)
		chain.ForEachV2HostAnnouncement(update.Block, func(hk types.PublicKey, addrs []chain.NetAddress) {
			filtered := make(map[chain.Protocol][]chain.NetAddress)
			for _, addr := range addrs {
				if err := validateAddress(addr); err != nil {
					m.log.Debug("ignoring host announcement", zap.Stringer("hk", hk), zap.Error(err))
				} else if len(filtered[addr.Protocol]) < maxAddrsPerProtocol {
					filtered[addr.Protocol] = append(filtered[addr.Protocol], addr)
				}
			}
			for _, addrs := range filtered {
				has[hk] = append(has[hk], addrs...)
			}
		})

		for hk, ha := range has {
			if err := tx.AddHostAnnouncement(hk, ha, update.Block.Timestamp); err != nil {
				return fmt.Errorf("failed to add host announcement: %w", err)
			}
		}
	}
	return nil
}

func validateAddress(na chain.NetAddress) error {
	if !(na.Protocol == siamux.Protocol || na.Protocol == quic.Protocol) {
		return fmt.Errorf("unknown protocol %q", na.Protocol)
	}
	if na.Address == "" {
		return fmt.Errorf("empty address")
	}
	return nil
}
