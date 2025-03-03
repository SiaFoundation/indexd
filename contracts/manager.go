package contracts

import (
	"time"

	"go.uber.org/zap"
)

type (
	ContractManagerOpt func(*ContractManager)

	// ContractManager manages the host announcements.
	ContractManager struct {
		log *zap.Logger

		contractRejectBuffer           time.Duration
		expiredContractBroadcastBuffer uint64
		expiredContractPruneBuffer     uint64
	}
)

func WithLogger(l *zap.Logger) ContractManagerOpt {
	return func(cm *ContractManager) {
		cm.log = l
	}
}

func NewManager(opts ...ContractManagerOpt) (*ContractManager, error) {
	cm := &ContractManager{
		log: zap.NewNop(),

		contractRejectBuffer:           6 * time.Hour, // 6 hours after formation
		expiredContractBroadcastBuffer: 144,           // 144 block after expiration
		expiredContractPruneBuffer:     144,           // 144 blocks after broadcast
	}
	for _, opt := range opts {
		opt(cm)
	}
	return cm, nil
}

func (cm *ContractManager) Close() error {
	return nil
}
