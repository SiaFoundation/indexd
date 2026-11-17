package contracts

import (
	"context"
	"sync"

	"go.sia.tech/core/types"
)

// ContractLocker manages locks for contracts. It allows locking a contract by
// its ID and ensures that only one lock can be held for a contract at a time.
type ContractLocker struct {
	mu              sync.Mutex
	lockedContracts map[types.FileContractID]*LockedContract
}

// LockedContract represents a contract that is currently locked. A locked
// contract can be obtained by calling LockContract on the ContractLocker. The
// contract is unlocked by calling the function returned by that method.
// Methods that operate on a locked contract should accept a *LockedContract as
// an argument to make sure it can never be called without locking the contract
// first.
type LockedContract struct {
	id   types.FileContractID
	lock chan struct{} // buffered with capacity 1, acts as a mutex

	refcount int // locked by ContractLocker.mu
}

// NewContractLocker creates a new ContractLocker.
func NewContractLocker() *ContractLocker {
	return &ContractLocker{
		lockedContracts: make(map[types.FileContractID]*LockedContract),
	}
}

func (cl *ContractLocker) release(lc *LockedContract) {
	cl.mu.Lock()
	lc.refcount--
	if lc.refcount == 0 {
		delete(cl.lockedContracts, lc.id)
	}
	cl.mu.Unlock()
}

// LockContract locks the contract with the given ID and returns a function to
// unlock it. If the contract is already locked, it blocks until it can acquire
// the lock or the context is done.
func (cl *ContractLocker) LockContract(ctx context.Context, contractID types.FileContractID) (*LockedContract, func(), error) {
	cl.mu.Lock()
	lc, exists := cl.lockedContracts[contractID]
	if !exists {
		lc = &LockedContract{
			id:   contractID,
			lock: make(chan struct{}, 1),
		}
		cl.lockedContracts[contractID] = lc
	}
	lc.refcount++
	cl.mu.Unlock()

	select {
	case lc.lock <- struct{}{}:
	case <-ctx.Done():
		cl.release(lc)
		return nil, nil, ctx.Err()
	}
	return lc, func() {
		<-lc.lock
		cl.release(lc)
	}, nil
}
