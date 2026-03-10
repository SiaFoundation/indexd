package contracts

import (
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
// contract can be obtained by calling lockContract or tryLockContract on the
// ContractManager. The contract is unlocked by calling the function returned by
// those methods.
// Methods that operate on a locked contract should accept a *LockedContract as
// an argument to make sure it can never be called without locking the contract
// first.
type LockedContract struct {
	id types.FileContractID
	mu sync.Mutex // used to acquire contract

	refcount int // locked by ContractLocker.mu
}

// NewContractLocker creates a new ContractLocker.
func NewContractLocker() *ContractLocker {
	return &ContractLocker{
		lockedContracts: make(map[types.FileContractID]*LockedContract),
	}
}

// LockContract locks the contract with the given ID and returns a function to
// unlock it. If the contract is already locked, it blocks until it can acquire
// the lock.
func (cl *ContractLocker) LockContract(contractID types.FileContractID) (*LockedContract, func()) {
	cl.mu.Lock()
	lc, exists := cl.lockedContracts[contractID]
	if !exists {
		lc = &LockedContract{
			id: contractID,
		}
		cl.lockedContracts[contractID] = lc
	}
	lc.refcount++
	cl.mu.Unlock()

	lc.mu.Lock()
	return lc, func() {
		lc.mu.Unlock()

		cl.mu.Lock()
		lc.refcount--
		if lc.refcount == 0 {
			delete(cl.lockedContracts, contractID)
		}
		cl.mu.Unlock()
	}
}

// TryLockContract tries to lock the contract with the given ID and returns a
// function to unlock it. If the contract is already locked, it returns nil and
// does not block.
func (cl *ContractLocker) TryLockContract(contractID types.FileContractID) (*LockedContract, func()) {
	cl.mu.Lock()
	_, exists := cl.lockedContracts[contractID]
	if exists {
		cl.mu.Unlock()
		return nil, nil
	}
	lc := &LockedContract{
		id:       contractID,
		refcount: 1,
	}
	lc.mu.Lock()
	cl.lockedContracts[contractID] = lc
	cl.mu.Unlock()

	return lc, func() {
		lc.mu.Unlock()

		cl.mu.Lock()
		lc.refcount--
		if lc.refcount == 0 {
			delete(cl.lockedContracts, contractID)
		}
		cl.mu.Unlock()
	}
}
