package contracts

import (
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
)

func TestContractLockerLockUnlock(t *testing.T) {
	cl := NewContractLocker()
	id := types.FileContractID{1}

	// lock and unlock
	lc, unlock := cl.LockContract(id)
	if lc == nil {
		t.Fatal("expected non-nil LockedContract")
	}
	unlock()

	// after unlock the map should be empty
	cl.mu.Lock()
	if len(cl.lockedContracts) != 0 {
		t.Fatal("expected no locked contracts after unlock")
	}
	cl.mu.Unlock()
}

func TestContractLockerLockBlocks(t *testing.T) {
	cl := NewContractLocker()
	id := types.FileContractID{2}

	_, unlock1 := cl.LockContract(id)

	acquired := make(chan struct{})
	go func() {
		_, unlock2 := cl.LockContract(id)
		unlock2()
		close(acquired)
	}()

	// the second lock should be blocked
	select {
	case <-acquired:
		t.Fatal("second LockContract should block while first is held")
	case <-time.After(250 * time.Millisecond):
	}

	// unlock first, second should proceed
	unlock1()

	select {
	case <-acquired:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("second LockContract should have acquired after first unlock")
	}

	// map should be clean
	cl.mu.Lock()
	if len(cl.lockedContracts) != 0 {
		t.Fatal("expected no locked contracts")
	}
	cl.mu.Unlock()
}

func TestContractLockerDifferentContracts(t *testing.T) {
	cl := NewContractLocker()
	id1 := types.FileContractID{1}
	id2 := types.FileContractID{2}

	// locking different contracts should not block each other
	_, unlock1 := cl.LockContract(id1)
	_, unlock2 := cl.LockContract(id2)

	unlock1()
	unlock2()
}

func TestTryLockContract(t *testing.T) {
	cl := NewContractLocker()
	id := types.FileContractID{3}

	// try lock on unlocked contract should succeed
	lc, unlock := cl.TryLockContract(id)
	if lc == nil {
		t.Fatal("expected non-nil LockedContract")
	}

	unlock()

	// map should be clean after unlock
	cl.mu.Lock()
	if len(cl.lockedContracts) != 0 {
		t.Fatal("expected no locked contracts after unlock")
	}
	cl.mu.Unlock()
}

func TestTryLockContractAlreadyLocked(t *testing.T) {
	cl := NewContractLocker()
	id := types.FileContractID{4}

	// lock with LockContract first
	_, unlock := cl.LockContract(id)

	// TryLockContract on same ID should fail
	lc, tryUnlock := cl.TryLockContract(id)
	if lc != nil || tryUnlock != nil {
		t.Fatal("expected TryLockContract to return nil when contract is locked")
	}

	unlock()
}

func TestContractLockerConcurrent(t *testing.T) {
	cl := NewContractLocker()
	id := types.FileContractID{5}

	const n = 100
	var wg sync.WaitGroup
	counter := 0

	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, unlock := cl.LockContract(id)
			counter++ // protected by the contract lock
			unlock()
		}()
	}

	wg.Wait()

	if counter != n {
		t.Fatalf("expected counter %d, got %d", n, counter)
	}

	cl.mu.Lock()
	if len(cl.lockedContracts) != 0 {
		t.Fatal("expected no locked contracts")
	}
	cl.mu.Unlock()
}

func TestTryLockAfterUnlock(t *testing.T) {
	cl := NewContractLocker()
	id := types.FileContractID{6}

	// lock and unlock
	_, unlock := cl.LockContract(id)
	unlock()

	// try lock should succeed after unlock
	lc, tryUnlock := cl.TryLockContract(id)
	if lc == nil {
		t.Fatal("expected TryLockContract to succeed after unlock")
	}
	tryUnlock()
}
