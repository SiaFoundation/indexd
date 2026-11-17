package contracts

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
)

func TestContractLockerLockUnlock(t *testing.T) {
	cl := NewContractLocker()
	id := types.FileContractID{1}

	// lock and unlock
	lc, unlock, _ := cl.LockContract(t.Context(), id)
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

	_, unlock1, _ := cl.LockContract(t.Context(), id)

	acquired := make(chan struct{})
	go func() {
		_, unlock2, _ := cl.LockContract(t.Context(), id)
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
	_, unlock1, _ := cl.LockContract(t.Context(), id1)
	_, unlock2, _ := cl.LockContract(t.Context(), id2)

	unlock1()
	unlock2()
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
			_, unlock, _ := cl.LockContract(t.Context(), id)
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

func TestContractLockerContext(t *testing.T) {
	cl := NewContractLocker()
	id := types.FileContractID{4}

	_, unlock, err := cl.LockContract(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}

	// locking a held contract should time out
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := cl.LockContract(ctx, id); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	unlock()

	// a failed lock attempt must not leak its refcount
	cl.mu.Lock()
	if len(cl.lockedContracts) != 0 {
		t.Fatal("expected no locked contracts")
	}
	cl.mu.Unlock()

	// lock should succeed after unlock
	_, unlock, err = cl.LockContract(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	unlock()
}
