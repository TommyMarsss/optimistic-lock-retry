package optimistic

import (
	"sync"
	"testing"
)

// A stale version must be rejected: no silent overwrite, no lost update.
func TestStoreRejectsStaleVersion(t *testing.T) {
	s := NewStore(0)
	_, v0 := s.Read()

	if _, ok := s.CompareAndSwap(v0, 10); !ok {
		t.Fatal("first write at current version should succeed")
	}
	if _, ok := s.CompareAndSwap(v0, 20); ok {
		t.Fatal("write with stale version must be rejected")
	}
	if got := s.Value(); got != 10 {
		t.Fatalf("value = %d, want 10 (stale write must not overwrite)", got)
	}
	if got := s.Version(); got != 1 {
		t.Fatalf("version = %d, want 1", got)
	}
}

// Concurrent writers doing read-modify-write through the retry loop must
// not lose a single update: final value and version equal total commits.
func TestStoreConcurrentNoLostUpdates(t *testing.T) {
	const writers, perWriter = 16, 50
	s := NewStore(0)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				for {
					val, ver := s.Read()
					if _, ok := s.CompareAndSwap(ver, val+1); ok {
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	want := int64(writers * perWriter)
	if got := s.Value(); got != want {
		t.Fatalf("value = %d, want %d — updates were lost", got, want)
	}
	if got := s.Version(); got != uint64(want) {
		t.Fatalf("version = %d, want %d", got, want)
	}
}
