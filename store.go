// Package optimistic implements version-based optimistic concurrency control
// with exponential-backoff retries and livelock detection.
package optimistic

import "sync"

// Store is a single-versioned resource. All reads return the current value
// together with its version; writes must present the version they read and
// are rejected if the version has moved on (compare-and-swap semantics).
type Store struct {
	mu      sync.Mutex
	value   int64
	version uint64
}

// NewStore returns a Store holding initialValue at version 0.
func NewStore(initialValue int64) *Store {
	return &Store{value: initialValue}
}

// Read returns the current value and its version atomically.
func (s *Store) Read() (value int64, version uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value, s.version
}

// CompareAndSwap writes newValue only if expectedVersion still matches the
// store's version. On success it returns the new version and true; on
// version mismatch the write is rejected and false is returned — this is
// the check that prevents lost updates.
func (s *Store) CompareAndSwap(expectedVersion uint64, newValue int64) (newVersion uint64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.version != expectedVersion {
		return s.version, false
	}
	s.value = newValue
	s.version++
	return s.version, true
}

// Version returns the current version (for tests and diagnostics).
func (s *Store) Version() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version
}

// Value returns the current value (for tests and diagnostics).
func (s *Store) Value() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value
}
