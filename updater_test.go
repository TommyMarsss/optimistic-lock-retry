package optimistic

import (
	"errors"
	"testing"
	"time"
)

func noSleep(time.Duration) {}

// An updater that always loses the race must give up after MaxRetries and
// report ErrConflictExhausted — never loop forever, never skip the check.
func TestUpdaterGivesUpAfterMaxRetries(t *testing.T) {
	s := NewStore(0)
	bo := &Backoff{Base: time.Millisecond, Cap: 10 * time.Millisecond, MaxRetries: 3, Jitter: 0}
	log := NewEventLog()
	u := NewUpdater(0, s, bo, nil, log)
	u.SetSleep(noSleep)

	// The update function sabotages the store before the updater commits,
	// simulating a competitor that always wins the race.
	_, err := u.Update(func(old int64) int64 {
		v := s.Version()
		s.CompareAndSwap(v, old+1000) // external interfering write
		return old + 1
	})
	if !errors.Is(err, ErrConflictExhausted) {
		t.Fatalf("err = %v, want ErrConflictExhausted", err)
	}

	var conflicts, giveUps int
	for _, e := range log.Snapshot() {
		switch e.Type {
		case EventConflict:
			conflicts++
		case EventGiveUp:
			giveUps++
		}
	}
	if conflicts != bo.MaxRetries+1 { // initial attempt + MaxRetries retries
		t.Fatalf("conflicts = %d, want %d", conflicts, bo.MaxRetries+1)
	}
	if giveUps != 1 {
		t.Fatalf("give-up events = %d, want 1", giveUps)
	}
}

// An updater that loses a few races must retry, re-read, recompute and
// finally commit — and the committed value must reflect the fresh state.
func TestUpdaterRetriesThenCommits(t *testing.T) {
	s := NewStore(41)
	bo := &Backoff{Base: time.Millisecond, Cap: 10 * time.Millisecond, MaxRetries: 5, Jitter: 0}
	u := NewUpdater(0, s, bo, nil, nil)
	u.SetSleep(noSleep)

	calls := 0
	ver, err := u.Update(func(old int64) int64 {
		calls++
		if calls == 1 {
			// interfere exactly once, then let the retry through
			s.CompareAndSwap(s.Version(), old+100)
		}
		return old + 1
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("update fn ran %d times, want 2 (recompute on retry)", calls)
	}
	// First read saw 41; interference set 141; retry re-read 141 and added 1.
	if got := s.Value(); got != 142 {
		t.Fatalf("value = %d, want 142 (retry must recompute on fresh state)", got)
	}
	if ver != s.Version() {
		t.Fatalf("returned version %d != store version %d", ver, s.Version())
	}
}
