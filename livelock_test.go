package optimistic

import (
	"testing"
	"time"
)

// The detector must fire exactly when a writer's consecutive conflicts hit
// the threshold, grant that writer priority, force others to yield, and
// release once the priority writer commits.
func TestDetectorLifecycle(t *testing.T) {
	var victim = -1
	d := NewDetector(3, 5*time.Millisecond, 50*time.Millisecond, func(v int) { victim = v })

	for i := 0; i < 2; i++ {
		if d.RecordFailure(7) {
			t.Fatalf("fired early at failure %d", i+1)
		}
	}
	if !d.RecordFailure(7) {
		t.Fatal("did not fire at threshold")
	}
	if victim != 7 {
		t.Fatalf("priority victim = %d, want 7", victim)
	}
	if !d.Active() {
		t.Fatal("detector should be active after firing")
	}
	if d.Triggers() != 1 {
		t.Fatalf("triggers = %d, want 1", d.Triggers())
	}

	// Priority writer is not asked to yield; everyone else is.
	if _, yield := d.ShouldYield(7); yield {
		t.Fatal("priority writer must not yield")
	}
	wait, yield := d.ShouldYield(3)
	if !yield {
		t.Fatal("non-priority writer must yield during mitigation")
	}
	if wait < 5*time.Millisecond || wait >= 10*time.Millisecond {
		t.Fatalf("yield wait %v outside [5ms, 10ms)", wait)
	}

	// A commit from the priority writer ends the episode.
	d.RecordCommit(7)
	if d.Active() {
		t.Fatal("detector should deactivate after priority commit")
	}
	if _, yield := d.ShouldYield(3); yield {
		t.Fatal("no yields after episode ended")
	}
}

// A commit resets the writer's streak, so normal scattered conflicts never
// accumulate into a false livelock alarm.
func TestDetectorCommitResetsStreak(t *testing.T) {
	d := NewDetector(3, time.Millisecond, 10*time.Millisecond, nil)
	for round := 0; round < 10; round++ {
		d.RecordFailure(1)
		d.RecordFailure(1)
		d.RecordCommit(1)
	}
	if d.Triggers() != 0 {
		t.Fatalf("false positive: %d triggers", d.Triggers())
	}
}

// Integration: a synchronized high-contention run (jitter disabled) must
// trigger livelock detection, mitigation must produce yields, and the run
// must still terminate with every committed update preserved.
func TestSimulationDetectsAndBreaksLivelock(t *testing.T) {
	done := make(chan *SimResult, 1)
	go func() {
		done <- RunSimulation(SimConfig{
			Writers:          12,
			UpdatesPerWriter: 6,
			ComputeDelay:     2 * time.Millisecond,
			Backoff: Backoff{
				Base: 200 * time.Microsecond, Cap: 5 * time.Millisecond,
				MaxRetries: 20, Jitter: 0, // no jitter: retries stay synchronized
			},
			DetectThreshold: 8,
			YieldDuration:   3 * time.Millisecond,
			PriorityHold:    50 * time.Millisecond,
			Seed:            42,
		})
	}()

	var res *SimResult
	select {
	case res = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("simulation did not terminate within 30s — possible unbroken livelock")
	}

	if res.Livelocks == 0 {
		t.Error("livelock detector never fired in a synchronized high-contention run")
	}
	if res.Yields == 0 {
		t.Error("mitigation produced no forced yields")
	}
	if res.Commits == 0 {
		t.Fatal("no commits at all")
	}
	// No lost updates: every commit is one increment, so value == commits.
	if res.FinalValue != int64(res.Commits) {
		t.Errorf("final value %d != commits %d — lost updates", res.FinalValue, res.Commits)
	}
	if res.FinalVersion != uint64(res.Commits) {
		t.Errorf("final version %d != commits %d", res.FinalVersion, res.Commits)
	}
	t.Logf("commits=%d conflicts=%d give_ups=%d livelocks=%d yields=%d duration=%s",
		res.Commits, res.Conflicts, res.GiveUps, res.Livelocks, res.Yields, res.Duration)
}

// Integration: with jittered backoff and moderate contention, all updates
// succeed and the detector stays quiet or fires only rarely.
func TestSimulationNormalRunNoLostUpdates(t *testing.T) {
	res := RunSimulation(SimConfig{
		Writers:          8,
		UpdatesPerWriter: 10,
		ComputeDelay:     100 * time.Microsecond,
		Backoff: Backoff{
			Base: 200 * time.Microsecond, Cap: 10 * time.Millisecond,
			MaxRetries: 10, Jitter: 0.5,
		},
		DetectThreshold: 30,
		YieldDuration:   2 * time.Millisecond,
		PriorityHold:    20 * time.Millisecond,
		Seed:            1,
	})
	if res.GiveUps != 0 {
		t.Errorf("unexpected give-ups: %d", res.GiveUps)
	}
	want := int64(8 * 10)
	if res.FinalValue != want {
		t.Errorf("final value = %d, want %d — lost updates", res.FinalValue, want)
	}
	if int64(res.Commits) != want {
		t.Errorf("commits = %d, want %d", res.Commits, want)
	}
}
