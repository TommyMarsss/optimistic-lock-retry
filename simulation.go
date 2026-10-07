package optimistic

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// SimConfig configures one simulation run.
type SimConfig struct {
	Writers          int           // number of concurrent writers
	UpdatesPerWriter int           // increments each writer performs
	ComputeDelay     time.Duration // simulated work between read and commit (widens the race window)
	Backoff          Backoff       // retry policy template (each writer gets its own copy)
	DetectThreshold  int           // consecutive per-writer conflicts that declare livelock
	YieldDuration    time.Duration // forced yield for non-priority writers during mitigation
	PriorityHold     time.Duration // priority token lifetime
	Seed             int64         // random seed for reproducibility
}

// SimResult summarizes a finished run.
type SimResult struct {
	Events       []Event
	FinalValue   int64
	FinalVersion uint64
	Commits      int
	Conflicts    int
	GiveUps      int
	Livelocks    int
	Yields       int
	Duration     time.Duration
}

// RunSimulation starts cfg.Writers goroutines, each performing
// cfg.UpdatesPerWriter increment updates on a shared store, and collects
// the full event timeline. It returns after all writers finish.
func RunSimulation(cfg SimConfig) *SimResult {
	store := NewStore(0)
	log := NewEventLog()

	det := NewDetector(cfg.DetectThreshold, cfg.YieldDuration, cfg.PriorityHold, func(victim int) {
		log.Add(Event{Writer: victim, Type: EventLivelock, Detail: fmt.Sprintf("writer %d starved, priority granted", victim)})
	})

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < cfg.Writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			bo := cfg.Backoff // per-writer copy with its own deterministic rand stream
			bo.rand = rand.New(rand.NewSource(cfg.Seed + int64(id)*7919))
			u := NewUpdater(id, store, &bo, det, log)
			for n := 0; n < cfg.UpdatesPerWriter; n++ {
				// Give-ups are logged by Update itself; the simulation
				// keeps going so the timeline shows every abandoned update.
				_, _ = u.Update(func(old int64) int64 {
					if cfg.ComputeDelay > 0 {
						time.Sleep(cfg.ComputeDelay) // simulate read-modify latency
					}
					return old + 1
				})
			}
		}(i)
	}
	wg.Wait()

	res := &SimResult{
		Events:       log.Snapshot(),
		FinalValue:   store.Value(),
		FinalVersion: store.Version(),
		Livelocks:    det.Triggers(),
		Duration:     time.Since(start),
	}
	for _, e := range res.Events {
		switch e.Type {
		case EventCommit:
			res.Commits++
		case EventConflict:
			res.Conflicts++
		case EventGiveUp:
			res.GiveUps++
		case EventYield:
			res.Yields++
		}
	}
	return res
}
