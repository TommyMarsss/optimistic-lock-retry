// Command sim runs an optimistic-locking simulation and writes a single
// self-contained HTML file that replays every writer's attempts, conflicts,
// retries and livelock events frame by frame. No server, no dependencies.
package main

import (
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	optimistic "github.com/TommyMarsss/optimistic-lock-retry"
)

//go:embed replay_template.html
var template string

type jsonEvent struct {
	T       float64 `json:"t"` // milliseconds since simulation start
	Writer  int     `json:"writer"`
	Type    string  `json:"type"`
	Version uint64  `json:"version"`
	Attempt int     `json:"attempt"`
	Detail  string  `json:"detail,omitempty"`
}

type pageData struct {
	Events           []jsonEvent `json:"events"`
	Writers          int         `json:"writers"`
	UpdatesPerWriter int         `json:"updatesPerWriter"`
	TotalMs          float64     `json:"totalMs"`
	FinalValue       int64       `json:"finalValue"`
	FinalVersion     uint64      `json:"finalVersion"`
	BackoffBase      string      `json:"backoffBase"`
	BackoffCap       string      `json:"backoffCap"`
	MaxRetries       int         `json:"maxRetries"`
	Jitter           float64     `json:"jitter"`
	Threshold        int         `json:"threshold"`
}

func main() {
	out := flag.String("out", "replay.html", "output HTML file")
	writers := flag.Int("writers", 10, "concurrent writers")
	updates := flag.Int("updates", 8, "updates per writer")
	compute := flag.Duration("compute", 2*time.Millisecond, "simulated read-modify latency")
	base := flag.Duration("base", 500*time.Microsecond, "backoff base delay")
	cap_ := flag.Duration("cap", 20*time.Millisecond, "backoff cap")
	maxRetries := flag.Int("max-retries", 8, "max retries before giving up")
	jitter := flag.Float64("jitter", 0.3, "backoff jitter fraction (0 = synchronized retries)")
	threshold := flag.Int("livelock-threshold", 10, "consecutive conflicts that declare livelock")
	yield := flag.Duration("yield", 5*time.Millisecond, "forced yield duration during mitigation")
	hold := flag.Duration("hold", 40*time.Millisecond, "priority token lifetime")
	seed := flag.Int64("seed", 42, "random seed")
	flag.Parse()

	res := optimistic.RunSimulation(optimistic.SimConfig{
		Writers:          *writers,
		UpdatesPerWriter: *updates,
		ComputeDelay:     *compute,
		Backoff: optimistic.Backoff{
			Base: *base, Cap: *cap_, MaxRetries: *maxRetries, Jitter: *jitter,
		},
		DetectThreshold: *threshold,
		YieldDuration:   *yield,
		PriorityHold:    *hold,
		Seed:            *seed,
	})

	data := pageData{
		Writers:          *writers,
		UpdatesPerWriter: *updates,
		FinalValue:       res.FinalValue,
		FinalVersion:     res.FinalVersion,
		BackoffBase:      base.String(),
		BackoffCap:       cap_.String(),
		MaxRetries:       *maxRetries,
		Jitter:           *jitter,
		Threshold:        *threshold,
	}
	var maxT float64
	for _, e := range res.Events {
		ms := float64(e.T) / float64(time.Millisecond)
		if ms > maxT {
			maxT = ms
		}
		data.Events = append(data.Events, jsonEvent{
			T: ms, Writer: e.Writer, Type: string(e.Type),
			Version: e.Version, Attempt: e.Attempt, Detail: e.Detail,
		})
	}
	data.TotalMs = maxT

	payload, err := json.Marshal(data)
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}
	html := strings.Replace(template, "/*__DATA__*/null", string(payload), 1)
	if err := os.WriteFile(*out, []byte(html), 0o644); err != nil {
		log.Fatalf("write %s: %v", *out, err)
	}

	fmt.Printf("writers=%d updates/writer=%d  commits=%d conflicts=%d give_ups=%d livelocks=%d yields=%d\n",
		*writers, *updates, res.Commits, res.Conflicts, res.GiveUps, res.Livelocks, res.Yields)
	fmt.Printf("final value=%d version=%d  duration=%s\n", res.FinalValue, res.FinalVersion, res.Duration)
	fmt.Printf("wrote %s (%d events)\n", *out, len(res.Events))
}
