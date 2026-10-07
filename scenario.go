package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ScenarioConfig 描述一组并发更新场景的参数。
type ScenarioConfig struct {
	Name             string
	Writers          int           // 并发写者数量
	UpdatesPerWriter int           // 每个写者要完成的更新次数
	WorkDelay        time.Duration // 读取后模拟计算更新的耗时
	BackoffBase      time.Duration // 退避基础延迟
	BackoffCap       time.Duration // 退避上限
	MaxRetries       int           // 最大重试次数
	Jitter           float64       // 退避抖动比例
	FailThreshold    int           // 活锁检测：单写者连续失败阈值
	ConflictBurst    int           // 活锁检测：全局无成功连续冲突阈值
	BreakerYield     time.Duration // 活锁打破：强制让步窗口
	Seed             int64         // 随机种子
}

// ScenarioResult 是场景运行结果与统计。
type ScenarioResult struct {
	Config         ScenarioConfig
	Events         []Event
	Successes      int // 成功提交次数
	Conflicts      int // 冲突次数
	GiveUps        int // 放弃次数
	Livelocks      int // 活锁检测触发次数
	FinalValue     int
	FinalVersion   uint64
	ExpectedValue  int  // 若无丢失更新应等于 Successes
	LostUpdateFree bool // FinalValue == ExpectedValue
	DurationMs     float64
}

// RunScenario 运行一组并发更新：所有写者在同一个起跑栅栏后同时开始，
// 每个写者对共享资源做 UpdatesPerWriter 次 +1 更新。
// sleepFn 为 nil 时使用真实睡眠（演示用），测试中可注入空函数加速。
func RunScenario(cfg ScenarioConfig, sleepFn func(time.Duration)) ScenarioResult {
	if sleepFn == nil {
		sleepFn = time.Sleep
	}
	store := NewStore(0)
	rec := NewRecorder()
	breaker := NewBreaker(cfg.BreakerYield)

	var livelocks atomic.Int64
	var rndMu sync.Mutex
	rnd := rand.New(rand.NewSource(cfg.Seed))
	det := NewLivelockDetector(cfg.FailThreshold, cfg.ConflictBurst, func(reason string) {
		livelocks.Add(1)
		rndMu.Lock()
		champion := rnd.Intn(cfg.Writers)
		rndMu.Unlock()
		rec.Log(Event{Writer: -1, Type: EvLivelockDetected,
			Msg: fmt.Sprintf("%s；随机选中写者 %d 优先提交，其余写者强制让步 %v",
				reason, champion, cfg.BreakerYield)})
		breaker.Trigger(champion)
	})

	bo := NewBackoff(cfg.BackoffBase, cfg.BackoffCap, cfg.MaxRetries, cfg.Jitter, cfg.Seed)

	barrier := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < cfg.Writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w := &Writer{
				ID: id, Res: store, Backoff: bo, Rec: rec,
				Det: det, Breaker: breaker,
				WorkDelay: cfg.WorkDelay, Sleep: sleepFn,
			}
			<-barrier // 起跑栅栏：所有写者同时开始，制造最大竞争
			succ := 0
			for u := 0; u < cfg.UpdatesPerWriter; u++ {
				if err := w.RunOnce(func(cur int) int { return cur + 1 }); err == nil {
					succ++
				}
			}
			rec.Log(Event{Writer: id, Type: EvDone,
				Msg: fmt.Sprintf("完成 %d/%d 次更新", succ, cfg.UpdatesPerWriter)})
		}(i)
	}

	start := time.Now()
	close(barrier)
	wg.Wait()
	dur := time.Since(start)

	res := ScenarioResult{Config: cfg, Events: rec.Events(), DurationMs: msf(dur)}
	for _, e := range res.Events {
		switch e.Type {
		case EvCommitOK:
			res.Successes++
		case EvConflict:
			res.Conflicts++
		case EvGiveUp:
			res.GiveUps++
		case EvLivelockDetected:
			res.Livelocks++
		}
	}
	res.FinalValue, res.FinalVersion = store.Read()
	res.ExpectedValue = res.Successes
	res.LostUpdateFree = res.FinalValue == res.ExpectedValue
	return res
}
