package main

import (
	"testing"
	"time"
)

// 单写者连续失败达到阈值 → 触发活锁检测；成功后复位并可再次触发。
func TestDetectorTriggersOnConsecutiveFailures(t *testing.T) {
	var reasons []string
	d := NewLivelockDetector(5, 1000, func(r string) { reasons = append(reasons, r) })

	for i := 0; i < 4; i++ {
		d.OnConflict(1)
	}
	if len(reasons) != 0 {
		t.Fatalf("4 次连续失败不应触发（阈值 5）: %v", reasons)
	}
	d.OnConflict(1)
	if len(reasons) != 1 {
		t.Fatalf("5 次连续失败应触发 1 次，实际 %d", len(reasons))
	}
	// 触发后未复位前，更多冲突不应重复触发
	d.OnConflict(1)
	if len(reasons) != 1 {
		t.Fatalf("未复位前不应重复触发，实际 %d", len(reasons))
	}
	// 成功复位后，再次积累到阈值应重新触发
	d.OnSuccess(1)
	for i := 0; i < 5; i++ {
		d.OnConflict(1)
	}
	if len(reasons) != 2 {
		t.Fatalf("复位后应能再次触发，实际 %d", len(reasons))
	}
}

// 全局连续冲突（跨写者）无成功 → 触发。
func TestDetectorTriggersOnGlobalBurst(t *testing.T) {
	var fired int
	d := NewLivelockDetector(1000, 10, func(string) { fired++ })
	for i := 0; i < 9; i++ {
		d.OnConflict(i % 3) // 分散在多个写者，单写者阈值不触发
	}
	if fired != 0 {
		t.Fatal("9 次全局冲突不应触发（阈值 10）")
	}
	d.OnConflict(0)
	if fired != 1 {
		t.Fatalf("10 次全局连续冲突应触发，实际 %d", fired)
	}
	// 任何写者成功都复位全局计数
	d.OnSuccess(2)
	for i := 0; i < 9; i++ {
		d.OnConflict(i % 3)
	}
	if fired != 1 {
		t.Fatalf("复位后 9 次冲突不应再次触发，实际 %d", fired)
	}
}

// 打破器：非优先写者在窗口期内让步，优先写者不让步；窗口过期或解除后不再让步。
func TestBreakerYield(t *testing.T) {
	b := NewBreaker(50 * time.Millisecond)
	now := time.Now()
	b.now = func() time.Time { return now } // 注入可控时钟

	b.Trigger(3)
	if _, ok := b.ShouldYield(3); ok {
		t.Fatal("优先写者不应让步")
	}
	d, ok := b.ShouldYield(7)
	if !ok || d <= 0 || d > 50*time.Millisecond {
		t.Fatalf("非优先写者应在窗口期内让步，得到 %v, %v", d, ok)
	}
	now = now.Add(60 * time.Millisecond) // 窗口过期
	if _, ok := b.ShouldYield(7); ok {
		t.Fatal("窗口过期后不应再让步")
	}

	b.Trigger(3)
	b.Clear()
	if _, ok := b.ShouldYield(7); ok {
		t.Fatal("解除后不应让步")
	}
}

// 集成：高竞争场景下活锁检测应在合理时间内触发，
// 打破器介入后全部更新最终完成，且无丢失更新。
func TestLivelockDetectedAndBroken(t *testing.T) {
	cfg := ScenarioConfig{
		Name:             "livelock-test",
		Writers:          32,
		UpdatesPerWriter: 2,
		WorkDelay:        500 * time.Microsecond,
		BackoffBase:      200 * time.Microsecond,
		BackoffCap:       time.Millisecond,
		MaxRetries:       200,
		Jitter:           0.2,
		FailThreshold:    8,
		ConflictBurst:    50,
		BreakerYield:     5 * time.Millisecond,
		Seed:             7,
	}
	done := make(chan ScenarioResult, 1)
	go func() { done <- RunScenario(cfg, nil) }()

	var res ScenarioResult
	select {
	case res = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("场景未在 15s 内完成：活锁未被打破")
	}

	if res.Livelocks == 0 {
		t.Fatal("高竞争场景应触发活锁检测")
	}
	if !res.LostUpdateFree {
		t.Fatalf("丢失更新：最终值 %d，成功提交 %d", res.FinalValue, res.Successes)
	}
	if res.Successes+res.GiveUps != cfg.Writers*cfg.UpdatesPerWriter {
		t.Fatalf("成功 %d + 放弃 %d != 应完成更新 %d",
			res.Successes, res.GiveUps, cfg.Writers*cfg.UpdatesPerWriter)
	}
	t.Logf("活锁触发 %d 次，成功 %d，冲突 %d，放弃 %d，耗时 %.1fms",
		res.Livelocks, res.Successes, res.Conflicts, res.GiveUps, res.DurationMs)
}

// 正常竞争场景不应误报活锁。
func TestNormalContentionNoFalsePositive(t *testing.T) {
	cfg := normalScenario()
	res := RunScenario(cfg, func(time.Duration) {}) // 空睡眠加速
	if res.Livelocks != 0 {
		t.Fatalf("正常竞争不应触发活锁检测，触发 %d 次", res.Livelocks)
	}
	if !res.LostUpdateFree {
		t.Fatalf("丢失更新：最终值 %d，成功提交 %d", res.FinalValue, res.Successes)
	}
}
