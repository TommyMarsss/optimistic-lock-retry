package main

import (
	"errors"
	"fmt"
	"time"
)

// Writer 表示一个并发写者：读取 → 计算更新 → 携带版本号提交，
// 冲突时按指数退避重试，超过最大重试次数则放弃并报告。
type Writer struct {
	ID        int
	Res       Resource
	Backoff   *Backoff
	Rec       *Recorder
	Det       *LivelockDetector   // 可为 nil（不检测活锁）
	Breaker   *Breaker            // 可为 nil（不参与活锁打破）
	WorkDelay time.Duration       // 模拟"读取后计算更新"的耗时，扩大竞争窗口
	Sleep     func(time.Duration) // 可注入，测试中用空函数加速；nil 时用 time.Sleep
}

func (w *Writer) sleep(d time.Duration) {
	if w.Sleep != nil {
		w.Sleep(d)
		return
	}
	time.Sleep(d)
}

// RunOnce 执行一次完整的"读-改-写"乐观锁更新，update 由当前值计算新值。
// 成功返回 nil；超过最大重试次数返回带说明的错误。
func (w *Writer) RunOnce(update func(cur int) int) error {
	for attempt := 1; ; attempt++ {
		// 活锁打破：非优先写者在窗口期内强制让步。
		if w.Breaker != nil {
			if d, ok := w.Breaker.ShouldYield(w.ID); ok {
				w.Rec.Log(Event{Writer: w.ID, Type: EvLivelockYield, Attempt: attempt, DelayMs: msf(d),
					Msg: "活锁打破：强制让步，让优先写者先提交"})
				w.sleep(d)
			}
		}

		val, ver := w.Res.Read()
		w.Rec.Log(Event{Writer: w.ID, Type: EvRead, Attempt: attempt, Version: ver, Value: val})

		if w.WorkDelay > 0 {
			w.sleep(w.WorkDelay) // 模拟基于读取值计算更新的耗时
		}
		newVal := update(val)

		newVer, err := w.Res.Commit(ver, newVal)
		if err == nil {
			w.Rec.Log(Event{Writer: w.ID, Type: EvCommitOK, Attempt: attempt, Version: newVer, Value: newVal})
			if w.Det != nil {
				w.Det.OnSuccess(w.ID)
			}
			if w.Breaker != nil {
				w.Breaker.Clear()
			}
			return nil
		}
		if !errors.Is(err, ErrVersionConflict) {
			return err
		}

		w.Rec.Log(Event{Writer: w.ID, Type: EvConflict, Attempt: attempt, Version: ver,
			Msg: "版本号已被其他写者修改，本次写入被拒绝"})
		if w.Det != nil {
			w.Det.OnConflict(w.ID)
		}

		if attempt > w.Backoff.MaxRetries {
			w.Rec.Log(Event{Writer: w.ID, Type: EvGiveUp, Attempt: attempt,
				Msg: fmt.Sprintf("超过最大重试次数 %d，冲突无法解决，放弃", w.Backoff.MaxRetries)})
			return fmt.Errorf("writer %d: 冲突无法解决，%d 次重试后放弃: %w",
				w.ID, w.Backoff.MaxRetries, ErrVersionConflict)
		}

		d := w.Backoff.Delay(attempt)
		w.Rec.Log(Event{Writer: w.ID, Type: EvBackoff, Attempt: attempt, DelayMs: msf(d)})
		w.sleep(d)
	}
}
