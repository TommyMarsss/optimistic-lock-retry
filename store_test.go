package main

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// 版本号校验：过期版本号的提交必须被拒绝，且不得修改状态。
func TestCommitRejectsStaleVersion(t *testing.T) {
	s := NewStore(10)
	val, ver := s.Read()
	if val != 10 || ver != 0 {
		t.Fatalf("初始状态错误: val=%d ver=%d", val, ver)
	}
	if _, err := s.Commit(ver, 11); err != nil {
		t.Fatalf("首次提交应成功: %v", err)
	}
	// 用旧版本号再次提交 → 必须拒绝
	curVer, err := s.Commit(ver, 99)
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("过期版本号提交应返回 ErrVersionConflict，得到 %v", err)
	}
	if curVer != 1 {
		t.Fatalf("冲突时应返回当前版本号 1，得到 %d", curVer)
	}
	val, ver = s.Read()
	if val != 11 || ver != 1 {
		t.Fatalf("冲突提交不得修改状态: val=%d ver=%d", val, ver)
	}
}

// 并发写入下不允许丢失更新：N 个写者各做 M 次 +1，
// 最终值必须精确等于成功提交总数（每次成功提交恰好 +1）。
func TestConcurrentNoLostUpdates(t *testing.T) {
	const writers, updates = 16, 25
	s := NewStore(0)
	rec := NewRecorder()
	bo := NewBackoff(time.Millisecond, 20*time.Millisecond, 100, 0.5, 1)
	noSleep := func(time.Duration) {}

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w := &Writer{ID: id, Res: s, Backoff: bo, Rec: rec, Sleep: noSleep}
			for u := 0; u < updates; u++ {
				if err := w.RunOnce(func(cur int) int { return cur + 1 }); err != nil {
					t.Errorf("写者 %d 更新失败: %v", id, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	val, ver := s.Read()
	if val != writers*updates {
		t.Fatalf("丢失更新：最终值 %d，期望 %d", val, writers*updates)
	}
	if ver != writers*updates {
		t.Fatalf("版本号 %d 应等于提交次数 %d", ver, writers*updates)
	}
	// 事件流中的成功提交数也必须与最终值一致
	ok := 0
	for _, e := range rec.Events() {
		if e.Type == EvCommitOK {
			ok++
		}
	}
	if ok != val {
		t.Fatalf("成功提交事件数 %d 与最终值 %d 不一致", ok, val)
	}
}
