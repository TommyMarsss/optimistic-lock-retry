package main

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// alwaysFailResource 永远冲突的资源，用于验证"超过最大重试次数后放弃并报告"。
type alwaysFailResource struct{}

func (alwaysFailResource) Read() (int, uint64)                { return 0, 0 }
func (alwaysFailResource) Commit(uint64, int) (uint64, error) { return 0, ErrVersionConflict }

// flakyResource 前 n 次提交冲突，之后正常工作（带真实版本号语义）。
type flakyResource struct {
	n       int
	calls   int
	value   int
	version uint64
}

func (f *flakyResource) Read() (int, uint64) { return f.value, f.version }
func (f *flakyResource) Commit(exp uint64, v int) (uint64, error) {
	f.calls++
	if f.calls <= f.n {
		return f.version, ErrVersionConflict
	}
	if f.version != exp {
		return f.version, ErrVersionConflict
	}
	f.value = v
	f.version++
	return f.version, nil
}

func newTestWriter(res Resource, maxRetries int, rec *Recorder) *Writer {
	return &Writer{
		ID:      0,
		Res:     res,
		Backoff: NewBackoff(time.Millisecond, 10*time.Millisecond, maxRetries, 0.5, 1),
		Rec:     rec,
		Sleep:   func(time.Duration) {},
	}
}

// 超过最大重试次数后必须放弃、返回明确错误并记录 giveup 事件，
// 而不是无限重试。总尝试次数 = 1 + MaxRetries。
func TestGiveUpAfterMaxRetries(t *testing.T) {
	rec := NewRecorder()
	w := newTestWriter(alwaysFailResource{}, 5, rec)

	err := w.RunOnce(func(cur int) int { return cur + 1 })
	if err == nil {
		t.Fatal("持续冲突应返回错误")
	}
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("错误应包装 ErrVersionConflict: %v", err)
	}
	if !strings.Contains(err.Error(), "放弃") {
		t.Fatalf("错误信息应明确报告放弃: %v", err)
	}

	var conflicts, backoffs, giveups int
	for _, e := range rec.Events() {
		switch e.Type {
		case EvConflict:
			conflicts++
		case EvBackoff:
			backoffs++
		case EvGiveUp:
			giveups++
		}
	}
	if conflicts != 6 { // 1 次首次尝试 + 5 次重试
		t.Fatalf("应冲突 6 次（1+5），实际 %d", conflicts)
	}
	if backoffs != 5 {
		t.Fatalf("应退避 5 次，实际 %d", backoffs)
	}
	if giveups != 1 {
		t.Fatalf("应记录 1 次放弃事件，实际 %d", giveups)
	}
}

// 前几次冲突后应能重试成功：重新读取、重新计算、重新提交。
func TestRetrySucceedsAfterConflicts(t *testing.T) {
	rec := NewRecorder()
	res := &flakyResource{n: 3}
	w := newTestWriter(res, 10, rec)

	if err := w.RunOnce(func(cur int) int { return cur + 1 }); err != nil {
		t.Fatalf("第 4 次尝试应成功: %v", err)
	}
	if res.value != 1 || res.version != 1 {
		t.Fatalf("提交结果错误: value=%d version=%d", res.value, res.version)
	}
	var reads, oks int
	for _, e := range rec.Events() {
		switch e.Type {
		case EvRead:
			reads++
		case EvCommitOK:
			oks++
		}
	}
	if reads != 4 || oks != 1 {
		t.Fatalf("应重新读取 4 次、成功 1 次，实际 reads=%d oks=%d", reads, oks)
	}
}
