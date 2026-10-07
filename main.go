package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

//go:embed replay_template.html
var replayTemplate string

type kv struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type scenarioJSON struct {
	Name    string  `json:"name"`
	Writers int     `json:"writers"`
	Summary []kv    `json:"summary"`
	Events  []Event `json:"events"`
}

// normalScenario 正常竞争场景：适量写者、合理退避参数，
// 冲突偶发但退避足以错开写者，不应触发活锁检测。
func normalScenario() ScenarioConfig {
	return ScenarioConfig{
		Name:             "场景 A：正常竞争（6 写者）",
		Writers:          6,
		UpdatesPerWriter: 4,
		WorkDelay:        300 * time.Microsecond,
		BackoffBase:      1 * time.Millisecond,
		BackoffCap:       30 * time.Millisecond,
		MaxRetries:       12,
		Jitter:           0.5,
		FailThreshold:    15,
		ConflictBurst:    80,
		BreakerYield:     10 * time.Millisecond,
		Seed:             42,
	}
}

// livelockScenario 活锁场景：大量写者、极小退避上限与抖动，
// 退避无法把写者错开，冲突持续爆发，活锁检测应触发并由打破器解围。
func livelockScenario() ScenarioConfig {
	return ScenarioConfig{
		Name:             "场景 B：高竞争活锁（32 写者）",
		Writers:          32,
		UpdatesPerWriter: 2,
		WorkDelay:        800 * time.Microsecond,
		BackoffBase:      300 * time.Microsecond,
		BackoffCap:       1500 * time.Microsecond,
		MaxRetries:       60,
		Jitter:           0.2,
		FailThreshold:    8,
		ConflictBurst:    50,
		BreakerYield:     12 * time.Millisecond,
		Seed:             7,
	}
}

func printReport(res ScenarioResult) {
	check := "✓ 无丢失更新"
	if !res.LostUpdateFree {
		check = "✗ 检测到丢失更新！"
	}
	fmt.Printf("== %s ==\n", res.Config.Name)
	fmt.Printf("  写者数 %d，每写者更新 %d 次，耗时 %.1f ms\n",
		res.Config.Writers, res.Config.UpdatesPerWriter, res.DurationMs)
	fmt.Printf("  成功提交 %d，冲突 %d，放弃 %d，活锁检测触发 %d 次\n",
		res.Successes, res.Conflicts, res.GiveUps, res.Livelocks)
	fmt.Printf("  最终值 %d（期望 %d），最终版本号 %d —— %s\n\n",
		res.FinalValue, res.ExpectedValue, res.FinalVersion, check)
}

func toJSON(res ScenarioConfig, r ScenarioResult) scenarioJSON {
	c := res
	summary := []kv{
		{"写者数量", fmt.Sprintf("%d", c.Writers)},
		{"每写者更新次数", fmt.Sprintf("%d", c.UpdatesPerWriter)},
		{"退避参数", fmt.Sprintf("base=%v, cap=%v, jitter=%.1f, maxRetries=%d",
			c.BackoffBase, c.BackoffCap, c.Jitter, c.MaxRetries)},
		{"活锁检测阈值", fmt.Sprintf("单写者连续失败 ≥ %d 或全局连续冲突 ≥ %d",
			c.FailThreshold, c.ConflictBurst)},
		{"成功提交", fmt.Sprintf("%d", r.Successes)},
		{"冲突次数", fmt.Sprintf("%d", r.Conflicts)},
		{"放弃次数", fmt.Sprintf("%d", r.GiveUps)},
		{"活锁检测触发", fmt.Sprintf("%d 次", r.Livelocks)},
		{"最终值 / 期望值", fmt.Sprintf("%d / %d", r.FinalValue, r.ExpectedValue)},
		{"最终版本号", fmt.Sprintf("%d", r.FinalVersion)},
		{"丢失更新检查", map[bool]string{true: "✓ 通过（最终值 == 成功提交数）", false: "✗ 失败"}[r.LostUpdateFree]},
		{"总耗时", fmt.Sprintf("%.1f ms", r.DurationMs)},
	}
	return scenarioJSON{Name: c.Name, Writers: c.Writers, Summary: summary, Events: r.Events}
}

func main() {
	configs := []ScenarioConfig{normalScenario(), livelockScenario()}
	scenarios := make([]scenarioJSON, 0, len(configs))
	for _, cfg := range configs {
		res := RunScenario(cfg, nil) // 真实睡眠，事件时间戳即真实时序
		printReport(res)
		scenarios = append(scenarios, toJSON(cfg, res))
	}

	data, err := json.Marshal(map[string]any{"scenarios": scenarios})
	if err != nil {
		fmt.Fprintln(os.Stderr, "序列化事件数据失败:", err)
		os.Exit(1)
	}
	html := strings.Replace(replayTemplate, "/*__DATA__*/", string(data), 1)
	const out = "replay.html"
	if err := os.WriteFile(out, []byte(html), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "写入回放文件失败:", err)
		os.Exit(1)
	}
	fmt.Printf("已生成 %s（内嵌 %d 个场景的事件数据），用浏览器打开即可回放。\n", out, len(scenarios))
}
