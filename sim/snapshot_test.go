package sim

import (
	"testing"

	"github.com/xingjianluo/replicated-log-go/raft"
)

// TestSnapshotAndLaggingFollowerCatchUp 落后节点通过快照恢复：
// 领导者压缩日志后，长期掉线的节点重新上线，不能因日志被压缩而卡死，
// 必须先安装快照再追增量，且所有已提交状态不丢、不回退。
func TestSnapshotAndLaggingFollowerCatchUp(t *testing.T) {
	e, _, ids := newTestEnv(t, 11)
	lead := e.Leader()

	var lagging raft.NodeID
	for _, id := range ids {
		if id != lead {
			lagging = id
			break
		}
	}

	// 落后节点长期宕机；其余多数派持续提交。
	e.StopNode(lagging)
	for i := range 12 {
		proposeOrFatal(t, e, lead, "counter", string(rune('0'+i%10)))
	}

	// 领导者在已应用位置生成快照、压缩日志（压缩点远大于 1）。
	snap, err := e.CompactNode(lead)
	if err != nil {
		t.Fatalf("领导者生成快照失败: %v", err)
	}
	if snap.Index < 10 {
		t.Fatalf("快照位置过小: %d", snap.Index)
	}
	stLead := e.Status(lead)
	if stLead.Commit < snap.Index {
		t.Fatalf("快照点超过提交位置: snap=%d commit=%d", snap.Index, stLead.Commit)
	}

	// 快照恢复节点上线：磁盘为空白（比快照点旧得多），只能走 InstallSnapshot。
	e.StartNode(lagging)
	e.Run(60)

	// 已提交状态完整恢复，之后的新提案也能增量追上。
	v, ok := e.Get(lagging, "counter")
	if !ok || v == "" {
		t.Fatalf("落后节点快照恢复后缺少已提交键 counter")
	}
	if got := e.CommitIndex(lagging); got < snap.Index {
		t.Fatalf("落后节点提交位置 %d 未追过快照点 %d", got, snap.Index)
	}
	proposeOrFatal(t, e, e.Leader(), "after", "snap")
	e.Run(30)
	n, val, match := e.LogMatch("after")
	if !match || n != 3 || val != "snap" {
		t.Fatalf("快照追赶后增量复制失败: n=%d val=%q ok=%v", n, val, match)
	}
	n, val, match = e.LogMatch("counter")
	if !match || n != 3 || val != "1" { // 12 次提案 counter 终值: 0..9,0,1
		t.Fatalf("快照恢复后历史值不一致: n=%d val=%q ok=%v", n, val, match)
	}
}

// TestSnapshotDoesNotRegressCommit 快照是压缩手段而非状态重置：
// 生成快照后继续提案，提交位置只能单调增长。
func TestSnapshotDoesNotRegressCommit(t *testing.T) {
	e, _, _ := newTestEnv(t, 13)
	lead := e.Leader()
	for range 6 {
		proposeOrFatal(t, e, lead, "x", "y")
	}
	c1 := e.CommitIndex(lead)
	if _, err := e.CompactNode(lead); err != nil {
		t.Fatal(err)
	}
	proposeOrFatal(t, e, lead, "x", "z")
	if c := e.CommitIndex(lead); c <= c1 {
		t.Fatalf("快照后提交位置未增长: %d -> %d", c1, c)
	}
}
