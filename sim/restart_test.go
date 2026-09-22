package sim

import (
	"testing"

	"github.com/xingjianluo/replicated-log-go/raft"
)

// TestNodeRestartReplaysCommittedLog 节点宕机重启后：
// 磁盘保留任期/日志/快照，内存状态机重建，已提交数据不得丢失。
func TestNodeRestartReplaysCommittedLog(t *testing.T) {
	e, _, ids := newTestEnv(t, 7)
	lead := e.Leader()
	proposeOrFatal(t, e, lead, "a", "1")
	proposeOrFatal(t, e, lead, "b", "2")

	// 选择一个非领导者作为宕机受害者（保证宕机期间集群仍可提交）。
	var victim raft.NodeID
	for _, id := range ids {
		if id != lead {
			victim = id
			break
		}
	}
	before := e.CommitIndex(victim)
	e.StopNode(victim)
	e.Run(10)

	// 宕机期间集群继续提交。
	proposeOrFatal(t, e, lead, "c", "3")
	proposeOrFatal(t, e, lead, "a", "updated")

	e.StartNode(victim)
	e.Run(40) // 重启者以跟随者身份追平日志

	after := e.CommitIndex(victim)
	if after < before {
		t.Fatalf("重启后提交位置回退: %d -> %d", before, after)
	}
	n, val, ok := e.LogMatch("a")
	if !ok || n != 3 || val != "updated" {
		t.Fatalf("重启恢复后未收敛 a=updated: n=%d val=%q ok=%v", n, val, ok)
	}
	for _, k := range [][2]string{{"b", "2"}, {"c", "3"}} {
		v, has := e.Get(victim, k[0])
		if !has || v != k[1] {
			t.Fatalf("重启节点 %s 缺少已提交数据 %s=%s (got %q)", victim, k[0], k[1], v)
		}
	}
}

// TestDuplicateAndReorderedMessages 高重复率 + 全延迟抖动制造重复/乱序：
// 所有提案仍只能按序、幂等地收敛，冲突检测不得误伤正常条目。
func TestDuplicateAndReorderedMessages(t *testing.T) {
	e, _, ids := newTestEnv(t, 99)
	for _, id := range ids {
		e.Network().SetDuplicateRate(id, 0.7)
	}
	e.Network().SetDelayRange(0, 6) // 宽延迟区间 + 重复副本独立延迟 => 乱序

	lead := e.Leader()
	for i := range 8 {
		proposeOrFatal(t, e, lead, "seq", string(rune('0'+i)))
	}
	n, val, ok := e.LogMatch("seq")
	if !ok || n != 3 || val != "7" {
		t.Fatalf("重复/乱序下未正确收敛 seq=7: n=%d val=%q ok=%v", n, val, ok)
	}
	if e.Network().Stats().Duplicated == 0 {
		t.Fatalf("期望发生消息重复，但统计为 0（测试未真正覆盖该故障）")
	}
	// 所有节点的提交位置必须一致（重复消息幂等，未触发错误截断）。
	c := e.CommitIndex(ids[0])
	for _, id := range ids[1:] {
		if e.CommitIndex(id) != c {
			t.Fatalf("提交位置不一致: %s=%d vs %s=%d", ids[0], c, id, e.CommitIndex(id))
		}
	}
}

// TestLeaderSwitchDoesNotRollbackCommitted 领导者切换后，
// 前任任期已提交的条目在新任期不得丢失或回滚。
func TestLeaderSwitchDoesNotRollbackCommitted(t *testing.T) {
	e, _, _ := newTestEnv(t, 3)
	lead1 := e.Leader()
	proposeOrFatal(t, e, lead1, "term1", "keep")
	term1 := e.Status(lead1).Term

	// 隔离现任领导者，多数派选出更高任期领导者。
	e.Network().IsolateNode(lead1)
	e.Run(40)
	var lead2 raft.NodeID
	for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
		if id == lead1 {
			continue
		}
		if e.IsLeader(id) {
			lead2 = id
		}
	}
	if lead2 == "" {
		t.Fatalf("多数派未选出替代领导者")
	}
	if e.Status(lead2).Term <= term1 {
		t.Fatalf("新领导者任期未递增: %d <= %d", e.Status(lead2).Term, term1)
	}
	proposeOrFatal(t, e, lead2, "term2", "new")

	// 旧领导者恢复：带着未提交的本地提案追赶，必须接受新日志并保留旧提交。
	e.Network().HealNode(lead1)
	e.Run(50)

	n, v, ok := e.LogMatch("term1")
	if !ok || n != 3 || v != "keep" {
		t.Fatalf("领导者切换后已提交条目丢失: n=%d val=%q ok=%v", n, v, ok)
	}
	n, v, ok = e.LogMatch("term2")
	if !ok || n != 3 || v != "new" {
		t.Fatalf("新任期条目未收敛: n=%d val=%q ok=%v", n, v, ok)
	}
}
