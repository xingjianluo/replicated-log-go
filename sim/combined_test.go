package sim

import (
	"testing"

	"github.com/xingjianluo/replicated-log-go/raft"
)

// TestCombinedPartitionRestartDuplicates 复合故障：
// 分区两侧各自演化 + 一侧节点重启 + 全链路消息重复，
// 网络恢复后所有存活节点必须重新收敛到同一条日志。
func TestCombinedPartitionRestartDuplicates(t *testing.T) {
	for seed := int64(100); seed <= 104; seed++ {
		e, _, _ := newTestEnv(t, seed)
		for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
			e.Network().SetDuplicateRate(id, 0.5)
		}
		lead := e.Leader()
		proposeOrFatal(t, e, lead, "base", "0")

		// 1/2 分区：领导者一侧 vs 另一节点一侧（领导者可能在任一侧）。
		var isolated raft.NodeID
		for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
			if id != lead {
				isolated = id
				break
			}
		}
		e.Network().IsolateNode(isolated)
		e.Run(30)

		// 多数派（领导者 + 另一个跟随者）继续提交。
		var majorityLead raft.NodeID
		for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
			if id != isolated && e.IsLeader(id) {
				majorityLead = id
			}
		}
		if majorityLead == "" {
			t.Fatalf("seed=%d 多数派侧无领导者", seed)
		}
		proposeOrFatal(t, e, majorityLead, "base", "majority-side")

		// 多数派侧的跟随者在分区期间重启（磁盘保留）。
		var restartTarget raft.NodeID
		for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
			if id != isolated && id != majorityLead {
				restartTarget = id
			}
		}
		e.StopNode(restartTarget)
		e.Run(5)
		proposeOrFatal(t, e, majorityLead, "during-restart", "committed")
		e.StartNode(restartTarget)
		e.Run(30)

		// 被隔离节点在分区期间也可能重启。
		e.StopNode(isolated)
		e.Run(5)
		e.StartNode(isolated)
		e.Run(10)

		// 恢复网络，旧分区日志被显式修正，全部收敛。
		e.Network().HealNode(isolated)
		e.Run(250)

		alive := []raft.NodeID{"n1", "n2", "n3"}
		for key, want := range map[string]string{
			"base":           "majority-side",
			"during-restart": "committed",
		} {
			n, v, ok := e.LogMatchAmong(key, alive)
			if !ok || n != 3 || v != want {
				t.Fatalf("seed=%d 复合故障后 %s 未收敛: n=%d val=%q ok=%v",
					seed, key, n, v, ok)
			}
		}
		// 所有存活节点的 commit/lastIndex 必须完全一致（冲突显式修正的证据）。
		var c, li uint64
		first := true
		for _, id := range alive {
			st := e.Status(id)
			if first {
				c, li, first = st.Commit, st.LastIndex, false
				continue
			}
			if st.Commit != c || st.LastIndex != li {
				t.Fatalf("seed=%d 恢复后日志位置不一致: %s=(%d,%d) 基准=(%d,%d)",
					seed, id, st.Commit, st.LastIndex, c, li)
			}
		}
	}
}
