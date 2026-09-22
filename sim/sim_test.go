package sim

import (
	"bytes"
	"testing"

	"github.com/xingjianluo/replicated-log-go/raft"
)

// newTestEnv 构造一个三节点集群并选出领导者；日志写入缓冲区，
// 失败时随测试结果一并输出，使任期/多数派/提交位置判定可追溯。
func newTestEnv(t *testing.T, seed int64) (*Env, *bytes.Buffer, []raft.NodeID) {
	t.Helper()
	buf := &bytes.Buffer{}
	log := NewLogger(buf, true)
	e := NewEnv(log, seed)
	ids := []raft.NodeID{"n1", "n2", "n3"}
	e.Bootstrap(ids)
	if !e.RunUntilLeader(300) {
		t.Fatalf("种子 %d 下未选出领导者\n%s", seed, buf.String())
	}
	return e, buf, ids
}

func proposeOrFatal(t *testing.T, e *Env, lead raft.NodeID, key, val string) {
	t.Helper()
	if err := e.Propose(lead, key, val); err != nil {
		t.Fatalf("提案 %s=%s 失败: %v", key, val, err)
	}
	e.Run(20)
}

func assertKV(t *testing.T, e *Env, ids []raft.NodeID, key, want string) {
	t.Helper()
	for _, id := range ids {
		v, ok := e.Get(id, key)
		if !ok || v != want {
			t.Fatalf("节点 %s 键 %s = %q(ok=%v)，期望 %q", id, key, v, ok, want)
		}
	}
}

// TestBasicReplication 是基线：选主、连续提案、全部节点按序收敛。
func TestBasicReplication(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		e, _, ids := newTestEnv(t, seed)
		lead := e.Leader()
		for i := range 10 {
			proposeOrFatal(t, e, lead, "k", string(rune('a'+i)))
		}
		n, val, ok := e.LogMatch("k")
		if !ok || n != 3 || val != "j" {
			t.Fatalf("seed=%d 未收敛: n=%d val=%q ok=%v", seed, n, val, ok)
		}
		assertKV(t, e, ids, "k", "j")
	}
}

// TestMinorityPartitionCannotCommit 少数派分区不得自行推进提交：
// 隔离少数派（1 节点），其中的陈旧领导者提案必须失败/无法提交；
// 多数派重新选主并提交后，恢复网络，少数派回滚冲突并收敛。
func TestMinorityPartitionCannotCommit(t *testing.T) {
	e, _, _ := newTestEnv(t, 42)
	lead := e.Leader()

	// 找到领导者，再挑一个非领导者做少数派；若隔离非领导者，
	// 还要把领导者也放进少数派才能验证“少数派无法提交”。
	others := []raft.NodeID{}
	for _, x := range []raft.NodeID{"n1", "n2", "n3"} {
		if x != lead {
			others = append(others, x)
		}
	}
	minorityLead := lead
	majorityNode := others[0]
	// 分区：领导者单独一组，另外两台一组。
	e.Network().IsolateNode(lead)
	e.Run(30) // 多数派侧选出新领导者；旧领导者处于少数派

	// 旧领导者在分区期间的提案：客户端立即收到 not-leader/无法提交的反馈。
	err := e.Propose(minorityLead, "stale", "x")
	if err == nil {
		// 即便本节点仍自认为领导者并受理，该条目也绝不能在集群层面提交。
		e.Run(20)
		for _, id := range []raft.NodeID{majorityNode, others[1]} {
			if v, ok := e.Get(id, "stale"); ok {
				t.Fatalf("少数派提案泄漏到多数派节点 %s: %q", id, v)
			}
		}
		if c := e.CommitIndex(minorityLead); c >= uint64(0) {
			// 检查少数派本地提交不能推进到包含该提案：stale 提案位置
			// 一定高于分区时刻的集群提交位置。
			majorityCommit := e.CommitIndex(majorityNode)
			if e.Status(minorityLead).Commit > majorityCommit {
				t.Fatalf("少数派提交位置 %d 超过多数派 %d",
					e.Status(minorityLead).Commit, majorityCommit)
			}
		}
	} else {
		// 旧领导者已因更高任期下台，返回 ErrNotLeader——失败原因可区分。
		if err != raft.ErrNotLeader {
			t.Fatalf("期望 ErrNotLeader，得到 %v", err)
		}
	}

	// 多数派正常提交新值。
	newLead := e.Leader()
	if newLead == "" || newLead == lead {
		t.Fatalf("多数派未产生新的领导者，leader=%q", newLead)
	}
	proposeOrFatal(t, e, newLead, "safe", "committed")

	// 恢复网络：旧领导者回滚未提交冲突条目，追平多数派。
	e.Network().HealNode(lead)
	e.Run(40)

	if final := e.Leader(); final != newLead && e.Status(final).Term < e.Status(newLead).Term {
		t.Fatalf("恢复后领导者任期异常")
	}
	n, val, ok := e.LogMatch("safe")
	if !ok || n != 3 || val != "committed" {
		t.Fatalf("网络恢复后未收敛 safe=committed: n=%d val=%q ok=%v", n, val, ok)
	}
	if v, exists := e.Get(lead, "stale"); exists && v == "x" {
		t.Fatalf("已隔离的未提交条目 stale=x 在恢复后仍残留在旧领导者上")
	}
}
