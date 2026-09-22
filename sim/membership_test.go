package sim

import (
	"testing"

	"github.com/xingjianluo/replicated-log-go/raft"
)

// TestAddNodeThenCommitWithNewQuorum 新增节点后按新的 4 节点多数派(3)提交，
// 移除节点后再按 3 节点多数派(2)提交；被移除节点不能造成已提交数据丢失，
// 也不能继续影响多数派。
func TestAddRemoveNodeAndQuorum(t *testing.T) {
	e, _, _ := newTestEnv(t, 21)
	lead := e.Leader()
	proposeOrFatal(t, e, lead, "before", "data")

	// 1) 准备空白新节点并提交加入变更。
	e.PrepareNewNode("n4")
	if err := e.ProposeConfChange(lead, raft.ConfChange{Type: raft.ConfAddNode, Node: "n4"}); err != nil {
		t.Fatalf("添加 n4 失败: %v", err)
	}
	e.Run(40)

	conf := e.Status(lead).Config
	if len(conf.Voters) != 4 {
		t.Fatalf("加入后投票集大小=%d，期望4: %v", len(conf.Voters), conf.Voters)
	}
	// n4 必须追平已有数据，而不是空日志加入。
	if v, ok := e.Get("n4", "before"); !ok || v != "data" {
		t.Fatalf("新节点 n4 缺少加入前已提交数据 before=data, got %q ok=%v", v, ok)
	}

	// 2) 新多数派约束：4 节点需要 3 票。隔离两个（含原任一追随者），
	//    只剩 2 票的一侧无法提交。
	e.Network().IsolateNode("n2")
	e.Network().IsolateNode("n4")
	// 若当前领导者恰好在被隔离侧，等待多数派重新选主（同样验证新配置生效）。
	e.Run(40)
	var availLead raft.NodeID
	for _, id := range []raft.NodeID{lead, "n1", "n3"} {
		if id != "n2" && id != "n4" && e.IsLeader(id) {
			availLead = id
		}
	}
	// n1/n3 两台在线但被隔离的 n2/n4 缺席：4 节点配置下只有 2 票，不能提交。
	err := e.Propose(availLead, "blocked", "x")
	if err != nil {
		if err != raft.ErrNotLeader && err != raft.ErrClusterUnavailable && err != raft.ErrChangePending {
			t.Fatalf("意外错误: %v", err)
		}
	}
	e.Run(30)
	for _, id := range []raft.NodeID{"n1", "n3", "n2", "n4"} {
		if v, ok := e.Get(id, "blocked"); ok && v == "x" {
			t.Fatalf("不足新配置多数派时条目竟然在 %s 上生效", id)
		}
	}

	// 3) 恢复网络后提交；再移除一个节点，验证缩容规则。
	e.Network().HealAll()
	e.Run(40)
	lead = e.Leader()
	proposeOrFatal(t, e, lead, "added-era", "v")

	if err := e.ProposeConfChange(lead, raft.ConfChange{Type: raft.ConfRemoveNode, Node: "n2"}); err != nil {
		t.Fatalf("移除 n2 失败: %v", err)
	}
	e.Run(40)
	conf = e.Status(lead).Config
	if len(conf.Voters) != 3 {
		t.Fatalf("移除后投票集大小=%d，期望3: %v", len(conf.Voters), conf.Voters)
	}

	// 被移除节点下线后，剩余 3 节点仍以 2 票多数派正常提交，历史数据不丢。
	e.Network().IsolateNode("n2")
	e.Run(40)
	curLead := e.Leader()
	proposeOrFatal(t, e, curLead, "after-remove", "ok")

	remaining := []raft.NodeID{"n1", "n3", "n4"}
	for _, id := range remaining {
		for k, want := range map[string]string{"before": "data", "added-era": "v", "after-remove": "ok"} {
			v, ok := e.Get(id, k)
			if !ok || v != want {
				t.Fatalf("缩容后节点 %s 上 %s=%q(ok=%v)，期望 %q（已提交数据不得丢失）",
					id, k, v, ok, want)
			}
		}
	}
}

// TestConfChangeRejectsInvalid 成员变更的失败原因必须可区分：
// 非领导者、空操作、在途变更。
func TestConfChangeRejectsInvalid(t *testing.T) {
	e, _, ids := newTestEnv(t, 22)
	lead := e.Leader()
	var follower raft.NodeID
	for _, id := range ids {
		if id != lead {
			follower = id
		}
	}
	if err := e.ProposeConfChange(follower, raft.ConfChange{Type: raft.ConfAddNode, Node: "x"}); err != raft.ErrNotLeader {
		t.Fatalf("跟随者变更期望 ErrNotLeader，得到 %v", err)
	}
	if err := e.ProposeConfChange(lead, raft.ConfChange{Type: raft.ConfAddNode, Node: ids[0]}); err != raft.ErrNoChange {
		t.Fatalf("重复添加期望 ErrNoChange，得到 %v", err)
	}
	if err := e.ProposeConfChange(lead, raft.ConfChange{Type: raft.ConfRemoveNode, Node: "ghost"}); err != raft.ErrNoChange {
		t.Fatalf("移除不存在节点期望 ErrNoChange，得到 %v", err)
	}
}
