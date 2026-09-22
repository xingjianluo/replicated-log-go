package raft

import "testing"

// TestAddNodeThenCommit 3 节点增加第 4 个投票节点：
// 新节点先以空日志加入，ConfAddVoter 提交后按 4 节点多数派(3)工作，
// 新节点通过日志追赶获得全部已提交状态。
func TestAddNodeThenCommit(t *testing.T) {
	c := mustCluster(t, 71, "n1", "n2", "n3")
	// 先有已提交数据。
	idx, _ := c.Propose("cl", []byte("before-add"))
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}

	if err := c.AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	cfgIdx, r := c.ProposeConfChange("cl", ConfChange{Op: ConfAddVoter, Node: "n4"})
	if r != ReasonOK {
		t.Fatalf("成员变更被拒: %s", r)
	}
	if err := c.WaitCommit(cfgIdx, 300); err != nil {
		t.Fatal(err)
	}
	c.RunUntil(20)

	// 新节点配置必须生效且包含已提交数据（经日志重放，若需要也可经快照）。
	s4 := c.State("n4")
	if !containsID(s4.Members, "n4") {
		t.Fatalf("n4 配置未生效: %v", s4.Members)
	}
	assertAllCommands(t, c, []string{"before-add"}, "n1", "n2", "n3", "n4")

	// 此后按 4 节点多数派提交。
	idx2, _ := c.Propose("cl", []byte("after-add"))
	if err := c.WaitCommit(idx2, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"before-add", "after-add"}, "n1", "n2", "n3", "n4")
	t.Logf("扩容完成：成员=%v 多数派=%d", s4.Members, NewMembership(s4.Members...).Quorum())
}

// TestRemoveNodeThenCommit 5 节点移除一个投票节点后按 4 节点多数派继续工作；
// 被移除节点停止参与，但其已提交数据在其余节点完好保留。
func TestRemoveNodeThenCommit(t *testing.T) {
	c := mustCluster(t, 72, "n1", "n2", "n3", "n4", "n5")
	idx, _ := c.Propose("cl", []byte("keep-me"))
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	cfgIdx, r := c.ProposeConfChange("cl", ConfChange{Op: ConfRemoveVoter, Node: "n5"})
	if r != ReasonOK {
		t.Fatalf("移除成员被拒: %s", r)
	}
	if err := c.WaitCommit(cfgIdx, 300); err != nil {
		t.Fatal(err)
	}
	// n5 标记为 removed，不再处理协议消息；从编排移除。
	if !c.nodes["n5"].removed {
		t.Fatal("被移除节点应标记 removed")
	}
	c.RemoveNode("n5")

	// 停掉另一个节点，4 节点集群的多数派=3 仍可提交。
	c.StopNode("n4")
	c.RunUntil(20)
	idx2, r2 := c.Propose("cl", []byte("after-remove"))
	if r2 != ReasonOK {
		t.Fatalf("缩容后多数派提交失败: %s", r2)
	}
	if err := c.WaitCommit(idx2, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"keep-me", "after-remove"}, "n1", "n2", "n3")
	t.Logf("缩容后 4 节点（存活3）多数派=3 提交成功，已提交数据无丢失")
}

// TestPendingConfChangeBlocks 上一次成员变更未提交前，拒绝下一次。
func TestPendingConfChangeBlocks(t *testing.T) {
	c := mustCluster(t, 73, "n1", "n2", "n3")
	if err := c.AddNode("x9"); err != nil {
		t.Fatal(err)
	}
	// 第一次变更落盘但先不等待提交，立即发第二次，应被 pending 阻塞。
	_, r1 := c.ProposeConfChange("cl", ConfChange{Op: ConfAddVoter, Node: "x9"})
	if r1 != ReasonOK {
		t.Fatal(r1)
	}
	// 从非领导者视角再发起将得到 not-leader；直接在领导者上验证 pending。
	ack := c.nodes[c.Leader()].ProposeConfChange("cl", ConfChange{Op: ConfAddVoter, Node: "x8"})
	if ack.Reason != ReasonMembershipPending {
		t.Fatalf("期望 membership-pending，实际 %s", ack.Reason)
	}
}

// TestRemoveUnknownVoterRejected 移除不存在的成员/重复添加必须被显式拒绝。
func TestRemoveUnknownVoterRejected(t *testing.T) {
	c := mustCluster(t, 74, "n1", "n2", "n3")
	ack := c.nodes[c.Leader()].ProposeConfChange("cl", ConfChange{Op: ConfRemoveVoter, Node: "ghost"})
	if ack.Reason != ReasonUnknownVoter {
		t.Fatalf("期望 unknown-voter，实际 %s", ack.Reason)
	}
	if err := c.AddNode("dup"); err != nil {
		t.Fatal(err)
	}
	cfgIdx, _ := c.ProposeConfChange("cl", ConfChange{Op: ConfAddVoter, Node: "dup"})
	if err := c.WaitCommit(cfgIdx, 200); err != nil {
		t.Fatal(err)
	}
	ack2 := c.nodes[c.Leader()].ProposeConfChange("cl", ConfChange{Op: ConfAddVoter, Node: "dup"})
	if ack2.Reason != ReasonMembershipPending && ack2.Reason != ReasonUnknownVoter {
		// 提交后再次添加同节点：应得到 unknown-voter（已存在）；未提交窗口内为 pending。
		t.Fatalf("重复添加应被拒绝，实际 %s", ack2.Reason)
	}
}

func containsID(ids []NodeID, want NodeID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
