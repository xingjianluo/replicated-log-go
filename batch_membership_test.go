package raft

import "testing"

// TestBatchAddTwoVoters 一次打包加入两台机器：两阶段联合共识落定后，
// 所有存活节点收敛到同一份 5 节点成员表，并按 5 节点多数派(3)继续提交。
func TestBatchAddTwoVoters(t *testing.T) {
	c := mustCluster(t, 101, "n1", "n2", "n3")
	idx, _ := c.Propose("cl", []byte("before-batch-add"))
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNode("n5"); err != nil {
		t.Fatal(err)
	}
	enterIdx, r := c.ProposeConfChangeBatch("cl", []NodeID{"n4", "n5"}, nil)
	if r != ReasonOK {
		t.Fatalf("批量加人被拒: %s", r)
	}

	// 变更进行中：普通日志必须能继续提交，不被成员变更卡住。
	midIdx, r := c.Propose("cl", []byte("during-batch-add"))
	if r != ReasonOK {
		t.Fatalf("变更期间普通提案被拒: %s", r)
	}

	want := []NodeID{"n1", "n2", "n3", "n4", "n5"}
	if err := c.WaitMembership(want, 400); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitCommit(midIdx, 200); err != nil {
		t.Fatalf("变更期间命令最终未提交: %v", err)
	}
	// enter-joint 之后的普通日志必须确实进入提交。
	if err := c.WaitCommit(enterIdx, 200); err != nil {
		t.Fatal(err)
	}
	c.RunUntil(10)

	for _, id := range want {
		s := c.State(id)
		if s.Joint || s.Pending {
			t.Fatalf("%s 变更后仍处于 joint/pending: %+v", id, s)
		}
		if !NewMembership(s.Members...).Equal(NewMembership(want...)) {
			t.Fatalf("%s 成员表不符: %v", id, s.Members)
		}
	}
	assertAllCommands(t, c, []string{"before-batch-add", "during-batch-add"}, want...)

	// 落定后按 5 节点多数派继续工作。
	idx2, r := c.Propose("cl", []byte("after-batch-add"))
	if r != ReasonOK {
		t.Fatal(r)
	}
	if err := c.WaitCommit(idx2, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c,
		[]string{"before-batch-add", "during-batch-add", "after-batch-add"}, want...)
}

// TestBatchRemoveTwoVoters 一次打包移除两台机器：5 -> 3，
// 落定后按 3 节点多数派(2)继续服务，被移除节点停止参与且已提交数据保留。
func TestBatchRemoveTwoVoters(t *testing.T) {
	c := mustCluster(t, 102, "n1", "n2", "n3", "n4", "n5")
	idx, _ := c.Propose("cl", []byte("keep-all"))
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	if _, r := c.ProposeConfChangeBatch("cl", nil, []NodeID{"n4", "n5"}); r != ReasonOK {
		t.Fatalf("批量删人被拒: %s", r)
	}
	want := []NodeID{"n1", "n2", "n3"}
	if err := c.WaitMembership(want, 400); err != nil {
		t.Fatal(err)
	}
	if !c.nodes["n4"].removed || !c.nodes["n5"].removed {
		t.Fatal("被批量移除的节点都应标记 removed")
	}
	c.RemoveNode("n4")
	c.RemoveNode("n5")

	idx2, r := c.Propose("cl", []byte("after-batch-remove"))
	if r != ReasonOK {
		t.Fatalf("缩容后提交失败: %s", r)
	}
	if err := c.WaitCommit(idx2, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"keep-all", "after-batch-remove"}, want...)
}

// TestBatchMixedAddAndRemove 一次提交里既加又删：3 节点集群移除 n2/n3、加入 n4/n5，
// 目标配置为 {n1,n4,n5}；全程成员表原子切换，落定后所有存活节点一致。
func TestBatchMixedAddAndRemove(t *testing.T) {
	c := mustCluster(t, 103, "n1", "n2", "n3")
	idx, _ := c.Propose("cl", []byte("base"))
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNode("n5"); err != nil {
		t.Fatal(err)
	}
	if _, r := c.ProposeConfChangeBatch("cl", []NodeID{"n4", "n5"}, []NodeID{"n2", "n3"}); r != ReasonOK {
		t.Fatalf("混合变更被拒: %s", r)
	}
	want := []NodeID{"n1", "n4", "n5"}
	if err := c.WaitMembership(want, 400); err != nil {
		t.Fatal(err)
	}
	c.RunUntil(10)
	if !c.nodes["n2"].removed || !c.nodes["n3"].removed {
		t.Fatal("被移除节点应标记 removed")
	}
	for _, id := range want {
		s := c.State(id)
		if s.Joint || s.Pending || !NewMembership(s.Members...).Equal(NewMembership(want...)) {
			t.Fatalf("%s 混合变更后状态错误: %+v", id, s)
		}
	}
	assertAllCommands(t, c, []string{"base"}, want...)

	// 新配置下继续提交。
	idx2, r := c.Propose("cl", []byte("post-mixed"))
	if r != ReasonOK {
		t.Fatal(r)
	}
	if err := c.WaitCommit(idx2, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"base", "post-mixed"}, want...)
}

// TestBatchChangeValidation 非法批量变更必须被显式拒绝。
func TestBatchChangeValidation(t *testing.T) {
	c := mustCluster(t, 104, "n1", "n2", "n3")
	leader := c.nodes[c.Leader()]

	// 空批次。
	if ack := leader.ProposeConfChangeBatch("cl", ConfChange{Op: ConfBatchVoters}); ack.Reason != ReasonBadConfChange {
		t.Fatalf("空批次应 bad-conf-change，实际 %s", ack.Reason)
	}
	// 同一批又加又删同一节点。
	ack := leader.ProposeConfChangeBatch("cl", ConfChange{
		Op: ConfBatchVoters, Add: []NodeID{"x"}, Remove: []NodeID{"x"},
	})
	if ack.Reason != ReasonBadConfChange {
		t.Fatalf("同节点又加又删应 bad-conf-change，实际 %s", ack.Reason)
	}
	// 删除不存在的成员。
	ack = leader.ProposeConfChangeBatch("cl", ConfChange{
		Op: ConfBatchVoters, Remove: []NodeID{"ghost"},
	})
	if ack.Reason != ReasonUnknownVoter {
		t.Fatalf("删除不存在成员应 unknown-voter，实际 %s", ack.Reason)
	}
	// 加入已存在成员。
	ack = leader.ProposeConfChangeBatch("cl", ConfChange{
		Op: ConfBatchVoters, Add: []NodeID{"n1"},
	})
	if ack.Reason != ReasonUnknownVoter {
		t.Fatalf("重复加入应 unknown-voter，实际 %s", ack.Reason)
	}
	// 把集群删空。
	ack = leader.ProposeConfChangeBatch("cl", ConfChange{
		Op: ConfBatchVoters, Remove: []NodeID{"n1", "n2", "n3"},
	})
	if ack.Reason != ReasonBadConfChange {
		t.Fatalf("删空集群应 bad-conf-change，实际 %s", ack.Reason)
	}
}
