package raft

import "testing"

// TestBatchAddTwoVoters 一次批量加入两个投票节点：
// 两者先以 learner 追平，enter/leave-joint 提交后全部 5 个节点收敛到同一份成员表。
func TestBatchAddTwoVoters(t *testing.T) {
	c := mustCluster(t, 81, "n1", "n2", "n3")
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
	// 生效前：n4/n5 是无投票权 learner。
	if !c.State("n4").Learner || !c.State("n5").Learner {
		t.Fatalf("新增节点生效前应为 learner: n4=%v n5=%v", c.State("n4").Learner, c.State("n5").Learner)
	}

	cfgIdx, r := c.ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"n4", "n5"}})
	if r != ReasonOK {
		t.Fatalf("批量加入被拒: %s", r)
	}
	want := []NodeID{"n1", "n2", "n3", "n4", "n5"}
	if err := c.WaitMembershipConverged(want, 400); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitCommit(cfgIdx, 200); err != nil {
		t.Fatal(err)
	}

	for _, id := range want {
		s := c.State(id)
		if s.Joint {
			t.Fatalf("%s 变更完成后仍处于 joint 状态", id)
		}
		if s.Learner {
			t.Fatalf("%s 变更完成后不应再是 learner", id)
		}
	}
	assertAllCommands(t, c, []string{"before-batch-add"}, want...)

	// 5 节点多数派=3：停 2 个仍可提交。
	c.StopNode("n4")
	c.StopNode("n5")
	c.RunUntil(30)
	idx2, r2 := c.Propose("cl", []byte("after-batch-add"))
	if r2 != ReasonOK {
		t.Fatalf("批量扩容后提交失败: %s", r2)
	}
	if err := c.WaitCommit(idx2, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"before-batch-add", "after-batch-add"}, "n1", "n2", "n3")
	t.Logf("批量扩容完成：%v 多数派=%d", want, NewMembership(want...).Quorum())
}

// TestBatchRemoveTwoVoters 5 节点一次批量移除两个投票节点：
// 联合阶段被移除者仍凭 C_old 身份参与双多数派；leave-joint 提交后它们退出，
// 剩余 3 节点收敛到同一份成员表并继续正常提交。
func TestBatchRemoveTwoVoters(t *testing.T) {
	c := mustCluster(t, 82, "n1", "n2", "n3", "n4", "n5")
	idx, _ := c.Propose("cl", []byte("before-batch-remove"))
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	cfgIdx, r := c.ProposeBatchConfChange("cl", BatchConfChange{Removes: []NodeID{"n4", "n5"}})
	if r != ReasonOK {
		t.Fatalf("批量移除被拒: %s", r)
	}
	want := []NodeID{"n1", "n2", "n3"}
	if err := c.WaitMembershipConverged(want, 400); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitCommit(cfgIdx, 200); err != nil {
		t.Fatal(err)
	}
	if !c.nodes["n4"].removed || !c.nodes["n5"].removed {
		t.Fatal("被批量移除的节点应标记 removed")
	}
	c.RemoveNode("n4")
	c.RemoveNode("n5")

	assertAllCommands(t, c, []string{"before-batch-remove"}, "n1", "n2", "n3")
	// 剩余 3 节点停 1 个，多数派=2 仍可提交。
	c.StopNode("n3")
	c.RunUntil(30)
	idx2, r2 := c.Propose("cl", []byte("after-batch-remove"))
	if r2 != ReasonOK {
		t.Fatalf("批量缩容后提交失败: %s", r2)
	}
	if err := c.WaitCommit(idx2, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"before-batch-remove", "after-batch-remove"}, "n1", "n2")
	t.Logf("批量缩容完成：%v 多数派=%d", want, NewMembership(want...).Quorum())
}

// TestBatchMixedAddAndRemove 同一次批量变更里既加又删：
// 4 节点 {n1..n4} 一次变成 {n1,n2,n3,n6}（删 n4、加 n6），必须原子生效。
func TestBatchMixedAddAndRemove(t *testing.T) {
	c := mustCluster(t, 83, "n1", "n2", "n3", "n4")
	idx, _ := c.Propose("cl", []byte("before-mixed"))
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNode("n6"); err != nil {
		t.Fatal(err)
	}
	cfgIdx, r := c.ProposeBatchConfChange("cl", BatchConfChange{
		Adds:    []NodeID{"n6"},
		Removes: []NodeID{"n4"},
	})
	if r != ReasonOK {
		t.Fatalf("混合批量变更被拒: %s", r)
	}
	want := []NodeID{"n1", "n2", "n3", "n6"}
	if err := c.WaitMembershipConverged(want, 400); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitCommit(cfgIdx, 200); err != nil {
		t.Fatal(err)
	}
	if !c.nodes["n4"].removed {
		t.Fatal("n4 应在混合变更后被移除")
	}
	if s := c.State("n6"); s.Learner || s.Joint || !containsID(s.Members, "n6") {
		t.Fatalf("n6 状态不正确: %+v", s)
	}
	c.RemoveNode("n4")
	c.RunUntil(10)
	assertAllCommands(t, c, []string{"before-mixed"}, "n1", "n2", "n3", "n6")

	// 新 4 节点集群继续提交，新节点 n6 参与。
	idx2, r2 := c.Propose("cl", []byte("after-mixed"))
	if r2 != ReasonOK {
		t.Fatalf("混合变更后提交失败: %s", r2)
	}
	if err := c.WaitCommit(idx2, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"before-mixed", "after-mixed"}, "n1", "n2", "n3", "n6")
	t.Logf("混合批量变更完成：%v", want)
}
