package raft

import "testing"

// TestBatchAddLearnerRecoversViaSnapshot 新增 learner 落后较多、必须经快照
// 追赶后，再随批量变更加入；快照路径携带的配置必须正确，加入后参与多数派。
func TestBatchAddLearnerRecoversViaSnapshot(t *testing.T) {
	c := mustCluster(t, 111, "n1", "n2", "n3")
	// 先积累足够多已提交命令。
	for i := 0; i < 8; i++ {
		idx, _ := c.Propose("cl", []byte("cmd-"+itoa(i)))
		if err := c.WaitCommit(idx, 200); err != nil {
			t.Fatal(err)
		}
	}
	leader := c.Leader()
	base := c.nodes[leader].CommitIndex()
	// 领导者做快照压缩旧日志。
	if err := c.TriggerSnapshot(leader, base); err != nil {
		t.Fatal(err)
	}
	// 现在加入 learner，它只能通过快照 + 快照后日志追赶。
	if err := c.AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNode("n5"); err != nil {
		t.Fatal(err)
	}
	// 继续产生命令，迫使新节点走快照恢复。
	for i := 0; i < 4; i++ {
		idx, _ := c.Propose("cl", []byte("post-snap-"+itoa(i)))
		if err := c.WaitCommit(idx, 200); err != nil {
			t.Fatal(err)
		}
	}
	// 批量加入两个经快照追赶的节点。
	cfgIdx, r := c.ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"n4", "n5"}})
	if r != ReasonOK {
		t.Fatalf("批量加入被拒: %s", r)
	}
	want := []NodeID{"n1", "n2", "n3", "n4", "n5"}
	if err := c.WaitMembershipConverged(want, 400); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitCommit(cfgIdx, 300); err != nil {
		t.Fatal(err)
	}
	// 两节点必须拥有全部已提交命令（经快照恢复 + 日志追赶）。
	var cmds []string
	for i := 0; i < 8; i++ {
		cmds = append(cmds, "cmd-"+itoa(i))
	}
	for i := 0; i < 4; i++ {
		cmds = append(cmds, "post-snap-"+itoa(i))
	}
	assertAllCommands(t, c, cmds, "n4", "n5")
	t.Logf("learner 经快照追平后批量加入成功，命令数=%d", len(cmds))
}

// TestSnapshotDuringJointCommitted 已提交的快照不会落在未提交联合中间态，
// 且联合结束后快照中的配置为最终 C_new。
func TestSnapshotDuringJointCommitted(t *testing.T) {
	c := mustCluster(t, 112, "n1", "n2", "n3", "n4", "n5")
	idx, _ := c.Propose("cl", []byte("x"))
	c.WaitCommit(idx, 200)
	cfg, r := c.ProposeBatchConfChange("cl", BatchConfChange{Removes: []NodeID{"n4", "n5"}})
	if r != ReasonOK {
		t.Fatal(r)
	}
	want := []NodeID{"n1", "n2", "n3"}
	if err := c.WaitMembershipConverged(want, 400); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitCommit(cfg, 300); err != nil {
		t.Fatal(err)
	}
	c.RunUntil(10)
	// 联合落定后在提交点做快照，快照配置应为最终 3 节点且非 joint。
	leader := c.Leader()
	if err := c.TriggerSnapshot(leader, 0); err != nil {
		t.Fatal(err)
	}
	snap := c.stores[leader].SnapshotCopy()
	if snap.JointValid {
		t.Fatal("落定后的快照不应标记 joint")
	}
	if !snap.Membership.Equal(NewMembership(want...)) {
		t.Fatalf("快照配置错误: %v", snap.Membership.List())
	}
	t.Logf("联合落定后快照配置正确：%v", snap.Membership.List())
}
