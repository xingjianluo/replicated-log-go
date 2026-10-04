package raft

import "testing"

// 本文件覆盖“客户端命令内容恰好等于内部空操作占位文本”的回归场景。
//
// 背景：集群在推进未完成的联合共识变更时，会由领导者自动追加内部空操作
// 条目（KindNoop，负载文本为 "joint-noop"），用于以当前任期凑齐双多数派。
// 内部条目靠“条目类型”与客户端命令区分，绝不看负载内容；因此客户端提交
// 任何内容（包括与占位文本完全相同的 "joint-noop"）都必须正常落盘、提交
// 并原样应用到所有存活节点的状态机。

// countInCommands 统计命令序列中某文本出现次数。
func countInCommands(cmds []string, want string) int {
	n := 0
	for _, c := range cmds {
		if c == want {
			n++
		}
	}
	return n
}

// countLogKind 统计节点日志中指定类型条目的数量。
func countLogKind(n *Node, kind EntryKind) int {
	cnt := 0
	for _, e := range n.st.AllEntries() {
		if e.Kind == kind {
			cnt++
		}
	}
	return cnt
}

// aliveMemberIDs 返回全部在线且未被移除的节点。
func aliveMemberIDs(c *Cluster) []NodeID {
	var ids []NodeID
	for id, n := range c.nodes {
		if n.Alive() && !n.removed {
			ids = append(ids, id)
		}
	}
	sortIDs(ids)
	return ids
}

// TestClientCommandMatchingInternalNoopPayload 最基本的回归：
// 客户端命令内容恰好等于内部空操作占位文本时，必须被正常提交并应用到全部节点。
func TestClientCommandMatchingInternalNoopPayload(t *testing.T) {
	c := mustCluster(t, 700, "n1", "n2", "n3")
	// 先提交一条普通命令，确认基线行为。
	idx0, r := c.Propose("cl", []byte("normal-before"))
	if r != ReasonOK {
		t.Fatalf("基线命令提交被拒: %s", r)
	}
	if err := c.WaitCommit(idx0, 200); err != nil {
		t.Fatal(err)
	}
	// 提交内容与内部占位文本完全相同的命令。
	idx, r := c.Propose("cl", []byte(jointNoopPayload))
	if r != ReasonOK {
		t.Fatalf("与占位文本相同的命令提交被拒: %s", r)
	}
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatalf("与占位文本相同的命令未提交: %v", err)
	}
	// 报告成功的命令必须原样出现在所有节点状态机中。
	assertAllCommands(t, c, []string{"normal-before", jointNoopPayload}, "n1", "n2", "n3")
	// 且它在日志中是普通的 KindCommand 条目，而非内部条目。
	e, ok := c.nodes[c.Leader()].st.EntryAt(idx)
	if !ok || e.Kind != KindCommand {
		t.Fatalf("客户端命令落盘类型错误 ok=%v kind=%v", ok, e.Kind)
	}
	t.Logf("内容等于内部占位文本的客户端命令已在全部节点应用 index=%d", idx)
}

// setupJointLeaderCrash 构造“leave-joint 已复制但未提交时领导者崩溃”的场景：
// 新领导者接管后必须追加内部 KindNoop 条目来续推联合变更。
// 返回集群与崩溃前的领导者。
func setupJointLeaderCrash(t *testing.T, seed int64) (*Cluster, NodeID) {
	t.Helper()
	c := mustCluster(t, seed, "n1", "n2", "n3")
	for _, add := range []NodeID{"n4", "n5"} {
		if err := c.AddNode(add); err != nil {
			t.Fatal(err)
		}
	}
	leader := c.Leader()
	ln := c.nodes[leader]
	if r := c.waitLearnersCaughtUp(ln, []NodeID{"n4", "n5"}, 400); r != ReasonOK {
		t.Fatalf("learner 追赶失败: %s", r)
	}
	ack := ln.ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"n4", "n5"}})
	if !ack.Success {
		t.Fatalf("enter-joint 落盘失败: %s", ack.Reason)
	}
	// 逐 tick 推进，直到某节点日志出现 leave-joint（尚未提交），立即崩溃领导者。
	hasLeave := func(n *Node) bool {
		for _, e := range n.st.AllEntries() {
			if e.Kind != KindConfChange {
				continue
			}
			if cc, err := decodeConfChange(e.Payload); err == nil && cc.Op == ConfLeaveJoint {
				return true
			}
		}
		return false
	}
	seen := false
	for i := 0; i < 60 && !seen; i++ {
		c.Step()
		for _, n := range c.nodes {
			if hasLeave(n) {
				seen = true
			}
		}
	}
	if !seen {
		t.Fatal("未观察到 leave-joint 条目")
	}
	c.StopNode(leader)
	return c, leader
}

// TestInternalNoopEntriesStayOutOfFSM 内部空操作条目必须被复制、提交，
// 但绝不进入任何节点的状态机（状态机里只能出现客户端提交过的命令）。
func TestInternalNoopEntriesStayOutOfFSM(t *testing.T) {
	c, crashed := setupJointLeaderCrash(t, 701)
	c.RunUntil(300)
	// 场景有效性：确实产生了内部 KindNoop 条目（否则测试没有覆盖到目标路径）。
	noopTotal := 0
	for _, n := range c.nodes {
		noopTotal += countLogKind(n, KindNoop)
	}
	if noopTotal == 0 {
		t.Fatal("场景未产生内部 KindNoop 条目，覆盖无效")
	}
	// 集群应收敛到 5 节点配置，联合状态结束。
	if err := c.WaitMembershipConverged([]NodeID{"n1", "n2", "n3", "n4", "n5"}, 300); err != nil {
		t.Fatal(err)
	}
	// 崩溃的领导者重启后也要追平。
	if err := c.StartNode(crashed); err != nil {
		t.Fatal(err)
	}
	c.RunUntil(100)
	// 整个过程中没有任何客户端命令：所有状态机必须为空，
	// 特别是不允许出现内部占位文本。
	for _, id := range aliveMemberIDs(c) {
		cmds := c.Commands(id)
		if len(cmds) != 0 {
			t.Fatalf("节点 %s 状态机出现非客户端命令: %v", id, cmds)
		}
	}
	t.Logf("内部 KindNoop 条目共 %d 份，全部跳过状态机，集群已收敛", noopTotal)
}

// TestNoopPayloadCommandDuringJointLeaderCrash 联合变更途中领导者崩溃：
// 新领导者用内部 no-op 续推变更的同时，客户端提交内容等于占位文本的命令，
// 恢复后该命令必须在全部存活节点（含重启的崩溃领导者）上原样应用。
func TestNoopPayloadCommandDuringJointLeaderCrash(t *testing.T) {
	c, crashed := setupJointLeaderCrash(t, 702)
	// 等新领导者产生（它将以内部 no-op 续推联合变更）。
	if err := c.WaitLeader(200); err != nil {
		t.Fatal(err)
	}
	// 变更尚未完全落定时提交“内容等于占位文本”的客户端命令。
	idx, r := c.Propose("cl", []byte(jointNoopPayload))
	if r != ReasonOK {
		t.Fatalf("联合恢复期间提交被拒: %s", r)
	}
	if err := c.WaitCommit(idx, 400); err != nil {
		t.Fatalf("联合恢复期间命令未提交: %v", err)
	}
	// 重启崩溃的旧领导者，让它追平。
	if err := c.StartNode(crashed); err != nil {
		t.Fatal(err)
	}
	c.RunUntil(150)
	if err := c.WaitMembershipConverged([]NodeID{"n1", "n2", "n3", "n4", "n5"}, 300); err != nil {
		t.Fatal(err)
	}
	// 全部 5 个节点的状态机必须恰好各应用一次该命令（内部 no-op 不得混入）。
	for _, id := range aliveMemberIDs(c) {
		cmds := c.Commands(id)
		if got := countInCommands(cmds, jointNoopPayload); got != 1 {
			t.Fatalf("节点 %s 应用占位文本命令次数=%d（期望1） cmds=%v", id, got, cmds)
		}
	}
	t.Logf("联合变更途中领导者崩溃：占位文本命令在 %d 个节点各应用一次", len(aliveMemberIDs(c)))
}

// TestNoopPayloadCommandWithIsolationAndRestart 正常提交占位文本命令后，
// 叠加跟随者隔离与另一节点崩溃重启：愈合/重启后所有节点状态机一致且不丢命令。
func TestNoopPayloadCommandWithIsolationAndRestart(t *testing.T) {
	c := mustCluster(t, 703, "n1", "n2", "n3")
	idx1, r := c.Propose("cl", []byte(jointNoopPayload))
	if r != ReasonOK {
		t.Fatalf("提交被拒: %s", r)
	}
	if err := c.WaitCommit(idx1, 200); err != nil {
		t.Fatal(err)
	}
	// 隔离一个跟随者，再提交第二条同样内容的命令（多数派仍可提交）。
	follower := NodeID("n3")
	if c.Leader() == "n3" {
		follower = "n2"
	}
	c.Isolate(follower)
	idx2, r := c.Propose("cl", []byte(jointNoopPayload))
	if r != ReasonOK {
		t.Fatalf("隔离期间提交被拒: %s", r)
	}
	if err := c.WaitCommit(idx2, 300); err != nil {
		t.Fatalf("隔离期间命令未提交: %v", err)
	}
	// 崩溃并重启另一个节点，然后愈合分区。
	other := NodeID("n1")
	if c.Leader() == "n1" || follower == "n1" {
		other = "n2"
	}
	c.StopNode(other)
	if err := c.StartNode(other); err != nil {
		t.Fatal(err)
	}
	c.HealAll()
	c.RunUntil(200)
	if err := c.WaitCommit(idx2, 300); err != nil {
		t.Fatalf("愈合后未收敛: %v", err)
	}
	// 两条内容相同的客户端命令必须按序各应用一次，三个节点完全一致。
	assertAllCommands(t, c, []string{jointNoopPayload, jointNoopPayload}, "n1", "n2", "n3")
	t.Logf("隔离+重启后两条占位文本命令在全部节点一致应用 idx1=%d idx2=%d", idx1, idx2)
}

// TestNoopPayloadCommandAfterRemoveReadd 节点被移除后又以同名重新加入：
// 新配置下提交内容等于占位文本的命令，全部成员（含重新加入者）必须应用。
func TestNoopPayloadCommandAfterRemoveReadd(t *testing.T) {
	c := mustCluster(t, 704, "n1", "n2", "n3")
	// 批量移除 n3。
	if _, r := c.ProposeBatchConfChange("cl", BatchConfChange{Removes: []NodeID{"n3"}}); r != ReasonOK {
		t.Fatalf("批量移除被拒: %s", r)
	}
	if err := c.WaitMembershipConverged([]NodeID{"n1", "n2"}, 300); err != nil {
		t.Fatal(err)
	}
	c.RemoveNode("n3")
	// 同名重新加入。
	if err := c.AddNode("n3"); err != nil {
		t.Fatal(err)
	}
	if _, r := c.ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"n3"}}); r != ReasonOK {
		t.Fatalf("重新加入被拒: %s", r)
	}
	want := []NodeID{"n1", "n2", "n3"}
	if err := c.WaitMembershipConverged(want, 400); err != nil {
		t.Fatal(err)
	}
	// 提交占位文本命令，全部成员必须应用。
	idx, r := c.Propose("cl", []byte(jointNoopPayload))
	if r != ReasonOK {
		t.Fatalf("提交被拒: %s", r)
	}
	if err := c.WaitCommit(idx, 300); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{jointNoopPayload}, want...)
	t.Logf("移除后再加入：占位文本命令在 %v 全部应用 index=%d", want, idx)
}
