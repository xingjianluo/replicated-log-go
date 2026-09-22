package raft

import "testing"

// TestSnapshotLaggingNodeRecovery 领导者压缩日志后，严重落后节点通过 InstallSnapshot
// 恢复到一致状态，期间已提交数据不丢、提交位置不回退。
func TestSnapshotLaggingNodeRecovery(t *testing.T) {
	c := mustCluster(t, 61, "n1", "n2", "n3")
	leader := c.Leader()

	// 提交 10 条命令。
	var lastIdx int
	for i := 0; i < 10; i++ {
		idx, _ := c.Propose("cl", []byte("snap-"+itoa(i)))
		if err := c.WaitCommit(idx, 200); err != nil {
			t.Fatal(err)
		}
		lastIdx = idx
	}

	// 在领导者上对前 8 条做快照压缩。
	if err := c.TriggerSnapshot(leader, lastIdx-2); err != nil {
		t.Fatal(err)
	}
	ls := c.stores[leader]
	if ls.FirstIndex() != lastIdx-1 {
		t.Fatalf("压缩后首索引应为 %d，实际 %d", lastIdx-1, ls.FirstIndex())
	}
	if !ls.HasSnapshot() || ls.SnapshotCopy().LastIncludedIndex != lastIdx-2 {
		t.Fatal("快照状态不正确")
	}

	// 停掉一个跟随者，再提交若干条，使其落后到快照点之前。
	var victim NodeID = "n2"
	if leader == victim {
		victim = "n3"
	}
	c.StopNode(victim)
	for i := 0; i < 4; i++ {
		idx, _ := c.Propose("cl", []byte("after-snap-"+itoa(i)))
		if err := c.WaitCommit(idx, 200); err != nil {
			t.Fatal(err)
		}
		lastIdx = idx
	}

	// 再次压缩领导者，使受害者需要的前缀已不在日志中。
	if err := c.TriggerSnapshot(leader, -1); err != nil {
		t.Fatal(err)
	}

	// 重启受害者：其 nextIndex 远落后于领导者日志首索引，应收到快照。
	if err := c.StartNode(victim); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitCommit(lastIdx, 300); err != nil {
		t.Fatal(err)
	}

	var want []string
	for i := 0; i < 10; i++ {
		want = append(want, "snap-"+itoa(i))
	}
	for i := 0; i < 4; i++ {
		want = append(want, "after-snap-"+itoa(i))
	}
	assertAllCommands(t, c, want, "n1", "n2", "n3")
	if !c.stores[victim].HasSnapshot() {
		t.Fatalf("落后节点 %s 应已安装快照", victim)
	}
	t.Logf("节点 %s 经快照恢复 commit=%d applied=%d 快照=%d",
		victim, c.State(victim).CommitIndex, c.State(victim).LastApplied,
		c.stores[victim].SnapshotCopy().LastIncludedIndex)
}

// TestSnapshotDoesNotRegressCommit 不允许用落后快照回退提交位置。
func TestSnapshotDoesNotRegressCommit(t *testing.T) {
	n := newTestNode("x", NewMembership("x", "y", "z"))
	n.Start()
	n.st.AppendNew(1, LogEntry{Kind: KindCommand, Payload: []byte("a")})
	n.commitIndex = 2
	n.lastApplied = 1
	_ = n.fsm.Apply(2, 1, []byte("a"))
	n.lastApplied = 2
	// 伪造一个更旧的快照安装请求。
	out := n.HandleSnapshot(InstallSnapshot{
		From: "y", Term: n.Term(), LastIncludedIndex: 1, LastIncludedTerm: 0,
		Membership: NewMembership("x", "y", "z"), Data: nil,
	})
	res := out[0].RPC.(*InstallSnapshotResult)
	if res.Success {
		t.Fatal("旧快照必须被拒绝")
	}
	if res.Reason != ReasonSnapshotBehindCommit {
		t.Fatalf("失败原因应为 snapshot-behind-commit，实际 %s", res.Reason)
	}
	if n.CommitIndex() != 2 {
		t.Fatal("提交位置不得回退")
	}
}

// TestCompactRejectsUncommitted 压缩位置不得超过提交点。
func TestCompactRejectsUncommitted(t *testing.T) {
	n := newTestNode("x", NewMembership("x", "y", "z"))
	n.Start()
	n.st.AppendNew(1, LogEntry{Kind: KindCommand, Payload: []byte("a")})
	// commitIndex 仍为 1（引导），压缩 index=2 必须失败。
	if _, err := n.CreateSnapshot(2); err == nil {
		t.Fatal("压缩未提交位置必须失败")
	}
}

// TestSnapshotFromNewLeaderAfterElection 领导者切换后，新领导者压缩日志，
// 落后节点必须收到新领导者的快照并继续追平新条目（快照点作为追加锚点）。
func TestSnapshotFromNewLeaderAfterElection(t *testing.T) {
	c := mustCluster(t, 88, "a", "b", "d")
	old := c.Leader()

	// 停掉旧领导者，另两个节点选出新领导者并提交若干条。
	c.StopNode(old)
	c.RunUntil(60)
	var newL NodeID
	for _, id := range []NodeID{"a", "b", "d"} {
		if id != old && c.nodes[id].Role() == Leader {
			newL = id
		}
	}
	if newL == "" {
		t.Fatal("未产生新领导者")
	}
	for i := 0; i < 4; i++ {
		idx, _ := c.Propose("cl", []byte("nl-"+itoa(i)))
		if err := c.WaitCommit(idx, 200); err != nil {
			t.Fatal(err)
		}
	}
	// 新领导者在当前提交点压缩。
	last := c.State(newL).CommitIndex
	if err := c.TriggerSnapshot(newL, last); err != nil {
		t.Fatal(err)
	}
	// 再提交两条快照之后的条目。
	for i := 0; i < 2; i++ {
		idx, _ := c.Propose("cl", []byte("post-"+itoa(i)))
		if err := c.WaitCommit(idx, 200); err != nil {
			t.Fatal(err)
		}
		last = idx
	}
	// 旧领导者重启：其日志落后于新领导者快照点，必须走 InstallSnapshot 再追日志。
	if err := c.StartNode(old); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitCommit(last, 300); err != nil {
		t.Fatal(err)
	}
	var want []string
	for i := 0; i < 4; i++ {
		want = append(want, "nl-"+itoa(i))
	}
	for i := 0; i < 2; i++ {
		want = append(want, "post-"+itoa(i))
	}
	assertAllCommands(t, c, want, "a", "b", "d")
	if !c.stores[old].HasSnapshot() {
		t.Fatalf("重启节点 %s 应通过快照恢复", old)
	}
}
