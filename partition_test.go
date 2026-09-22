package raft

import "testing"

// TestMinorityCannotCommit 隔离领导者（形成少数派分区）后，少数派不得推进提交；
// 多数派分区重新选主并提交新条目；网络恢复后旧领导者退位，全部节点收敛一致。
func TestMinorityCannotCommit(t *testing.T) {
	c := mustCluster(t, 11, "n1", "n2", "n3", "n4", "n5")
	oldLeader := c.Leader()

	// 提交两条基线命令。
	for _, cmd := range []string{"k1", "k2"} {
		idx, _ := c.Propose("cl", []byte(cmd))
		if err := c.WaitCommit(idx, 200); err != nil {
			t.Fatal(err)
		}
	}

	// 隔离旧领导者：它与剩余 4 个节点构成 1 vs 4 两个分区。
	c.Isolate(oldLeader)
	c.RunUntil(30) // 让多数派完成重新选主

	var majorityLeader NodeID
	for id, n := range c.nodes {
		if id == oldLeader {
			continue
		}
		if n.Alive() && n.Role() == Leader && !c.net.partitioned[id] {
			majorityLeader = id
			break
		}
	}
	if majorityLeader == "" {
		t.Fatal("多数派分区未选出新领导者")
	}
	if majorityLeader == oldLeader {
		t.Fatal("新领导者不应是被隔离节点")
	}

	// 旧领导者（少数派）的提案无法获得多数派提交。
	oldCommitBefore := c.nodes[oldLeader].CommitIndex()
	ack := c.nodes[oldLeader].Propose("cl", []byte("stale-write"))
	if ack.Success {
		c.RunUntil(40)
	}
	if c.nodes[oldLeader].CommitIndex() > oldCommitBefore {
		t.Fatalf("少数派分区非法推进提交: %d -> %d",
			oldCommitBefore, c.nodes[oldLeader].CommitIndex())
	}
	t.Logf("少数派旧领导者 %s 提交位置停留在 %d（满足多数派约束）", oldLeader, oldCommitBefore)

	// 多数派分区正常提交。
	idx2, r2 := c.Propose("cl", []byte("k3"))
	if r2 != ReasonOK {
		t.Fatalf("多数派提案被拒: %s", r2)
	}
	if err := c.WaitCommit(idx2, 200); err != nil {
		t.Fatal(err)
	}

	// 网络恢复：旧领导者必须发现更高任期而退位，并补齐日志，已提交数据不丢。
	c.HealAll()
	if err := c.WaitCommit(idx2, 200); err != nil {
		t.Fatal(err)
	}
	if c.nodes[oldLeader].Role() == Leader && c.nodes[oldLeader].Term() == c.Term() {
		t.Fatal("旧领导者恢复后必须退位")
	}
	assertAllCommands(t, c, []string{"k1", "k2", "k3"}, "n1", "n2", "n3", "n4", "n5")
	t.Logf("网络恢复后全部节点收敛：领导者=%s 任期=%d commit=%d",
		c.Leader(), c.Term(), c.nodes[c.Leader()].CommitIndex())
}

// TestSplitMajorityLeaderSide 3 节点中隔离 2 个（含领导者）：
// 多数派侧可继续提交，孤立单节点不能提交，恢复后单节点覆盖掉本地冲突候选条目。
func TestPartitionThenConflictRepair(t *testing.T) {
	c := mustCluster(t, 23, "a", "b", "d")
	leader := c.Leader()

	// 找到两个非领导者节点作为多数派侧（含或不含领导者均可，先确定身份）。
	var others []NodeID
	for _, id := range []NodeID{"a", "b", "d"} {
		if id != leader {
			others = append(others, id)
		}
	}

	// 隔离领导者单节点 -> 另外两个构成多数派重新选主。
	c.Isolate(leader)
	c.RunUntil(40)
	var newLeader NodeID
	for _, id := range others {
		if c.nodes[id].Role() == Leader {
			newLeader = id
		}
	}
	if newLeader == "" {
		t.Fatal("两个节点的多数派侧未选出领导者")
	}

	// 在孤立旧领导者上制造一条未提交的冲突条目。
	_ = c.nodes[leader].Propose("cl", []byte("conflict-from-old"))
	c.RunUntil(20)

	// 多数派提交新命令。
	idx, r := c.Propose("cl", []byte("majority-cmd"))
	if r != ReasonOK {
		t.Fatalf("多数派提案失败: %s", r)
	}
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}

	// 恢复网络：旧领导者的冲突条目必须被显式截断修正，而非静默保留。
	c.HealAll()
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"majority-cmd"}, "a", "b", "d")
	st := c.stores[leader]
	if e, ok := st.EntryAt(idx); !ok || string(e.Payload) != "majority-cmd" {
		t.Fatalf("旧领导者 index=%d 未被多数派日志修正: %+v", idx, e)
	}
}
