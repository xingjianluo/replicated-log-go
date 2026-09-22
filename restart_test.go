package raft

import "testing"

// TestLeaderRestart 领导者崩溃重启：持久状态保留，重启后以跟随者身份重新收敛，已提交数据不丢。
func TestLeaderRestart(t *testing.T) {
	c := mustCluster(t, 31, "n1", "n2", "n3")
	leader := c.Leader()
	var idxs []int
	for _, cmd := range []string{"r1", "r2", "r3"} {
		idx, r := c.Propose("cl", []byte(cmd))
		if r != ReasonOK {
			t.Fatal(r)
		}
		if err := c.WaitCommit(idx, 200); err != nil {
			t.Fatal(err)
		}
		idxs = append(idxs, idx)
	}

	// 崩溃领导者。
	c.StopNode(leader)
	// 重启前多数派继续提交一条。
	c.RunUntil(40)
	idx, _ := c.Propose("cl", []byte("r4"))
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}

	// 重启：从同一 Storage 恢复任期、投票与日志。
	if err := c.StartNode(leader); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	if c.State(leader).CommitIndex < idx {
		t.Fatalf("重启节点未追平提交点: %d < %d", c.State(leader).CommitIndex, idx)
	}
	assertAllCommands(t, c, []string{"r1", "r2", "r3", "r4"}, "n1", "n2", "n3")
	t.Logf("领导者 %s 重启后以 %s 身份收敛 commit=%d term=%d",
		leader, c.State(leader).Role, c.State(leader).CommitIndex, c.State(leader).Term)
}

// TestFollowerWipeAndRestart 跟随者宕机期间落后多条，重启后正常追赶。
func TestFollowerWipeAndRestart(t *testing.T) {
	c := mustCluster(t, 32, "n1", "n2", "n3")
	var victim NodeID = "n2"
	if c.Leader() == victim {
		victim = "n3"
	}
	c.StopNode(victim)
	for i := 0; i < 10; i++ {
		idx, _ := c.Propose("cl", []byte("bulk-"+itoa(i)))
		if err := c.WaitCommit(idx, 200); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.StartNode(victim); err != nil {
		t.Fatal(err)
	}
	lastIdx := c.nodes[c.Leader()].CommitIndex()
	if err := c.WaitCommit(lastIdx, 300); err != nil {
		t.Fatal(err)
	}
	var want []string
	for i := 0; i < 10; i++ {
		want = append(want, "bulk-"+itoa(i))
	}
	assertAllCommands(t, c, want, "n1", "n2", "n3")
}

// TestElectionDuringSingleNodeDown 3 节点挂 1 个，剩余 2 个仍构成多数派可选举与提交。
func TestElectionDuringSingleNodeDown(t *testing.T) {
	c := mustCluster(t, 33, "n1", "n2", "n3")
	leader := c.Leader()
	c.StopNode(leader)
	c.RunUntil(60)
	newLeader := ""
	for id, n := range c.nodes {
		if id != leader && n.Role() == Leader {
			newLeader = string(id)
		}
	}
	if newLeader == "" {
		t.Fatal("存活的 2 节点多数派未能重新选主")
	}
	idx, r := c.Propose("cl", []byte("survive"))
	if r != ReasonOK {
		t.Fatalf("降级期间多数派无法提交: %s", r)
	}
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
}
