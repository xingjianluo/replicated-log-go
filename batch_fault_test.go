package raft

import "testing"

// advanceIntoJoint 提交一次批量变更并推进到“enter-joint 已复制到部分节点、
// 但尚未完成 leave-joint”的中间窗口，返回当时的领导者。
func advanceIntoJoint(t *testing.T, seed int64, b BatchConfChange) (*Cluster, NodeID) {
	t.Helper()
	c := mustCluster(t, seed, "n1", "n2", "n3")
	idx, _ := c.Propose("cl", []byte("before"))
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	for _, add := range b.Adds {
		if err := c.AddNode(add); err != nil {
			t.Fatal(err)
		}
	}
	leader := c.Leader()
	ln := c.nodes[leader]
	if r := c.waitLearnersCaughtUp(ln, b.Adds, 400); r != ReasonOK {
		t.Fatalf("learner 追赶失败: %s", r)
	}
	ack := ln.ProposeBatchConfChange("cl", b)
	if !ack.Success {
		t.Fatalf("enter-joint 落盘失败: %s", ack.Reason)
	}
	// 推进若干 tick，使 enter-joint 进入联合中间态但不等待它最终落定。
	for i := 0; i < 6; i++ {
		c.Step()
	}
	return c, leader
}

// TestBatchLeaderCrashDuringJoint 联合共识中间窗口领导者崩溃：
// 恢复后变更要么安全完成、要么安全放弃，所有存活节点收敛到同一份成员表，
// 且期间的已提交数据不丢不回滚。
func TestBatchLeaderCrashDuringJoint(t *testing.T) {
	c, leader := advanceIntoJoint(t, 91, BatchConfChange{Adds: []NodeID{"n4", "n5"}})
	c.StopNode(leader)
	c.RunUntil(60)
	// 多数派应选出新领导者；若新领导者日志中有 enter-joint 就续推完成，
	// 若没有则冲突条目被截断、变更安全放弃——两种结果都合法，但必须收敛一致。
	if err := c.WaitLeader(200); err != nil {
		t.Fatal(err)
	}
	// 给足时间让新领导者把联合变更推到完成（典型路径）。
	c.RunUntil(120)
	var want []NodeID
	newLeader := c.nodes[c.Leader()]
	want = newLeader.Membership().List()
	if newLeader.Joint() {
		t.Fatalf("恢复后不应长期停在 joint 状态")
	}
	if err := c.WaitMembershipConverged(want, 300); err != nil {
		t.Fatal(err)
	}

	// 重启崩溃的领导者，它必须追平到相同配置与相同已提交日志。
	if err := c.StartNode(leader); err != nil {
		t.Fatal(err)
	}
	c.RunUntil(80)
	for id, n := range c.nodes {
		if !n.Alive() || n.removed {
			continue
		}
		if n.Joint() {
			t.Fatalf("%s 仍处于 joint 状态", id)
		}
		if !n.Membership().Equal(NewMembership(want...)) {
			t.Fatalf("%s 成员表未收敛 got=%v want=%v", id, n.Membership().List(), want)
		}
	}

	// 崩溃前已提交的命令必须还在，之后集群继续正常提交不回滚。
	idx, r := c.Propose("cl", []byte("after-crash"))
	if r != ReasonOK {
		t.Fatalf("恢复后无法提交: %s", r)
	}
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	// 所有未移除成员的命令序列都应以 before 开头、以 after-crash 结尾。
	for id, n := range c.nodes {
		if !n.Alive() || n.removed {
			continue
		}
		cmds := c.Commands(id)
		if len(cmds) == 0 || cmds[0] != "before" {
			t.Fatalf("%s 崩溃前数据丢失: %v", id, cmds)
		}
		if cmds[len(cmds)-1] != "after-crash" {
			t.Fatalf("%s 恢复后未应用最新命令: %v", id, cmds)
		}
	}
	t.Logf("联合阶段领导者崩溃后收敛：成员表=%v", want)
}

// TestBatchPartitionDuringJoint 联合期间把领导者隔离进少数派：
// 旧领导者无法在联合配置下凑齐双多数派提交；分区愈合后变更安全完成，
// 成员表最终一致。
func TestBatchPartitionDuringJoint(t *testing.T) {
	c, leader := advanceIntoJoint(t, 92, BatchConfChange{Adds: []NodeID{"n4", "n5"}})
	c.Isolate(leader)
	c.RunUntil(80)
	// 隔离期间多数派可能重新选主；无论谁领导，都不允许联合变更被错误提交一半。
	c.HealAll()
	c.RunUntil(200)
	l := c.Leader()
	if l == "" {
		t.Fatal("分区愈合后无领导者")
	}
	want := c.nodes[l].Membership().List()
	if c.nodes[l].Joint() {
		// 再给时间让续推完成。
		c.RunUntil(150)
		want = c.nodes[c.Leader()].Membership().List()
	}
	if err := c.WaitMembershipConverged(want, 300); err != nil {
		t.Fatal(err)
	}
	for id, n := range c.nodes {
		if !n.Alive() || n.removed {
			continue
		}
		if n.Joint() {
			t.Fatalf("%s 愈合后仍处于 joint", id)
		}
	}
	idx, r := c.Propose("cl", []byte("after-partition"))
	if r != ReasonOK {
		t.Fatalf("分区恢复后无法提交: %s", r)
	}
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	t.Logf("联合期间分区后收敛：成员表=%v", want)
}
