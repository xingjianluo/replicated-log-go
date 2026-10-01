package raft

import (
	"fmt"
	"testing"
)

func runBatchFuzz(t *testing.T, seed int64, fault string) {
	t.Helper()
	c := NewCluster([]NodeID{"n1", "n2", "n3", "n4", "n5"},
		WithSeed(seed), WithFaultRates(0.08, 0.06, 0.06))
	if err := c.Start(300); err != nil {
		t.Fatal(err)
	}
	idx, _ := c.Propose("cl", []byte("base"))
	if err := c.WaitCommit(idx, 300); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNode("n6"); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNode("n7"); err != nil {
		t.Fatal(err)
	}

	// 在 learner 追平之后、批量变更发起的同时注入一次故障。
	leader := c.nodes[c.Leader()]
	if r := c.waitLearnersCaughtUp(leader, []NodeID{"n6", "n7"}, 400); r != ReasonOK {
		t.Fatalf("learner 追平失败: %s", r)
	}
	ack := leader.ProposeBatchConfChange("cl", BatchConfChange{
		Adds: []NodeID{"n6", "n7"}, Removes: []NodeID{"n4", "n5"},
	})
	if !ack.Success {
		t.Fatalf("enter-joint 失败: %s", ack.Reason)
	}
	// 故障注入：联合窗口里干掉当前领导者（崩溃或隔离）；none 不注入。
	victim := c.Leader()
	switch fault {
	case "crash":
		c.StopNode(victim)
		c.RunUntil(60)
		if err := c.StartNode(victim); err != nil {
			t.Fatal(err)
		}
	case "partition":
		c.Isolate(victim)
		c.RunUntil(60)
		c.HealAll()
	}

	want := []NodeID{"n1", "n2", "n3", "n6", "n7"}
	// 联合窗口失去领导者时，变更可能被安全放弃（enter-joint 随旧领导者日志
	// 一起被截断，全员留在旧配置）；这是合法结果。恢复后允许重新提交直到完成。
	converged := false
	for attempt := 0; attempt < 5; attempt++ {
		if err := c.WaitMembershipConverged(want, 200); err == nil {
			converged = true
			break
		}
		// 未收敛：确认集群处于唯一旧配置的健康状态后，重新发起批量变更。
		if err := c.WaitLeader(200); err != nil {
			t.Fatalf("seed=%d fault=%s 恢复后无领导者", seed, fault)
		}
		l := c.nodes[c.Leader()]
		if l.Joint() || l.PendingConfChange() {
			c.RunUntil(40)
			continue
		}
		if _, r := c.ProposeBatchConfChange("cl", BatchConfChange{
			Adds: []NodeID{"n6", "n7"}, Removes: []NodeID{"n4", "n5"},
		}); r != ReasonOK && r != ReasonMembershipPending {
			c.RunUntil(40)
		}
	}
	if !converged {
		t.Fatalf("seed=%d fault=%s 重试后成员表仍未收敛", seed, fault)
	}
	// 恢复后可能由新领导者续推；等待 enter/leave 全部落定。
	if err := c.WaitCommit(c.nodes[c.Leader()].CommitIndex(), 400); err != nil {
		t.Fatal(err)
	}
	// 再等几个心跳周期，让“移除自己”的提交点传播到被移除节点（它们随后标记 removed）。
	c.RunUntil(15)
	for _, id := range []NodeID{"n4", "n5"} {
		if n, ok := c.nodes[id]; ok && !n.removed {
			t.Fatalf("seed=%d %s 应被移除", seed, id)
		}
		c.RemoveNode(id)
	}
	idx2, r2 := c.Propose("cl", []byte("after-shuffle"))
	if r2 != ReasonOK {
		t.Fatalf("seed=%d 变更后提交失败: %s", seed, r2)
	}
	if err := c.WaitCommit(idx2, 300); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"base", "after-shuffle"}, want...)
}

// TestBatchFuzzSeeds 多种子批量混合变更（无额外故障）。
func TestBatchFuzzSeeds(t *testing.T) {
	for _, seed := range []int64{301, 304, 307, 310, 312} {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runBatchFuzz(t, seed, "none")
		})
	}
}

// TestBatchFuzzLeaderCrash 联合窗口领导者崩溃，多种子。
func TestBatchFuzzLeaderCrash(t *testing.T) {
	for _, seed := range []int64{401, 402, 405, 406, 409} {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runBatchFuzz(t, seed, "crash")
		})
	}
}

// TestBatchFuzzLeaderPartition 联合窗口领导者被隔离，多种子。
func TestBatchFuzzLeaderPartition(t *testing.T) {
	for _, seed := range []int64{501, 503, 505, 506, 509} {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runBatchFuzz(t, seed, "partition")
		})
	}
}
