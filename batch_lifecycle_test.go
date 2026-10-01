package raft

import "testing"

// TestBatchChangeDoesNotBlockCommands 批量成员变更的两阶段期间，
// 普通命令必须持续提交，不能因成员变更停摆。
func TestBatchChangeDoesNotBlockCommands(t *testing.T) {
	c := mustCluster(t, 101, "n1", "n2", "n3")
	if err := c.AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNode("n5"); err != nil {
		t.Fatal(err)
	}
	leader := c.Leader()
	ln := c.nodes[leader]
	if r := c.waitLearnersCaughtUp(ln, []NodeID{"n4", "n5"}, 400); r != ReasonOK {
		t.Fatal(r)
	}
	ack := ln.ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"n4", "n5"}})
	if !ack.Success {
		t.Fatal(ack.Reason)
	}

	// 在 enter-joint 到 leave-joint 落定的整个窗口里穿插普通命令，
	// 它们必须都能被联合双多数派提交。
	var committed []string
	i := 0
	for !ln.IsBatchConfComplete() && c.Leader() != "" && i < 300 {
		if i%3 == 0 {
			cmd := "during-" + itoa(i)
			idx, r := c.Propose("cl", []byte(cmd))
			if r == ReasonOK {
				if err := c.WaitCommit(idx, 200); err == nil {
					committed = append(committed, cmd)
				}
			}
		}
		c.Step()
		i++
	}
	if ln.Joint() || ln.PendingConfChange() {
		t.Fatal("批量变更未在限定时间内落定")
	}
	if len(committed) == 0 {
		t.Fatal("变更窗口内没有任何普通命令成功提交，服务被卡住")
	}
	c.RunUntil(20)
	want := append([]string{}, committed...)
	assertAllCommands(t, c, want, "n1", "n2", "n3", "n4", "n5")
	t.Logf("变更窗口内成功提交 %d 条普通命令", len(committed))
}

// TestBatchInvalidChangesRejected 非法批量变更必须被显式拒绝。
func TestBatchInvalidChangesRejected(t *testing.T) {
	c := mustCluster(t, 102, "n1", "n2", "n3")
	ln := c.nodes[c.Leader()]

	// 空批次。
	if ack := ln.ProposeBatchConfChange("cl", BatchConfChange{}); ack.Reason != ReasonBadConfChange {
		t.Fatalf("空批次应拒绝，实际 %s", ack.Reason)
	}
	// 重复添加同一节点。
	if ack := ln.ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"x", "x"}}); ack.Reason != ReasonBadConfChange {
		t.Fatalf("重复添加应拒绝，实际 %s", ack.Reason)
	}
	// 既增又删同一节点。
	if ack := ln.ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"n2"}, Removes: []NodeID{"n2"}}); ack.Reason != ReasonBadConfChange {
		t.Fatalf("同节点既增又删应拒绝，实际 %s", ack.Reason)
	}
	// 删除不存在的成员。
	if ack := ln.ProposeBatchConfChange("cl", BatchConfChange{Removes: []NodeID{"ghost"}}); ack.Reason != ReasonUnknownVoter {
		t.Fatalf("删除非成员应拒绝，实际 %s", ack.Reason)
	}
	// 已存在的成员再添加。
	if ack := ln.ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"n1"}}); ack.Reason != ReasonUnknownVoter {
		t.Fatalf("添加已存在成员应拒绝，实际 %s", ack.Reason)
	}
	// 删空整个集群。
	if ack := ln.ProposeBatchConfChange("cl", BatchConfChange{Removes: []NodeID{"n1", "n2", "n3"}}); ack.Reason != ReasonBadConfChange {
		t.Fatalf("删空集群应拒绝，实际 %s", ack.Reason)
	}
	// learner 未追平时拒绝。
	if err := c.AddNode("n9"); err != nil {
		t.Fatal(err)
	}
	if ack := ln.ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"n9"}}); ack.Reason != ReasonLearnerLagging {
		t.Fatalf("learner 未追平应拒绝，实际 %s", ack.Reason)
	}
	// 上一次变更未落定前拒绝新批量变更。
	if r := c.waitLearnersCaughtUp(ln, []NodeID{"n9"}, 400); r != ReasonOK {
		t.Fatal(r)
	}
	if ack := ln.ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"n9"}}); !ack.Success {
		t.Fatal(ack.Reason)
	}
	if ack := ln.ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"n8"}}); ack.Reason != ReasonMembershipPending {
		t.Fatalf("重叠成员变更应被 pending 拒绝，实际 %s", ack.Reason)
	}
}

// TestRemoveThenReaddNode 节点被移除后过一阵以同名重新加入：
// 旧成员表/旧日志不影响集群，重新加入后正常参与复制与选举。
func TestRemoveThenReaddNode(t *testing.T) {
	c := mustCluster(t, 103, "n1", "n2", "n3", "n4", "n5")
	idx, _ := c.Propose("cl", []byte("epoch-1"))
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	// 移除 n5。
	cfgIdx, r := c.ProposeBatchConfChange("cl", BatchConfChange{Removes: []NodeID{"n5"}})
	if r != ReasonOK {
		t.Fatal(r)
	}
	if err := c.WaitMembershipConverged([]NodeID{"n1", "n2", "n3", "n4"}, 300); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitCommit(cfgIdx, 200); err != nil {
		t.Fatal(err)
	}
	if !c.nodes["n5"].removed {
		t.Fatal("n5 应已被移除")
	}
	// 让被移除的旧节点带着旧任期/旧日志继续存在一小段时间，它不能干扰集群。
	c.RunUntil(30)
	idx2, _ := c.Propose("cl", []byte("epoch-2"))
	if err := c.WaitCommit(idx2, 200); err != nil {
		t.Fatal(err)
	}
	c.RemoveNode("n5")

	// 同名重新加入：空日志新身份，先 learner 追赶再批量加入。
	if err := c.AddNode("n5"); err != nil {
		t.Fatal(err)
	}
	if !c.State("n5").Learner {
		t.Fatal("重新加入的 n5 初始应为 learner")
	}
	cfgIdx2, r2 := c.ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"n5"}})
	if r2 != ReasonOK {
		t.Fatalf("重新加入失败: %s", r2)
	}
	want := []NodeID{"n1", "n2", "n3", "n4", "n5"}
	if err := c.WaitMembershipConverged(want, 400); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitCommit(cfgIdx2, 200); err != nil {
		t.Fatal(err)
	}

	// n5 必须拥有移除期间集群提交的全部数据（epoch-2），证明它以新日志追赶，
	// 旧成员表/旧日志没有污染判断。
	assertAllCommands(t, c, []string{"epoch-1", "epoch-2"}, want...)
	// n5 重新成为投票成员后，停掉其余多数派中的两个，验证它能参与选举与提交。
	leader := c.Leader()
	victim1, victim2 := pickTwoOthers(t, c, leader, "n5")
	c.StopNode(victim1)
	c.StopNode(victim2)
	c.RunUntil(120)
	if c.Leader() == "" {
		t.Fatal("n5 重新加入后，剩余多数派未能选主")
	}
	idx3, r3 := c.Propose("cl", []byte("epoch-3"))
	if r3 != ReasonOK {
		t.Fatalf("n5 重新参与后提交失败: %s", r3)
	}
	if err := c.WaitCommit(idx3, 200); err != nil {
		t.Fatal(err)
	}
	t.Logf("n5 移除后重新加入，正常参与复制与选举，最新提交=%d", idx3)
}

func pickTwoOthers(t *testing.T, c *Cluster, exclude ...NodeID) (NodeID, NodeID) {
	t.Helper()
	skip := map[NodeID]bool{}
	for _, e := range exclude {
		skip[e] = true
	}
	var out []NodeID
	for _, id := range c.sortedNodeIDs() {
		if !skip[id] && c.nodes[id].Alive() && !c.nodes[id].removed {
			out = append(out, id)
		}
	}
	if len(out) < 2 {
		t.Fatalf("无法选出两个其他节点: %v", out)
	}
	return out[0], out[1]
}
