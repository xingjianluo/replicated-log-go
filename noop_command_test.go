package raft

import "testing"

// TestClientCommandEqualToInternalNoopPayload 复现并回归：
// 客户端命令正文恰好等于内部联合 no-op 占位文本（"joint-noop"）时，
// 仍必须作为普通命令被提交并原样应用到全部存活节点，
// 不允许出现“报告成功却查无此命令”。
func TestClientCommandEqualToInternalNoopPayload(t *testing.T) {
	c := mustCluster(t, 11, "n1", "n2", "n3")
	idx, r := c.Propose("cl", []byte(jointNoopPayload))
	if r != ReasonOK {
		t.Fatalf("提交占位文本命令被拒: %s", r)
	}
	if err := c.WaitCommit(idx, 300); err != nil {
		t.Fatalf("等待提交: %v", err)
	}
	assertAllCommands(t, c, []string{jointNoopPayload}, "n1", "n2", "n3")
	t.Logf("占位文本命令 index=%d 已被全部存活节点应用", idx)
}

// TestInternalNoopNeverReachesFSM 覆盖内部条目不污染客户端数据：
// 联合变更途中领导者崩溃，新领导者以内部 no-op 续推旧任期配置条目；
// 该内部 no-op（负载同为 "joint-noop"）必须被所有节点跳过、不进状态机，
// 状态机里只能看到客户端真正提交的命令。
func TestInternalNoopNeverReachesFSM(t *testing.T) {
	// 搭建：三节点集群提交一条普通命令后，发起批量扩容并只推进 3 个 tick，
	// 使 enter-joint 已复制但未提交（旧任期条目），此刻崩溃领导者，
	// 新领导者必须以内部 no-op 续推（continueJointIfNeeded 形态 2）。
	c := mustCluster(t, 91, "n1", "n2", "n3")
	idx, _ := c.Propose("cl", []byte("before"))
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	for _, add := range []NodeID{"n4", "n5"} {
		if err := c.AddNode(add); err != nil {
			t.Fatal(err)
		}
	}
	leader := c.Leader()
	if r := c.waitLearnersCaughtUp(c.nodes[leader], []NodeID{"n4", "n5"}, 400); r != ReasonOK {
		t.Fatalf("learner 追赶失败: %s", r)
	}
	ack := c.nodes[leader].ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"n4", "n5"}})
	if !ack.Success {
		t.Fatalf("enter-joint 落盘失败: %s", ack.Reason)
	}
	c.RunUntil(3)
	c.StopNode(leader)
	if err := c.WaitLeader(200); err != nil {
		t.Fatal(err)
	}
	c.RunUntil(150)
	// 集群必须最终脱离联合中间态并收敛成员表。
	newLeader := c.nodes[c.Leader()]
	if newLeader.Joint() {
		t.Fatal("恢复后不应长期停在 joint 状态")
	}
	want := newLeader.Membership().List()
	if err := c.WaitMembershipConverged(want, 300); err != nil {
		t.Fatal(err)
	}
	if err := c.StartNode(leader); err != nil {
		t.Fatal(err)
	}
	c.RunUntil(80)
	// 全部存活节点：状态机只允许出现客户端命令 "before"，不得出现内部占位文本。
	noopSeen := false
	for id, n := range c.nodes {
		if !n.Alive() || n.removed {
			continue
		}
		for _, e := range n.st.AllEntries() {
			if isJointNoop(e) {
				noopSeen = true
			}
		}
		for _, cmd := range c.fsms[id].Commands() {
			if cmd == jointNoopPayload {
				t.Fatalf("节点 %s 的状态机混入了内部 no-op 条目", id)
			}
		}
		if got := c.fsms[id].Commands(); len(got) != 1 || got[0] != "before" {
			t.Fatalf("节点 %s 状态机内容不符: got=%v want=[before]", id, got)
		}
	}
	if !noopSeen {
		t.Fatal("本用例应真实触发内部 no-op 续推路径，但日志中未找到内部条目")
	}
	t.Logf("内部 no-op 已被全部节点正确跳过，成员表收敛为 %v", want)
}

// TestNoopPayloadCommandSurvivesJointCrashAndRestart 覆盖组合场景：
// 联合变更进行中提交内容为内部占位文本的普通命令，随后领导者崩溃（触发内部
// no-op 续推）并重启、另一节点被隔离再恢复——该命令必须不丢不重，最终原样
// 出现在所有存活节点的状态机中，且内部续推条目不会混入状态机。
func TestNoopPayloadCommandSurvivesJointCrashAndRestart(t *testing.T) {
	c, leader := advanceIntoJoint(t, 77, BatchConfChange{Adds: []NodeID{"n4"}})
	// 联合中间态提交占位文本命令；变更期间普通命令不应被阻塞。
	idx, r := c.Propose("cl", []byte(jointNoopPayload))
	if r != ReasonOK {
		t.Fatalf("联合期间提交占位文本命令被拒: %s", r)
	}
	if err := c.WaitCommit(idx, 300); err != nil {
		t.Fatalf("联合期间占位文本命令未提交: %v", err)
	}
	// 崩溃领导者：新领导者接管未完成的联合变更（内部 no-op 续推）。
	c.StopNode(leader)
	if err := c.WaitLeader(300); err != nil {
		t.Fatal(err)
	}
	c.RunUntil(150)
	cur := c.nodes[c.Leader()]
	if cur.Joint() {
		t.Fatal("领导者崩溃恢复后不应长期停在 joint 状态")
	}
	want := cur.Membership().List()
	if err := c.WaitMembershipConverged(want, 400); err != nil {
		t.Fatal(err)
	}
	// 先重启崩溃的领导者恢复完整多数派，再隔离一个跟随者：
	// 4 成员配置下多数派为 3，隔离一台后仍可提交。
	if err := c.StartNode(leader); err != nil {
		t.Fatal(err)
	}
	c.RunUntil(80)
	follower := NodeID("n2")
	if leader == "n2" {
		follower = "n3"
	}
	c.Isolate(follower)
	idx2, r := c.Propose("cl", []byte("after-noop"))
	if r != ReasonOK {
		t.Fatalf("隔离期间提交被拒: %s", r)
	}
	if err := c.WaitCommit(idx2, 400); err != nil {
		t.Fatalf("隔离期间命令未提交: %v", err)
	}
	c.Heal(follower)
	c.RunUntil(120)
	// 再提交一条命令确认集群完全可用，结果必须和状态机最终内容对得上。
	idx3, r := c.Propose("cl", []byte("final"))
	if r != ReasonOK {
		t.Fatalf("恢复后提交被拒: %s", r)
	}
	if err := c.WaitCommit(idx3, 400); err != nil {
		t.Fatal(err)
	}
	// 所有存活节点：占位文本命令恰好出现一次，内部 no-op 不出现，顺序一致。
	wantCmds := []string{"before", jointNoopPayload, "after-noop", "final"}
	for id, n := range c.nodes {
		if !n.Alive() || n.removed {
			continue
		}
		if c.learners[id] && !n.isVoter(id) {
			continue
		}
		got := c.fsms[id].Commands()
		if len(got) != len(wantCmds) {
			t.Fatalf("节点 %s 命令数不符: got=%v want=%v", id, got, wantCmds)
		}
		for i := range wantCmds {
			if got[i] != wantCmds[i] {
				t.Fatalf("节点 %s 命令[%d] 不符: got=%q want=%q", id, i, got[i], wantCmds[i])
			}
		}
	}
	t.Logf("占位文本命令在崩溃/隔离/重启后仍被全部存活节点原样应用，成员表收敛为 %v", want)
}

// TestNoopPayloadCommandAcrossRemoveAndReadd 覆盖“移除后再加入”场景：
// 在移除前、移除后、重新加入后分别提交内容为内部占位文本的命令，
// 全部必须原样落到所有存活节点（含重新加入的节点）的状态机。
func TestNoopPayloadCommandAcrossRemoveAndReadd(t *testing.T) {
	c := mustCluster(t, 103, "n1", "n2", "n3", "n4", "n5")
	proposeNoop := func(tag string) {
		t.Helper()
		idx, r := c.Propose("cl", []byte(jointNoopPayload))
		if r != ReasonOK {
			t.Fatalf("%s: 提交占位文本命令被拒: %s", tag, r)
		}
		if err := c.WaitCommit(idx, 300); err != nil {
			t.Fatalf("%s: 占位文本命令未提交: %v", tag, err)
		}
	}
	proposeNoop("移除前")
	// 批量移除 n5。
	if _, r := c.ProposeBatchConfChange("cl", BatchConfChange{Removes: []NodeID{"n5"}}); r != ReasonOK {
		t.Fatalf("移除 n5 失败: %s", r)
	}
	if err := c.WaitMembershipConverged([]NodeID{"n1", "n2", "n3", "n4"}, 400); err != nil {
		t.Fatal(err)
	}
	proposeNoop("移除后")
	c.RemoveNode("n5")
	// 同名重新加入并批量加回。
	if err := c.AddNode("n5"); err != nil {
		t.Fatal(err)
	}
	if _, r := c.ProposeBatchConfChange("cl", BatchConfChange{Adds: []NodeID{"n5"}}); r != ReasonOK {
		t.Fatalf("重新加入 n5 失败: %s", r)
	}
	want := []NodeID{"n1", "n2", "n3", "n4", "n5"}
	if err := c.WaitMembershipConverged(want, 400); err != nil {
		t.Fatal(err)
	}
	proposeNoop("重新加入后")
	// 三次占位文本命令都必须按序出现在全部五个节点的状态机中。
	assertAllCommands(t, c, []string{jointNoopPayload, jointNoopPayload, jointNoopPayload}, want...)
	t.Logf("移除/重加前后三条占位文本命令均被全部存活节点（含重新加入的 n5）原样应用")
}
