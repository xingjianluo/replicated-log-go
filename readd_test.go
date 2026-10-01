package raft

import "testing"

// TestRemoveThenReaddVoter 节点被移除后，过一段时间以同一 ID 重新加入：
// 必须作为全新空日志节点追赶，通过快照/日志重放得到当前成员表与全部数据，
// 重新正常参与复制与选举；它被移除期间的旧成员表、旧日志不得影响集群判断。
func TestRemoveThenReaddVoter(t *testing.T) {
	c := mustCluster(t, 401, "n1", "n2", "n3")
	idx, _ := c.Propose("cl", []byte("before-remove"))
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	// 移除 n3。
	if _, r := c.ProposeConfChange("cl", ConfChange{Op: ConfRemoveVoter, Node: "n3"}); r != ReasonOK {
		t.Fatal(r)
	}
	if err := c.WaitMembership([]NodeID{"n1", "n2"}, 300); err != nil {
		t.Fatal(err)
	}
	if !c.nodes["n3"].removed {
		t.Fatal("n3 应已被移除")
	}
	c.RemoveNode("n3")
	// 被移除的领导者卸任后，剩余成员需要一两个超时补选新领导者。
	if err := c.WaitLeader(200); err != nil {
		t.Fatal(err)
	}

	// 移除期间集群继续提交多条数据。
	for _, cmd := range []string{"while-gone-1", "while-gone-2"} {
		i, r := c.Propose("cl", []byte(cmd))
		if r != ReasonOK {
			t.Fatal(r)
		}
		if err := c.WaitCommit(i, 200); err != nil {
			t.Fatal(err)
		}
	}

	// 同一 ID 重新加入：必须是全新空日志节点（不带旧成员表、旧日志）。
	if err := c.AddNode("n3"); err != nil {
		t.Fatal(err)
	}
	s3 := c.State("n3")
	if s3.LastIndex != 0 || len(s3.Members) != 0 {
		t.Fatalf("重新加入的 n3 必须空日志空配置: last=%d members=%v", s3.LastIndex, s3.Members)
	}
	if _, r := c.ProposeConfChange("cl", ConfChange{Op: ConfAddVoter, Node: "n3"}); r != ReasonOK {
		t.Fatalf("重新加入被拒: %s", r)
	}
	if err := c.WaitMembership([]NodeID{"n1", "n2", "n3"}, 300); err != nil {
		t.Fatal(err)
	}
	c.RunUntil(10)

	// n3 必须追平全部历史，且以“当前成员表”参与，不残留旧视图。
	want := []string{"before-remove", "while-gone-1", "while-gone-2"}
	assertAllCommands(t, c, want, "n1", "n2", "n3")
	st3 := c.State("n3")
	if !NewMembership(st3.Members...).Equal(NewMembership("n1", "n2", "n3")) || st3.Joint || st3.Pending {
		t.Fatalf("readd 后 n3 状态错误: %+v", st3)
	}

	// 重新加入后 n3 能正常参与：停掉另一个节点，剩余多数派仍提交。
	c.StopNode("n2")
	i, r := c.Propose("cl", []byte("after-readd"))
	if r != ReasonOK {
		t.Fatal(r)
	}
	if err := c.WaitCommit(i, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, append(want, "after-readd"), "n1", "n3")
}

// TestBatchSwapNodes 批量“换人”：一次移除 {n2}、加入 {n4}，
// 随后再用一批把 n2 加回、n4 移除，验证同一节点在批量变更间反复进出仍一致。
func TestBatchSwapNodes(t *testing.T) {
	c := mustCluster(t, 402, "n1", "n2", "n3")
	if err := c.AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	// 第一批：踢 n2，进 n4。
	if _, r := c.ProposeConfChangeBatch("cl", []NodeID{"n4"}, []NodeID{"n2"}); r != ReasonOK {
		t.Fatal(r)
	}
	if err := c.WaitMembership([]NodeID{"n1", "n3", "n4"}, 300); err != nil {
		t.Fatal(err)
	}
	if !c.nodes["n2"].removed {
		t.Fatal("n2 应已移除")
	}
	c.RemoveNode("n2")

	// 第二批：n2 以空日志重新加入，同时踢掉 n4。
	if err := c.AddNode("n2"); err != nil {
		t.Fatal(err)
	}
	if _, r := c.ProposeConfChangeBatch("cl", []NodeID{"n2"}, []NodeID{"n4"}); r != ReasonOK {
		t.Fatal(r)
	}
	if err := c.WaitMembership([]NodeID{"n1", "n2", "n3"}, 300); err != nil {
		t.Fatal(err)
	}
	c.RunUntil(10)
	if !c.nodes["n4"].removed {
		t.Fatal("n4 应已移除")
	}
	for _, id := range []NodeID{"n1", "n2", "n3"} {
		s := c.State(id)
		if s.Joint || s.Pending || !NewMembership(s.Members...).Equal(NewMembership("n1", "n2", "n3")) {
			t.Fatalf("%s 换回后状态错误: %+v", id, s)
		}
	}
	idx, r := c.Propose("cl", []byte("post-swap"))
	if r != ReasonOK {
		t.Fatal(r)
	}
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"post-swap"}, "n1", "n2", "n3")
}
