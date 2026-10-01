package raft

import "testing"

// TestBatchChangeChaos 在连续批量增删成员的过程中叠加随机丢包/重复/乱序、
// 周期性分区与节点崩溃重启，持续提交普通命令。断言：
//  1. 全程任何 tick 至多一个“存活且未被隔离”的可提交领导者；
//  2. 故障关闭、网络愈合后，最终存活节点收敛到同一份成员表与同一条已提交日志；
//  3. 已提交命令在所有存活节点完全一致（不丢、不重、不回滚）。
func TestBatchChangeChaos(t *testing.T) {
	c := NewCluster([]NodeID{"n1", "n2", "n3"},
		WithSeed(9090), WithFaultRates(0.05, 0.08, 0.08))
	if err := c.Start(400); err != nil {
		t.Fatal(err)
	}

	// 预置两台将被加入的节点。
	if err := c.AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNode("n5"); err != nil {
		t.Fatal(err)
	}

	totalTicks := 600
	for tick := 0; tick < totalTicks; tick++ {
		// 周期性普通写入（变更期间也不停止）。
		if tick%12 == 3 {
			if idx, r := c.Propose("cl", []byte("k"+itoa(tick))); r == ReasonOK {
				_ = idx
			}
		}
		switch tick % 200 {
		case 30:
			// 第一批：加入 n4、n5。
			_, _ = c.ProposeConfChangeBatch("cl", []NodeID{"n4", "n5"}, nil)
		case 120:
			// 第二批：移除 n5（已加入的节点）。
			if c.Node("n5") != nil {
				_, _ = c.ProposeConfChangeBatch("cl", nil, []NodeID{"n5"})
			}
		}
		// 周期性分区与崩溃重启。
		switch tick % 160 {
		case 40:
			c.Isolate("n2")
		case 90:
			c.Heal("n2")
		}
		switch tick % 240 {
		case 60:
			c.StopNode("n3")
		case 110:
			_ = c.StartNode("n3")
		}

		// 活性/安全不变量：任意 tick 至多一个在线未隔离的领导者。
		leaders := 0
		for _, n := range c.nodes {
			if n.Alive() && !c.net.partitioned[n.id] && !n.removed && n.Role() == Leader {
				leaders++
			}
		}
		if leaders > 1 {
			t.Fatalf("tick=%d 出现 %d 个可提交领导者", tick, leaders)
		}
		c.Step()
	}

	// 收尾：关故障、愈合分区、重启崩溃节点，等待成员表最终落定。
	c.net.SetFaultRates(0, 0, 0)
	c.HealAll()
	_ = c.StartNode("n3")
	// 以当前领导者的成员表为收敛目标。
	if err := c.WaitLeader(400); err != nil {
		t.Fatal(err)
	}
	var want []NodeID
	for _, n := range c.nodes {
		if n.Role() == Leader {
			want = n.Membership().PlainList()
			break
		}
	}
	if err := c.WaitMembership(want, 500); err != nil {
		t.Fatal(err)
	}

	// 所有仍在成员表中的存活节点：提交位置与已提交命令序列一致。
	var leader *Node
	for _, n := range c.nodes {
		if n.Role() == Leader {
			leader = n
			break
		}
	}
	base := leader.CommitIndex()
	if err := c.WaitCommit(base, 400); err != nil {
		t.Fatalf("未收敛到提交点 %d: %v", base, err)
	}
	var wantCmds []string
	for i := 2; i <= base; i++ {
		e, ok := leader.st.EntryAt(i)
		if !ok {
			t.Fatalf("领导者缺少 index=%d", i)
		}
		if e.Kind == KindCommand {
			wantCmds = append(wantCmds, string(e.Payload))
		}
	}
	for _, id := range want {
		n := c.nodes[id]
		if n == nil || !n.Alive() {
			continue
		}
		got := c.Commands(id)
		if len(got) != len(wantCmds) {
			t.Fatalf("%s 命令数不符: got=%v want=%d", id, got, len(wantCmds))
		}
		for i := range wantCmds {
			if got[i] != wantCmds[i] {
				t.Fatalf("%s 命令[%d]=%q want %q", id, i, got[i], wantCmds[i])
			}
		}
		if n.IsJoint() || n.PendingConf() {
			t.Fatalf("%s 收尾后仍 joint/pending", id)
		}
	}
}
