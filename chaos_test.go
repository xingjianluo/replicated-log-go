package raft

import "testing"

// TestChaosMixedFailures 在 5 节点上叠加：周期性分区、节点崩溃重启、
// 丢包/重复/乱序注入，持续提交命令；最终恢复后所有存活成员的已提交日志与状态机必须一致。
func TestChaosMixedFailures(t *testing.T) {
	c := NewCluster([]NodeID{"n1", "n2", "n3", "n4", "n5"},
		WithSeed(20260923),
		WithFaultRates(0.06, 0.08, 0.08))
	if err := c.Start(400); err != nil {
		t.Fatal(err)
	}

	committed := map[int]string{}
	next := 2 // index 1 为引导配置
	propose := func(cmd string) {
		idx, r := c.Propose("cl", []byte(cmd))
		if r == ReasonOK {
			committed[idx] = cmd
			next = idx + 1
		}
	}

	totalTicks := 400
	for tick := 0; tick < totalTicks; tick++ {
		// 每 10 个 tick 尝试提一条命令。
		if tick%10 == 2 {
			propose("c" + itoa(next))
		}
		// 周期性制造/愈合少数派分区。
		switch tick % 80 {
		case 20:
			c.Isolate("n5")
		case 50:
			c.Heal("n5")
		}
		// 周期性让 n4 崩溃重启。
		switch tick % 120 {
		case 30:
			c.StopNode("n4")
		case 70:
			_ = c.StartNode("n4")
		}
		c.Step()
	}

	// 关闭故障、愈合分区，给足时间收敛。
	c.net.SetFaultRates(0, 0, 0)
	c.HealAll()
	_ = c.StartNode("n4")
	c.RunUntil(300)

	// 找到任期最高的领导者与它的提交点，作为收敛基准。
	var leader *Node
	for _, n := range c.nodes {
		if n.Role() == Leader {
			if leader == nil || n.Term() > leader.Term() {
				leader = n
			}
		}
	}
	if leader == nil {
		t.Fatal("混沌结束后无领导者")
	}
	base := leader.CommitIndex()
	if err := c.WaitCommit(base, 400); err != nil {
		t.Fatalf("存活节点未收敛到提交点 %d: %v", base, err)
	}

	// 断言所有存活成员的提交位置、最后日志索引/任期、状态机命令完全一致。
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
	for id, n := range c.nodes {
		if n.removed || !n.Alive() {
			continue
		}
		if n.CommitIndex() != base || n.LastApplied() != base {
			t.Fatalf("节点 %s 未收敛 commit=%d applied=%d base=%d",
				id, n.CommitIndex(), n.LastApplied(), base)
		}
		// 已提交前缀的索引/任期逐条一致（冲突条目必须已被显式修正）。
		for i := 2; i <= base; i++ {
			lt, _ := leader.st.TermAt(i)
			gt, ok := n.st.TermAt(i)
			if !ok || lt != gt {
				t.Fatalf("节点 %s index=%d 日志任期不一致 leader=%d got=%d(%v)",
					id, i, lt, gt, ok)
			}
		}
		assertAllCommands(t, c, wantCmds, id)
	}
	dropped, part, dup := c.NetworkStats()
	t.Logf("混沌测试收敛：提交到 index=%d 命令数=%d 丢包=%d 分区拦截=%d 重复=%d",
		base, len(wantCmds), dropped, part, dup)
}
