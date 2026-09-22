package raft

import "testing"

// TestDuplicateAndReorder 在重复与乱序注入下提交多条命令：
// 重复 AppendEntries 与重复应答必须被幂等处理，最终状态一致。
func TestDuplicateAndReorder(t *testing.T) {
	c := NewCluster([]NodeID{"n1", "n2", "n3"},
		WithSeed(55),
		WithFaultRates(0.0, 0.25, 0.25)) // 无丢包：重复25%，乱序25%
	if err := c.Start(300); err != nil {
		t.Fatal(err)
	}
	var last int
	for i := 0; i < 8; i++ {
		idx, r := c.Propose("cl", []byte("dup-"+itoa(i)))
		if r != ReasonOK {
			t.Fatal(r)
		}
		last = idx
		if err := c.WaitCommit(idx, 400); err != nil {
			t.Fatalf("第 %d 条提交失败: %v", i, err)
		}
	}
	if err := c.WaitCommit(last, 100); err != nil {
		t.Fatal(err)
	}
	var want []string
	for i := 0; i < 8; i++ {
		want = append(want, "dup-"+itoa(i))
	}
	assertAllCommands(t, c, want, "n1", "n2", "n3")
	_, _, dup := c.NetworkStats()
	if dup == 0 {
		t.Fatal("期望发生至少一次重复消息注入")
	}
	t.Logf("重复消息注入次数=%d，集群在重复/乱序下保持一致", dup)
}

// TestDropThenRecover 间歇丢包下系统仍可推进，停掉丢包后完全收敛。
func TestDropThenRecover(t *testing.T) {
	c := NewCluster([]NodeID{"n1", "n2", "n3"},
		WithSeed(56),
		WithFaultRates(0.2, 0.0, 0.1))
	if err := c.Start(400); err != nil {
		t.Fatal(err)
	}
	idx, r := c.Propose("cl", []byte("lossy-cmd"))
	if r != ReasonOK {
		t.Fatal(r)
	}
	// 丢包存在时不要求有界时间内成功，故先关闭注入再等待收敛。
	c.net.SetFaultRates(0, 0, 0)
	if err := c.WaitCommit(idx, 400); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"lossy-cmd"}, "n1", "n2", "n3")
	dropped, _, _ := c.NetworkStats()
	t.Logf("丢包注入次数=%d，恢复后一致提交 index=%d", dropped, idx)
}

// TestDeterministicFaultRun 同种子的故障注入运行必须逐节点复现。
func TestDeterministicFaultRun(t *testing.T) {
	snapshot := func(seed int64) string {
		c := NewCluster([]NodeID{"n1", "n2", "n3"},
			WithSeed(seed), WithFaultRates(0.05, 0.1, 0.1))
		if err := c.Start(400); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 5; i++ {
			idx, _ := c.Propose("cl", []byte("f-"+itoa(i)))
			if err := c.WaitCommit(idx, 400); err != nil {
				t.Fatal(err)
			}
		}
		out := ""
		for _, id := range []NodeID{"n1", "n2", "n3"} {
			s := c.State(id)
			out += string(id) + ":" + s.Role.String() + ":" + itoa(s.Term) +
				":" + itoa(s.CommitIndex) + ":" + itoa(s.LastApplied) + ";"
			for _, cmd := range c.fsms[id].Commands() {
				out += cmd + ","
			}
			out += "|"
		}
		d, p, du := c.NetworkStats()
		out += "net:" + itoa(d) + ":" + itoa(p) + ":" + itoa(du)
		return out
	}
	if a, b := snapshot(77), snapshot(77); a != b {
		t.Fatalf("故障注入运行不可复现:\n%s\n%s", a, b)
	}
}
