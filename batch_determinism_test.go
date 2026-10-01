package raft

import (
	"bytes"
	"testing"
)

// TestBatchChangeDeterministic 相同种子两次运行批量增删成员 + 故障注入，
// 全部节点的角色/任期/提交位置/命令序列与网络故障统计必须逐字节一致。
func TestBatchChangeDeterministic(t *testing.T) {
	snapshot := func(seed int64) string {
		c := NewCluster([]NodeID{"n1", "n2", "n3"},
			WithSeed(seed), WithFaultRates(0.05, 0.1, 0.1))
		if err := c.Start(400); err != nil {
			t.Fatal(err)
		}
		_ = c.AddNode("n4")
		_ = c.AddNode("n5")
		if _, r := c.ProposeConfChangeBatch("cl", []NodeID{"n4", "n5"}, nil); r != ReasonOK {
			t.Fatal(r)
		}
		if err := c.WaitMembership([]NodeID{"n1", "n2", "n3", "n4", "n5"}, 500); err != nil {
			t.Fatal(err)
		}
		i, _ := c.Propose("cl", []byte("between"))
		if err := c.WaitCommit(i, 300); err != nil {
			t.Fatal(err)
		}
		// 再做一批：移除 n5、加入新节点 n6。
		_ = c.AddNode("n6")
		if _, r := c.ProposeConfChangeBatch("cl", []NodeID{"n6"}, []NodeID{"n5"}); r != ReasonOK {
			t.Fatal(r)
		}
		if err := c.WaitMembership([]NodeID{"n1", "n2", "n3", "n4", "n6"}, 500); err != nil {
			t.Fatal(err)
		}
		i2, _ := c.Propose("cl", []byte("done"))
		if err := c.WaitCommit(i2, 300); err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		for _, id := range []NodeID{"n1", "n2", "n3", "n4", "n6"} {
			s := c.State(id)
			buf.WriteString(string(id))
			buf.WriteString(s.Role.String())
			buf.WriteString(itoa(s.Term))
			buf.WriteString(itoa(s.CommitIndex))
			buf.WriteString(itoa(s.LastApplied))
			for _, cmd := range c.fsms[id].Commands() {
				buf.WriteString(cmd)
			}
			buf.WriteString("|")
		}
		d, p, du := c.NetworkStats()
		buf.WriteString("net:")
		buf.WriteString(itoa(d))
		buf.WriteString(itoa(p))
		buf.WriteString(itoa(du))
		return buf.String()
	}
	a, b := snapshot(808), snapshot(808)
	if a != b {
		t.Fatalf("批量成员变更同种子结果不确定:\n%s\n%s", a, b)
	}
}
