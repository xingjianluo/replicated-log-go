package raft

import (
	"bytes"
	"testing"
)

// mustCluster 创建三节点集群并选出领导者。
func mustCluster(t *testing.T, seed int64, ids ...NodeID) *Cluster {
	t.Helper()
	c := NewCluster(ids, WithSeed(seed))
	if err := c.Start(200); err != nil {
		t.Fatalf("选主失败: %v", err)
	}
	return c
}

// assertAllCommands 断言所有给定节点的 FSM 命令序列与 want 一致。
func assertAllCommands(t *testing.T, c *Cluster, want []string, ids ...NodeID) {
	t.Helper()
	for _, id := range ids {
		got := c.fsms[id].Commands()
		if len(got) != len(want) {
			t.Fatalf("节点 %s 命令数不符: got=%v want=%v", id, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("节点 %s 命令[%d] 不符: got=%q want=%q", id, i, got[i], want[i])
			}
		}
	}
}

func TestElectUniqueLeaderPerTerm(t *testing.T) {
	c := mustCluster(t, 42, "n1", "n2", "n3")
	leader := c.Leader()
	if leader == "" {
		t.Fatal("未选出领导者")
	}
	// 同一任期至多一个领导者：其余节点必须不是 Leader。
	term := c.Term()
	leaderCount := 0
	for _, n := range c.nodes {
		if n.Role() == Leader && n.Term() == term {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("任期 %d 出现 %d 个领导者", term, leaderCount)
	}
	t.Logf("唯一领导者=%s 任期=%d 多数派=%d", leader, term, NewMembership("n1", "n2", "n3").Quorum())
}

func TestBasicReplicationAndApply(t *testing.T) {
	c := mustCluster(t, 7, "n1", "n2", "n3")
	for i, cmd := range []string{"SET a=1", "SET b=2", "SET c=3"} {
		idx, r := c.Propose("cl", []byte(cmd))
		if r != ReasonOK {
			t.Fatalf("提案 %d 被拒: %s", i, r)
		}
		if err := c.WaitCommit(idx, 200); err != nil {
			t.Fatalf("提交 %d: %v", idx, err)
		}
	}
	want := []string{"SET a=1", "SET b=2", "SET c=3"}
	assertAllCommands(t, c, want, "n1", "n2", "n3")
}

func TestIdempotentApply(t *testing.T) {
	s := NewKVState()
	if err := s.Apply(2, 1, []byte("x")); err != nil {
		t.Fatal(err)
	}
	// 同一索引重复应用必须幂等。
	if err := s.Apply(2, 1, []byte("x")); err != nil {
		t.Fatalf("重复应用应幂等: %v", err)
	}
	if len(s.Commands()) != 1 {
		t.Fatalf("幂等应用后命令数应为1, got %v", s.Commands())
	}
	// 相同索引不同内容必须报错（冲突显式化）。
	if err := s.Apply(2, 1, []byte("y")); err == nil {
		t.Fatal("相同索引不同内容应被拒绝")
	}
	// 快照/恢复往返一致。
	if err := s.Restore(s.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if s.LastIndex() != 2 || s.Commands()[0] != "x" {
		t.Fatalf("快照恢复后状态错误: %v", s.Commands())
	}
}

// TestDeterministic 相同种子两次运行的全部节点状态必须完全一致。
func TestDeterministic(t *testing.T) {
	run := func(seed int64) string {
		c := mustCluster(t, seed, "n1", "n2", "n3")
		for _, cmd := range []string{"p", "q", "r", "s"} {
			idx, _ := c.Propose("cl", []byte(cmd))
			if err := c.WaitCommit(idx, 200); err != nil {
				t.Fatal(err)
			}
		}
		var buf bytes.Buffer
		for _, id := range []NodeID{"n1", "n2", "n3"} {
			s := c.State(id)
			buf.WriteString(string(id))
			buf.WriteString(s.Role.String())
			buf.WriteString(itoa(s.Term))
			buf.WriteString(itoa(s.CommitIndex))
			buf.WriteString(itoa(s.LastApplied))
			for _, cmd := range c.fsms[id].Commands() {
				buf.WriteString(cmd)
			}
		}
		return buf.String()
	}
	a, b := run(99), run(99)
	if a != b {
		t.Fatalf("相同种子结果不确定:\n%s\n%s", a, b)
	}
}
