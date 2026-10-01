package raft

import "testing"

// TestBatchChangeLeaderCrash 批量变更进行中（联合阶段）领导者崩溃：
// 重启/重新选主后，变更必须被安全地推到完成（自动补追加 leave-joint），
// 已提交数据不丢、不回滚，普通日志继续提交。
func TestBatchChangeLeaderCrash(t *testing.T) {
	c := mustCluster(t, 201, "n1", "n2", "n3")
	idx, _ := c.Propose("cl", []byte("pre-crash"))
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNode("n5"); err != nil {
		t.Fatal(err)
	}
	leader := c.Leader()
	enterIdx, r := c.ProposeConfChangeBatch("cl", []NodeID{"n4", "n5"}, nil)
	if r != ReasonOK {
		t.Fatal(r)
	}
	// 等 enter-joint 在旧领导者处已提交（C_old,new 生效），但 leave-joint 尚未提交，
	// 在这个“变更中”的窗口杀掉领导者；新领导者必须接管并补追加 leave-joint。
	if err := c.WaitCommit(enterIdx, 200); err != nil {
		t.Fatal(err)
	}
	c.StopNode(leader)
	c.RunUntil(40) // 多数派重新选主

	want := []NodeID{"n1", "n2", "n3", "n4", "n5"}
	if err := c.WaitMembership(want, 400); err != nil {
		t.Fatalf("领导者崩溃后变更未收敛: %v", err)
	}
	// 崩溃的领导者稍后重启：必须追上同一成员表与全部已提交日志。
	if err := c.StartNode(leader); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitMembership(want, 300); err != nil {
		t.Fatal(err)
	}

	idx2, r := c.Propose("cl", []byte("post-crash"))
	if r != ReasonOK {
		t.Fatal(r)
	}
	if err := c.WaitCommit(idx2, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"pre-crash", "post-crash"}, want...)
	for _, id := range want {
		s := c.State(id)
		if s.Joint || s.Pending {
			t.Fatalf("%s 恢复后仍 joint/pending: %+v", id, s)
		}
	}
}

// TestBatchChangeEnterLeaderCrash 恰好在 enter-joint 已提交、
// leave-joint 尚未追加时领导者崩溃：新领导者必须接管并补追加 leave-joint。
func TestBatchChangeEnterLeaderCrash(t *testing.T) {
	c := mustCluster(t, 202, "n1", "n2", "n3")
	if err := c.AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	leader := c.Leader()
	if _, r := c.ProposeConfChangeBatch("cl", []NodeID{"n4"}, nil); r != ReasonOK {
		t.Fatal(r)
	}
	// 等 enter-joint 已复制到至少一个存活跟随者（新领导者拿得到该条目），
	// 但领导者仍处于联合阶段（leave-joint 尚未提交），在这个接管窗口杀掉领导者。
	enterReplicated := func() bool {
		l := c.nodes[leader]
		idx := l.lastConfIndex()
		if idx == 0 || !l.IsJoint() {
			return false
		}
		copies := 0
		for id, n := range c.nodes {
			if id == leader || !n.Alive() {
				continue
			}
			if n.st.LastIndex() >= idx {
				copies++
			}
		}
		return copies >= 1
	}
	deadline := c.Ticks() + 200
	killed := false
	for c.Ticks() < deadline && !killed {
		c.Step()
		if enterReplicated() {
			c.StopNode(leader)
			killed = true
		}
	}
	if !killed {
		t.Fatal("未能在 enter-joint 已复制的接管窗口内杀掉领导者")
	}
	c.RunUntil(30)
	want := []NodeID{"n1", "n2", "n3", "n4"}
	if err := c.WaitMembership(want, 400); err != nil {
		t.Fatalf("接管后变更未完成: %v", err)
	}
	if err := c.StartNode(leader); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitMembership(want, 300); err != nil {
		t.Fatal(err)
	}
	idx, r := c.Propose("cl", []byte("after-takeover"))
	if r != ReasonOK {
		t.Fatal(r)
	}
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"after-takeover"}, want...)
}

// TestBatchChangeWithPartition 批量加人期间把旧集合多数派隔开（3 节点隔开 2 个）：
// 联合阶段缺了旧集合多数派，enter-joint 不允许在隔离期间提交落定；
// 任何存活节点都不得切换到新成员表，更不得出现两个可提交的领导者；
// 网络恢复后变更安全完成，成员表最终收敛一致。
func TestBatchChangeWithPartition(t *testing.T) {
	c := mustCluster(t, 203, "n1", "n2", "n3")
	if err := c.AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNode("n5"); err != nil {
		t.Fatal(err)
	}
	// 隔离两个旧成员（若领导者在其中则换一种选择：始终保留旧领导者在多数派侧的反例，
	// 这里直接隔离 n2、n3 两个非领导者旧成员；若种子让其中之一是领导者则重选种子无必要，
	// 因为 WaitLeader 后我们按实际领导者调整）。
	leader := c.Leader()
	victims := []NodeID{"n1", "n2", "n3"}
	var isolated []NodeID
	for _, id := range victims {
		if id != leader && len(isolated) < 2 {
			isolated = append(isolated, id)
		}
	}
	if len(isolated) != 2 {
		t.Fatalf("测试前提不成立: leader=%s", leader)
	}
	for _, id := range isolated {
		c.Isolate(id)
	}
	c.RunUntil(5)
	if _, r := c.ProposeConfChangeBatch("cl", []NodeID{"n4", "n5"}, nil); r != ReasonOK {
		t.Fatal(r)
	}
	// 隔离期间给足时间：只有旧领导者与两个新节点互通，旧集合 {n1,n2,n3} 凑不齐多数派。
	target := NewMembership("n1", "n2", "n3", "n4", "n5")
	c.RunUntil(50)
	for id, n := range c.nodes {
		if !n.Alive() || c.net.partitioned[id] {
			continue
		}
		if !n.IsJoint() && n.membership.Equal(target) {
			t.Fatalf("%s 在旧多数派缺失期间不应落定到新成员表", id)
		}
	}
	// 同一时刻至多一个可推进提交的领导者（旧领导者缺旧多数派，无法提交含 enter 的位置）。
	leaders := 0
	for _, n := range c.nodes {
		if n.Alive() && !c.net.partitioned[n.id] && n.Role() == Leader {
			leaders++
		}
	}
	if leaders > 1 {
		t.Fatalf("变更期间出现 %d 个领导者", leaders)
	}
	// 恢复后变更必须完成。
	c.HealAll()
	want := []NodeID{"n1", "n2", "n3", "n4", "n5"}
	if err := c.WaitMembership(want, 400); err != nil {
		t.Fatal(err)
	}
	idx, r := c.Propose("cl", []byte("post-heal"))
	if r != ReasonOK {
		t.Fatal(r)
	}
	if err := c.WaitCommit(idx, 200); err != nil {
		t.Fatal(err)
	}
	assertAllCommands(t, c, []string{"post-heal"}, want...)
}
