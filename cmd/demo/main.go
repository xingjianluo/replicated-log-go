// Command demo 在确定性模拟器中演示复制日志一致性组件在各类故障下的
// 行为：正常复制 → 少数派网络分区 → 节点重启恢复 → 成员增删 → 快照追赶。
// 运行方式：go run ./cmd/demo（加 -debug 可看到每次计票与提交判定）。
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/xingjianluo/replicated-log-go/raft"
	"github.com/xingjianluo/replicated-log-go/sim"
)

func main() {
	debug := flag.Bool("debug", false, "打印细粒度判定依据（计票/回退/阻断）")
	seed := flag.Int64("seed", 20260923, "确定性随机种子")
	flag.Parse()

	log := sim.NewLogger(os.Stdout, *debug)
	e := sim.NewEnv(log, *seed)
	ids := []raft.NodeID{"n1", "n2", "n3"}
	e.Bootstrap(ids)

	stage := func(title string) {
		fmt.Printf("\n==== %s ====\n", title)
	}
	check := func(key, want string) {
		n, v, ok := e.LogMatch(key)
		status := "OK"
		if !ok || v != want {
			status = "MISMATCH"
		}
		fmt.Printf("[校验] %s: %d 个在线节点值=%q 期望=%q -> %s\n", key, n, v, want, status)
	}
	// votingLeader 返回“当前投票配置内”的领导者，排除已移除但尚在降级途中的节点。
	votingLeader := func() raft.NodeID {
		for _, id := range []raft.NodeID{"n1", "n2", "n3", "n4"} {
			if e.IsLeader(id) {
				for _, v := range e.Status(id).Config.Voters {
					if v == id {
						return id
					}
				}
			}
		}
		return ""
	}

	stage("1. 选举与正常复制")
	if !e.RunUntilLeader(300) {
		panic("未选出领导者")
	}
	lead := e.Leader()
	fmt.Printf("领导者=%s term=%d\n", lead, e.Status(lead).Term)
	for i := range 5 {
		must(e.Propose(lead, "k", fmt.Sprintf("v%d", i)))
		e.Run(10)
	}
	check("k", "v4")

	stage("2. 少数派分区：隔离领导者，少数派不得自行提交")
	e.Network().IsolateNode(lead)
	e.Run(40)
	if err := e.Propose(lead, "stale", "x"); err != nil {
		fmt.Printf("少数派旧领导者提案被拒（原因可区分）: %v\n", err)
	}
	e.Run(20)
	var newLead raft.NodeID
	for _, id := range ids {
		if id != lead && e.IsLeader(id) {
			newLead = id
		}
	}
	fmt.Printf("多数派新领导者=%s term=%d\n", newLead, e.Status(newLead).Term)
	must(e.Propose(newLead, "k", "majority"))
	e.Run(10)

	stage("3. 网络恢复：冲突条目被显式修正，重新收敛")
	e.Network().HealNode(lead)
	e.Run(40)
	check("k", "majority")

	stage("4. 节点重启：磁盘保留、内存重建、已提交日志重放")
	lead = e.Leader()
	e.StopNode("n3")
	e.Run(5)
	must(e.Propose(lead, "after-restart", "kept"))
	e.Run(10)
	e.StartNode("n3")
	e.Run(40)
	check("after-restart", "kept")

	stage("5. 成员变更：新增 n4（按新多数派提交）后移除 n2")
	lead = e.Leader()
	e.PrepareNewNode("n4")
	must(e.ProposeConfChange(lead, raft.ConfChange{Type: raft.ConfAddNode, Node: "n4"}))
	e.Run(40)
	must(e.Propose(lead, "with-n4", "yes"))
	e.Run(20)
	lead = e.Leader()
	must(e.ProposeConfChange(lead, raft.ConfChange{Type: raft.ConfRemoveNode, Node: "n2"}))
	e.Run(40)
	check("with-n4", "yes")

	stage("6. 快照压缩 + 长期落后节点追赶")
	// n2 已被移除出配置，解除其网络隔离（它不再参与多数派投票）。
	e.Network().HealNode("n2")
	e.Run(20)
	lead = votingLeader()
	e.StopNode("n4")
	// 落后节点下线期间先产生一批日志，领导者在这批日志之后压缩。
	for i := range 5 {
		must(e.Propose(lead, "old", fmt.Sprintf("o%d", i)))
		e.Run(5)
	}
	lead = votingLeader()
	snap, err := e.CompactNode(lead)
	must(err)
	fmt.Printf("领导者快照 index=%d term=%d（n4 已落后于压缩点）\n", snap.Index, snap.Term)
	// 压缩之后再产生增量日志：n4 必须“先装快照、再追增量”才能追平。
	for i := range 5 {
		must(e.Propose(lead, "log", fmt.Sprintf("e%d", i)))
		e.Run(5)
	}
	e.StartNode("n4")
	e.Run(60)
	members := e.VotingMembers(votingLeader())
	n, v, ok := e.LogMatchAmong("old", members)
	fmt.Printf("[校验] 快照覆盖的 old: %d 个投票节点值=%q 收敛=%v\n", n, v, ok)
	n, v, ok = e.LogMatchAmong("log", members)
	fmt.Printf("[校验] 快照后的增量 log: %d 个投票节点值=%q 收敛=%v\n", n, v, ok)

	stage("完成：全部已提交状态在故障后一致且可解释")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
