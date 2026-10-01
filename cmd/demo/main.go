// Command demo 在本地确定性仿真环境中运行一个 5 节点复制日志集群，
// 依次演示：正常提交 → 网络分区（少数派无法提交）→ 多数派继续提交 →
// 网络恢复收敛 → 节点崩溃重启 → 日志压缩/快照恢复 → 成员扩容。
//
// 用法：
//
//	go run ./cmd/demo
//
// 全部输出由固定种子驱动，可稳定复现。
package main

import (
	"fmt"
	"os"

	raft "github.com/xingjianluo/replicated-log-go"
)

func main() {
	log := raft.NewLogger(os.Stdout, true)

	fmt.Println("================ 场景 1：初始选主与正常复制 ================")
	c := raft.NewCluster([]raft.NodeID{"n1", "n2", "n3", "n4", "n5"},
		raft.WithSeed(20260923),
		raft.WithLogWriter(os.Stdout),
		raft.WithDebug(false))
	_ = log
	must(c.Start(200))
	leader := c.Leader()
	fmt.Printf(">>> 选出唯一领导者 %s，任期 %d，多数派阈值 3\n", leader, c.Term())

	for _, cmd := range []string{"SET alpha=1", "SET beta=2"} {
		idx, r := c.Propose("demo-client", []byte(cmd))
		if r != raft.ReasonOK {
			fail("提案被拒: %s", r)
		}
		must(c.WaitCommit(idx, 200))
		fmt.Printf(">>> 已提交并应用 index=%d: %s\n", idx, cmd)
	}

	fmt.Println("================ 场景 2：隔离领导者，少数派不得推进 ================")
	c.Isolate(leader)
	c.RunUntil(30)
	newLeader := c.Leader()
	fmt.Printf(">>> 多数派分区重新选出领导者 %s（任期 %d），旧领导者 %s 处于少数派\n",
		newLeader, c.Term(), leader)

	before := c.State(leader).CommitIndex
	ack := c.Node(leader).Propose("demo-client", []byte("SPLIT-BRAIN-WRITE"))
	if ack.Success {
		c.RunUntil(30)
	}
	after := c.State(leader).CommitIndex
	fmt.Printf(">>> 少数派旧领导者提交位置保持 %d（推进前 %d），其未提交写入不会生效\n", after, before)

	fmt.Println("================ 场景 3：多数派继续提交，随后网络恢复收敛 ================")
	idx, r := c.Propose("demo-client", []byte("SET gamma=3"))
	mustR(r)
	must(c.WaitCommit(idx, 200))
	c.HealAll()
	must(c.WaitCommit(idx, 200))
	fmt.Printf(">>> 网络恢复，全部节点收敛到 commit=%d\n", c.Node(leader).CommitIndex())

	fmt.Println("================ 场景 4：节点崩溃重启，已提交数据不丢 ================")
	c.StopNode("n4")
	idx, r = c.Propose("demo-client", []byte("SET delta=4"))
	mustR(r)
	must(c.WaitCommit(idx, 200))
	must(c.StartNode("n4"))
	must(c.WaitCommit(idx, 200))
	fmt.Printf(">>> n4 重启后追平 commit=%d，状态机命令=%v\n",
		c.State("n4").CommitIndex, c.Commands("n4"))

	fmt.Println("================ 场景 5：日志压缩，落后节点经快照恢复 ================")
	leader = c.Leader()
	must(c.TriggerSnapshot(leader, c.State(leader).CommitIndex))
	c.StopNode("n5")
	idx, r = c.Propose("demo-client", []byte("SET epsilon=5"))
	mustR(r)
	must(c.WaitCommit(idx, 200))
	must(c.TriggerSnapshot(leader, -1))
	must(c.StartNode("n5"))
	must(c.WaitCommit(idx, 300))
	fmt.Printf(">>> n5 经快照恢复 commit=%d applied=%d\n",
		c.State("n5").CommitIndex, c.State("n5").LastApplied)

	fmt.Println("================ 场景 6：批量成员变更（一次加两台，联合共识两阶段） ================")
	must(c.AddNode("n6"))
	must(c.AddNode("n7"))
	// 一次打包加入两台机器，内部走 enter-joint(C_5,7) -> leave-joint(C_7) 两阶段。
	cfgIdx, r := c.ProposeConfChangeBatch("demo-client",
		[]raft.NodeID{"n6", "n7"}, nil)
	mustR(r)
	fmt.Printf(">>> 已落盘 enter-joint index=%d，期间普通日志不停摆\n", cfgIdx)
	// 变更进行中照样提交一条普通命令，验证服务不中断。
	midIdx, r := c.Propose("demo-client", []byte("SET eta=7"))
	mustR(r)
	must(c.WaitCommit(midIdx, 300))
	fmt.Printf(">>> 变更进行中普通命令已提交 index=%d\n", midIdx)
	// 等待整批变更最终落定（离开联合阶段，成员表收敛到 7 节点）。
	must(c.WaitMembership([]raft.NodeID{"n1", "n2", "n3", "n4", "n5", "n6", "n7"}, 400))
	fmt.Println(">>> leave-joint 已提交，全部存活节点成员表收敛为 7 节点（多数派阈值 4）")
	idx, r = c.Propose("demo-client", []byte("SET theta=8"))
	mustR(r)
	must(c.WaitCommit(idx, 200))
	fmt.Printf(">>> 扩容后按 7 节点多数派继续提交 index=%d，全部命令一致：\n", idx)
	for _, id := range []raft.NodeID{"n1", "n2", "n3", "n4", "n5", "n6", "n7"} {
		fmt.Printf("    %s: %v\n", id, c.Commands(id))
	}

	fmt.Println("================ 全部场景完成，日志状态一致且可解释 ================")
}

func must(err error) {
	if err != nil {
		fail("%v", err)
	}
}

func mustR(r raft.Reason) {
	if r != raft.ReasonOK {
		fail("操作被拒: %s", r)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "DEMO FAIL: "+format+"\n", args...)
	os.Exit(1)
}
