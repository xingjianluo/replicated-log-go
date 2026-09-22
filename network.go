package raft

import (
	"math/rand"
	"sort"
)

// Network 是确定性消息网络：消息进入队列后按 Tick 驱动投递，
// 支持分区、丢包、重复与乱序注入，全部行为由注入的随机源决定，可复现。
type Network struct {
	log *Logger
	rng *rand.Rand

	// queued 消息按延迟计数，延迟归零即可投递。
	queued []queuedMsg

	// partitioned 标记被隔离的节点：涉及隔离节点的消息一律不可达。
	partitioned map[NodeID]bool
	// alive 由集群同步，用于对停止节点的消息给出 ReasonShutdown。
	alive func(NodeID) bool

	// 故障注入概率（确定性随机决定）
	dropRate      float64
	duplicateRate float64
	reorderRate   float64

	// 统计计数，便于测试区分不同失败原因。
	droppedCount     int
	partitionedCount int
	duplicatedCount  int
}

type queuedMsg struct {
	msg   OutMsg
	from  NodeID
	delay int
	seq   int
}

var netSeq int

// NewNetwork 创建网络；seed 决定全部随机行为。
func NewNetwork(log *Logger, seed int64, alive func(NodeID) bool) *Network {
	return &Network{
		log:         log,
		rng:         rand.New(rand.NewSource(seed)),
		partitioned: map[NodeID]bool{},
		alive:       alive,
	}
}

// SetFaultRates 设置丢包、重复、乱序概率（0~1）。
func (n *Network) SetFaultRates(drop, duplicate, reorder float64) {
	n.dropRate, n.duplicateRate, n.reorderRate = drop, duplicate, reorder
}

// reachable 判断消息在当前分区状态下是否可达（任一端被隔离即不可达）。
func (n *Network) reachable(from, to NodeID) bool {
	return !n.partitioned[from] && !n.partitioned[to]
}

// Send 入队一条消息；分区、停机、丢包在此判定并计入统计。
func (n *Network) Send(from NodeID, m OutMsg) {
	if n.alive != nil && !n.alive(m.To) {
		n.log.Warnf(string(m.To), 0, 0, 0, ReasonShutdown, "目标节点已停止，丢弃消息 from=%s", from)
		n.droppedCount++
		return
	}
	if !n.reachable(from, m.To) {
		n.log.Warnf(string(from), 0, 0, 0, ReasonPartitioned,
			"网络分区，消息不可达 to=%s 类型=%T", m.To, m.RPC)
		n.partitionedCount++
		return
	}
	if n.rng.Float64() < n.dropRate {
		n.log.Warnf(string(from), 0, 0, 0, ReasonDropped,
			"故障注入：丢包 to=%s 类型=%T", m.To, m.RPC)
		n.droppedCount++
		return
	}
	netSeq++
	delay := 1
	if n.rng.Float64() < n.reorderRate {
		// 乱序通过拉长个别消息的确定性延迟来体现：后发可能先至。
		delay = 3
	}
	n.queued = append(n.queued, queuedMsg{msg: m, from: from, delay: delay, seq: netSeq})

	if n.rng.Float64() < n.duplicateRate {
		// 重复消息晚 2 个 Tick 再投递一份。
		netSeq++
		n.queued = append(n.queued, queuedMsg{msg: m, from: from, delay: delay + 2, seq: netSeq})
		n.duplicatedCount++
		n.log.Warnf(string(from), 0, 0, 0, ReasonDuplicate,
			"故障注入：重复消息 to=%s 类型=%T", m.To, m.RPC)
	}
}

// Delivered 是一条实际投递给接收方的消息。
type Delivered struct {
	From NodeID
	Msg  OutMsg
}

// Deliver 推进一个 Tick 并返回此刻应投递的消息（按 seq 稳定排序）。
func (n *Network) Deliver() []Delivered {
	var due []queuedMsg
	var keep []queuedMsg
	for _, q := range n.queued {
		q.delay--
		if q.delay <= 0 {
			due = append(due, q)
		} else {
			keep = append(keep, q)
		}
	}
	n.queued = keep
	sort.SliceStable(due, func(i, j int) bool { return due[i].seq < due[j].seq })
	out := make([]Delivered, 0, len(due))
	for _, q := range due {
		out = append(out, Delivered{From: q.from, Msg: q.msg})
	}
	return out
}

// Isolate 将节点加入隔离分区。
func (n *Network) Isolate(id NodeID) {
	if !n.partitioned[id] {
		n.partitioned[id] = true
		n.log.Warnf(string(id), 0, 0, 0, ReasonPartitioned, "节点被隔离入少数派分区")
	}
}

// Heal 解除节点隔离。
func (n *Network) Heal(id NodeID) {
	if n.partitioned[id] {
		delete(n.partitioned, id)
		n.log.Infof(string(id), 0, 0, 0, ReasonOK, "节点网络恢复")
	}
}

// HealAll 解除全部分区。
func (n *Network) HealAll() {
	for id := range n.partitioned {
		delete(n.partitioned, id)
	}
	if len(n.partitioned) == 0 {
		n.log.Infof("cluster", 0, 0, 0, ReasonOK, "全部分区已愈合")
	}
}

// Pending 返回尚未投递的消息数（测试观察用）。
func (n *Network) Pending() int { return len(n.queued) }

// Stats 返回故障统计。
func (n *Network) Stats() (dropped, partitioned, duplicated int) {
	return n.droppedCount, n.partitionedCount, n.duplicatedCount
}
