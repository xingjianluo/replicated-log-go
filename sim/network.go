package sim

import (
	"math/rand"

	"github.com/xingjianluo/replicated-log-go/raft"
)

// TimedMessage 是一条带投递时刻的在途消息。
type TimedMessage struct {
	DeliverAt int64
	Msg       raft.Message
}

// Network 模拟节点间消息传输，所有异常均可注入且由随机种子确定：
//   - 分区：双向断连矩阵，或整节点隔离；
//   - 延迟/乱序：每条消息在 [min,max] 内独立抽取延迟，自然产生乱序；
//   - 重复：按源节点配置的概率额外复制一份（延迟独立抽取）；
//   - 丢弃：按源节点配置的概率静默丢弃。
type Network struct {
	rng      *rand.Rand
	log      *Logger
	parted   map[[2]raft.NodeID]bool // 无向：a/b 之间双向阻断
	isolated map[raft.NodeID]bool    // 某节点与所有人阻断
	dropRate map[raft.NodeID]float64
	dupRate  map[raft.NodeID]float64
	delayMin int
	delayMax int
	stats    NetStats
}

// NetStats 是网络层的故障统计，便于测试区分失败原因。
type NetStats struct {
	Dropped     int
	Duplicated  int
	Partitioned int
}

// NewNetwork 创建全连通、无丢弃、无重复的网络，默认延迟 1~3 tick。
func NewNetwork(seed int64, log *Logger) *Network {
	return &Network{
		rng:      rand.New(rand.NewSource(seed)),
		log:      log,
		parted:   map[[2]raft.NodeID]bool{},
		isolated: map[raft.NodeID]bool{},
		dropRate: map[raft.NodeID]float64{},
		dupRate:  map[raft.NodeID]float64{},
		delayMin: 1,
		delayMax: 3,
	}
}

// SetDelayRange 设置消息延迟的闭区间（tick）。max>=min>=0。
func (n *Network) SetDelayRange(min, max int) {
	if min < 0 {
		min = 0
	}
	if max < min {
		max = min
	}
	n.delayMin, n.delayMax = min, max
}

func pairKey(a, b raft.NodeID) [2]raft.NodeID {
	if a < b {
		return [2]raft.NodeID{a, b}
	}
	return [2]raft.NodeID{b, a}
}

// SetPartition 设置/解除 a、b 之间的双向隔离。
func (n *Network) SetPartition(a, b raft.NodeID, isolated bool) {
	n.parted[pairKey(a, b)] = isolated
}

// IsolateNode 隔离单个节点（与所有其他节点双向断连）。
func (n *Network) IsolateNode(id raft.NodeID) { n.isolated[id] = true }

// HealNode 解除单节点隔离。
func (n *Network) HealNode(id raft.NodeID) { delete(n.isolated, id) }

// HealAll 解除全部分区与隔离（丢包/重复概率配置保留）。
func (n *Network) HealAll() {
	n.parted = map[[2]raft.NodeID]bool{}
	n.isolated = map[raft.NodeID]bool{}
}

// Isolated 报告节点是否被整节点隔离（IsolateNode）。
// 点对点分区的完整连通性由路由时的 reachable 判定。
func (n *Network) Isolated(id raft.NodeID) bool {
	return n.isolated[id]
}

// reachable 判断消息此刻是否允许通过（分区对延迟/重复不敏感，立即阻断）。
func (n *Network) reachable(from, to raft.NodeID) bool {
	if from == to {
		return true
	}
	if n.isolated[from] || n.isolated[to] {
		return false
	}
	return !n.parted[pairKey(from, to)]
}

// SetDropRate 设置源节点出口消息的丢弃概率 [0,1]。
func (n *Network) SetDropRate(id raft.NodeID, r float64) { n.dropRate[id] = clampRate(r) }

// SetDuplicateRate 设置源节点出口消息的重复概率 [0,1]。
func (n *Network) SetDuplicateRate(id raft.NodeID, r float64) { n.dupRate[id] = clampRate(r) }

func clampRate(r float64) float64 {
	if r < 0 {
		return 0
	}
	if r > 1 {
		return 1
	}
	return r
}

// Route 决定一条消息在 now 时刻如何投递：
// 返回 0 条（分区阻断/丢弃）、1 条（正常）或 2 条（重复）。
// 所有随机性只来自初始化种子，因此同一脚本结果可稳定复现。
func (n *Network) Route(m raft.Message, now int64) []TimedMessage {
	if !n.reachable(m.From, m.To) {
		n.stats.Partitioned++
		n.log.Debugf("网络阻断 %s->%s %s（分区）", m.From, m.To, m.Type)
		return nil
	}
	if r := n.dropRate[m.From]; r > 0 && n.rng.Float64() < r {
		n.stats.Dropped++
		n.log.Debugf("网络丢弃 %s->%s %s（概率 %.2f）", m.From, m.To, m.Type, r)
		return nil
	}
	delay := n.delayMin
	if n.delayMax > n.delayMin {
		delay += n.rng.Intn(n.delayMax - n.delayMin + 1)
	}
	out := []TimedMessage{{DeliverAt: now + int64(delay), Msg: cloneMessage(m)}}
	if r := n.dupRate[m.From]; r > 0 && n.rng.Float64() < r {
		n.stats.Duplicated++
		// 重复副本独立延迟，刻意制造乱序与重复并存。
		d2 := n.delayMin
		if n.delayMax > n.delayMin {
			d2 += n.rng.Intn(n.delayMax - n.delayMin + 1)
		}
		out = append(out, TimedMessage{DeliverAt: now + int64(d2), Msg: cloneMessage(m)})
		n.log.Debugf("网络复制 %s->%s %s（概率 %.2f，延迟 %d/%d）",
			m.From, m.To, m.Type, r, delay, d2)
	}
	return out
}

// Stats 返回网络统计快照。
func (n *Network) Stats() NetStats { return n.stats }

func cloneMessage(m raft.Message) raft.Message {
	cp := m
	if len(m.Entries) > 0 {
		cp.Entries = append([]raft.Entry(nil), m.Entries...)
	}
	return cp
}
