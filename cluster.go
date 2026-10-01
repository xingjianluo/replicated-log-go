package raft

import (
	"fmt"
	"io"
	"math/rand"
)

// ClusterOption 配置确定性集群。
type ClusterOption func(*Cluster)

// WithNetworkSeed 注入网络随机种子（默认与集群种子一致）。
func WithNetworkSeed(seed int64) ClusterOption {
	return func(c *Cluster) { c.netSeed = seed }
}

// WithFaultRates 配置丢包/重复/乱序概率。
func WithFaultRates(drop, duplicate, reorder float64) ClusterOption {
	return func(c *Cluster) { c.drop, c.dup, c.reorder = drop, duplicate, reorder }
}

// WithLogWriter 注入日志输出。
func WithLogWriter(w io.Writer) ClusterOption {
	return func(c *Cluster) { c.logWriter = w }
}

// WithDebug 开启调试日志。
func WithDebug(on bool) ClusterOption {
	return func(c *Cluster) { c.debug = on }
}

// WithSeed 设置集群总种子（节点选举抖动与网络均由此派生，缺省为 1）。
func WithSeed(seed int64) ClusterOption {
	return func(c *Cluster) { c.seed = seed }
}

// Cluster 编排多个 Node 与确定性 Network，构成可本地运行的仿真集群。
type Cluster struct {
	log    *Logger
	nodes  map[NodeID]*Node
	stores map[NodeID]*Storage
	fsms   map[NodeID]*KVState
	net    *Network
	rng    *rand.Rand
	ticks  int

	// 客户端提案应答收集（键为客户端标识）。
	acks map[NodeID][]ClientAck

	seed      int64
	netSeed   int64
	drop      float64
	dup       float64
	reorder   float64
	logWriter io.Writer
	debug     bool

	nodeSeedCounter int64
}

// NewCluster 创建集群；ids 为初始投票成员，存储全新创建。
func NewCluster(ids []NodeID, opts ...ClusterOption) *Cluster {
	c := &Cluster{
		nodes:     map[NodeID]*Node{},
		stores:    map[NodeID]*Storage{},
		fsms:      map[NodeID]*KVState{},
		acks:      map[NodeID][]ClientAck{},
		seed:      1,
		logWriter: io.Discard,
	}
	for _, o := range opts {
		o(c)
	}
	if c.netSeed == 0 {
		c.netSeed = c.seed
	}
	c.log = NewLogger(c.logWriter, c.debug)
	c.net = NewNetwork(c.log, c.netSeed, func(id NodeID) bool {
		n, ok := c.nodes[id]
		return ok && n.Alive()
	})
	c.net.SetFaultRates(c.drop, c.dup, c.reorder)

	members := NewMembership(ids...)
	for _, id := range ids {
		c.nodeSeedCounter++
		st := NewStorage(id, c.log)
		fsm := NewKVState()
		n := NewNode(id, members, st, fsm, c.log, c.seed*1000+c.nodeSeedCounter)
		n.SetEmitter(func(m OutMsg) { c.collectAck(m) })
		c.bindReachable(n)
		c.nodes[id] = n
		c.stores[id] = st
		c.fsms[id] = fsm
	}
	return c
}

// bindReachable 注入“存活且未被网络隔离”判定，供节点在联合阶段
// 计算可表态成员多数派（活性）使用。
func (c *Cluster) bindReachable(n *Node) {
	id := n.ID()
	n.SetReachable(func(peer NodeID) bool {
		p, ok := c.nodes[peer]
		if !ok || !p.Alive() || p.removed {
			return false
		}
		return !c.net.partitioned[peer] && !c.net.partitioned[id]
	})
}

func (c *Cluster) collectAck(m OutMsg) {
	if a, ok := m.RPC.(*ClientAck); ok {
		c.acks[a.To] = append(c.acks[a.To], *a)
	}
}

// Acks 返回某客户端收到的全部应答并清空（测试用）。
func (c *Cluster) Acks(client NodeID) []ClientAck {
	out := c.acks[client]
	c.acks[client] = nil
	return out
}

// Start 启动全部节点并推进到选出稳定领导者（或超时返回错误）。
func (c *Cluster) Start(maxTicks int) error {
	for _, n := range c.nodes {
		n.Start()
	}
	// 让初始引导配置（index=1）被视为已提交：所有初始节点冷启动时已内置 commit=1。
	if err := c.WaitLeader(maxTicks); err != nil {
		return err
	}
	return nil
}

// WaitLeader 推进直到出现领导者。
func (c *Cluster) WaitLeader(maxTicks int) error {
	for i := 0; i < maxTicks; i++ {
		if c.Leader() != "" {
			return nil
		}
		c.Step()
	}
	return fmt.Errorf("等待领导者超时（%d ticks）", maxTicks)
}

// Leader 返回当前唯一领导者：同一任期取多数派成员里 role=Leader 的节点；
// 若出现同任期多个 Leader（理论上不应发生），返回空并交由日志解释。
func (c *Cluster) Leader() NodeID {
	var best NodeID
	bestTerm := 0
	for _, n := range c.nodes {
		if !n.Alive() || n.removed {
			continue
		}
		if n.Role() == Leader && n.Term() >= bestTerm {
			if best != "" && n.Term() == bestTerm {
				// 同任期双领导者属于协议违例信号。
				c.log.Warnf(string(n.id), n.Term(), 0, n.CommitIndex(), ReasonDeposed,
					"检测到同任期多个领导者：%s 与 %s", best, n.id)
				return ""
			}
			best = n.id
			bestTerm = n.Term()
		}
	}
	return best
}

// Term 返回领导者任期（无领导者返回 0）。
func (c *Cluster) Term() int {
	if l := c.Leader(); l != "" {
		return c.nodes[l].Term()
	}
	return 0
}

// Propose 向领导者提交命令，返回条目索引（仅表示落盘，不代表提交）。
func (c *Cluster) Propose(client NodeID, payload []byte) (int, Reason) {
	l := c.Leader()
	if l == "" {
		return 0, ReasonNoQuorum
	}
	ack := c.nodes[l].Propose(client, payload)
	if !ack.Success {
		return 0, ack.Reason
	}
	return ack.Index, ReasonOK
}

// ProposeConfChange 提交单节点成员变更（向后兼容的便捷接口，内部走联合共识）。
func (c *Cluster) ProposeConfChange(client NodeID, cc ConfChange) (int, Reason) {
	l := c.Leader()
	if l == "" {
		return 0, ReasonNoQuorum
	}
	ack := c.nodes[l].ProposeConfChange(client, cc)
	if !ack.Success {
		return 0, ack.Reason
	}
	return ack.Index, ReasonOK
}

// ProposeConfChangeBatch 一次提交批量成员变更（同时增/删多个节点）。
// 返回的索引是 enter-joint 条目的索引；整批变更最终落定请用 WaitMembership 等待。
func (c *Cluster) ProposeConfChangeBatch(client NodeID, add, remove []NodeID) (int, Reason) {
	l := c.Leader()
	if l == "" {
		return 0, ReasonNoQuorum
	}
	ack := c.nodes[l].ProposeConfChangeBatch(client, ConfChange{
		Op:     ConfBatchVoters,
		Add:    append([]NodeID(nil), add...),
		Remove: append([]NodeID(nil), remove...),
	})
	if !ack.Success {
		return 0, ack.Reason
	}
	return ack.Index, ReasonOK
}

// WaitMembership 推进直到所有在线、未隔离且仍属于集群的成员都离开联合阶段、
// 没有待生效变更，且生效成员表与 want 完全一致（顺序无关）；超时返回错误。
// 用于等待一批成员变更“最终落定”，而不仅是 enter-joint 条目提交。
func (c *Cluster) WaitMembership(want []NodeID, maxTicks int) error {
	wantSet := NewMembership(want...)
	for i := 0; i < maxTicks; i++ {
		if c.membershipSettled(wantSet) {
			return nil
		}
		c.Step()
	}
	return fmt.Errorf("等待成员表收敛到 %v 超时（%d ticks）", want, maxTicks)
}

func (c *Cluster) membershipSettled(want Membership) bool {
	ok := false
	for _, n := range c.nodes {
		if !n.Alive() || n.removed || c.net.partitioned[n.id] {
			continue
		}
		// 生效配置已不含自己的节点（刚提交完移除自己）不纳入收敛判定。
		if !n.membership.IsJoint() && n.membership.Size() > 0 && !n.membership.Contains(n.id) {
			continue
		}
		if n.membership.IsJoint() || n.pendingConf {
			return false
		}
		if !n.membership.Equal(want) {
			return false
		}
		ok = true
	}
	return ok
}

// Step 推进一个确定性时间单位：节点计时 -> 网络投递 -> 处理应答。
func (c *Cluster) Step() {
	// 1) 所有在线节点产生的定时消息入队（顺序按 NodeID 排序保证确定）。
	for _, id := range c.sortedNodeIDs() {
		n := c.nodes[id]
		if !n.Alive() || n.removed {
			continue
		}
		for _, m := range n.Tick() {
			c.net.Send(id, m)
		}
	}
	// 2) 网络投递本 Tick 到期消息。
	delivered := c.net.Deliver()
	// 3) 逐消息分发，处理产生的响应继续入队，在下一 Tick 送达（无嵌套时序）。
	for _, d := range delivered {
		target, ok := c.nodes[d.Msg.To]
		if !ok {
			continue
		}
		if !target.Alive() || target.removed {
			continue
		}
		var replies []OutMsg
		switch m := d.Msg.RPC.(type) {
		case *RequestVote:
			replies = target.HandleVote(*m)
		case *RequestVoteResult:
			replies = target.HandleVoteResult(*m)
		case *AppendEntries:
			replies = target.HandleAppend(*m)
		case *AppendResult:
			replies = target.HandleAppendResult(*m)
		case *InstallSnapshot:
			replies = target.HandleSnapshot(*m)
		case *InstallSnapshotResult:
			replies = target.HandleSnapshotResult(*m)
		case *ClientAck:
			c.collectAck(d.Msg)
			continue
		default:
			continue
		}
		for _, r := range replies {
			c.net.Send(target.id, r)
		}
	}
	c.ticks++
}

// RunUntil 推进 ticks 个时间单位。
func (c *Cluster) RunUntil(ticks int) {
	for i := 0; i < ticks; i++ {
		c.Step()
	}
}

// WaitCommit 推进直到 index 被多数派提交且所有在线未移除节点应用，或超时。
func (c *Cluster) WaitCommit(index, maxTicks int) error {
	for i := 0; i < maxTicks; i++ {
		if c.committedEverywhere(index) {
			return nil
		}
		c.Step()
	}
	return fmt.Errorf("等待 index=%d 提交超时（%d ticks）", index, maxTicks)
}

// committedEverywhere 判定 index 是否被当前在线、未被隔离、且仍属于其生效配置的成员提交并应用。
// 已通过成员变更移除的节点不再属于集群，不参与收敛判定（其已提交数据保留在其它节点）。
func (c *Cluster) committedEverywhere(index int) bool {
	for _, n := range c.nodes {
		if !n.Alive() || n.removed || c.net.partitioned[n.id] {
			continue
		}
		// 节点自身的生效配置已不含自己：说明“移除自己”的变更已在该节点提交。
		if n.Membership().Size() > 0 && !n.Membership().Contains(n.id) {
			continue
		}
		if n.CommitIndex() < index || n.LastApplied() < index {
			return false
		}
	}
	return true
}

// Isolate / Heal / HealAll 操作网络分区。
func (c *Cluster) Isolate(id NodeID) { c.net.Isolate(id) }
func (c *Cluster) Heal(id NodeID)    { c.net.Heal(id) }
func (c *Cluster) HealAll()          { c.net.HealAll() }

// StopNode 模拟节点崩溃（保留持久状态）。
func (c *Cluster) StopNode(id NodeID) {
	if n, ok := c.nodes[id]; ok {
		n.Stop()
	}
}

// StartNode 重启节点：复用同一 Storage/FSM，按持久状态恢复并继续协议。
func (c *Cluster) StartNode(id NodeID) error {
	n, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("未知节点 %s", id)
	}
	if n.Alive() {
		return nil
	}
	// 重新构造逻辑层即可模拟“重启”：Storage 持久状态不丢，FSM 保留。
	c.nodeSeedCounter++
	repl := NewNode(id, n.membership, c.stores[id], c.fsms[id], c.log, c.seed*2000+c.nodeSeedCounter)
	repl.SetEmitter(func(m OutMsg) { c.collectAck(m) })
	c.bindReachable(repl)
	repl.Start()
	c.nodes[id] = repl
	return nil
}

// AddNode 创建一个全新空日志节点并纳入编排；必须随后提交 ConfAddVoter 变更，
// 新节点才会成为投票成员并通过日志/快照追赶到一致状态。
func (c *Cluster) AddNode(id NodeID) error {
	if _, exists := c.nodes[id]; exists {
		return fmt.Errorf("节点 %s 已存在", id)
	}
	c.nodeSeedCounter++
	st := NewStorage(id, c.log)
	fsm := NewKVState()
	// 新节点空日志加入，不写本地引导配置，完全由领导者补齐。
	n := NewJoinNode(id, st, fsm, c.log, c.seed*3000+c.nodeSeedCounter)
	n.SetEmitter(func(m OutMsg) { c.collectAck(m) })
	c.bindReachable(n)
	n.Start()
	c.nodes[id] = n
	c.stores[id] = st
	c.fsms[id] = fsm
	c.log.Infof(string(id), 0, 0, 0, ReasonOK, "新节点已加入编排（等待成员变更生效）")
	return nil
}

// RemoveNode 从编排中彻底移除节点（通常在 ConfRemoveVoter 提交之后调用）。
func (c *Cluster) RemoveNode(id NodeID) {
	delete(c.nodes, id)
	c.log.Warnf(string(id), 0, 0, 0, ReasonOK, "节点已从集群编排移除")
}

// TriggerSnapshot 要求节点在 index 处做日志压缩快照（index<=0 表示在提交点）。
func (c *Cluster) TriggerSnapshot(id NodeID, index int) error {
	n, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("未知节点 %s", id)
	}
	if index <= 0 {
		index = n.CommitIndex()
	}
	_, err := n.CreateSnapshot(index)
	return err
}

// State 返回节点一致性观察视图。
func (c *Cluster) State(id NodeID) NodeView {
	n, ok := c.nodes[id]
	if !ok {
		return NodeView{ID: id}
	}
	return NodeView{
		ID:          id,
		Role:        n.Role(),
		Term:        n.Term(),
		Leader:      n.Leader(),
		CommitIndex: n.CommitIndex(),
		LastApplied: n.LastApplied(),
		LastIndex:   n.st.LastIndex(),
		Alive:       n.Alive(),
		Members:     n.Membership().List(),
		Joint:       n.Membership().IsJoint(),
		Pending:     n.PendingConf(),
	}
}

// NodeView 是节点一致性状态的只读视图。
type NodeView struct {
	ID          NodeID
	Role        Role
	Term        int
	Leader      NodeID
	CommitIndex int
	LastApplied int
	LastIndex   int
	Alive       bool
	Members     []NodeID
	// Joint 为 true 表示该节点当前生效的是联合配置 C_old,new。
	Joint bool
	// Pending 为 true 表示存在未提交配置条目或 leave-joint 尚未追加。
	Pending bool
}

// Ticks 返回已推进的时间单位数。
func (c *Cluster) Ticks() int { return c.ticks }

// NetworkStats 暴露网络故障统计（丢包/分区拦截/重复），便于区分失败原因。
func (c *Cluster) NetworkStats() (dropped, partitioned, duplicated int) {
	return c.net.Stats()
}

func (c *Cluster) sortedNodeIDs() []NodeID {
	ids := make([]NodeID, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	sortIDs(ids)
	return ids
}

func sortIDs(ids []NodeID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
}

// Node 返回指定节点（演示与深度断言用）；不存在返回 nil。
func (c *Cluster) Node(id NodeID) *Node { return c.nodes[id] }

// Commands 返回某节点状态机中已按序应用的命令（演示/断言用）。
func (c *Cluster) Commands(id NodeID) []string {
	if f, ok := c.fsms[id]; ok {
		return f.Commands()
	}
	return nil
}
