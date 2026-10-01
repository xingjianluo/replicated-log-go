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

	// learners 记录以无投票权身份加入编排、等待成员变更生效的追赶节点。
	// 它们会收到日志/快照复制但不计入多数派；成员变更落定后自动从中移除。
	learners map[NodeID]bool

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
		learners:  map[NodeID]bool{},
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
		c.nodes[id] = n
		c.stores[id] = st
		c.fsms[id] = fsm
	}
	return c
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

// ProposeConfChange 提交成员变更。
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
	// learner 的日志一旦收到包含自己的 enter-joint 条目，它在联合期间就已是
	// C_new 的正式投票成员（须参与新配置多数派）；此时即应从 learner 视图摘除，
	// 不必等到 leave-joint 提交，否则会被错误地排除出收敛判定。
	for id := range c.learners {
		if n, ok := c.nodes[id]; ok && n.isVoter(id) {
			delete(c.learners, id)
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
// 已通过成员变更移除的节点不再属于集群，不参与收敛判定（其已提交数据保留在其它节点）；
// 无投票权 learner 也不参与收敛判定（追赶进度不影响集群提交）。
func (c *Cluster) committedEverywhere(index int) bool {
	for _, n := range c.nodes {
		if !n.Alive() || n.removed || c.net.partitioned[n.id] {
			continue
		}
		if c.learners[n.id] && !n.isVoter(n.id) {
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
	var repl *Node
	if c.stores[id].LastIndex() == 0 && !c.stores[id].HasSnapshot() {
		// 尚未追平任何日志就重启的节点仍是空日志追赶者，走 join 路径。
		repl = NewJoinNode(id, c.stores[id], c.fsms[id], c.log, c.seed*2000+c.nodeSeedCounter)
	} else {
		repl = NewNode(id, n.membership, c.stores[id], c.fsms[id], c.log, c.seed*2000+c.nodeSeedCounter)
	}
	repl.SetEmitter(func(m OutMsg) { c.collectAck(m) })
	// 重启后重建 learner 注册表：集群已知的 learner 全部重新登记。
	for lid := range c.learners {
		repl.AddLearner(lid)
	}
	repl.Start()
	c.nodes[id] = repl
	return nil
}

// AddNode 创建一个全新空日志节点并纳入编排，以无投票权 learner 身份开始追赶：
// 它立即接收领导者的日志/快照复制，但不投票、不计入多数派、不能参选；
// 必须随后通过 ProposeConfChange(ConfAddVoter) 或 ProposeBatchConfChange
// 提交成员变更，落定后才成为投票成员。
// 若同名节点此前已被移除（含其持久存储），这里以全新空身份重新加入，
// 旧成员表与旧日志不会影响集群判断。
func (c *Cluster) AddNode(id NodeID) error {
	if _, exists := c.nodes[id]; exists {
		return fmt.Errorf("节点 %s 已存在", id)
	}
	// 曾经存在且被移除的节点：清掉旧持久状态，按全新成员重新加入。
	delete(c.stores, id)
	delete(c.fsms, id)
	c.nodeSeedCounter++
	st := NewStorage(id, c.log)
	fsm := NewKVState()
	n := NewJoinNode(id, st, fsm, c.log, c.seed*3000+c.nodeSeedCounter)
	n.SetEmitter(func(m OutMsg) { c.collectAck(m) })
	n.Start()
	c.nodes[id] = n
	c.stores[id] = st
	c.fsms[id] = fsm
	c.learners[id] = true
	// 向所有在线节点注册 learner：领导者立即开始向它复制，
	// 其余节点注册后可在当选领导者时无缝接管复制。
	for _, peer := range c.nodes {
		peer.AddLearner(id)
	}
	n.AddLearner(id) // 自身也记录，防止它在追平前因超时误发起选举
	c.log.Infof(string(id), 0, 0, 0, ReasonOK, "新节点以 learner 身份加入编排（无投票权，等待成员变更生效）")
	return nil
}

// ProposeBatchConfChange 原子提交一批成员变更（联合共识两阶段）。
// 调用前请先用 AddNode 把新增节点加入编排；本方法会先等待新增 learner 追平，
// 再提交 enter-joint，并推进到 leave-joint 提交、全部存活成员收敛后返回。
// 变更期间普通命令的提交不被阻塞。
func (c *Cluster) ProposeBatchConfChange(client NodeID, b BatchConfChange) (int, Reason) {
	l := c.Leader()
	if l == "" {
		return 0, ReasonNoQuorum
	}
	leader := c.nodes[l]
	// 先让全部新增 learner 追平领导者日志，保证联合阶段不拖慢普通提交。
	for _, add := range b.Adds {
		if _, ok := c.nodes[add]; !ok {
			return 0, ReasonUnknownVoter
		}
	}
	if r := c.waitLearnersCaughtUp(leader, b.Adds, 400); r != ReasonOK {
		return 0, r
	}
	ack := leader.ProposeBatchConfChange(client, b)
	if !ack.Success {
		return 0, ack.Reason
	}
	if r := c.waitBatchConfComplete(500); r != ReasonOK {
		return ack.Index, r
	}
	return ack.Index, ReasonOK
}

// waitLearnersCaughtUp 推进仿真直到领导者视角里全部新增节点都已复制并确认到
// 领导者最新日志。以领导者 matchIndex 为准（而非 learner 本地提交点）：
// 必须确认成功应答已回到领导者，随后 enter-joint 落盘时它们才能立刻计票。
func (c *Cluster) waitLearnersCaughtUp(leader *Node, adds []NodeID, maxTicks int) Reason {
	for i := 0; i < maxTicks; i++ {
		caught := true
		for _, add := range adds {
			n, ok := c.nodes[add]
			if !ok || !n.Alive() {
				return ReasonShutdown
			}
			if leader.matchIndex[add] < leader.st.LastIndex() {
				caught = false
			}
		}
		if caught {
			return ReasonOK
		}
		c.Step()
	}
	return ReasonLearnerLagging
}

// waitBatchConfComplete 推进直到领导者完成联合共识（leave-joint 提交）、
// 不再处于联合/待生效状态，或超时。
func (c *Cluster) waitBatchConfComplete(maxTicks int) Reason {
	for i := 0; i < maxTicks; i++ {
		l := c.Leader()
		if l != "" {
			leader := c.nodes[l]
			if leader.IsBatchConfComplete() {
				// 再推进若干 tick，让提交点与最终配置扩散到全部存活复制目标。
				c.RunUntil(10)
				return ReasonOK
			}
		}
		c.Step()
	}
	return ReasonNoQuorum
}

// WaitMembershipConverged 推进直到所有在线、未分区、未移除的成员
// 都收敛到完全相同的投票成员表（按字典序比较），或超时。
func (c *Cluster) WaitMembershipConverged(want []NodeID, maxTicks int) error {
	wantM := NewMembership(want...)
	for i := 0; i < maxTicks; i++ {
		ok := true
		for _, n := range c.nodes {
			if !n.Alive() || n.removed || c.net.partitioned[n.id] {
				continue
			}
			if n.Membership().Size() > 0 && !n.Membership().Contains(n.id) {
				continue // 已在本节点提交移除自己，等待编排摘除
			}
			if n.Joint() || !n.Membership().Equal(wantM) {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		c.Step()
	}
	return fmt.Errorf("等待成员表收敛到 %v 超时（%d ticks）", want, maxTicks)
}

// RemoveNode 从编排中彻底移除节点（通常在 ConfRemoveVoter/批量缩容提交之后调用）。
// 其持久存储一并丢弃；日后以同名 AddNode 重新加入时按全新空日志节点对待。
func (c *Cluster) RemoveNode(id NodeID) {
	delete(c.nodes, id)
	delete(c.stores, id)
	delete(c.fsms, id)
	delete(c.learners, id)
	for _, n := range c.nodes {
		n.RemoveLearner(id)
	}
	c.log.Warnf(string(id), 0, 0, 0, ReasonOK, "节点已从集群编排移除（旧存储丢弃）")
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
		Joint:       n.Joint(),
		Learner:     c.learners[id] && !n.isVoter(id),
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
	Joint       bool
	Learner     bool
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
