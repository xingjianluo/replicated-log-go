package raft

import (
	"fmt"
	"math/rand"
)

// 确定性仿真的超时常量（单位：Tick）。
const (
	heartbeatTicks = 2
	electionMin    = 8
	electionMax    = 15
	maxElectionGap = electionMax - electionMin
)

// OutMsg 是节点发出的待投递消息信封。
type OutMsg struct {
	To  NodeID
	RPC any // *RequestVote / *RequestVoteResult / *AppendEntries / *AppendResult / *InstallSnapshot / *InstallSnapshotResult / *ClientAck
}

// ClientAck 是对客户端提案（命令或成员变更）的应答。
type ClientAck struct {
	To      NodeID
	Term    int
	Index   int // 条目落盘后的索引；未提交不等于成功
	Success bool
	Reason  Reason
}

// Node 是单个 Raft 节点的纯逻辑状态机：所有输入来自 Handle* 与 Tick，
// 所有输出为 OutMsg；不自行起协程、不访问真实网络与时钟，保证确定性。
type Node struct {
	id   NodeID
	log  *Logger
	st   *Storage
	fsm  FSM
	rng  *rand.Rand // 注入的确定性随机源，用于选举超时抖动
	role Role

	// 易失状态
	commitIndex int
	lastApplied int
	leader      NodeID
	alive       bool

	// 选举计时（单位为 Tick）
	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	// 当前生效配置：常规期间为当前投票集合；联合共识期间为目标配置 C_new。
	membership Membership
	// prevMembership 为“未提交单节点变更”之前的旧配置；
	// 变更条目提交前，领导者需向旧∪新并集复制，确保被移除者也能收到移除条目。
	prevMembership Membership
	// joint=true 表示当前处于联合共识中间态（enter-joint 已落盘，
	// leave-joint 尚未提交）：jointOld 为 C_old，membership 为 C_new，
	// 提交与选举都必须同时满足双方多数派。
	joint    bool
	jointOld Membership
	// 上一次成员变更尚未提交时为 true（单节点变更与联合共识两阶段均算）。
	pendingConf bool
	// 本节点已被成员变更移除
	removed bool
	// learners 为无投票权的追赶节点集合：接收日志与快照、回报进度，
	// 但不计入任何多数派，也不能参选或投票；批量变更的新增节点先作为 learner 追平。
	learners map[NodeID]struct{}
	// 已复制（尚未提交）的联合共识状态：仅用于领导者向旧∪新并集复制。
	// 注意：多数派计数、选举、提交判定一律只使用“已提交配置视图”
	// （membership / joint / jointOld），跟随者收到未提交的 leave-joint 时
	// 不能据此提前退出联合态，否则会按单多数派提交，破坏联合共识安全性。
	uncommittedJoint    bool
	uncommittedJointOld Membership
	uncommittedMembers  Membership // 已复制但未提交的目标配置 C_new
	hasUncommittedConf  bool

	// removedPeers 是刚刚在 leave-joint 中被移出配置的节点：它们已经收到
	// leave 条目，但可能还没收到携带“leave 提交点”的最后一次心跳。
	// 领导者在它们确认该提交点之前继续向其发送心跳（不计入任何多数派），
	// 使其能在本地确认自己被移除并干净退出，随后再摘除。
	removedPeers map[NodeID]int
	// everVoter 表示本节点是否曾作为正式投票成员进入过“已提交”的配置视图。
	// 全新追赶节点（空日志 join）初始为 false；只有包含自己的配置到达提交点后
	// 才置 true（单调），用于区分“被移除的老成员”与“追平中的新成员”。
	// 注意：只复制到未提交日志（可能随后被冲突截断）不能置位。
	everVoter bool

	// 领导者可变状态
	nextIndex  map[NodeID]int
	matchIndex map[NodeID]int
	votes      map[NodeID]bool

	// 待复制提案队列（领导者）：propose 已落盘，等待提交后回调
	pending []pendingProposal

	// emit 由 Cluster 注入，用于异步客户端应答（不经过 RPC 通道）。
	emit func(OutMsg)
}

// SetEmitter 注入客户端应答输出回调。
func (n *Node) SetEmitter(f func(OutMsg)) { n.emit = f }

type pendingProposal struct {
	index int
	ackTo NodeID
	kind  EntryKind
}

// NewNode 创建初始集群节点；若存储中已有状态（重启），据此恢复。
// 初始集群成员冷启动时写入 index=1 的引导配置条目并视为已提交；
// 已有存储时从快照与日志重建配置视图。
// 后续加入集群的追赶节点请用 NewJoinNode（空日志、不写引导配置）。
func NewNode(id NodeID, initial Membership, st *Storage, fsm FSM, log *Logger, seed int64) *Node {
	return newNode(id, initial, st, fsm, log, seed, false)
}

// NewJoinNode 创建以空日志加入集群的新节点：不写本地引导配置，
// 完全通过领导者的快照或日志补齐 index=1 起的历史；加入配置前不参与选举。
func NewJoinNode(id NodeID, st *Storage, fsm FSM, log *Logger, seed int64) *Node {
	return newNode(id, NewMembership(), st, fsm, log, seed, true)
}

func newNode(id NodeID, initial Membership, st *Storage, fsm FSM, log *Logger, seed int64, freshJoin bool) *Node {
	n := &Node{
		id:           id,
		log:          log,
		st:           st,
		fsm:          fsm,
		rng:          rand.New(rand.NewSource(seed)),
		role:         Follower,
		nextIndex:    map[NodeID]int{},
		matchIndex:   map[NodeID]int{},
		votes:        map[NodeID]bool{},
		learners:     map[NodeID]struct{}{},
		removedPeers: map[NodeID]int{},
		membership:   initial,
	}
	n.resetElectionTimeout()

	switch {
	case st.HasSnapshot():
		// 重启且存在快照：恢复快照状态。
		snap := st.SnapshotCopy()
		n.commitIndex = snap.LastIncludedIndex
		n.lastApplied = snap.LastIncludedIndex
		n.membership = snap.Membership
		if err := fsm.Restore(snap.Data); err != nil {
			n.log.Warnf(string(id), st.CurrentTerm(), snap.Membership.Quorum(), n.commitIndex,
				ReasonRejectedByApplication, "FSM 快照恢复失败: %v", err)
		}
		n.rebuildMembership()
		n.everVoter = snap.Membership.Contains(id) || (n.joint && n.jointOld.Contains(id))
		n.log.Infof(string(id), st.CurrentTerm(), snap.Membership.Quorum(), n.commitIndex,
			ReasonOK, "从快照重启 lastApplied=%d 成员数=%d", n.lastApplied, snap.Membership.Size())
	case st.LastIndex() > 0:
		// 重启但没有快照：从持久日志重建配置视图。
		n.rebuildMembership()
		n.everVoter = n.membership.Contains(id) || (n.joint && n.jointOld.Contains(id)) || n.removed
		n.log.Infof(string(id), st.CurrentTerm(), n.membership.Quorum(), n.commitIndex,
			ReasonOK, "从持久日志重启 lastIndex=%d", st.LastIndex())
	default:
		if freshJoin {
			// 追赶节点：空日志、空配置、提交位置为 0，等待领导者补齐。
			n.membership = NewMembership()
			n.everVoter = false
			n.log.Infof(string(id), 0, 0, 0, ReasonOK,
				"新节点空日志加入，等待领导者快照/日志补齐")
			break
		}
		// 初始集群成员冷启动：写入引导配置条目（term 0，index 1），彼此一致。
		st.TruncateAndAppend(0, []LogEntry{{
			Kind:    KindConfChange,
			Payload: encodeConfChange(ConfChange{Op: ConfSetVoters, Members: initial.List()}),
		}})
		n.membership = initial
		n.everVoter = initial.Contains(id)
		n.commitIndex = 1
		n.lastApplied = 1
		n.log.Infof(string(id), 0, initial.Quorum(), 1,
			ReasonOK, "冷启动写入引导配置 成员数=%d 多数派=%d", initial.Size(), initial.Quorum())
	}
	return n
}

// configView 是配置重放到某一日志位置时得到的配置视图。
type configView struct {
	// current 是该位置生效的配置：常规期间为当前投票集合，
	// 联合共识中间态为目标配置 C_new。
	current  Membership
	joint    bool
	jointOld Membership // joint=true 时的旧配置 C_old
	// confIndex 是该位置及之前最后一个配置条目的索引（无则为 0）。
	confIndex int
}

// replayConfigView 从快照配置出发，重放快照点之后（含）直到 throughIndex 的全部配置条目。
// 单节点增删/引导条目直接就地改写当前配置；
// enter-joint 条目进入联合中间态（current=C_new, jointOld=C_old）；
// leave-joint 条目退出联合态并固定为 C_new。
func replayConfigView(st *Storage, through int) configView {
	v := configView{}
	if st.HasSnapshot() {
		snap := st.SnapshotCopy()
		v.current = snap.Membership
		v.joint = snap.JointValid
		v.jointOld = snap.JointOld
		v.confIndex = snap.LastIncludedIndex
	}
	for _, e := range st.AllEntries() {
		if e.Index > through {
			break
		}
		if e.Kind != KindConfChange {
			continue
		}
		cc, err := decodeConfChange(e.Payload)
		if err != nil {
			continue
		}
		switch cc.Op {
		case ConfEnterJoint:
			v.jointOld = v.current
			v.current = NewMembership(cc.Members...)
			v.joint = true
			v.confIndex = e.Index
		case ConfLeaveJoint:
			v.current = NewMembership(cc.Members...)
			v.joint = false
			v.jointOld = Membership{}
			v.confIndex = e.Index
		default:
			v.current = v.current.Apply(cc)
			v.confIndex = e.Index
		}
	}
	return v
}

// rebuildMembership 从快照配置出发重放全部配置条目，重建当前配置、
// 联合共识状态与 pendingConf（重启恢复路径）。
func (n *Node) rebuildMembership() {
	v := replayConfigView(n.st, n.st.LastIndex())
	n.membership = v.current
	n.joint = v.joint
	n.jointOld = v.jointOld
	// 联合共识期间 enter-joint 虽已提交，但 leave-joint 未落定，仍算 pending；
	// 非联合期间 pending 仅取决于最后一个配置条目是否尚未提交。
	n.pendingConf = v.joint || v.confIndex > n.commitIndex
	if v.joint {
		n.prevMembership = v.jointOld
	}
	if !v.current.Contains(n.id) && v.confIndex > 0 && v.confIndex <= n.commitIndex && !v.joint {
		n.removed = true
	}
}

// ID 返回节点标识。
func (n *Node) ID() NodeID { return n.id }

// Term 返回当前任期。
func (n *Node) Term() int { return n.st.CurrentTerm() }

// CommitIndex 返回已提交索引。
func (n *Node) CommitIndex() int { return n.commitIndex }

// LastApplied 返回已应用到状态机的索引。
func (n *Node) LastApplied() int { return n.lastApplied }

// Role 返回当前角色。
func (n *Node) Role() Role { return n.role }

// Leader 返回当前认可的领导者。
func (n *Node) Leader() NodeID { return n.leader }

// Membership 返回当前生效配置的副本。
// 常规期间为当前投票集合；联合共识期间为目标配置 C_new。
func (n *Node) Membership() Membership { return n.membership }

// Joint 返回当前是否处于联合共识中间态。
func (n *Node) Joint() bool { return n.joint }

// PendingConfChange 返回是否存在尚未落定的成员变更。
func (n *Node) PendingConfChange() bool { return n.pendingConf }

// effectiveVoters 返回当前真正有投票权的成员集合：
// 常规期间即当前配置；联合期间为 C_old∪C_new。
func (n *Node) effectiveVoters() Membership {
	if n.joint {
		return n.jointOld.Union(n.membership)
	}
	return n.membership
}

// isVoter 判断节点当前是否有投票权（含联合共识双方配置）。
func (n *Node) isVoter(id NodeID) bool {
	if n.membership.Contains(id) {
		return true
	}
	return n.joint && n.jointOld.Contains(id)
}

// AddLearner 注册一个无投票权的追赶节点：它会收到日志/快照复制，
// 但不参与选举与多数派计数。通常在集群编排新增节点时对所有节点调用；
// 对节点自身也生效（追赶节点需要据此抑制参选与“自我移除”误判）。
func (n *Node) AddLearner(id NodeID) {
	if n.isVoter(id) {
		return
	}
	n.learners[id] = struct{}{}
}

// RemoveLearner 注销追赶节点。
func (n *Node) RemoveLearner(id NodeID) {
	delete(n.learners, id)
}

// IsLearner 判断节点当前是否为 learner。
func (n *Node) IsLearner(id NodeID) bool { _, ok := n.learners[id]; return ok }

// pruneLearners 丢弃已经进入投票配置的 learner（成员变更在本节点落定后调用）。
func (n *Node) pruneLearners() {
	for id := range n.learners {
		if n.isVoter(id) {
			delete(n.learners, id)
		}
	}
}

// Alive 返回节点是否在线。
func (n *Node) Alive() bool { return n.alive }

// FSM 返回关联的状态机（测试断言用）。
func (n *Node) FSM() FSM { return n.fsm }

// ---- 内部：角色转换与计时 ----

func (n *Node) quorum() int { return n.membership.Quorum() }

func (n *Node) resetElectionTimeout() {
	n.electionTimeout = electionMin + n.rng.Intn(maxElectionGap+1)
	n.electionElapsed = 0
}

func (n *Node) becomeFollower(term int, leader NodeID) {
	// 进入（新）任期：若是更高任期则必须清空本任期投票，使撞车选举可在下一任期化解；
	// 若 term 等于当前任期（如收到现任领导者心跳），保留已有投票。
	votedFor := n.st.VotedFor()
	if term > n.st.CurrentTerm() {
		votedFor = ""
	}
	n.st.SaveTermVote(term, votedFor)
	n.role = Follower
	n.leader = leader
	n.votes = map[NodeID]bool{}
	n.resetElectionTimeout()
	n.log.Infof(string(n.id), term, n.quorum(), n.commitIndex, ReasonOK,
		"角色转为 follower 认可领导者=%s", leader)
}

func (n *Node) becomeCandidate() []OutMsg {
	// learner / 已不在任何生效配置中的节点不能拉票参选。
	if !n.isVoter(n.id) {
		n.log.Warnf(string(n.id), n.st.CurrentTerm(), n.quorum(), n.commitIndex, ReasonNotVoter,
			"本节点无投票权（成员数=%d），不发起选举", n.effectiveVoters().Size())
		n.resetElectionTimeout()
		return nil
	}
	term := n.st.CurrentTerm() + 1
	n.st.SaveTermVote(term, n.id) // 候选人先投自己
	n.role = Candidate
	n.leader = ""
	n.votes = map[NodeID]bool{n.id: true}
	n.resetElectionTimeout()

	var out []OutMsg
	lastTerm, _ := n.st.TermAt(n.st.LastIndex())
	rv := RequestVote{
		From:         n.id,
		Term:         term,
		LastLogIndex: n.st.LastIndex(),
		LastLogTerm:  lastTerm,
	}
	peers := n.effectiveVoters().List()
	for _, peer := range peers {
		if peer == n.id {
			continue
		}
		out = append(out, OutMsg{To: peer, RPC: &rv})
	}
	n.log.Infof(string(n.id), term, n.quorum(), n.commitIndex, ReasonOK,
		"开始选举 自投1票 向 %d 个成员拉票 lastIndex=%d lastTerm=%d",
		len(peers)-1, rv.LastLogIndex, rv.LastLogTerm)
	return out
}

func (n *Node) becomeLeader() []OutMsg {
	term := n.st.CurrentTerm()
	n.role = Leader
	n.leader = n.id
	if !n.joint {
		n.prevMembership = n.membership
	}
	n.initReplicationState()
	// 接手“最近一次 leave-joint 已提交但旧成员尚未确认”的场景，补发最后心跳。
	n.recoverRemovedPeers()
	n.heartbeatElapsed = 0
	n.log.Infof(string(n.id), term, n.quorum(), n.commitIndex, ReasonOK,
		"获得多数派，成为领导者 成员数=%d 多数派=%d joint=%v",
		n.effectiveVoters().Size(), n.quorum(), n.joint)
	// 立即广播一次心跳以宣告权威。
	out := n.broadcastAppend()
	// 新领导者若接手了尚未完成的联合变更，自动把它推进到完成。
	out = append(out, n.continueJointIfNeeded()...)
	return out
}

// recoverRemovedPeers 在领导者当选时，从已提交日志中恢复“刚被最近一次
// leave-joint 移出、但可能尚未确认自己移除已提交”的旧节点集合。
// 这覆盖“提交 leave 的领导者在最后心跳前下台、新领导者接管”的场景：
// 新领导者同样需要向这些旧成员补发携带 leave 提交点的心跳。
func (n *Node) recoverRemovedPeers() {
	var leaveIdx int
	var target Membership
	for _, e := range n.st.AllEntries() {
		if e.Kind != KindConfChange {
			continue
		}
		if cc, err := decodeConfChange(e.Payload); err == nil && cc.Op == ConfLeaveJoint {
			leaveIdx = e.Index
			target = NewMembership(cc.Members...)
		}
	}
	// leave 必须已提交：我们要补发的正是携带“leave 提交点”的最后心跳；
	// leave 尚未提交时联合变更仍在正常复制流程中，无需特殊处理。
	if leaveIdx == 0 || leaveIdx > n.commitIndex {
		return
	}
	prev := replayConfigView(n.st, leaveIdx-1)
	before := prev.current
	if prev.joint {
		before = prev.jointOld.Union(prev.current)
	}
	for _, id := range before.List() {
		if !target.Contains(id) && n.matchIndex[id] < leaveIdx {
			// 被移除节点此前是集群正式成员，日志至多落后最后一两个配置条目；
			// 直接从 leave 条目开始补发（而非从领导者尾部探测），让它尽快收到
			// 携带 leave 提交点的心跳。任期不一致时仍由常规冲突回退处理。
			n.nextIndex[id] = leaveIdx
			if n.matchIndex[id] == 0 {
				n.matchIndex[id] = leaveIdx - 1
			}
			n.removedPeers[id] = 0
		}
	}
}

// initReplicationState 为当前全部复制目标（有效投票成员 + learner）
// 初始化 nextIndex/matchIndex；已有的进度记录予以保留（领导者未变化时幂等）。
func (n *Node) initReplicationState() {
	for _, peer := range n.effectiveVoters().List() {
		if _, ok := n.nextIndex[peer]; !ok {
			n.nextIndex[peer] = n.st.LastIndex() + 1
			n.matchIndex[peer] = 0
		}
	}
	for l := range n.learners {
		if _, ok := n.nextIndex[l]; !ok {
			n.nextIndex[l] = n.st.LastIndex() + 1
			n.matchIndex[l] = 0
		}
	}
	n.matchIndex[n.id] = n.st.LastIndex()
}

// Start 将节点标记为在线（初始启动或重启）。
func (n *Node) Start() {
	n.alive = true
	n.heartbeatElapsed = 0
	n.resetElectionTimeout()
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"节点启动 role=%s", n.role)
}

// Stop 模拟节点崩溃停止；持久状态保留，重启后恢复。
func (n *Node) Stop() {
	n.alive = false
	n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonShutdown,
		"节点崩溃停止（持久状态保留）")
}

// replicationPeers 返回当前需要复制的对等节点集合：
// 常规单节点变更提交前为旧配置与新配置的并集；
// 联合共识期间为 C_old∪C_new；此外始终包含全部 learner（无投票权的追赶节点）。
func (n *Node) replicationPeers() []NodeID {
	set := map[NodeID]struct{}{}
	for _, id := range n.replicationMembers().List() {
		set[id] = struct{}{}
	}
	// 单节点变更（非联合）提交前的旧配置并集。
	if !n.hasUncommittedConf && !n.joint && n.pendingConf && n.prevMembership.Size() > 0 {
		for _, id := range n.prevMembership.List() {
			set[id] = struct{}{}
		}
	}
	for l := range n.learners {
		set[l] = struct{}{}
	}
	// 刚被 leave-joint 移出的旧节点：继续发送心跳直到它们确认自己的移除已提交。
	if n.role == Leader {
		for r := range n.removedPeers {
			set[r] = struct{}{}
		}
	}
	out := make([]NodeID, 0, len(set))
	for id := range set {
		if id != n.id {
			out = append(out, id)
		}
	}
	sortNodeIDs(out)
	return out
}

// broadcastAppend 向所有其他成员发送追加或快照消息。
func (n *Node) broadcastAppend() []OutMsg {
	var out []OutMsg
	for _, peer := range n.replicationPeers() {
		out = append(out, n.appendTo(peer))
	}
	return out
}

// removedPeerGraceTicks 是领导者为刚移除节点保留“最后心跳”的最长时间：
// 正常情况下它们 1~2 个心跳即确认；节点已宕机时到时放弃，避免无限复制。
const removedPeerGraceTicks = 20

// expireRemovedPeers 在领导者心跳周期推进移除宽限计数，清理长期不应答者。
func (n *Node) expireRemovedPeers() {
	for id, age := range n.removedPeers {
		if age+1 >= removedPeerGraceTicks {
			delete(n.removedPeers, id)
			delete(n.nextIndex, id)
			delete(n.matchIndex, id)
			n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonShutdown,
				"被移除节点 %s 宽限期内未确认，停止向其复制（其重启后将自行退出）", id)
			continue
		}
		n.removedPeers[id] = age + 1
	}
}

// appendTo 为单个跟随者构造追加或快照消息。
func (n *Node) appendTo(peer NodeID) OutMsg {
	if _, ok := n.nextIndex[peer]; !ok {
		// 防御：复制目标在领导者当选后才注册（如编排新增 learner），从领导者尾部开始探测。
		n.nextIndex[peer] = n.st.LastIndex() + 1
		n.matchIndex[peer] = 0
	}
	next := n.nextIndex[peer]
	prevIndex := next - 1
	sendSnapshot := false
	if n.st.HasSnapshot() {
		snap := n.st.SnapshotCopy()
		// 需要复制的下一条仍在快照覆盖范围内时，只能发快照；
		// next == snapLast+1 时 PrevLogIndex 恰为快照点，可走正常追加（跟随者用快照 term 校验）。
		if next <= snap.LastIncludedIndex {
			sendSnapshot = true
		}
	}
	// 防御：prevIndex 既不在现存日志、也不是快照点时（过时状态），只能快照补齐。
	if !sendSnapshot && prevIndex > 0 {
		if _, ok := n.st.TermAt(prevIndex); !ok {
			sendSnapshot = true
		}
	}
	if sendSnapshot {
		snap := n.st.SnapshotCopy()
		return OutMsg{To: peer, RPC: &InstallSnapshot{
			From:              n.id,
			Term:              n.Term(),
			LastIncludedIndex: snap.LastIncludedIndex,
			LastIncludedTerm:  snap.LastIncludedTerm,
			Membership:        snap.Membership,
			JointValid:        snap.JointValid,
			JointOld:          snap.JointOld,
			Data:              snap.Data,
		}}
	}
	prevTerm := 0
	if prevIndex > 0 {
		prevTerm, _ = n.st.TermAt(prevIndex)
	}
	entries := n.st.EntriesRange(next, n.st.LastIndex())
	return OutMsg{To: peer, RPC: &AppendEntries{
		From:         n.id,
		Term:         n.Term(),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}}
}

// appendAck 构造追加应答。
func (n *Node) appendAck(to NodeID, success bool, match, conflictIndex, conflictTerm int, reason Reason) OutMsg {
	return OutMsg{To: to, RPC: &AppendResult{
		From:          n.id,
		To:            to,
		Term:          n.Term(),
		Success:       success,
		MatchIndex:    match,
		ConflictIndex: conflictIndex,
		ConflictTerm:  conflictTerm,
		Reason:        reason,
	}}
}

// Tick 推进一个确定性时间单位，返回需要发出的消息（选举超时/心跳）。
func (n *Node) Tick() []OutMsg {
	if !n.alive || n.removed {
		return nil
	}
	var out []OutMsg
	switch n.role {
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= heartbeatTicks {
			n.heartbeatElapsed = 0
			n.expireRemovedPeers()
			// 当选后才收到他人提交的 leave-joint 时，也登记被移除节点补发心跳。
			n.recoverRemovedPeers()
			// 处于联合中间态时，心跳顺带把未完成的 enter/leave/no-op 续推。
			heartbeats := n.broadcastAppend()
			out = append(out, n.continueJointIfNeeded()...)
			out = append(out, heartbeats...)
		}
	default:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.electionElapsed = 0
			out = append(out, n.becomeCandidate()...)
		}
	}
	return out
}

// stepDownFromHigherTerm 收到更高任期消息时统一退位；返回是否发生退位。
func (n *Node) stepDownFromHigherTerm(term int) bool {
	if term > n.st.CurrentTerm() {
		n.log.Warnf(string(n.id), term, n.quorum(), n.commitIndex, ReasonDeposed,
			"发现更高任期 %d（本地 %d），退位为 follower", term, n.st.CurrentTerm())
		n.becomeFollower(term, "")
		return true
	}
	return false
}

// Propose 由领导者落盘一条客户端命令；非领导者返回 ReasonNotLeader。
// 返回成功仅表示条目已在领导者落盘，真正提交需经多数派确认。
func (n *Node) Propose(client NodeID, payload []byte) ClientAck {
	if !n.alive || n.removed {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonShutdown}
	}
	if n.role != Leader {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonNotLeader}
	}
	idx := n.st.AppendNew(n.Term(), LogEntry{Kind: KindCommand, Payload: append([]byte(nil), payload...)})
	n.matchIndex[n.id] = idx
	n.pending = append(n.pending, pendingProposal{index: idx, ackTo: client, kind: KindCommand})
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"领导者落盘命令 index=%d（待多数派提交）", idx)
	n.netEmit(ClientAck{To: client, Term: n.Term(), Index: idx, Success: true, Reason: ReasonOK})
	return ClientAck{To: client, Term: n.Term(), Index: idx, Success: true, Reason: ReasonOK}
}

// ProposeConfChange 提交单节点成员变更（增/删投票成员）。
// 规则：一次只允许一个未提交的变更；不允许把集群降到没有多数派意义的空集；
// 领导者移除自己时，条目仍正常走多数派提交，提交后本节点停止参与。
func (n *Node) ProposeConfChange(client NodeID, cc ConfChange) ClientAck {
	if !n.alive || n.removed {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonShutdown}
	}
	if n.role != Leader {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonNotLeader}
	}
	if n.pendingConf {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonMembershipPending}
	}
	next := n.membership.Apply(cc)
	if next.Size() == 0 {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonUnknownVoter}
	}
	if cc.Op == ConfRemoveVoter && !n.membership.Contains(cc.Node) {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonUnknownVoter}
	}
	if cc.Op == ConfAddVoter && n.membership.Contains(cc.Node) {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonUnknownVoter}
	}
	n.prevMembership = n.membership
	idx := n.st.AppendNew(n.Term(), LogEntry{Kind: KindConfChange, Payload: encodeConfChange(cc)})
	n.matchIndex[n.id] = idx
	n.pendingConf = true
	n.pending = append(n.pending, pendingProposal{index: idx, ackTo: client, kind: KindConfChange})
	// 单节点变更规则：领导者条目一落盘即按新配置计算多数派与复制目标。
	n.membership = next
	if cc.Op == ConfAddVoter {
		if _, ok := n.matchIndex[cc.Node]; !ok {
			n.matchIndex[cc.Node] = 0
			n.nextIndex[cc.Node] = n.st.LastIndex() + 1
		}
	}
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"领导者落盘成员变更 index=%d op=%d node=%s 新成员数=%d 新多数派=%d",
		idx, cc.Op, cc.Node, next.Size(), next.Quorum())
	n.netEmit(ClientAck{To: client, Term: n.Term(), Index: idx, Success: true, Reason: ReasonOK})
	return ClientAck{To: client, Term: n.Term(), Index: idx, Success: true, Reason: ReasonOK}
}

// ProposeBatchConfChange 原子提交一批成员变更（可同时增/删多个节点）。
// 内部通过联合共识两阶段完成：先追加 enter-joint（C_old,new）条目，
// 提交后由领导者自动追加 leave-joint（C_new）条目；返回的索引是 enter-joint 索引，
// 调用方应通过 IsBatchConfComplete 判断整批变更何时落定。
//
// 安全前提：
//   - 上一次成员变更尚未落定时拒绝（membership-pending），配置变更永不重叠；
//   - Adds 中的新节点必须先以 learner 身份把日志追平到领导者，否则拒绝
//     （learner-lagging）：否则它们在联合阶段拖慢甚至卡死双多数派，
//     影响变更期间普通日志的正常提交；
//   - 目标配置不能为空；同一节点不能既增又删、不能重复增、不能删非成员。
func (n *Node) ProposeBatchConfChange(client NodeID, b BatchConfChange) ClientAck {
	if !n.alive || n.removed {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonShutdown}
	}
	if n.role != Leader {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonNotLeader}
	}
	if n.pendingConf || n.joint {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonMembershipPending}
	}
	if reason := n.validateBatch(b); reason != ReasonOK {
		return ClientAck{To: client, Term: n.Term(), Reason: reason}
	}
	// 新增节点必须先作为 learner 追平：保证联合阶段一开始它们就能投票，
	// 成员变更窗口内普通日志提交不被长期卡住。
	for _, add := range b.Adds {
		if n.matchIndex[add] < n.st.LastIndex() {
			n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonLearnerLagging,
				"新增节点 %s 尚未追平 match=%d leaderLast=%d", add, n.matchIndex[add], n.st.LastIndex())
			return ClientAck{To: client, Term: n.Term(), Reason: ReasonLearnerLagging}
		}
	}
	target := n.membership.TargetMembership(b)
	// enter-joint 落盘时尚未提交：不改动“已提交视图”，只设置未提交视图，
	// 使复制目标立即扩展到 C_old∪C_new；多数派/选举仍按已提交的 C_old 直到提交。
	n.prevMembership = n.membership
	idx := n.st.AppendNew(n.Term(), LogEntry{Kind: KindConfChange, Payload: encodeConfChange(ConfChange{
		Op: ConfEnterJoint, Members: target.List(),
	})})
	n.matchIndex[n.id] = idx
	n.pendingConf = true
	n.hasUncommittedConf = true
	n.uncommittedJoint = true
	n.uncommittedJointOld = n.membership
	n.uncommittedMembers = target
	n.initReplicationState()
	n.pending = append(n.pending, pendingProposal{index: idx, ackTo: client, kind: KindConfChange})
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"领导者落盘 enter-joint index=%d 目标成员数=%d 联合成员数=%d 旧多数派=%d 新多数派=%d",
		idx, target.Size(), n.replicationMembers().Size(), n.prevMembership.Quorum(), target.Quorum())
	n.netEmit(ClientAck{To: client, Term: n.Term(), Index: idx, Success: true, Reason: ReasonOK})
	return ClientAck{To: client, Term: n.Term(), Index: idx, Success: true, Reason: ReasonOK}
}

// validateBatch 校验批量变更内容，返回原因码（OK 表示合法）。
func (n *Node) validateBatch(b BatchConfChange) Reason {
	if len(b.Adds) == 0 && len(b.Removes) == 0 {
		return ReasonBadConfChange
	}
	seen := map[NodeID]bool{}
	// 先做批次内部一致性检查（重复增、既增又删），再做与当前配置的关系检查。
	for _, add := range b.Adds {
		if seen[add] {
			return ReasonBadConfChange // 同一节点重复增
		}
		seen[add] = true
	}
	for _, rem := range b.Removes {
		if seen[rem] {
			return ReasonBadConfChange // 同一批次既增又删
		}
		seen[rem] = true
	}
	for _, add := range b.Adds {
		if n.membership.Contains(add) {
			return ReasonUnknownVoter // 已经是投票成员
		}
	}
	for _, rem := range b.Removes {
		if !n.membership.Contains(rem) {
			return ReasonUnknownVoter // 要删的不是当前成员
		}
	}
	if n.membership.TargetMembership(b).Size() == 0 {
		return ReasonBadConfChange // 不允许删空整个投票集群
	}
	return ReasonOK
}

// IsBatchConfComplete 报告当前是否已不存在未落定的成员变更
// （leave-joint 已提交、或单节点变更已提交），供批量变更调用方等待收敛。
func (n *Node) IsBatchConfComplete() bool { return !n.pendingConf && !n.joint }

// HandleVote 处理投票请求。
func (n *Node) HandleVote(m RequestVote) []OutMsg {
	if !n.alive || n.removed {
		return nil
	}
	reply := RequestVoteResult{From: n.id, To: m.From, Term: n.st.CurrentTerm()}

	if m.Term < n.st.CurrentTerm() {
		reply.Reason = ReasonStaleTerm
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, reply.Reason,
			"拒绝投票：请求任期 %d 落后于本地 %d", m.Term, n.Term())
		return []OutMsg{{To: m.From, RPC: &reply}}
	}
	n.stepDownFromHigherTerm(m.Term)
	reply.Term = n.st.CurrentTerm()

	// 无投票权的节点（learner，或已不在常规/联合任何生效配置中的节点）不参与选举。
	if _, isLearner := n.learners[m.From]; isLearner || !n.isVoter(m.From) {
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonNotVoter,
			"拒绝投票给 %s：对方无投票权 learner=%v joint=%v", m.From, isLearner, n.joint)
		return []OutMsg{{To: m.From, RPC: &RequestVoteResult{
			From: n.id, To: m.From, Term: n.st.CurrentTerm(), Reason: ReasonNotVoter,
		}}}
	}
	// 规则0：近期仍在接收现任领导者心跳时，拒绝拉票（标准 Raft 的领导者租约保护），
	// 这也防止已被成员变更移除的节点在超时前用旧身份干扰集群。
	if n.leader != "" && n.electionElapsed < electionMin && m.From != n.leader {
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonLeaderLease,
			"拒绝投票给 %s：现任领导者 %s 心跳仍在有效期内（elapsed=%d<%d）",
			m.From, n.leader, n.electionElapsed, electionMin)
		return []OutMsg{{To: m.From, RPC: &RequestVoteResult{
			From: n.id, To: m.From, Term: n.st.CurrentTerm(), Reason: ReasonLeaderLease,
		}}}
	}
	// 规则1：本任期只能投给一个候选人（已投自己或他人均拒绝）。
	if n.st.VotedFor() != "" && n.st.VotedFor() != m.From {
		reply.Reason = ReasonAlreadyVoted
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, reply.Reason,
			"拒绝投票给 %s：本任期已投给 %s", m.From, n.st.VotedFor())
		return []OutMsg{{To: m.From, RPC: &reply}}
	}
	// 规则2：候选人日志必须至少与自己一样新（先比最后条目的任期，再比索引）。
	myLastTerm, _ := n.st.TermAt(n.st.LastIndex())
	if m.LastLogTerm < myLastTerm ||
		(m.LastLogTerm == myLastTerm && m.LastLogIndex < n.st.LastIndex()) {
		reply.Reason = ReasonLogNotUpToDate
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, reply.Reason,
			"拒绝投票：候选人日志 (t=%d,i=%d) 旧于本地 (t=%d,i=%d)",
			m.LastLogTerm, m.LastLogIndex, myLastTerm, n.st.LastIndex())
		return []OutMsg{{To: m.From, RPC: &reply}}
	}

	n.st.SaveTermVote(n.st.CurrentTerm(), m.From)
	n.resetElectionTimeout()
	reply.Granted = true
	reply.Term = n.st.CurrentTerm()
	reply.Reason = ReasonOK
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"投票给候选人 %s（候选人日志 t=%d i=%d）", m.From, m.LastLogTerm, m.LastLogIndex)
	return []OutMsg{{To: m.From, RPC: &reply}}
}

// HandleVoteResult 处理投票应答。
func (n *Node) HandleVoteResult(m RequestVoteResult) []OutMsg {
	if !n.alive || n.removed {
		return nil
	}
	n.stepDownFromHigherTerm(m.Term)
	if n.role != Candidate || m.Term != n.st.CurrentTerm() {
		// 已不再是该任期的候选人，应答仅用于任期校正，忽略计票。
		return nil
	}
	if m.Granted {
		n.votes[m.From] = true
	} else {
		n.votes[m.From] = false
		n.log.Debugf(string(n.id), n.Term(), n.quorum(), n.commitIndex, m.Reason,
			"未获得 %s 的选票", m.From)
	}
	// 联合共识期间，当选必须同时获得 C_old 与 C_new 双方多数派；
	// 这保证任何一个配置的多数派都不可能再选出另一个能提交的领导者。
	oldVotes, newVotes, oldGranted, newGranted := 0, 0, 0, 0
	for id, v := range n.votes {
		if !v {
			continue
		}
		if n.joint && n.jointOld.Contains(id) {
			oldVotes++
		}
		if n.membership.Contains(id) {
			newVotes++
		}
	}
	oldGranted, newGranted = oldVotes, newVotes
	quorum := n.membership.Quorum()
	if n.joint {
		n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
			"计票（联合）：旧配置赞成=%d/%d 新配置赞成=%d/%d",
			oldGranted, n.jointOld.Quorum(), newGranted, n.membership.Quorum())
		if oldGranted >= n.jointOld.Quorum() && newGranted >= n.membership.Quorum() {
			return n.becomeLeader()
		}
		return nil
	}
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"计票：赞成=%d 多数派=%d 总成员=%d", newGranted, quorum, n.membership.Size())
	if newGranted >= quorum {
		return n.becomeLeader()
	}
	return nil
}

// HandleAppend 处理日志复制/心跳。
func (n *Node) HandleAppend(m AppendEntries) []OutMsg {
	if !n.alive || n.removed {
		return nil
	}
	// 规则：任期落后的领导者消息一律拒绝，并回传本地任期帮助对端退位。
	if m.Term < n.st.CurrentTerm() {
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonStaleTerm,
			"拒绝任期落后的追加请求 leaderTerm=%d localTerm=%d", m.Term, n.Term())
		return []OutMsg{n.appendAck(m.From, false, 0, 0, 0, ReasonStaleTerm)}
	}
	if m.Term > n.st.CurrentTerm() || n.role != Follower {
		n.becomeFollower(m.Term, m.From)
	} else {
		n.leader = m.From
	}
	n.resetElectionTimeout()

	// 一致性检查：PrevLogIndex 严格落在快照点之前 -> 无法补齐，需要领导者重发快照。
	if m.PrevLogIndex > 0 && n.st.HasSnapshot() &&
		m.PrevLogIndex < n.st.SnapshotCopy().LastIncludedIndex {
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonLogGap,
			"PrevLogIndex=%d 早于本地快照点=%d，请求快照",
			m.PrevLogIndex, n.st.SnapshotCopy().LastIncludedIndex)
		return []OutMsg{n.appendAck(m.From, false, 0, n.st.FirstIndex(), 0, ReasonLogGap)}
	}
	// 一致性检查：PrevLogIndex 处任期必须匹配。
	if m.PrevLogIndex > 0 {
		localTerm, ok := n.st.TermAt(m.PrevLogIndex)
		if !ok || localTerm != m.PrevLogTerm {
			conflictIndex, conflictTerm := n.findConflict(m.PrevLogIndex)
			n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonLogConflict,
				"PrevLog 不一致 index=%d 期望Term=%d 本地Term=%d -> 回退提示 index=%d term=%d",
				m.PrevLogIndex, m.PrevLogTerm, localTerm, conflictIndex, conflictTerm)
			return []OutMsg{n.appendAck(m.From, false, 0, conflictIndex, conflictTerm, ReasonLogConflict)}
		}
	}

	// 显式处理冲突后缀与缺口追加（TruncateAndAppend 内部记录删除日志）。
	n.st.TruncateAndAppend(m.PrevLogIndex, m.Entries)
	match := m.PrevLogIndex + len(m.Entries)
	if len(m.Entries) > 0 {
		n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
			"追加 %d 条条目 prevIndex=%d matchIndex=%d lastIndex=%d",
			len(m.Entries), m.PrevLogIndex, match, n.st.LastIndex())
	}

	// 提交位置取 min(leaderCommit, 本地最后索引)，绝不自行越过领导者提交点。
	if m.LeaderCommit > n.commitIndex {
		newCommit := m.LeaderCommit
		if newCommit > n.st.LastIndex() {
			newCommit = n.st.LastIndex()
		}
		n.advanceCommitTo(newCommit, "跟随领导者提交点")
	}
	// 提交点可能推进（即使本拍无新条目）：必须按最新提交点重建配置视图，
	// 让刚提交的单节点移除/leave-joint 立即生效为已提交配置。
	n.refreshMembershipThrough(match)
	n.applyCommitted()
	return []OutMsg{n.appendAck(m.From, true, match, 0, 0, ReasonOK)}
}

// findConflict 依据本地日志给出快速回退提示：
// 若 prevIndex 处存在旧任期，返回该任期的第一条索引；否则返回 prevIndex。
func (n *Node) findConflict(prevIndex int) (int, int) {
	if e, ok := n.st.EntryAt(prevIndex); ok {
		term := e.Term
		idx := prevIndex
		for idx-1 >= n.st.FirstIndex() {
			if t, ok := n.st.TermAt(idx - 1); ok && t == term {
				idx--
				continue
			}
			break
		}
		return idx, term
	}
	return prevIndex, 0
}

// refreshMembershipThrough 在追加日志（through 为追加后末索引）后刷新配置视图。
// 关键规则：
//   - “已提交配置视图”（membership/joint/jointOld）严格重放到 commitIndex 得出，
//     多数派计数、选举、提交判定只认它。跟随者收到未提交的 enter/leave-joint
//     绝不提前切换，避免按单多数派提交造成双领导者。
//   - “已复制未提交视图”（uncommitted*）重放到 through 得出，仅用于复制目标：
//     联合期间领导者/跟随者都要向 C_old∪C_new 并集复制，被移除者也能收到
//     携带其移除提交点的最后心跳。
//
// 节点“退出协议（removed）”只依据已提交视图，保证被移除者能确认自己的移除已提交。
func (n *Node) refreshMembershipThrough(through int) {
	committed := replayConfigView(n.st, n.commitIndex)
	appended := replayConfigView(n.st, through)
	if appended.confIndex == 0 && committed.confIndex == 0 {
		return
	}
	prev := n.membership
	wasJoint := n.joint
	// 已提交视图：用于协议判定。
	n.membership = committed.current
	n.joint = committed.joint
	if committed.joint {
		n.jointOld = committed.jointOld
	}
	n.pendingConf = committed.joint || appended.confIndex > n.commitIndex
	// 已复制未提交视图：仅用于复制目标。
	if appended.confIndex > n.commitIndex {
		n.hasUncommittedConf = true
		n.uncommittedJoint = appended.joint
		n.uncommittedMembers = appended.current
		if appended.joint {
			n.uncommittedJointOld = appended.jointOld
		}
	} else {
		n.hasUncommittedConf = false
		n.uncommittedJoint = false
		n.uncommittedMembers = Membership{}
		n.uncommittedJointOld = Membership{}
	}
	if !prev.Equal(committed.current) || wasJoint != n.joint || n.hasUncommittedConf {
		n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
			"配置随日志刷新 已提交成员数=%d joint=%v 存在未提交配置=%v pendingConf=%v",
			committed.current.Size(), n.joint, n.hasUncommittedConf, n.pendingConf)
	}
	n.pruneLearners()
	// 只有“已提交位置”的配置包含自己，才确认本节点是正式投票成员（单调）。
	// 未提交的 enter-joint 可能随后被冲突截断，不能据此置位，否则回退后会
	// 因收到旧配置而误判自己被移除。
	if committed.current.Contains(n.id) || (committed.joint && committed.jointOld.Contains(n.id)) {
		n.everVoter = true
	}
	n.observeSelfRemoved()
}

// replicationView 返回“当前需要据以复制”的配置状态：
// 常规情况下即已提交视图；若存在已复制未提交的配置条目，则其联合中间态
// 决定复制并集（C_old∪C_new），让被移除节点也收到移除条目与最终提交点。
func (n *Node) replicationView() (current Membership, joint bool, jointOld Membership) {
	current = n.membership
	joint = n.joint
	jointOld = n.jointOld
	if n.hasUncommittedConf && n.uncommittedJoint {
		joint = true
		jointOld = n.uncommittedJointOld
		current = n.uncommittedMembers
	}
	return
}

// replicationMembers 返回复制目标成员集合（已提交配置 ∪ 未提交联合并集）。
func (n *Node) replicationMembers() Membership {
	cur, joint, old := n.replicationView()
	if joint {
		return old.Union(cur)
	}
	return cur
}

// observeSelfRemoved 严格依据“已提交位置”的重放视图判断自己是否已被移除：
// 已提交配置不再包含自己、且联合共识已结束（非 joint）时退出协议。
// 联合期间即使不在 C_new 中，仍是 C_old 成员，必须继续参与双多数派。
// 不使用可能陈旧的 n.membership 做短路，避免漏掉刚提交的单节点移除。
func (n *Node) observeSelfRemoved() {
	// 从未成为正式投票成员的节点（空日志追赶者）在追平过程中必然收到不含
	// 自己的旧配置/快照，绝不能据此判定自己被移除。
	if !n.everVoter || n.removed {
		return
	}
	v := replayConfigView(n.st, n.commitIndex)
	if v.confIndex > 0 && !v.joint && !v.current.Contains(n.id) {
		n.markRemoved()
	}
}

func (n *Node) markRemoved() {
	if !n.removed {
		n.removed = true
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
			"本节点已被成员变更移除，停止参与协议（已提交状态保留）")
	}
}

// advanceCommitTo 单调推进提交位置。
func (n *Node) advanceCommitTo(target int, why string) {
	if target <= n.commitIndex {
		return
	}
	n.commitIndex = target
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"提交位置推进到 %d（%s）", target, why)
}

// applyCommitted 把 (lastApplied, commitIndex] 区间条目按序、幂等地应用到 FSM。
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		idx := n.lastApplied + 1
		e, ok := n.st.EntryAt(idx)
		if !ok {
			break
		}
		// 客户端命令（KindCommand）无论负载内容如何都必须原样进入 FSM；
		// 内部空操作（KindNoop）与成员变更（KindConfChange）条目永远跳过。
		if e.Kind == KindCommand {
			if err := n.fsm.Apply(idx, e.Term, e.Payload); err != nil {
				n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex,
					ReasonRejectedByApplication, "应用 index=%d 失败: %v", idx, err)
				break
			}
		}
		n.lastApplied = idx
	}
	// 成员变更一旦提交，解除 pending 阻塞；联合共识在 leave-joint 提交后整体落定。
	if n.pendingConf || n.joint {
		v := replayConfigView(n.st, n.commitIndex)
		if !v.joint && v.confIndex > 0 && v.confIndex <= n.commitIndex {
			n.pendingConf = false
			n.joint = false
			n.jointOld = Membership{}
			n.prevMembership = Membership{}
			n.pruneLearners()
			n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
				"成员变更已落定（index=%d），允许下一次变更 成员数=%d",
				v.confIndex, v.current.Size())
		}
	}
	n.resolvePendingProposals()
	n.observeSelfRemoved()
}

// jointNoopPayload 是内部空操作条目的负载，仅用于日志可读性；
// 是否跳过状态机完全由条目类型 KindNoop 决定，与负载内容无关。
const jointNoopPayload = "joint-noop"

// continueJointIfNeeded 由领导者调用，自动把联合共识推进到完成。它覆盖领导者
// 在联合中间态可能遇到的全部接管形态：
//  1. 最后配置条目是已提交的 enter-joint，其后还没有 leave-joint：追加 leave；
//  2. 最后配置条目是未提交的 enter/leave（属于旧任期）：追加一条当前任期的
//     no-op 命令条目。受图 8 安全性约束，旧任期条目不能被新领导者直接提交，
//     no-op 凑齐双多数派后会连带把它们提交，从而续推或完成联合变更；
//  3. enter/leave 尚未复制到本节点：交给常规复制，不做处理。
func (n *Node) continueJointIfNeeded() []OutMsg {
	if n.role != Leader || !n.joint {
		return nil
	}
	var lastKind ConfChangeOp
	var lastIndex int
	var target Membership
	for _, e := range n.st.AllEntries() {
		if e.Kind != KindConfChange {
			continue
		}
		if cc, err := decodeConfChange(e.Payload); err == nil {
			lastKind, lastIndex = cc.Op, e.Index
			if cc.Op == ConfEnterJoint {
				target = NewMembership(cc.Members...)
			}
		}
	}
	// 形态 1：enter 已提交、缺 leave。
	if lastKind == ConfEnterJoint && lastIndex <= n.commitIndex {
		idx := n.st.AppendNew(n.Term(), LogEntry{Kind: KindConfChange, Payload: encodeConfChange(ConfChange{
			Op: ConfLeaveJoint, Members: target.List(),
		})})
		n.matchIndex[n.id] = idx
		n.pending = append(n.pending, pendingProposal{index: idx, ackTo: "joint-internal", kind: KindConfChange})
		n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
			"enter-joint 已提交，自动追加 leave-joint index=%d 目标成员数=%d", idx, target.Size())
		return n.broadcastAppend()
	}
	// 形态 2：最后的 enter/leave 仍是未提交的旧任期条目：用当前任期 no-op 续推。
	// 幂等条件：仅当本地最后一条不是当前任期的 no-op（避免重复追加）。
	needsPush := (lastKind == ConfEnterJoint || lastKind == ConfLeaveJoint) && lastIndex > n.commitIndex
	if !needsPush {
		return nil
	}
	lastEntry, ok := n.st.EntryAt(n.st.LastIndex())
	if ok && lastEntry.Term == n.Term() {
		return nil // 当前任期已有条目在途，等它拿到双多数派即可，不重复追加。
	}
	idx := n.st.AppendNew(n.Term(), LogEntry{Kind: KindNoop, Payload: []byte(jointNoopPayload)})
	n.matchIndex[n.id] = idx
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"接管未完成的联合变更（旧任期配置条目 index=%d），追加当前任期 no-op index=%d 推动双多数派",
		lastIndex, idx)
	return n.broadcastAppend()
}

// resolvePendingProposals 向已提交条目的客户端返回成功应答。
func (n *Node) resolvePendingProposals() {
	var keep []pendingProposal
	for _, p := range n.pending {
		if p.index <= n.commitIndex {
			ack := ClientAck{To: p.ackTo, Term: n.Term(), Index: p.index, Success: true, Reason: ReasonOK}
			n.netEmit(ack)
		} else {
			keep = append(keep, p)
		}
	}
	n.pending = keep
}

// netEmit 由 Cluster 注入实际投递；Node 自身通过回调输出（见 cluster.go 的绑定）。
func (n *Node) netEmit(ack ClientAck) {
	if n.emit != nil {
		n.emit(OutMsg{To: ack.To, RPC: &ack})
	}
}

// HandleAppendResult 处理复制应答。
func (n *Node) HandleAppendResult(m AppendResult) []OutMsg {
	if !n.alive || n.removed {
		return nil
	}
	n.stepDownFromHigherTerm(m.Term)
	if n.role != Leader || m.Term != n.st.CurrentTerm() {
		return nil
	}
	peer := m.From
	// 接受四类对端的复制应答：当前生效配置成员、联合旧配置成员、
	// learner、刚被移出但还需确认自己移除已提交的旧节点。
	if !n.isVoter(peer) {
		_, isLearner := n.learners[peer]
		_, isRemovedPeer := n.removedPeers[peer]
		if !isLearner && !isRemovedPeer {
			return nil
		}
	}
	var out []OutMsg
	if m.Success {
		// 幂等：只有更靠后的确认才推进 matchIndex（重复/乱序应答不产生回退）。
		if m.MatchIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = m.MatchIndex
			n.nextIndex[peer] = m.MatchIndex + 1
			// 被移除节点确认到 leave-joint 提交点后，停止向其复制。
			if _, ok := n.removedPeers[peer]; ok && m.MatchIndex >= n.commitIndex {
				delete(n.removedPeers, peer)
				delete(n.nextIndex, peer)
				delete(n.matchIndex, peer)
				n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
					"被移除节点 %s 已确认自己的移除，停止复制", peer)
				return nil
			}
			n.log.Debugf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
				"peer=%s 复制确认 match=%d", peer, m.MatchIndex)
		} else {
			n.log.Debugf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonDuplicate,
				"peer=%s 重复确认 match=%d（幂等忽略）", peer, m.MatchIndex)
			return nil
		}
	} else {
		switch m.Reason {
		case ReasonLogGap:
			// 对端比快照还旧：立刻改发快照。
			return []OutMsg{n.appendTo(peer)}
		case ReasonLogConflict:
			// 快速回退：优先用冲突任期回退，否则直接退到冲突索引。
			newNext := m.ConflictIndex
			if m.ConflictTerm > 0 {
				newNext = n.lastIndexOfTerm(m.ConflictTerm)
				if newNext == 0 {
					newNext = m.ConflictIndex
				} else {
					newNext++ // 该任期最后一条之后
				}
			}
			if newNext < n.nextIndex[peer] {
				n.nextIndex[peer] = newNext
			}
			n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonLogConflict,
				"peer=%s 日志冲突，nextIndex 回退到 %d", peer, n.nextIndex[peer])
			return []OutMsg{n.appendTo(peer)}
		case ReasonStaleTerm:
			return nil
		default:
			return nil

		}
	}

	// 多数派提交判定：在当前任期的日志中，找出复制到多数派的最高索引 N，
	// 仅提交当前任期条目（图 8 安全性），历史任期条目随后随 N 一起提交。
	// 联合共识期间 N 必须同时复制到 C_old 与 C_new 双方多数派；
	// learner 只推进 matchIndex，绝不计入任何一方多数派。
	last := n.st.LastIndex()
	for cand := last; cand > n.commitIndex; cand-- {
		term, ok := n.st.TermAt(cand)
		if !ok || term != n.st.CurrentTerm() {
			continue
		}
		// 候选索引处的配置视图（可能包含尚未提交的 enter/leave-joint）：
		// 联合共识期间提交必须同时满足 C_old 与 C_new 双方多数派。
		view := replayConfigView(n.st, cand)
		newSet := view.current
		oldSet := newSet
		jointAt := view.joint
		if jointAt {
			oldSet = view.jointOld
		}
		// 领导者自身一票按其实际所属配置计入。
		newCount, oldCount := 0, 0
		if newSet.Contains(n.id) {
			newCount = 1
		}
		if !jointAt || oldSet.Contains(n.id) {
			oldCount = 1
		}
		for _, voter := range newSet.List() {
			if voter != n.id && n.matchIndex[voter] >= cand {
				newCount++
			}
		}
		if jointAt {
			for _, voter := range oldSet.List() {
				if voter != n.id && n.matchIndex[voter] >= cand {
					oldCount++
				}
			}
		}
		quorumMet := newCount >= newSet.Quorum()
		if jointAt {
			quorumMet = quorumMet && oldCount >= oldSet.Quorum()
		}
		if quorumMet {
			// 先固定复制并集，保证被移除节点也收到携带新提交点的最后一次心跳。
			finalPeers := n.replicationPeers()
			if jointAt {
				n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
					"联合多数派确认 index=%d：旧配置 %d/%d 新配置 %d/%d",
					cand, oldCount, oldSet.Quorum(), newCount, newSet.Quorum())
			}
			n.advanceCommitTo(cand, "多数派复制确认")
			wasJoint := n.joint
			var prevOld Membership
			if wasJoint {
				prevOld = n.jointOld
			}
			n.refreshMembershipThrough(n.commitIndex)
			n.applyCommitted()
			// 若本次提交使 leave-joint 落定（联合 -> 常规），登记全部被移出的
			// 旧节点，在它们确认 leave 提交点前持续补发心跳，使其能干净地
			// 自我移除。登记幂等：已确认者下一次应答即被摘除。
			if wasJoint && !n.joint {
				for _, id := range prevOld.List() {
					if !n.membership.Contains(id) {
						n.removedPeers[id] = 0
					}
				}
			}
			// enter-joint 提交后自动追加 leave-joint，再连同提交点一并广播。
			out = append(out, n.continueJointIfNeeded()...)
			for _, p := range finalPeers {
				out = append(out, n.appendTo(p))
			}
			// 提交点变化后向全部复制目标再广播一次，加速收敛。
			out = append(out, n.broadcastAppend()...)
			break
		}
	}
	return out
}

// lastIndexOfTerm 返回指定任期在本地日志中的最后一条索引，没有则返回 0。
func (n *Node) lastIndexOfTerm(term int) int {
	for i := n.st.LastIndex(); i >= n.st.FirstIndex(); i-- {
		if t, ok := n.st.TermAt(i); ok && t == term {
			return i
		}
	}
	return 0
}

// HandleSnapshot 处理快照安装请求。
func (n *Node) HandleSnapshot(m InstallSnapshot) []OutMsg {
	if !n.alive || n.removed {
		return nil
	}
	if m.Term < n.st.CurrentTerm() {
		return []OutMsg{{To: m.From, RPC: &InstallSnapshotResult{
			From: n.id, To: m.From, Term: n.Term(), Success: false, Reason: ReasonStaleTerm,
		}}}
	}
	if m.Term > n.st.CurrentTerm() || n.role != Follower {
		n.becomeFollower(m.Term, m.From)
	} else {
		n.leader = m.From
	}
	n.resetElectionTimeout()

	// 不允许用快照回退已提交位置。
	if m.LastIncludedIndex < n.commitIndex {
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonSnapshotBehindCommit,
			"拒绝旧快照 snapshotIndex=%d commitIndex=%d", m.LastIncludedIndex, n.commitIndex)
		return []OutMsg{{To: m.From, RPC: &InstallSnapshotResult{
			From: n.id, To: m.From, Term: n.Term(), Success: false, Reason: ReasonSnapshotBehindCommit,
		}}}
	}

	snap := Snapshot{
		LastIncludedIndex: m.LastIncludedIndex,
		LastIncludedTerm:  m.LastIncludedTerm,
		Membership:        m.Membership,
		Data:              append([]byte(nil), m.Data...),
	}
	if err := n.st.ApplySnapshot(snap); err != nil {
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonLogConflict,
			"快照安装失败: %v", err)
		return []OutMsg{{To: m.From, RPC: &InstallSnapshotResult{
			From: n.id, To: m.From, Term: n.Term(), Success: false, Reason: ReasonLogConflict,
		}}}
	}
	if err := n.fsm.Restore(snap.Data); err != nil {
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex,
			ReasonRejectedByApplication, "FSM 恢复失败: %v", err)
	}
	n.commitIndex = m.LastIncludedIndex
	n.lastApplied = m.LastIncludedIndex
	n.membership = m.Membership
	n.joint = m.JointValid
	n.jointOld = m.JointOld
	n.pendingConf = m.JointValid
	if m.JointValid {
		n.prevMembership = m.JointOld
	}
	// 注意：不在此依据快照配置判定“自己被移除”。
	// 追赶中的新节点可能收到不含自己的旧配置快照；是否被移除必须在补齐
	// 快照之后的配置条目后，由 observeSelfRemoved 结合完整配置视图判定。
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"落后节点通过快照恢复到 index=%d term=%d 成员数=%d joint=%v",
		m.LastIncludedIndex, m.LastIncludedTerm, m.Membership.Size(), m.JointValid)
	return []OutMsg{{To: m.From, RPC: &InstallSnapshotResult{
		From: n.id, To: m.From, Term: n.Term(), Success: true, MatchIndex: m.LastIncludedIndex, Reason: ReasonOK,
	}}}
}

// HandleSnapshotResult 处理快照安装应答。
func (n *Node) HandleSnapshotResult(m InstallSnapshotResult) []OutMsg {
	if !n.alive || n.removed {
		return nil
	}
	n.stepDownFromHigherTerm(m.Term)
	if n.role != Leader || m.Term != n.st.CurrentTerm() {
		return nil
	}
	if !m.Success {
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, m.Reason,
			"peer=%s 拒绝快照", m.From)
		return nil
	}
	if m.MatchIndex > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.MatchIndex
		n.nextIndex[m.From] = m.MatchIndex + 1
	}
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"peer=%s 快照安装完成 match=%d，转为日志追赶", m.From, m.MatchIndex)
	// 快照后可能还有新条目，立即补发；同时重新做多数派提交判定。
	out := []OutMsg{n.appendTo(m.From)}
	return append(out, n.HandleAppendResult(AppendResult{
		From: m.From, To: n.id, Term: n.Term(), Success: true, MatchIndex: m.MatchIndex, Reason: ReasonOK,
	})...)
}

// CreateSnapshot 在不超过已提交位置的 index 处压缩日志：
// 捕获 FSM 状态、丢弃旧日志、在快照中保留当时成员配置。
// 不允许压缩未提交位置，避免影响已提交状态的可恢复性。
func (n *Node) CreateSnapshot(index int) (Snapshot, error) {
	if !n.alive {
		return Snapshot{}, fmt.Errorf("node shutdown")
	}
	if index > n.commitIndex {
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonSnapshotBehindCommit,
			"拒绝压缩未提交位置 index=%d commit=%d", index, n.commitIndex)
		return Snapshot{}, fmt.Errorf("snapshot: index %d 超过已提交位置 %d", index, n.commitIndex)
	}
	data := n.fsm.Snapshot()
	snap, err := n.st.Compact(index, data)
	if err != nil {
		return Snapshot{}, err
	}
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"快照生成 index=%d term=%d 保留日志首索引=%d",
		snap.LastIncludedIndex, snap.LastIncludedTerm, n.st.FirstIndex())
	return snap, nil
}

func sortNodeIDs(ids []NodeID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
}
