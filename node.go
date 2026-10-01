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

	// 当前生效配置（联合共识期间为 C_old,new 双集合）
	membership Membership
	// prevMembership 仅在“最后一条配置条目尚未提交”时使用：
	// 保存该条目生效之前的配置，复制目标取两者并集——
	// enter-joint 未落盘/未提交时它是 C_old，leave-joint 未提交时它是 C_old,new，
	// 保证被移除者也能收到“移除自己”的条目与携带提交点的最后心跳。
	prevMembership Membership
	// 成员变更尚在进行：存在未提交配置条目，或联合配置已生效但 leave-joint 尚未追加。
	pendingConf bool
	// 本节点已被成员变更移除
	removed bool

	// 领导者可变状态
	nextIndex  map[NodeID]int
	matchIndex map[NodeID]int
	votes      map[NodeID]bool
	// preVotes 为 pre-vote 阶段的预投票计票；preVoting 标记当前处于预投票。
	preVotes  map[NodeID]bool
	preVoting bool

	// 待复制提案队列（领导者）：propose 已落盘，等待提交后回调
	pending []pendingProposal

	// emit 由 Cluster 注入，用于异步客户端应答（不经过 RPC 通道）。
	emit func(OutMsg)

	// reachable 由 Cluster 注入，判断某成员当前是否存活且未被网络隔离。
	// 仅用于联合阶段选举的活性判定：确认不可达（停止/隔离）的成员无法再对
	// 投票表态，不应让其永久占用多数派席位而锁死集群（Ongaro 论文 §6.3）。
	// 缺省（nil）时一律视为可达，退化为最保守的多数派定义。
	reachable func(NodeID) bool
}

// SetEmitter 注入客户端应答输出回调。
func (n *Node) SetEmitter(f func(OutMsg)) { n.emit = f }

// SetReachable 注入成员可达性判定（联合阶段选举活性用）。
func (n *Node) SetReachable(f func(NodeID) bool) { n.reachable = f }

func (n *Node) canReach(id NodeID) bool {
	if n.reachable == nil {
		return true
	}
	return n.reachable(id)
}

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
		id:         id,
		log:        log,
		st:         st,
		fsm:        fsm,
		rng:        rand.New(rand.NewSource(seed)),
		role:       Follower,
		nextIndex:  map[NodeID]int{},
		matchIndex: map[NodeID]int{},
		votes:      map[NodeID]bool{},
		preVotes:   map[NodeID]bool{},
		membership: initial,
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
		n.log.Infof(string(id), st.CurrentTerm(), snap.Membership.Quorum(), n.commitIndex,
			ReasonOK, "从快照重启 lastApplied=%d 成员数=%d", n.lastApplied, snap.Membership.Size())
	case st.LastIndex() > 0:
		// 重启但没有快照：从持久日志重建配置视图。
		n.rebuildMembership()
		n.log.Infof(string(id), st.CurrentTerm(), n.membership.Quorum(), n.commitIndex,
			ReasonOK, "从持久日志重启 lastIndex=%d", st.LastIndex())
	default:
		if freshJoin {
			// 追赶节点：空日志、空配置、提交位置为 0，等待领导者补齐。
			n.membership = NewMembership()
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
		n.commitIndex = 1
		n.lastApplied = 1
		n.log.Infof(string(id), 0, initial.Quorum(), 1,
			ReasonOK, "冷启动写入引导配置 成员数=%d 多数派=%d", initial.Size(), initial.Quorum())
	}
	return n
}

// rebuildMembership 从快照配置出发重放全部配置条目，重建当前配置与 pendingConf。
// 用于节点重启：联合共识的 enter/leave 两阶段条目都会被完整重放。
func (n *Node) rebuildMembership() {
	n.reconcileMembership()
}

// confBase 返回配置重放的起点：存在快照时为快照点配置，
// 否则从空配置开始（index=1 的引导 ConfSetVoters 条目会完成首次建立）。
func (n *Node) confBase() Membership {
	if n.st.HasSnapshot() {
		return n.st.SnapshotCopy().Membership
	}
	return NewMembership()
}

// membershipAt 重放到 throughIndex（含）为止的配置条目，返回该位置的生效配置。
func (n *Node) membershipAt(through int) Membership {
	m := n.confBase()
	for _, e := range n.st.AllEntries() {
		if e.Index > through {
			break
		}
		if e.Kind == KindConfChange {
			if cc, err := decodeConfChange(e.Payload); err == nil {
				m = m.Apply(cc)
			}
		}
	}
	return m
}

// lastConfIndex 返回现存日志（快照之后）最后一条配置条目的索引，没有则返回 0。
func (n *Node) lastConfIndex() int {
	last := 0
	for _, e := range n.st.AllEntries() {
		if e.Kind == KindConfChange {
			last = e.Index
		}
	}
	return last
}

// reconcileMembership 以快照+日志为唯一事实来源重建当前生效配置、pendingConf
// 与“上一条配置生效前”的 prevMembership。任何时刻调用都是幂等的，
// 不再依赖增量重放，避免条目截断后配置视图残留。
func (n *Node) reconcileMembership() {
	last := n.lastConfIndex()
	n.membership = n.membershipAt(n.st.LastIndex())
	n.pendingConf = n.membership.IsJoint() || last > n.commitIndex
	n.prevMembership = Membership{}
	if last > n.commitIndex {
		n.prevMembership = n.membershipAt(last - 1)
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
func (n *Node) Membership() Membership { return n.membership }

// PendingConf 报告是否仍有成员变更未最终落定。
func (n *Node) PendingConf() bool { return n.pendingConf }

// IsJoint 报告当前是否处于联合配置阶段。
func (n *Node) IsJoint() bool { return n.membership.IsJoint() }

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
	n.preVotes = map[NodeID]bool{}
	n.preVoting = false
	n.resetElectionTimeout()
	n.log.Infof(string(n.id), term, n.quorum(), n.commitIndex, ReasonOK,
		"角色转为 follower 认可领导者=%s", leader)
}

// startPreVote 发起一轮预投票：任期号用 currentTerm+1 但不持久化、不自投，
// 接收方也只做“日志新旧 + 是否已有权威领导者”的判断而不记录投票。
// 预投票凑齐多数派后才真正 becomeCandidate，避免陈旧节点用真实高任期打断在任领导者。
func (n *Node) startPreVote() []OutMsg {
	term := n.st.CurrentTerm() + 1
	n.preVoting = true
	n.preVotes = map[NodeID]bool{}
	n.leader = "" // 已判定现任领导者失联，清掉残留标记以免压制同任期竞选者
	lastTerm, _ := n.st.TermAt(n.st.LastIndex())
	rv := RequestVote{
		From:         n.id,
		Term:         term,
		LastLogIndex: n.st.LastIndex(),
		LastLogTerm:  lastTerm,
		PreVote:      true,
	}
	var out []OutMsg
	for _, peer := range n.membership.List() {
		if peer == n.id {
			continue
		}
		out = append(out, OutMsg{To: peer, RPC: &rv})
	}
	n.log.Infof(string(n.id), n.st.CurrentTerm(), n.quorum(), n.commitIndex, ReasonOK,
		"发起预投票 探测任期=%d lastIndex=%d lastTerm=%d 成员=%d",
		term, rv.LastLogIndex, rv.LastLogTerm, n.membership.Size())
	return out
}

func (n *Node) becomeCandidate() []OutMsg {
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
	for _, peer := range n.membership.List() {
		if peer == n.id {
			continue
		}
		out = append(out, OutMsg{To: peer, RPC: &rv})
	}
	n.log.Infof(string(n.id), term, n.quorum(), n.commitIndex, ReasonOK,
		"开始选举 自投1票 向 %d 个成员拉票 lastIndex=%d lastTerm=%d",
		len(n.membership.List())-1, rv.LastLogIndex, rv.LastLogTerm)
	return out
}

func (n *Node) becomeLeader() []OutMsg {
	term := n.st.CurrentTerm()
	n.role = Leader
	n.leader = n.id
	n.prevMembership = Membership{}
	// 从持久日志/快照重建配置状态：若接管了一个已提交 enter-joint、
	// 但 leave-joint 尚未追加的变更，新领导者要负责把变更推到完成。
	n.reconcileMembership()
	// 复制进度对旧∪新并集全部成员初始化，避免漏发新增节点。
	inited := map[NodeID]bool{n.id: true}
	for _, peer := range n.membership.List() {
		n.nextIndex[peer] = n.st.LastIndex() + 1
		n.matchIndex[peer] = 0
		inited[peer] = true
	}
	for _, peer := range n.prevMembership.List() {
		if !inited[peer] {
			n.nextIndex[peer] = n.st.LastIndex() + 1
			n.matchIndex[peer] = 0
			inited[peer] = true
		}
	}
	n.matchIndex[n.id] = n.st.LastIndex()
	n.heartbeatElapsed = 0
	// 写入当前任期的 no-op 条目锚定提交点（图 8 安全性）：
	// 它随多数派复制提交后，前一任期遗留条目（可能含 enter-joint）一并被提交，
	// 保证新领导者能把接管到的未完成成员变更尽快、确定地推到完成。
	noopIdx := n.st.AppendNew(term, LogEntry{Kind: KindNoop, Payload: nil})
	n.matchIndex[n.id] = noopIdx
	n.log.Infof(string(n.id), term, n.quorum(), n.commitIndex, ReasonOK,
		"获得多数派，成为领导者 成员数=%d 多数派=%d joint=%v noopIndex=%d",
		n.membership.Size(), n.quorum(), n.membership.IsJoint(), noopIdx)
	var out []OutMsg
	// 接管未完成的联合变更：必要时自动追加 leave-joint 条目。
	out = append(out, n.maybeCompleteJointChange()...)
	// 立即广播一次心跳/no-op 以宣告权威。
	out = append(out, n.broadcastAppend()...)
	return out
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
// 成员变更提交前为旧配置与新配置的并集，提交后仅为新配置。
func (n *Node) replicationPeers() []NodeID {
	set := map[NodeID]struct{}{}
	for _, id := range n.membership.List() {
		set[id] = struct{}{}
	}
	if n.pendingConf && n.prevMembership.Size() > 0 {
		for _, id := range n.prevMembership.List() {
			set[id] = struct{}{}
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

// appendTo 为单个跟随者构造追加或快照消息。
func (n *Node) appendTo(peer NodeID) OutMsg {
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
			// 幂等地推进未完成的联合变更（enter 已提交则补追加 leave-joint）。
			out = append(out, n.maybeCompleteJointChange()...)
			out = append(out, n.broadcastAppend()...)
		}
	default:
		// 尚未进入任何成员表的空日志追赶节点不得发起选举：
		// 它在被加入配置前既没有投票权也没有日志，参选只会制造噪声，绝不可能赢。
		if n.membership.Size() == 0 && n.st.LastIndex() == 0 {
			break
		}
		n.electionElapsed++
		// 所认可的领导者已被成员变更移除（离开当前成员表）时，
		// 立即发起预投票补选，而不必傻等整个选举超时。
		if n.leader != "" && n.leader != n.id && !n.membership.Contains(n.leader) {
			n.electionElapsed = n.electionTimeout
		}
		if n.electionElapsed >= n.electionTimeout {
			n.electionElapsed = 0
			// 先发 pre-vote 探测（不增任期、不投自己），赢得预投票才真正选举。
			out = append(out, n.startPreVote()...)
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

// ProposeConfChange 提交单节点成员变更（增/删一个投票成员）。
// 这是向后兼容的便捷接口，等价于只含一个 Add 或 Remove 的批量变更：
// 内部同样通过联合共识两阶段生效。
func (n *Node) ProposeConfChange(client NodeID, cc ConfChange) ClientAck {
	switch cc.Op {
	case ConfAddVoter:
		cc.Add, cc.Remove = []NodeID{cc.Node}, nil
	case ConfRemoveVoter:
		cc.Add, cc.Remove = nil, []NodeID{cc.Node}
	default:
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonBadConfChange}
	}
	cc.Op = ConfBatchVoters
	return n.ProposeConfChangeBatch(client, cc)
}

// ProposeConfChangeBatch 提交一批成员变更：Add 与 Remove 可各自包含多个节点，
// 也允许同时非空（一次里既加又删）。整批变更原子打包为一条 enter-joint
// 配置条目，按联合共识两阶段生效：
//
//  1. enter-joint（C_old,new）落盘后，选举/复制/提交同时受旧、新两份成员表约束；
//  2. enter-joint 经旧、新双方多数派提交后，领导者自动追加 leave-joint（C_new）；
//  3. leave-joint 提交后整批变更才算最终落定，期间拒绝发起下一批。
//
// 返回成功仅表示 enter-joint 条目已在领导者落盘。
func (n *Node) ProposeConfChangeBatch(client NodeID, cc ConfChange) ClientAck {
	if !n.alive || n.removed {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonShutdown}
	}
	if n.role != Leader {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonNotLeader}
	}
	if n.pendingConf {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonMembershipPending}
	}
	cc.normalize()
	if cc.Op != ConfBatchVoters {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonBadConfChange}
	}
	// 合法性校验（全部针对当前“已落定”的普通配置）。
	cur := n.membership.PlainList()
	curSet := map[NodeID]struct{}{}
	for _, id := range cur {
		curSet[id] = struct{}{}
	}
	addSet := map[NodeID]struct{}{}
	remSet := map[NodeID]struct{}{}
	for _, id := range cc.Add {
		addSet[id] = struct{}{}
	}
	for _, id := range cc.Remove {
		remSet[id] = struct{}{}
	}
	if len(cc.Add) == 0 && len(cc.Remove) == 0 {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonBadConfChange}
	}
	for id := range addSet {
		if _, dup := remSet[id]; dup {
			// 同一批里又加又删同一个节点属于歧义请求，显式拒绝。
			return ClientAck{To: client, Term: n.Term(), Reason: ReasonBadConfChange}
		}
		if _, exists := curSet[id]; exists {
			return ClientAck{To: client, Term: n.Term(), Reason: ReasonUnknownVoter}
		}
	}
	for id := range remSet {
		if _, exists := curSet[id]; !exists {
			return ClientAck{To: client, Term: n.Term(), Reason: ReasonUnknownVoter}
		}
	}
	// 目标配置 C_new 不能为空（否则集群失去多数派意义）。
	target := NewMembership()
	for _, id := range cur {
		if _, drop := remSet[id]; !drop {
			target.oldVoters[id] = struct{}{}
		}
	}
	for id := range addSet {
		target.oldVoters[id] = struct{}{}
	}
	if target.Size() == 0 {
		return ClientAck{To: client, Term: n.Term(), Reason: ReasonBadConfChange}
	}

	// 落盘 enter-joint 条目（保留原始单节点 Op 语义仅为日志可读，
	// 协议处理统一以 Phase=enter + Add/Remove 为准）。
	enter := ConfChange{Op: cc.Op, Add: cc.Add, Remove: cc.Remove, Phase: ConfPhaseEnter}
	if len(cc.Add) == 1 && len(cc.Remove) == 0 {
		enter.Op = ConfAddVoter
		enter.Node = cc.Add[0]
	} else if len(cc.Remove) == 1 && len(cc.Add) == 0 {
		enter.Op = ConfRemoveVoter
		enter.Node = cc.Remove[0]
	}
	n.prevMembership = n.membership
	idx := n.st.AppendNew(n.Term(), LogEntry{Kind: KindConfChange, Payload: encodeConfChange(enter)})
	n.matchIndex[n.id] = idx
	n.pendingConf = true
	n.pending = append(n.pending, pendingProposal{index: idx, ackTo: client, kind: KindConfChange})
	// 联合共识规则：enter-joint 一落盘即按 C_old,new 计算双方多数派与复制目标。
	n.reconcileMembership()
	// 为新增节点初始化复制进度。
	for _, peer := range n.membership.NewList() {
		if _, ok := n.matchIndex[peer]; !ok {
			n.matchIndex[peer] = 0
			n.nextIndex[peer] = n.st.LastIndex() + 1
		}
	}
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"领导者落盘 enter-joint index=%d 增=%v 删=%v 旧成员=%d 新成员=%d 旧多数派=%d 新多数派=%d",
		idx, cc.Add, cc.Remove, len(n.prevMembership.PlainList()),
		n.membership.NewQuorum(), n.membership.OldQuorum(), n.membership.NewQuorum())
	n.netEmit(ClientAck{To: client, Term: n.Term(), Index: idx, Success: true, Reason: ReasonOK})
	return ClientAck{To: client, Term: n.Term(), Index: idx, Success: true, Reason: ReasonOK}
}

// maybeCompleteJointChange 在领导者处于联合配置（C_old,new）且 enter-joint 已提交、
// 但尚无 leave-joint 条目时，自动追加 leave-joint 条目，把变更推到完成。
// 该方法幂等：任何 Tick、提交推进或新领导者接管时都可安全调用。
func (n *Node) maybeCompleteJointChange() []OutMsg {
	if n.role != Leader || !n.membership.IsJoint() {
		return nil
	}
	last := n.lastConfIndex()
	if last == 0 {
		return nil
	}
	e, ok := n.st.EntryAt(last)
	if !ok || e.Kind != KindConfChange {
		return nil
	}
	prevCC, err := decodeConfChange(e.Payload)
	if err != nil {
		return nil
	}
	// 已有 leave-joint（无论是否提交），不得重复追加。
	if prevCC.Phase == ConfPhaseLeave {
		return nil
	}
	// enter-joint 未提交前不允许追加 leave-joint。
	if last > n.commitIndex {
		return nil
	}
	leave := ConfChange{
		Op:     prevCC.Op,
		Node:   prevCC.Node,
		Add:    prevCC.Add,
		Remove: prevCC.Remove,
		Phase:  ConfPhaseLeave,
	}
	n.prevMembership = n.membership
	idx := n.st.AppendNew(n.Term(), LogEntry{Kind: KindConfChange, Payload: encodeConfChange(leave)})
	n.matchIndex[n.id] = idx
	n.pendingConf = true
	// leave 一落盘即切换到 C_new；提交前复制目标仍取 C_old,new ∪ C_new（=并集）。
	n.reconcileMembership()
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"enter-joint 已提交，领导者自动追加 leave-joint index=%d 新成员=%v 新多数派=%d",
		idx, n.membership.PlainList(), n.membership.NewQuorum())
	return n.broadcastAppend()
}

// HandleVote 处理投票请求（含预投票）。
func (n *Node) HandleVote(m RequestVote) []OutMsg {
	if !n.alive || n.removed {
		return nil
	}
	reply := RequestVoteResult{From: n.id, To: m.From, Term: n.st.CurrentTerm()}

	// 预投票：不改变任何持久状态（任期、投票均不动），只做资格判断。
	// 陈旧节点的预投票拿不到多数派，就永远不会用真实高任期打断在任领导者。
	if m.PreVote {
		// 预投票任期号必须严格高于本地当前任期（候选人探测的是“下一任期”）。
		if m.Term <= n.st.CurrentTerm() {
			return []OutMsg{{To: m.From, RPC: &RequestVoteResult{
				From: n.id, To: m.From, Term: n.st.CurrentTerm(), Reason: ReasonStaleTerm, PreVote: true,
			}}}
		}
		// 仅当近期确实收到现任领导者心跳（租约窗口内）才拒绝；
		// 领导者已停摆超过一个选举超时后，残留的 n.leader 不得继续压制选举。
		// 本节点自己也已发起预投票时，同样不再用残留租约压制同任期竞选者
		//（双方都判定领导者失联，应允许预投票决出更合适的候选人）。
		if !n.preVoting && n.leader != "" && n.electionElapsed < electionMin && m.From != n.leader {
			n.log.Debugf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonLeaderLease,
				"拒绝预投票给 %s：现任领导者 %s 心跳仍有效", m.From, n.leader)
			return []OutMsg{{To: m.From, RPC: &RequestVoteResult{
				From: n.id, To: m.From, Term: n.st.CurrentTerm(), Reason: ReasonLeaderLease, PreVote: true,
			}}}
		}
		myLastTerm, _ := n.st.TermAt(n.st.LastIndex())
		if m.LastLogTerm < myLastTerm ||
			(m.LastLogTerm == myLastTerm && m.LastLogIndex < n.st.LastIndex()) {
			return []OutMsg{{To: m.From, RPC: &RequestVoteResult{
				From: n.id, To: m.From, Term: n.st.CurrentTerm(), Reason: ReasonLogNotUpToDate, PreVote: true,
			}}}
		}
		n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
			"预投票赞成 %s（候选人日志 t=%d i=%d）", m.From, m.LastLogTerm, m.LastLogIndex)
		return []OutMsg{{To: m.From, RPC: &RequestVoteResult{
			From: n.id, To: m.From, Term: n.st.CurrentTerm(), Granted: true, Reason: ReasonOK, PreVote: true,
		}}}
	}

	if m.Term < n.st.CurrentTerm() {
		reply.Reason = ReasonStaleTerm
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, reply.Reason,
			"拒绝投票：请求任期 %d 落后于本地 %d", m.Term, n.Term())
		return []OutMsg{{To: m.From, RPC: &reply}}
	}
	n.stepDownFromHigherTerm(m.Term)
	reply.Term = n.st.CurrentTerm()

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

// voteMajority 在给定赞成票集合上判断是否满足当选多数派：
// 普通配置一次多数派；联合配置必须旧集合与新集合分别多数派
// （保证联合阶段绝不可能选出一个只被单边承认、却能推进提交的领导者）。
// 多数派只在“存活且可达”的成员上计算：确认已停止/被隔离的成员无法再表态，
// 不应永久占用席位锁死集群（Ongaro 论文 §6.3 的联合阶段活性处理）。
func (n *Node) voteMajority(ballots map[NodeID]bool) bool {
	count := func(voters []NodeID) bool {
		granted, reachable := 0, 0
		for _, v := range voters {
			if !n.canReach(v) {
				continue
			}
			reachable++
			if ballots[v] {
				granted++
			}
		}
		need := reachable/2 + 1
		return reachable > 0 && granted >= need
	}
	if !count(n.membership.NewList()) {
		return false
	}
	if n.membership.IsJoint() && !count(n.membership.OldList()) {
		return false
	}
	return true
}

// HandleVoteResult 处理投票应答（含预投票应答）。
func (n *Node) HandleVoteResult(m RequestVoteResult) []OutMsg {
	if !n.alive || n.removed {
		return nil
	}

	// 预投票应答：节点此时仍是 follower/preVoting，任期未变。
	if m.PreVote {
		if !n.preVoting {
			return nil
		}
		// 应答带回的任期高于本地，说明集群已有更高任期的活动：放弃本轮预投票。
		if m.Term > n.st.CurrentTerm() {
			n.stepDownFromHigherTerm(m.Term)
			n.preVoting = false
			return nil
		}
		n.preVotes[m.From] = m.Granted
		if !m.Granted {
			n.log.Debugf(string(n.id), n.Term(), n.quorum(), n.commitIndex, m.Reason,
				"预投票未获得 %s 赞成", m.From)
		}
		// 预投票里自己算一张赞成票（自己当然支持自己参选）。
		pre := map[NodeID]bool{n.id: true}
		for k, v := range n.preVotes {
			pre[k] = v
		}
		if n.voteMajority(pre) {
			n.preVoting = false
			n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
				"预投票获得多数派，开始正式选举 joint=%v", n.membership.IsJoint())
			return n.becomeCandidate()
		}
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
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"计票：赞成=%d 多数派=%d 总成员=%d joint=%v",
		len(n.votes), n.quorum(), n.membership.Size(), n.membership.IsJoint())
	if n.voteMajority(n.votes) {
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

	// 应用领导者带来的成员变更视图（条目一旦落盘即按新配置计票/计多数派）。
	n.refreshMembershipThrough(match)

	// 提交位置取 min(leaderCommit, 本地最后索引)，绝不自行越过领导者提交点。
	if m.LeaderCommit > n.commitIndex {
		newCommit := m.LeaderCommit
		if newCommit > n.st.LastIndex() {
			newCommit = n.st.LastIndex()
		}
		n.advanceCommitTo(newCommit, "跟随领导者提交点")
	}
	n.applyCommitted()
	return []OutMsg{n.appendAck(m.From, true, match, 0, 0, ReasonOK)}
}

// findConflict 依据本地日志给出快速回退提示：
//   - prevIndex 处存在条目但任期不符：返回该冲突任期在本地的第一条索引；
//   - prevIndex 处缺条目（日志太短）：返回本地最后一条之后的位置，
//     即“把 nextIndex 退到本地末尾之后”，让领导者从本地末尾紧接着补发。
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
	// 本地没有 prevIndex：缺口从本地最后一条之后开始。
	return n.st.LastIndex() + 1, 0
}

// refreshMembershipThrough 依据落盘到 throughIndex 的日志重建生效配置。
// 条目一旦落盘即按新配置计票/计多数派（联合共识规则：
// enter-joint 落盘即要求旧∪新双方多数派才能提交）；
// 但节点“退出协议（removed）”只能在移除配置已提交后发生：
// 否则被移除者会错过携带提交点的心跳，永远无法确认自己的移除已提交。
func (n *Node) refreshMembershipThrough(through int) {
	jointBefore := n.membership.IsJoint()
	n.reconcileMembership()
	jointAfter := n.membership.IsJoint()
	last := n.lastConfIndex()
	if last > 0 && (last <= through || jointBefore || jointAfter) {
		n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
			"配置随日志重建 成员数=%d 多数派=%d joint=%v pendingConf=%v",
			n.membership.Size(), n.quorum(), jointAfter, n.pendingConf)
	}
	n.observeSelfRemoved()
}

// observeSelfRemoved 只在“移除自己”的配置已经提交、且配置已离开联合阶段后
// 才退出协议：联合阶段被移除者仍属于 C_old，必须继续参与复制与投票，
// 直到 leave-joint（C_new）提交，才能确认自己被安全移除。
func (n *Node) observeSelfRemoved() {
	if n.removed || n.membership.IsJoint() || n.membership.Size() == 0 {
		return
	}
	if n.membership.Contains(n.id) {
		return
	}
	// 定位当前配置的来源位置：优先现存日志最后一条配置条目，
	// 配置仅存在于快照中时退化为快照点。
	lastConfIndex := n.lastConfIndex()
	if lastConfIndex == 0 && n.st.HasSnapshot() {
		lastConfIndex = n.st.SnapshotCopy().LastIncludedIndex
	}
	if lastConfIndex > 0 && lastConfIndex <= n.commitIndex {
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
		if e.Kind == KindCommand {
			if err := n.fsm.Apply(idx, e.Term, e.Payload); err != nil {
				n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex,
					ReasonRejectedByApplication, "应用 index=%d 失败: %v", idx, err)
				break
			}
		}
		n.lastApplied = idx
	}
	// 配置条目提交后重建配置视图：可能解除 pending，或从联合阶段转入下一阶段。
	n.reconcileMembership()
	n.resolvePendingProposals()
	n.observeSelfRemoved()
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
	if !n.membership.Contains(peer) {
		return nil
	}
	var out []OutMsg
	if m.Success {
		// 幂等：只有更靠后的确认才推进 matchIndex（重复/乱序应答不产生回退）。
		if m.MatchIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = m.MatchIndex
			n.nextIndex[peer] = m.MatchIndex + 1
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
	// 联合阶段必须分别获得 C_old 与 C_new 的多数派。
	last := n.st.LastIndex()
	for cand := last; cand > n.commitIndex; cand-- {
		term, ok := n.st.TermAt(cand)
		if !ok || term != n.st.CurrentTerm() {
			continue
		}
		if !n.membership.CountMajorities(cand, n.id, n.matchIndex) {
			continue
		}
		// 提交可能让 enter-joint 落定：先固定旧∪新并集，
		// 保证被移除节点也收到携带新提交点的最后一次追加/心跳。
		finalPeers := n.replicationPeers()
		n.advanceCommitTo(cand, "多数派复制确认")
		n.refreshMembershipThrough(n.commitIndex)
		for _, peer := range finalPeers {
			out = append(out, n.appendTo(peer))
		}
		n.applyCommitted()
		// enter-joint 提交后自动进入第二阶段：追加 leave-joint 条目。
		out = append(out, n.maybeCompleteJointChange()...)
		// 提交点变化后向当前复制集合再广播一次，加速收敛。
		out = append(out, n.broadcastAppend()...)
		break
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
	// 快照本身只会在非联合位置生成；快照之后若仍有配置条目则统一重放重建。
	n.reconcileMembership()
	// 注意：不在此依据快照配置判定“自己被移除”。
	// 追赶中的新节点可能收到不含自己的旧配置快照；是否被移除必须在补齐
	// 快照之后的配置条目后，由 observeSelfRemoved 结合完整配置视图判定。
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"落后节点通过快照恢复到 index=%d term=%d 成员数=%d",
		m.LastIncludedIndex, m.LastIncludedTerm, m.Membership.Size())
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
	// 联合配置期间不允许压缩：快照只保存一份成员表，无法完整携带 C_old,new
	// 双集合状态；在联合阶段停留只是暂时的，等 leave-joint 提交后再压缩即可。
	snapMembership := n.membershipAt(index)
	if snapMembership.IsJoint() {
		n.log.Warnf(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonSnapshotJoint,
			"拒绝在联合配置生效位置压缩 index=%d（等待 leave-joint 提交）", index)
		return Snapshot{}, fmt.Errorf("snapshot: index %d 处于联合配置，无法压缩", index)
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
