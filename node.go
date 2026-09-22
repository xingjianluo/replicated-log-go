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

	// 当前生效配置
	membership Membership
	// prevMembership 为“未提交成员变更”之前的旧配置；
	// 变更条目提交前，领导者需向旧∪新并集复制，确保被移除者也能收到移除条目。
	prevMembership Membership
	// 上一次成员变更尚未提交时为 true
	pendingConf bool
	// 本节点已被成员变更移除
	removed bool

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
		id:         id,
		log:        log,
		st:         st,
		fsm:        fsm,
		rng:        rand.New(rand.NewSource(seed)),
		role:       Follower,
		nextIndex:  map[NodeID]int{},
		matchIndex: map[NodeID]int{},
		votes:      map[NodeID]bool{},
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
func (n *Node) rebuildMembership() {
	m := n.membership
	if n.st.HasSnapshot() {
		m = n.st.SnapshotCopy().Membership
	}
	lastConfIndex := 0
	for _, e := range n.st.AllEntries() {
		if e.Kind == KindConfChange {
			if cc, err := decodeConfChange(e.Payload); err == nil {
				m = m.Apply(cc)
				lastConfIndex = e.Index
			}
		}
	}
	n.membership = m
	n.pendingConf = lastConfIndex > n.commitIndex
	if !m.Contains(n.id) && lastConfIndex > 0 {
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
func (n *Node) Membership() Membership { return n.membership }

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
	n.prevMembership = n.membership
	for _, peer := range n.membership.List() {
		n.nextIndex[peer] = n.st.LastIndex() + 1
		n.matchIndex[peer] = 0
	}
	n.matchIndex[n.id] = n.st.LastIndex()
	n.heartbeatElapsed = 0
	n.log.Infof(string(n.id), term, n.quorum(), n.commitIndex, ReasonOK,
		"获得多数派，成为领导者 成员数=%d 多数派=%d", n.membership.Size(), n.quorum())
	// 立即广播一次心跳以宣告权威。
	return n.broadcastAppend()
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
			out = append(out, n.broadcastAppend()...)
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
	granted := 0
	for _, v := range n.votes {
		if v {
			granted++
		}
	}
	n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
		"计票：赞成=%d 多数派=%d 总成员=%d", granted, n.quorum(), n.membership.Size())
	if granted >= n.quorum() {
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

// refreshMembershipThrough 重放至 throughIndex 的配置条目，更新生效配置。
// 多数派计数可以在条目落盘后立即按新配置进行（单节点变更规则），
// 但节点“退出协议（removed）”只能在该变更已提交后发生：
// 否则被移除者会错过携带提交点的心跳，永远无法确认自己的移除已提交。
func (n *Node) refreshMembershipThrough(through int) {
	lastConfIndex := 0
	for _, e := range n.st.AllEntries() {
		if e.Index > through {
			break
		}
		if e.Kind == KindConfChange {
			if cc, err := decodeConfChange(e.Payload); err == nil {
				n.membership = n.membership.Apply(cc)
				lastConfIndex = e.Index
			}
		}
	}
	if lastConfIndex > 0 {
		n.pendingConf = lastConfIndex > n.commitIndex
		n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
			"配置随日志生效 成员数=%d 多数派=%d pendingConf=%v",
			n.membership.Size(), n.quorum(), n.pendingConf)
	}
	n.observeSelfRemoved()
}

// observeSelfRemoved 在“移除自己”的配置已提交后才退出协议。
// 判定依据：当前生效配置（来自日志或快照，均为已提交/已确认的配置视图）
// 不再包含自己，且该配置的来源位置不超过 commitIndex。
func (n *Node) observeSelfRemoved() {
	if n.removed || n.membership.Size() == 0 || n.membership.Contains(n.id) {
		return
	}
	// 找到最近一次配置条目的索引；若配置仅来自快照，则位置就是快照点。
	lastConfIndex := 0
	if n.st.HasSnapshot() {
		lastConfIndex = n.st.SnapshotCopy().LastIncludedIndex
	}
	for _, e := range n.st.AllEntries() {
		if e.Kind == KindConfChange {
			lastConfIndex = e.Index
		}
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
	// 成员变更一旦提交，解除 pending 阻塞。
	if n.pendingConf {
		lastConf := 0
		for _, e := range n.st.AllEntries() {
			if e.Kind == KindConfChange {
				lastConf = e.Index
			}
		}
		if lastConf <= n.commitIndex && lastConf > 0 {
			n.pendingConf = false
			n.prevMembership = Membership{}
			n.log.Infof(string(n.id), n.Term(), n.quorum(), n.commitIndex, ReasonOK,
				"成员变更已提交（index=%d），允许下一次变更", lastConf)
		}
	}
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
	last := n.st.LastIndex()
	for cand := last; cand > n.commitIndex; cand-- {
		term, ok := n.st.TermAt(cand)
		if !ok || term != n.st.CurrentTerm() {
			continue
		}
		count := 1 // 领导者自身
		for _, voter := range n.membership.List() {
			if voter != n.id && n.matchIndex[voter] >= cand {
				count++
			}
		}
		if count >= n.quorum() {
			// 若本次提交结束了一个待生效成员变更，先固定旧∪新并集，
			// 保证被移除节点也收到携带新提交点的最后一次追加/心跳。
			finalPeers := n.replicationPeers()
			n.advanceCommitTo(cand, "多数派复制确认")
			n.refreshMembershipThrough(n.commitIndex)
			for _, peer := range finalPeers {
				out = append(out, n.appendTo(peer))
			}
			n.applyCommitted()
			// 提交点变化后向新配置集合再广播一次，加速收敛。
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
	n.pendingConf = false
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
