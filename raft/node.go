package raft

import (
	"hash/fnv"
	"math/rand"
)

// Logger 是节点诊断日志接口（判定依据必须可解释）。
type Logger interface {
	Logf(format string, args ...any)
}

// discardLogger 是默认的空日志实现。
type discardLogger struct{}

func (discardLogger) Logf(string, ...any) {}

// Config 是单个 Raft 节点的构造参数。
type Config struct {
	ID            NodeID
	Peers         []NodeID // 全新集群的初始成员（仅在空存储时生效）
	Storage       Storage
	StateMachine  StateMachine
	ElectionTick  int // 选举超时基准 tick 数；实际超时在 [n,2n) 内随机
	HeartbeatTick int // 心跳间隔 tick 数
	Seed          int64
	Logger        Logger
}

// Status 是节点当前状态的只读快照，供测试与演示断言。
type Status struct {
	ID        NodeID
	Role      Role
	Term      uint64
	Leader    NodeID
	VotedFor  NodeID
	Commit    uint64
	Applied   uint64
	LastIndex uint64
	Config    Configuration
}

// progress 记录领导者视角下某个跟随者的复制进度。
type progress struct {
	nextIndex  uint64
	matchIndex uint64
}

// Node 是一个 Raft 节点状态机。它本身不感知真实时间与网络：
// Tick 推进逻辑时钟，Step 注入消息，产生的回复通过返回值/DrainMessages 取出。
// 所有输入串行处理，因此在单 goroutine 的确定性模拟器中结果可复现。
type Node struct {
	id      NodeID
	st      Storage
	sm      StateMachine
	log     Logger
	rng     *rand.Rand
	electTo int
	hbTo    int

	role Role
	term uint64
	vote NodeID
	lead NodeID

	conf Configuration

	elapsed int // 距上次收到心跳/发起选举经过的 tick
	timeout int // 当前随机化的选举超时

	// 领导者/候选者状态
	pr               map[NodeID]*progress
	votes            map[NodeID]bool
	pendingConfIndex uint64 // 尚未提交的成员变更条目位置；0 表示无在途变更

	commitIndex uint64

	msgs    []Message
	stopped bool
}

const (
	defaultElectionTick  = 10
	defaultHeartbeatTick = 2
)

// NewNode 根据配置创建节点。
// 空存储会被引导（bootstrap）：写入 index=1、term=0 的初始配置条目；
// 非空存储按持久化内容恢复任期、投票、配置与快照位置（节点重启路径）。
func NewNode(cfg Config) (*Node, error) {
	if cfg.Storage == nil {
		cfg.Storage = NewMemoryStore()
	}
	if cfg.StateMachine == nil {
		cfg.StateMachine = NewKVStateMachine()
	}
	if cfg.Logger == nil {
		cfg.Logger = discardLogger{}
	}
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = defaultElectionTick
	}
	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = defaultHeartbeatTick
	}
	seed := cfg.Seed
	if cfg.ID != "" {
		h := fnv.New64a()
		_, _ = h.Write([]byte(cfg.ID))
		seed += int64(h.Sum64())
	}
	n := &Node{
		id:      cfg.ID,
		st:      cfg.Storage,
		sm:      cfg.StateMachine,
		log:     cfg.Logger,
		rng:     rand.New(rand.NewSource(seed)),
		electTo: cfg.ElectionTick,
		hbTo:    cfg.HeartbeatTick,
		role:    Follower,
		pr:      map[NodeID]*progress{},
		votes:   map[NodeID]bool{},
	}

	snap := n.st.Snapshot()
	n.commitIndex = snap.Index
	if n.st.LastIndex() == 0 && snap.Index == 0 {
		// 全新集群：写入初始配置条目。
		n.conf = Configuration{Voters: append([]NodeID(nil), cfg.Peers...)}
		boot := Entry{Term: 0, Index: 1, Type: EntryConfChange, Data: encodeConf(n.conf)}
		if err := n.st.Append([]Entry{boot}); err != nil {
			return nil, err
		}
		if err := n.st.SaveHardState(0, ""); err != nil {
			return nil, err
		}
	} else {
		t, v, err := n.st.HardState()
		if err != nil {
			return nil, err
		}
		n.term, n.vote = t, v
		// Raft 规则：是否投票/复制一律以日志中最新配置为准（即使它尚未提交）。
		conf, err := latestConfigAt(n.st, snap, n.st.LastIndex())
		if err != nil {
			return nil, err
		}
		n.conf = conf
	}
	n.resetTimeout()
	return n, nil
}

// ---- 基础工具 ----

func (n *Node) resetTimeout() {
	// 确定性的“随机”超时：仅依赖注入的种子，区间 [electTo, 2*electTo)。
	n.timeout = n.electTo + n.rng.Intn(n.electTo)
	n.elapsed = 0
}

func (n *Node) persist() {
	_ = n.st.SaveHardState(n.term, n.vote)
}

func (n *Node) send(m Message) {
	if m.From == "" {
		m.From = n.id
	}
	n.msgs = append(n.msgs, m)
}

func (n *Node) reply(to Message, mt MessageType) Message {
	return Message{Type: mt, From: n.id, To: to.From, Term: n.term}
}

// replicaSet 返回领导者需要复制的全部节点（含自己）。
func (n *Node) replicaSet() []NodeID {
	seen := map[NodeID]bool{n.id: true}
	out := []NodeID{n.id}
	add := func(ids []NodeID) {
		for _, id := range ids {
			if !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
	}
	add(n.conf.Voters)
	if n.conf.Joint {
		add(n.conf.NewVoters)
	}
	return out
}

func (n *Node) lastIndex() uint64 { return n.st.LastIndex() }

func (n *Node) lastTerm() uint64 {
	t, _ := n.st.Term(n.lastIndex())
	return t
}

// becomeFollower 归位为跟随者；term 变化时清空本任期投票。
func (n *Node) becomeFollower(term uint64, leader NodeID) {
	if term > n.term {
		n.term, n.vote = term, ""
		n.persist()
	}
	n.role, n.lead = Follower, leader
	n.resetTimeout()
}

// ---- 对外 API ----

// Tick 推进一个逻辑时钟单位：领导者按心跳间隔广播，
// 其余角色在随机选举超时无消息时发起选举。
func (n *Node) Tick() []Message {
	if n.stopped {
		return nil
	}
	n.elapsed++
	switch {
	case n.role == Leader:
		if n.elapsed%n.hbTo == 0 {
			n.bcastAppend()
		}
	case n.elapsed >= n.timeout:
		n.campaign()
	}
	return n.DrainMessages()
}

// Step 处理一条消息：MsgHup/MsgBeat 为本地定时器消息，其余为网络消息。
func (n *Node) Step(m Message) ([]Message, error) {
	if n.stopped {
		return nil, ErrStepStopped
	}
	// 统一的任期归位规则（Raft 5.1）：
	// 发现更高任期立即转为跟随者；更低任期的请求一律拒绝并说明原因。
	switch m.Type {
	case MsgHup, MsgBeat:
		// 本地消息，From 必须是自己。
	default:
		if m.From != n.id && m.Term > n.term {
			leader := m.From
			if m.Type == MsgVote || m.Type == MsgVoteResp || m.Type == MsgAppResp || m.Type == MsgSnapResp {
				leader = "" // 拉票/响应消息不携带有效领导者身份
			}
			n.becomeFollower(m.Term, leader)
		}
		if m.Term < n.term && isRequest(m.Type) {
			n.rejectStale(m)
			return n.DrainMessages(), nil
		}
	}

	switch m.Type {
	case MsgHup:
		if n.role != Leader {
			n.campaign()
		}
	case MsgBeat:
		if n.role == Leader {
			n.bcastAppend()
		}
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgApp:
		n.handleAppend(m)
	case MsgAppResp:
		n.handleAppendResp(m)
	case MsgSnap:
		n.handleSnapshot(m)
	case MsgSnapResp:
		n.handleSnapshotResp(m)
	default:
	}
	n.applyCommitted()
	return n.DrainMessages(), nil
}

func isRequest(t MessageType) bool {
	return t == MsgVote || t == MsgApp || t == MsgSnap
}

func (n *Node) rejectStale(m Message) {
	reason := RejectStaleTerm
	var r Message
	switch m.Type {
	case MsgVote:
		r = n.reply(m, MsgVoteResp)
		r.Reject, r.RejectReason = true, reason
	case MsgApp:
		r = n.reply(m, MsgAppResp)
		r.Reject, r.RejectReason = true, reason
	case MsgSnap:
		r = n.reply(m, MsgSnapResp)
		r.Reject, r.RejectReason = true, reason
	}
	n.log.Logf("[%s] term=%d 拒绝 %s 来自 %s：%s（对端任期 %d 落后）",
		n.id, n.term, m.Type, m.From, reason, m.Term)
	n.send(r)
}

// DrainMessages 取出并清空待发送消息。
func (n *Node) DrainMessages() []Message {
	out := n.msgs
	n.msgs = nil
	return out
}

// Propose 由领导者受理一条普通客户端命令；非领导者返回 ErrNotLeader，
// 成员变更在途时返回 ErrChangePending。
func (n *Node) Propose(data []byte) error {
	if n.stopped {
		return ErrStepStopped
	}
	if n.role != Leader {
		return ErrNotLeader
	}
	if n.pendingConfIndex > 0 {
		return ErrChangePending
	}
	e := Entry{Term: n.term, Index: n.lastIndex() + 1, Type: EntryNormal, Data: append([]byte(nil), data...)}
	if err := n.st.Append([]Entry{e}); err != nil {
		return err
	}
	n.log.Logf("[%s] term=%d 领导者追加提案 index=%d", n.id, n.term, e.Index)
	n.bcastAppend()
	return nil
}

// ProposeConfChange 由领导者受理一次单节点成员变更。
// 规则：同一时刻至多一个在途变更；空操作返回 ErrNoChange；
// 不允许移除最后一个投票成员。
func (n *Node) ProposeConfChange(cc ConfChange) error {
	if n.stopped {
		return ErrStepStopped
	}
	if n.role != Leader {
		return ErrNotLeader
	}
	if n.pendingConfIndex > 0 {
		return ErrChangePending
	}
	newConf, err := applyConfChange(n.conf, cc)
	if err != nil {
		return err
	}
	if len(newConf.Voters) == 0 {
		return ErrNoChange
	}
	e := Entry{Term: n.term, Index: n.lastIndex() + 1, Type: EntryConfChange, Data: encodeConf(newConf)}
	if err := n.st.Append([]Entry{e}); err != nil {
		return err
	}
	n.pendingConfIndex = e.Index
	n.conf = confClone(newConf) // 领导者立即按新配置复制（单成员变更直接切换）
	for _, id := range n.replicaSet() {
		if _, ok := n.pr[id]; !ok {
			n.pr[id] = &progress{nextIndex: n.lastIndex() + 1, matchIndex: 0}
		}
	}
	n.log.Logf("[%s] term=%d 成员变更提案 index=%d：%v -> 投票集=%v（需复制到新多数派后提交）",
		n.id, n.term, e.Index, cc.Node, newConf.Voters)
	n.bcastAppend()
	return nil
}

// TakeSnapshot 以当前已应用位置为界压缩日志：状态机产生快照字节，
// 存储丢弃快照点之前的日志；已提交位置与之后的增量日志不受影响。
func (n *Node) TakeSnapshot() (Snapshot, error) {
	idx := n.sm.AppliedIndex()
	if idx <= n.st.Snapshot().Index {
		return n.st.Snapshot(), ErrCompacted
	}
	t, err := n.st.Term(idx)
	if err != nil {
		return Snapshot{}, err
	}
	data, err := n.sm.Snapshot()
	if err != nil {
		return Snapshot{}, err
	}
	conf, err := latestConfigAt(n.st, n.st.Snapshot(), idx)
	if err != nil {
		return Snapshot{}, err
	}
	snap := Snapshot{Data: data, Index: idx, Term: t, Conf: conf}
	if err := n.st.Compact(snap); err != nil {
		return Snapshot{}, err
	}
	n.log.Logf("[%s] term=%d 生成快照 index=%d term=%d，压缩后日志区间 [%d,%d]",
		n.id, n.term, snap.Index, snap.Term, n.st.FirstIndex(), n.st.LastIndex())
	return n.st.Snapshot(), nil
}

// Status 返回节点当前状态。
func (n *Node) Status() Status {
	return Status{
		ID: n.id, Role: n.role, Term: n.term, Leader: n.lead, VotedFor: n.vote,
		Commit: n.commitIndex, Applied: n.sm.AppliedIndex(), LastIndex: n.lastIndex(),
		Config: confClone(n.conf),
	}
}

// Stop 关闭节点。
func (n *Node) Stop() { n.stopped = true }

// applyCommitted 按序把 (applied, commit] 应用到状态机，并激活已提交配置。
// 应用幂等由状态机按位置保证；领导者若发现自己已被移除，提交后自动下台。
func (n *Node) applyCommitted() {
	if n.commitIndex <= n.st.Snapshot().Index {
		n.commitIndex = n.st.Snapshot().Index
	}
	for n.sm.AppliedIndex() < n.commitIndex {
		i := n.sm.AppliedIndex() + 1
		es, err := n.st.Entries(i, i+1)
		if err != nil || len(es) == 0 {
			break
		}
		e := es[0]
		if e.Type == EntryConfChange {
			c, err := decodeConf(e.Data)
			if err == nil {
				n.conf = c
			}
			if n.pendingConfIndex == e.Index {
				n.pendingConfIndex = 0
				n.log.Logf("[%s] term=%d 成员变更已随 index=%d 提交生效，投票集=%v（多数派=%d/%d）",
					n.id, n.term, e.Index, c.Voters, Quorum(len(c.Voters)), len(c.Voters))
			}
		}
		n.sm.Apply(e.Index, e.Term, e.Data)
	}
	if n.role == Leader && !inConfig(n.id, n.conf.Voters) {
		n.log.Logf("[%s] term=%d 提交配置已不含本节点，领导者自动降级为跟随者", n.id, n.term)
		n.becomeFollower(n.term, "")
	}
}

// maybeCommit 在复制进度推进后尝试提升提交位置。
// 关键约束：只有多数派复制了“当前任期”的条目才允许提交（Figure 8），
// 旧任期条目不能仅凭副本数提交。
func (n *Node) maybeCommit() {
	for c := n.lastIndex(); c > n.commitIndex; c-- {
		t, err := n.st.Term(c)
		if err != nil || t != n.term {
			continue
		}
		ack := map[NodeID]bool{n.id: n.lastIndex() >= c}
		for _, id := range n.replicaSet() {
			if id == n.id {
				continue
			}
			if p, ok := n.pr[id]; ok && p.matchIndex >= c {
				ack[id] = true
			}
		}
		if ackMajority(ack, n.conf) {
			n.commitIndex = c
			n.log.Logf("[%s] term=%d 提交位置推进 commit=%d（多数派=%d/%d）",
				n.id, n.term, c, Quorum(len(n.conf.Voters)), len(n.conf.Voters))
			n.applyCommitted()
			return
		}
	}
}
