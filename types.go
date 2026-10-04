package raft

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// NodeID 是集群内节点的唯一标识。
type NodeID string

// Role 表示节点在 Raft 协议中的角色。
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "follower"
	}
}

// EntryKind 区分普通命令条目、成员变更条目与内部空操作条目。
type EntryKind uint8

const (
	KindCommand EntryKind = iota
	KindConfChange
	// KindNoop 是集群内部为推进联合共识而追加的空操作条目：
	// 由协议自身产生、绝不进入状态机。它与客户端命令通过条目类型区分，
	// 因此客户端命令的负载内容没有任何保留字或禁止值。
	KindNoop
)

func (k EntryKind) String() string {
	switch k {
	case KindConfChange:
		return "conf-change"
	case KindNoop:
		return "noop"
	default:
		return "command"
	}
}

// LogEntry 是日志中的一个条目。
type LogEntry struct {
	Index   int
	Term    int
	Kind    EntryKind
	Payload []byte
}

// ConfChangeOp 是成员变更操作类型。
type ConfChangeOp uint8

const (
	ConfAddVoter ConfChangeOp = iota
	ConfRemoveVoter
	// ConfSetVoters 仅用于集群引导：用完整成员列表一次性建立初始配置，
	// 作为 index=1 的引导条目，使后续新增节点可通过日志重放得到配置。
	ConfSetVoters
	// ConfEnterJoint / ConfLeaveJoint 用于批量成员变更（联合共识两阶段）：
	// enter-joint 条目的 Members 是目标配置 C_new；该条目生效后集群处于
	// C_old∪C_new 联合配置，提交与选举都必须同时拿到双方多数派；
	// 紧随其后的 leave-joint 条目（Members 同为 C_new）提交后，集群切换到 C_new。
	ConfEnterJoint
	ConfLeaveJoint
)

// ConfChange 描述一次单节点成员变更。
// 协议规则：上一次成员变更条目未提交前，不允许发起下一次。
type ConfChange struct {
	Op      ConfChangeOp
	Node    NodeID
	Members []NodeID // ConfSetVoters / ConfEnterJoint / ConfLeaveJoint 使用
}

// BatchConfChange 描述一次原子批量成员变更：Adds 与 Removes 在同一次
// 联合共识变更中全部生效或全部不生效（随 enter-joint 条目被日志截断而放弃）。
// 同一节点不允许同时出现在 Adds 与 Removes 中；Adds 不可与现有投票成员重复，
// Removes 必须都是当前投票成员。
type BatchConfChange struct {
	Adds    []NodeID
	Removes []NodeID
}

// TargetMembership 以当前配置 m 为基准计算批量变更后的目标配置。
func (m Membership) TargetMembership(b BatchConfChange) Membership {
	set := map[NodeID]struct{}{}
	for _, id := range m.List() {
		set[id] = struct{}{}
	}
	for _, id := range b.Removes {
		delete(set, id)
	}
	for _, id := range b.Adds {
		set[id] = struct{}{}
	}
	out := make([]NodeID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return NewMembership(out...)
}

// Membership 是一个不可变的投票成员集合。
type Membership struct {
	voters map[NodeID]struct{}
}

// NewMembership 依据给定节点构造成员集合。
func NewMembership(ids ...NodeID) Membership {
	m := Membership{voters: map[NodeID]struct{}{}}
	for _, id := range ids {
		m.voters[id] = struct{}{}
	}
	return m
}

// Contains 判断节点是否为投票成员。
func (m Membership) Contains(id NodeID) bool {
	_, ok := m.voters[id]
	return ok
}

// Size 返回投票成员数量。
func (m Membership) Size() int { return len(m.voters) }

// Quorum 返回多数派阈值（size/2 + 1）。
func (m Membership) Quorum() int { return m.Size()/2 + 1 }

// List 按字典序返回全部投票成员，保证输出确定。
func (m Membership) List() []NodeID {
	out := make([]NodeID, 0, len(m.voters))
	for id := range m.voters {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Apply 返回应用一次单节点变更后的新成员集合。
func (m Membership) Apply(cc ConfChange) Membership {
	next := NewMembership(m.List()...)
	switch cc.Op {
	case ConfAddVoter:
		next.voters[cc.Node] = struct{}{}
	case ConfRemoveVoter:
		delete(next.voters, cc.Node)
	case ConfSetVoters:
		next = NewMembership(cc.Members...)
	}
	return next
}

// Equal 判断两个成员集合是否相同。
func (m Membership) Equal(other Membership) bool {
	if m.Size() != other.Size() {
		return false
	}
	for id := range m.voters {
		if !other.Contains(id) {
			return false
		}
	}
	return true
}

// Union 返回两个成员集合的并集（联合共识阶段 C_old∪C_new 的复制/投票对象）。
func (m Membership) Union(other Membership) Membership {
	set := map[NodeID]struct{}{}
	for id := range m.voters {
		set[id] = struct{}{}
	}
	for id := range other.voters {
		set[id] = struct{}{}
	}
	out := make([]NodeID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return NewMembership(out...)
}

// Snapshot 是日志压缩快照。
type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	// Membership 是快照对应位置已生效的成员配置。
	Membership Membership
	// JointValid 为 true 时表示快照点处于联合共识中间态：
	// Membership 为 C_new，JointOld 为 C_old，双多数派约束仍然有效。
	JointValid bool
	JointOld   Membership
	// Data 是应用层状态快照（本组件中为确定性命令序列的编码）。
	Data []byte
}

// ---- RPC 消息（均为值对象，由确定性网络投递）----

// RequestVote 是候选人发起的投票请求。
type RequestVote struct {
	From         NodeID
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteResult 是投票结果。Granted 为 false 时 Reason 给出可区分的原因码。
type RequestVoteResult struct {
	From    NodeID
	To      NodeID
	Term    int
	Granted bool
	Reason  Reason
}

// AppendEntries 是领导者的日志复制/心跳消息。
type AppendEntries struct {
	From         NodeID
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendResult 是追加结果；失败时通过冲突索引与任期帮助领导者快速回退。
type AppendResult struct {
	From          NodeID
	To            NodeID
	Term          int
	Success       bool
	MatchIndex    int
	ConflictIndex int
	ConflictTerm  int
	Reason        Reason
}

// InstallSnapshot 是领导者向严重落后节点发送快照的消息。
type InstallSnapshot struct {
	From              NodeID
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Membership        Membership
	// JointValid/JointOld 携带快照点的联合共识中间态，语义同 Snapshot。
	JointValid bool
	JointOld   Membership
	Data       []byte
}

// InstallSnapshotResult 是快照安装结果。
type InstallSnapshotResult struct {
	From       NodeID
	To         NodeID
	Term       int
	Success    bool
	MatchIndex int
	Reason     Reason
}

// encodeConfChange / decodeConfChange 用确定性 JSON 编码成员变更条目负载。
func encodeConfChange(cc ConfChange) []byte {
	s := `{"op":` + itoa(int(cc.Op)) + `,"node":"` + string(cc.Node) + `","members":[`
	for i, m := range cc.Members {
		if i > 0 {
			s += ","
		}
		s += `"` + string(m) + `"`
	}
	s += `]}`
	return []byte(s)
}

func decodeConfChange(data []byte) (ConfChange, error) {
	var cc ConfChange
	text := string(data)
	if len(text) == 0 {
		return cc, fmt.Errorf("empty conf change payload")
	}
	opPart := strings.TrimPrefix(text, `{"op":`)
	rest := strings.SplitN(opPart, `,"node":"`, 2)
	if len(rest) != 2 {
		return cc, fmt.Errorf("bad conf change payload: %s", text)
	}
	op, err := strconv.Atoi(rest[0])
	if err != nil {
		return cc, err
	}
	nodePart := strings.SplitN(rest[1], `","members":[`, 2)
	if len(nodePart) != 2 {
		return cc, fmt.Errorf("bad conf change payload: %s", text)
	}
	cc.Op = ConfChangeOp(op)
	cc.Node = NodeID(nodePart[0])
	mem := strings.TrimSuffix(nodePart[1], `]}`)
	if mem != "" {
		for _, m := range strings.Split(mem, `","`) {
			cc.Members = append(cc.Members, NodeID(strings.Trim(m, `"`)))
		}
	}
	return cc, nil
}

func itoa(i int) string { return strconv.Itoa(i) }
