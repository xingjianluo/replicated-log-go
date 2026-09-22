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

// EntryKind 区分普通命令条目与成员变更条目。
type EntryKind uint8

const (
	KindCommand EntryKind = iota
	KindConfChange
)

func (k EntryKind) String() string {
	switch k {
	case KindConfChange:
		return "conf-change"
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
)

// ConfChange 描述一次单节点成员变更。
// 协议规则：上一次成员变更条目未提交前，不允许发起下一次。
type ConfChange struct {
	Op      ConfChangeOp
	Node    NodeID
	Members []NodeID // 仅 ConfSetVoters 使用
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

// Snapshot 是日志压缩快照。
type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	// Membership 是快照对应位置已生效的成员配置。
	Membership Membership
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
	Data              []byte
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
