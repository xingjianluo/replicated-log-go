package raft

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
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
	// KindNoop 是领导者当选时写入的空操作条目：
	// 用于在当前任期锚定一个可提交点（Raft 图 8 安全性），
	// 使新领导者能尽快提交前一任期遗留的日志（含未完成的联合配置条目）。
	// no-op 不进入状态机、不产生客户端应答。
	KindNoop
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
	// ConfAddVoter / ConfRemoveVoter 为单节点增删（向后兼容的便捷操作，
	// 内部同样走联合共识两阶段提交）。
	ConfAddVoter ConfChangeOp = iota
	ConfRemoveVoter
	// ConfSetVoters 仅用于集群引导：用完整成员列表一次性建立初始配置，
	// 作为 index=1 的引导条目，使后续新增节点可通过日志重放得到配置。
	ConfSetVoters
	// ConfBatchVoters 为批量成员变更：一次提交中同时包含 Add/Remove 两个列表，
	// 条目进入日志后按联合共识（joint consensus）两阶段生效。
	ConfBatchVoters
)

// ConfPhase 标识一条成员变更条目处于联合共识的哪个阶段。
type ConfPhase uint8

const (
	// ConfPhaseNone 表示该条目不使用阶段标记（引导/旧式单节点编码也按 none 处理）。
	ConfPhaseNone ConfPhase = iota
	// ConfPhaseEnter 表示进入联合配置 C_old,new（两阶段的第一条）。
	ConfPhaseEnter
	// ConfPhaseLeave 表示离开联合配置、切换到 C_new（两阶段的第二条）。
	ConfPhaseLeave
)

// ConfChange 描述一次成员变更。
//
// 单节点便捷用法：Op=ConfAddVoter/ConfRemoveVoter 且 Node 非空，
// 与历史版本完全兼容。
//
// 批量用法：Op=ConfBatchVoters，Add/Remove 分别给出要加入与要移除的投票成员；
// 允许同时非空（一次里既加又删）。整个批量变更通过联合共识原子生效：
// 上一批变更的两阶段条目全部提交前，不允许发起下一批。
type ConfChange struct {
	Op      ConfChangeOp
	Node    NodeID    // ConfAddVoter / ConfRemoveVoter 使用
	Members []NodeID  // ConfSetVoters 使用
	Add     []NodeID  // ConfBatchVoters：要加入的成员（自动去重排序）
	Remove  []NodeID  // ConfBatchVoters：要移除的成员（自动去重排序）
	Phase   ConfPhase // 联合共识阶段：enter-joint / leave-joint
}

// normalize 规整成员列表：去重并按字典序排序，保证编码与日志逐字节确定。
// 单节点增删操作同时折算到 Add/Remove 视图，方便统一计算目标配置。
func (cc *ConfChange) normalize() {
	switch cc.Op {
	case ConfAddVoter:
		cc.Add = []NodeID{cc.Node}
		cc.Remove = nil
	case ConfRemoveVoter:
		cc.Add = nil
		cc.Remove = []NodeID{cc.Node}
	}
	cc.Add = sortedUnique(cc.Add)
	cc.Remove = sortedUnique(cc.Remove)
	cc.Members = sortedUnique(cc.Members)
}

func sortedUnique(ids []NodeID) []NodeID {
	if len(ids) == 0 {
		return nil
	}
	out := append([]NodeID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	w := 0
	for i := range out {
		if i > 0 && out[i] == out[i-1] {
			continue
		}
		out[w] = out[i]
		w++
	}
	return out[:w]
}

// Membership 是一个不可变的成员集合。
//
// 普通配置只有一份投票成员集合（oldVoters）。
// 联合配置（joint consensus 的 C_old,new 阶段）同时持有旧、新两份集合，
// 此时选举、投票与提交都必须分别获得旧集合多数派与新集合多数派。
type Membership struct {
	oldVoters map[NodeID]struct{}
	// newVoters 非空表示当前正处于联合配置：
	// 旧集合为 oldVoters，新集合为 newVoters，提交需要双方各自多数派。
	newVoters map[NodeID]struct{}
}

// NewMembership 依据给定节点构造普通（非联合）成员集合。
func NewMembership(ids ...NodeID) Membership {
	m := Membership{oldVoters: map[NodeID]struct{}{}}
	for _, id := range ids {
		m.oldVoters[id] = struct{}{}
	}
	return m
}

// newJointMembership 构造联合配置 C_old,new（仅协议内部使用）。
func newJointMembership(old, fresh Membership) Membership {
	return Membership{
		oldVoters: voterCopy(old.oldVoters),
		newVoters: voterCopy(fresh.oldVoters),
	}
}

func voterCopy(src map[NodeID]struct{}) map[NodeID]struct{} {
	dst := make(map[NodeID]struct{}, len(src))
	for id := range src {
		dst[id] = struct{}{}
	}
	return dst
}

// IsJoint 报告当前是否处于联合配置（enter-joint 已生效、leave-joint 未生效）。
func (m Membership) IsJoint() bool { return len(m.newVoters) > 0 }

// Contains 判断节点是否属于当前配置：联合阶段为旧∪新并集的成员。
// 即：被移除者在联合阶段仍可收/处理复制消息，新加入者在联合阶段已可投票。
func (m Membership) Contains(id NodeID) bool {
	if _, ok := m.oldVoters[id]; ok {
		return true
	}
	if m.IsJoint() {
		if _, ok := m.newVoters[id]; ok {
			return true
		}
	}
	return false
}

// Size 返回参与当前协议的成员数：联合阶段为旧∪新并集大小。
func (m Membership) Size() int { return len(m.UnionList()) }

// Quorum 返回多数派阈值。
// 普通配置返回 size/2+1；联合配置返回 max(旧多数派, 新多数派)，
// 仅用于日志展示——联合阶段真正的提交判定必须分别满足双方多数派。
func (m Membership) Quorum() int {
	q := len(m.oldVoters)/2 + 1
	if m.IsJoint() {
		if qn := len(m.newVoters)/2 + 1; qn > q {
			q = qn
		}
	}
	return q
}

// OldQuorum 返回联合配置中旧集合的多数派阈值（普通配置等同 Quorum）。
func (m Membership) OldQuorum() int { return len(m.oldVoters)/2 + 1 }

// NewQuorum 返回联合配置中新集合的多数派阈值（普通配置等同 Quorum）。
func (m Membership) NewQuorum() int {
	if m.IsJoint() {
		return len(m.newVoters)/2 + 1
	}
	return len(m.oldVoters)/2 + 1
}

// List 按字典序返回全部参与成员：联合阶段为旧∪新并集，普通配置为当前成员。
func (m Membership) List() []NodeID { return m.UnionList() }

// UnionList 按字典序返回旧∪新并集成员。
func (m Membership) UnionList() []NodeID {
	set := voterCopy(m.oldVoters)
	for id := range m.newVoters {
		set[id] = struct{}{}
	}
	out := make([]NodeID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// NewList 返回联合配置中新集合成员；普通配置返回当前成员。
func (m Membership) NewList() []NodeID {
	src := m.oldVoters
	if m.IsJoint() {
		src = m.newVoters
	}
	out := make([]NodeID, 0, len(src))
	for id := range src {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// OldList 返回联合配置中旧集合成员；普通配置返回当前成员。
func (m Membership) OldList() []NodeID {
	out := make([]NodeID, 0, len(m.oldVoters))
	for id := range m.oldVoters {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// CountMajorities 在给定各节点匹配位置 match 下，判断索引 idx 是否分别满足
// 旧集合多数派与新集合多数派（self 为领导者自身标识，其本地日志必含 idx）。
// 普通配置只有一次多数派判断；联合配置必须两次都成立。
func (m Membership) CountMajorities(idx int, self NodeID, match map[NodeID]int) bool {
	majority := func(voters map[NodeID]struct{}) bool {
		count := 0
		for v := range voters {
			if v == self || match[v] >= idx {
				count++
			}
		}
		return count >= len(voters)/2+1
	}
	if !majority(m.oldVoters) {
		return false
	}
	if m.IsJoint() && !majority(m.newVoters) {
		return false
	}
	return true
}

// Apply 返回应用一次成员变更条目后的新成员集合。
//
//   - ConfSetVoters：整体替换为给定成员（仅引导条目）。
//   - ConfAddVoter/ConfRemoveVoter/ConfBatchVoters（Phase=enter）：
//     从当前普通配置进入联合配置 C_old,new。
//   - 上述操作 Phase=leave：离开联合配置，落定为新配置 C_new。
func (m Membership) Apply(cc ConfChange) Membership {
	if cc.Op == ConfSetVoters {
		return NewMembership(cc.Members...)
	}
	cc.normalize()
	switch cc.Phase {
	case ConfPhaseEnter:
		next := NewMembership(m.PlainList()...)
		for _, id := range cc.Remove {
			delete(next.oldVoters, id)
		}
		for _, id := range cc.Add {
			next.oldVoters[id] = struct{}{}
		}
		// enter 必须从非联合配置发起；若本地已在联合配置则保持幂等。
		if m.IsJoint() {
			return m
		}
		return newJointMembership(m, next)
	case ConfPhaseLeave:
		// leave 落定为联合配置中的新集合；非联合状态下收到 leave 视为幂等。
		if m.IsJoint() {
			return NewMembership(m.NewList()...)
		}
		return m
	default:
		// 无阶段标记（历史编码）：退化为直接应用的单节点变更语义。
		next := NewMembership(m.PlainList()...)
		for _, id := range cc.Add {
			next.oldVoters[id] = struct{}{}
		}
		for _, id := range cc.Remove {
			delete(next.oldVoters, id)
		}
		return next
	}
}

// PlainList 返回当前“落定”配置（非联合视角）成员：
// 联合阶段返回新集合 C_new（leave 之后的目标配置），普通配置返回当前成员。
func (m Membership) PlainList() []NodeID {
	return m.NewList()
}

// Equal 判断两个成员集合是否完全相同（联合状态与旧、新集合逐一比较）。
func (m Membership) Equal(other Membership) bool {
	if m.IsJoint() != other.IsJoint() {
		return false
	}
	if !sameVoters(m.oldVoters, other.oldVoters) {
		return false
	}
	if m.IsJoint() && !sameVoters(m.newVoters, other.newVoters) {
		return false
	}
	return true
}

func sameVoters(a, b map[NodeID]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if _, ok := b[id]; !ok {
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
// PreVote=true 表示预投票（pre-vote）：候选人尚未递增任期、尚未投自己，
// 仅探测自己是否有机会赢得选举；预投票不改变接收方的任何持久状态，
// 从而防止陈旧/分区节点靠推高任期不断打断在任领导者（联合阶段新加入、
// 日志落后的节点尤其需要这个保护）。
type RequestVote struct {
	From         NodeID
	Term         int
	LastLogIndex int
	LastLogTerm  int
	PreVote      bool
}

// RequestVoteResult 是投票结果。Granted 为 false 时 Reason 给出可区分的原因码。
type RequestVoteResult struct {
	From    NodeID
	To      NodeID
	Term    int
	Granted bool
	Reason  Reason
	PreVote bool // 对应预投票请求的应答
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

// confChangeWire 是成员变更条目的确定性 JSON 线格式。
// 字段顺序固定、列表已去重排序，保证同一变更逐字节一致、同种子可复现。
type confChangeWire struct {
	Op      uint8    `json:"op"`
	Node    string   `json:"node"`
	Members []NodeID `json:"members"`
	Add     []NodeID `json:"add,omitempty"`
	Remove  []NodeID `json:"remove,omitempty"`
	Phase   uint8    `json:"phase,omitempty"`
}

// encodeConfChange 用确定性 JSON 编码成员变更条目负载。
func encodeConfChange(cc ConfChange) []byte {
	cc.normalize()
	w := confChangeWire{
		Op:      uint8(cc.Op),
		Node:    string(cc.Node),
		Members: cc.Members,
		Add:     cc.Add,
		Remove:  cc.Remove,
		Phase:   uint8(cc.Phase),
	}
	// 空切片输出为 []，与历史负载格式保持一致（members 字段）。
	if w.Members == nil {
		w.Members = []NodeID{}
	}
	data, err := json.Marshal(w)
	if err != nil {
		panic(fmt.Sprintf("encode conf change: %v", err))
	}
	return data
}

func decodeConfChange(data []byte) (ConfChange, error) {
	var w confChangeWire
	if len(data) == 0 {
		return ConfChange{}, fmt.Errorf("empty conf change payload")
	}
	if err := json.Unmarshal(data, &w); err != nil {
		return ConfChange{}, fmt.Errorf("bad conf change payload %q: %w", string(data), err)
	}
	cc := ConfChange{
		Op:      ConfChangeOp(w.Op),
		Node:    NodeID(w.Node),
		Members: w.Members,
		Add:     w.Add,
		Remove:  w.Remove,
		Phase:   ConfPhase(w.Phase),
	}
	switch cc.Op {
	case ConfAddVoter, ConfRemoveVoter, ConfSetVoters, ConfBatchVoters:
	default:
		return ConfChange{}, fmt.Errorf("unknown conf change op: %d", w.Op)
	}
	cc.normalize()
	return cc, nil
}

func itoa(i int) string { return strconv.Itoa(i) }
