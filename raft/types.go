// Package raft 实现一套简化的 Raft 复制日志一致性算法。
//
// 本包只包含与具体执行环境无关的算法逻辑：节点状态机、选举、日志复制、
// 提交规则、快照/压缩以及成员变更。所有的时间推进（选举超时、心跳）、
// 消息投递（分区、乱序、重复）与持久化媒介都通过接口由外部环境注入，
// 从而可以在确定性模拟器（package sim）中稳定复现各类故障场景。
package raft

import "fmt"

// NodeID 是集群内节点的唯一标识。
type NodeID string

// Role 表示节点在 Raft 状态机中的角色。
type Role int

const (
	Follower  Role = iota // 跟随者：接收领导者日志与心跳
	Candidate             // 候选者：发起选举
	Leader                // 领导者：唯一允许推进日志提交的角色
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("Role(%d)", int(r))
	}
}

// EntryType 区分日志条目的种类。
type EntryType int

const (
	// EntryNormal 是普通客户端命令。
	EntryNormal EntryType = iota
	// EntryConfChange 是成员变更命令（单成员增删），
	// 采用 Raft 论文中“一次只变更一个成员”的规则直接切换配置，
	// 故日志中只需保存变更后的完整配置。
	EntryConfChange
)

// Entry 是一条复制日志条目。
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// Configuration 描述某一时刻的投票成员集合。
type Configuration struct {
	// Joint 为 true 时表示联合配置（Cold ∪ Cnew），
	// 提交日志需要分别得到两个多数派的同意。
	Joint bool
	// Voters 是当前（联合阶段为 Cold∪Cnew）投票成员集合。
	Voters []NodeID
	// NewVoters 仅在 Joint 为 true 时有意义，表示 Cnew。
	NewVoters []NodeID
}

// ConfChangeType 是单节点成员变更类型。
type ConfChangeType int

const (
	ConfAddNode    ConfChangeType = iota // 新增投票成员
	ConfRemoveNode                       // 移除投票成员
)

// ConfChange 是成员变更命令的负载（序列化后写入 Entry.Data）。
type ConfChange struct {
	Type ConfChangeType
	Node NodeID
}

// MessageType 枚举节点间交互的 RPC。
type MessageType int

const (
	MsgHup      MessageType = iota // 本地：选举超时
	MsgBeat                        // 本地：心跳定时器
	MsgVote                        // RequestVote
	MsgVoteResp                    // RequestVote 响应
	MsgApp                         // AppendEntries
	MsgAppResp                     // AppendEntries 响应
	MsgSnap                        // InstallSnapshot
	MsgSnapResp                    // InstallSnapshot 响应
)

// Message 是节点之间或本地定时器与节点之间的消息。
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term uint64 // 发送者任期（投票/复制类消息）

	// 选举
	LastLogIndex uint64
	LastLogTerm  uint64
	VoteGranted  bool
	Reject       bool
	RejectReason RejectReason

	// 复制
	LogTerm    uint64   // prevLogTerm
	Entries    []Entry  // 追加的条目（快照消息不用）
	Commit     uint64   // 领导者已知的最高提交位置
	Index      uint64   // 响应：该节点已确认匹配的最高位置
	Snapshot   Snapshot // 快照传输
	RejectHint uint64   // 拒绝时的提示位置，帮助领导者快速回退 nextIndex
}

// Snapshot 是一次日志压缩的产物。
type Snapshot struct {
	Data  []byte // 状态机快照字节
	Index uint64 // 快照覆盖的最后一条日志位置
	Term  uint64 // 该位置条目的任期
	Conf  Configuration
}

// RejectReason 让不同的拒绝原因可以被区分与统计。
type RejectReason int

const (
	RejectNone          RejectReason = iota
	RejectStaleTerm                  // 任期落后
	RejectNotLeader                  // 非领导者，无法服务客户端提案
	RejectVotedAnother               // 本任期已经投给其他候选者
	RejectLogBehind                  // 候选者日志不够新
	RejectLogGap                     // 前序日志位置/任期不匹配
	RejectSnapshotStale              // 快照比本地状态更旧
	RejectUnknownNode                // 发送目标/来源不在当前配置中
	RejectChangePending              // 已有成员变更未完成
	RejectNoop                       // 空变更（目标节点已存在/不存在）
)

func (r RejectReason) String() string {
	switch r {
	case RejectNone:
		return "none"
	case RejectStaleTerm:
		return "stale-term"
	case RejectNotLeader:
		return "not-leader"
	case RejectVotedAnother:
		return "voted-another"
	case RejectLogBehind:
		return "log-behind"
	case RejectLogGap:
		return "log-gap"
	case RejectSnapshotStale:
		return "snapshot-stale"
	case RejectUnknownNode:
		return "unknown-node"
	case RejectChangePending:
		return "change-pending"
	case RejectNoop:
		return "noop-change"
	default:
		return fmt.Sprintf("reason(%d)", int(r))
	}
}
