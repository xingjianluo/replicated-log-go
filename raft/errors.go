package raft

import "errors"

// 提案路径上的错误：调用者可以据此区分失败原因。
var (
	// ErrNotLeader 表示当前节点不是领导者，提案无法受理。
	ErrNotLeader = errors.New("raft: node is not the leader")
	// ErrProposalDropped 表示提案被丢弃（如领导者刚建立、成员变更进行中等）。
	ErrProposalDropped = errors.New("raft: proposal dropped")
	// ErrCompacted 表示请求的日志位置已经被快照压缩。
	ErrCompacted = errors.New("raft: requested index is older than the compacted snapshot")
	// ErrUnavailable 表示日志尚不可用（位置太新或存储为空）。
	ErrUnavailable = errors.New("raft: requested entries are unavailable")
	// ErrNoPeer 表示目标节点不在当前配置中。
	ErrNoPeer = errors.New("raft: peer not in configuration")
	// ErrChangePending 表示上一次成员变更尚未提交，不能并发变更。
	ErrChangePending = errors.New("raft: a configuration change is already pending")
	// ErrNoChange 表示成员变更是空操作（添加已存在节点/移除不存在节点）。
	ErrNoChange = errors.New("raft: configuration change is a no-op")
	// ErrStepStopped 表示节点已经关闭。
	ErrStepStopped = errors.New("raft: node stopped")
	// ErrClusterUnavailable 表示多数派当前不可达，无法推进提交。
	ErrClusterUnavailable = errors.New("raft: no quorum available")
)
