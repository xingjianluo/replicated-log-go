package raft

import (
	"fmt"
	"io"
	"os"
	"time"
)

// Reason 是可区分的判定/失败原因码，便于测试与日志解释。
type Reason string

const (
	ReasonOK                    Reason = "ok"
	ReasonStaleTerm             Reason = "stale-term"              // 消息任期落后，拒绝
	ReasonAlreadyVoted          Reason = "already-voted"           // 本任期已投给他人
	ReasonLogNotUpToDate        Reason = "log-not-up-to-date"      // 候选人日志不够新
	ReasonLeaderLease           Reason = "leader-lease"            // 当前仍认可现任领导者
	ReasonLogGap                Reason = "log-gap"                 // PrevLogIndex 早于快照
	ReasonLogConflict           Reason = "log-conflict"            // PrevLog 处任期不匹配
	ReasonNotLeader             Reason = "not-leader"              // 非领导者无法处理写请求
	ReasonNoQuorum              Reason = "no-quorum"               // 未凑够多数派
	ReasonDeposed               Reason = "deposed"                 // 发现更高任期，领导者被罢免
	ReasonMembershipPending     Reason = "membership-pending"      // 上一次成员变更尚未提交
	ReasonUnknownVoter          Reason = "unknown-voter"           // 目标节点不在当前配置中
	ReasonNotVoter              Reason = "not-voter"               // 节点无投票权（learner/未入配置）
	ReasonBadConfChange         Reason = "bad-conf-change"         // 批量变更内容非法（冲突/空集）
	ReasonLearnerLagging        Reason = "learner-lagging"         // 新增节点尚未追平，暂不能发起批量变更
	ReasonSnapshotBehindCommit  Reason = "snapshot-behind-commit"  // 快照落后于已提交位置
	ReasonRejectedByApplication Reason = "rejected-by-application" // 应用层拒绝应用
	ReasonDuplicate             Reason = "duplicate"               // 幂等命中：重复消息/重复条目
	ReasonShutdown              Reason = "shutdown"                // 节点已停止
	ReasonPartitioned           Reason = "partitioned"             // 网络分区导致不可达
	ReasonDropped               Reason = "dropped"                 // 被确定性故障注入丢弃
)

// Logger 输出带节点、任期、多数派与提交位置等判定依据的结构化日志。
type Logger struct {
	w     io.Writer
	debug bool
	clock func() time.Time
}

// NewLogger 创建日志器；w 为 nil 时输出到标准输出。
func NewLogger(w io.Writer, debug bool) *Logger {
	if w == nil {
		w = os.Stdout
	}
	return &Logger{w: w, debug: debug, clock: func() time.Time {
		// 确定性时钟：默认使用固定零点，调用方可用 SetClock 注入仿真时钟。
		return time.Time{}
	}}
}

// SetClock 注入确定性时钟。
func (l *Logger) SetClock(f func() time.Time) { l.clock = f }

// Logf 输出一条判定日志，fields 以 key/value 交替给出。
func (l *Logger) Logf(level, node string, term int, quorum, commitIndex int, reason Reason, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.w, "%s level=%s node=%s term=%d quorum=%d commitIndex=%d reason=%s msg=%q\n",
		l.clock().Format("15:04:05.000"), level, node, term, quorum, commitIndex, reason, msg)
}

// Debugf 仅在调试模式输出。
func (l *Logger) Debugf(node string, term int, quorum, commitIndex int, reason Reason, format string, args ...any) {
	if l.debug {
		l.Logf("DEBUG", node, term, quorum, commitIndex, reason, format, args...)
	}
}

// Infof 输出信息级日志。
func (l *Logger) Infof(node string, term int, quorum, commitIndex int, reason Reason, format string, args ...any) {
	l.Logf("INFO", node, term, quorum, commitIndex, reason, format, args...)
}

// Warnf 输出告警级日志。
func (l *Logger) Warnf(node string, term int, quorum, commitIndex int, reason Reason, format string, args ...any) {
	l.Logf("WARN", node, term, quorum, commitIndex, reason, format, args...)
}
