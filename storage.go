package raft

import "fmt"

// Storage 是单个节点的持久状态（任期、投票、日志与快照）。
// 节点重启时复用同一个 Storage，从而模拟磁盘不丢；测试中由内存实现承载。
// 所有访问均在确定性仿真单线程内发生，不需要锁。
type Storage struct {
	currentTerm int
	votedFor    NodeID // 空字符串表示本任期尚未投票

	snapshot      Snapshot
	snapshotValid bool

	// entries 保存快照之后的日志条目，下标 0 对应 FirstIndex()。
	entries []LogEntry

	log *Logger
	id  NodeID
}

// NewStorage 创建属于节点 id 的空持久存储。
func NewStorage(id NodeID, log *Logger) *Storage {
	return &Storage{id: id, log: log}
}

// CurrentTerm 返回已持久化的当前任期。
func (s *Storage) CurrentTerm() int { return s.currentTerm }

// VotedFor 返回本任期已投给的节点（空表示未投）。
func (s *Storage) VotedFor() NodeID { return s.votedFor }

// SaveTermVote 持久化任期与投票。
func (s *Storage) SaveTermVote(term int, votedFor NodeID) {
	s.currentTerm = term
	s.votedFor = votedFor
}

// FirstIndex 返回存储中仍保留的最早日志索引。
func (s *Storage) FirstIndex() int {
	if s.snapshotValid {
		return s.snapshot.LastIncludedIndex + 1
	}
	return 1
}

// LastIndex 返回最后一条日志的索引（无快照无日志时为 0）。
func (s *Storage) LastIndex() int {
	if s.snapshotValid {
		return s.snapshot.LastIncludedIndex + len(s.entries)
	}
	return len(s.entries)
}

// pos 把日志索引转换为 entries 切片下标。
func (s *Storage) pos(index int) int {
	return index - s.FirstIndex()
}

// TermAt 返回指定索引处日志的任期。
// 索引等于快照最后位置时返回快照任期；快照之前返回 (0,false)。
func (s *Storage) TermAt(index int) (int, bool) {
	if s.snapshotValid && index == s.snapshot.LastIncludedIndex {
		return s.snapshot.LastIncludedTerm, true
	}
	e, ok := s.EntryAt(index)
	if !ok {
		return 0, false
	}
	return e.Term, true
}

// EntryAt 返回指定索引的条目副本。
func (s *Storage) EntryAt(index int) (LogEntry, bool) {
	p := s.pos(index)
	if p < 0 || p >= len(s.entries) {
		return LogEntry{}, false
	}
	return s.entries[p], true
}

// EntriesRange 返回 [lo, hi] 闭区间的条目副本。
func (s *Storage) EntriesRange(lo, hi int) []LogEntry {
	if lo > hi {
		return nil
	}
	lp := s.pos(lo)
	hp := s.pos(hi)
	if lp < 0 || hp >= len(s.entries) || lp > hp {
		return nil
	}
	out := make([]LogEntry, hp-lp+1)
	copy(out, s.entries[lp:hp+1])
	return out
}

// AllEntries 返回快照之后全部条目副本。
func (s *Storage) AllEntries() []LogEntry {
	out := make([]LogEntry, len(s.entries))
	copy(out, s.entries)
	return out
}

// AppendNew 以给定任期追加一条新条目并返回其索引。
func (s *Storage) AppendNew(term int, e LogEntry) int {
	e.Index = s.LastIndex() + 1
	e.Term = term
	s.entries = append(s.entries, e)
	return e.Index
}

// TruncateAndAppend 删除 prevIndex 之后与之冲突的后缀，并追加 entries。
// 冲突删除必须显式发生并记录日志，而不是静默覆盖。
func (s *Storage) TruncateAndAppend(prevIndex int, entries []LogEntry) {
	// prevIndex 必须不早于快照；快照段内容天然一致。
	if prevIndex < s.FirstIndex()-1 {
		panic(fmt.Sprintf("storage: prevIndex %d 早于快照边界 %d", prevIndex, s.FirstIndex()-1))
	}
	keep := prevIndex - (s.FirstIndex() - 1) // 保留的条目数
	if keep < 0 {
		keep = 0
	}
	if keep < len(s.entries) {
		dropped := s.entries[keep:]
		if s.log != nil {
			s.log.Warnf(string(s.id), s.currentTerm, 0, 0, ReasonLogConflict,
				"显式删除冲突后缀 prevIndex=%d 删除条目=%v", prevIndex, indexSpan(dropped))
		}
		s.entries = s.entries[:keep]
	}
	for i := range entries {
		e := entries[i]
		e.Index = s.LastIndex() + 1
		s.entries = append(s.entries, e)
	}
}

func indexSpan(es []LogEntry) [][2]int {
	if len(es) == 0 {
		return nil
	}
	span := [2]int{es[0].Index, es[len(es)-1].Index}
	return [][2]int{span}
}

// Compact 丢弃 index（含）之前的日志并生成/更新快照。
// index 必须存在且不早于现有快照；提交判定由调用方保证。
func (s *Storage) Compact(index int, data []byte) (Snapshot, error) {
	term, ok := s.TermAt(index)
	if !ok {
		return Snapshot{}, fmt.Errorf("compact: 索引 %d 不存在", index)
	}
	if s.snapshotValid && index < s.snapshot.LastIncludedIndex {
		return Snapshot{}, fmt.Errorf("compact: 索引 %d 早于现有快照 %d", index, s.snapshot.LastIncludedIndex)
	}
	membership, jointOld, jointValid := s.configViewAt(index)
	snap := Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Membership:        membership,
		JointValid:        jointValid,
		JointOld:          jointOld,
		Data:              append([]byte(nil), data...),
	}
	// 丢弃快照覆盖的条目；快照位置本身也不再保留在 entries 中。
	p := s.pos(index)
	if p >= 0 && p < len(s.entries) {
		s.entries = append([]LogEntry(nil), s.entries[p+1:]...)
	} else if p >= len(s.entries) {
		s.entries = nil
	}
	s.snapshot, s.snapshotValid = snap, true
	if s.log != nil {
		s.log.Infof(string(s.id), s.currentTerm, 0, 0, ReasonOK,
			"日志压缩完成 lastIncludedIndex=%d lastIncludedTerm=%d 剩余条目数=%d",
			snap.LastIncludedIndex, snap.LastIncludedTerm, len(s.entries))
	}
	return snap, nil
}

// configViewAt 找出 index（含）处的配置视图：返回当前配置、是否处于
// 联合共识中间态以及联合旧配置 C_old。重放从当前快照配置出发。
func (s *Storage) configViewAt(index int) (Membership, Membership, bool) {
	v := replayConfigView(s, index)
	return v.current, v.jointOld, v.joint
}

// ApplySnapshot 用领导者快照覆盖本地状态；不允许回退已提交位置由 Node 层保证，
// 这里仅拒绝比本地快照更旧的快照。
func (s *Storage) ApplySnapshot(snap Snapshot) error {
	if s.snapshotValid && snap.LastIncludedIndex < s.snapshot.LastIncludedIndex {
		return fmt.Errorf("snapshot: 快照 %d 旧于本地快照 %d", snap.LastIncludedIndex, s.snapshot.LastIncludedIndex)
	}
	s.snapshot = snap
	s.snapshotValid = true
	s.entries = nil
	if s.log != nil {
		s.log.Infof(string(s.id), s.currentTerm, 0, 0, ReasonOK,
			"安装快照 lastIncludedIndex=%d lastIncludedTerm=%d 成员数=%d",
			snap.LastIncludedIndex, snap.LastIncludedTerm, snap.Membership.Size())
	}
	return nil
}

// HasSnapshot 报告是否存在快照。
func (s *Storage) HasSnapshot() bool { return s.snapshotValid }

// SnapshotCopy 返回快照副本。
func (s *Storage) SnapshotCopy() Snapshot {
	out := s.snapshot
	out.Data = append([]byte(nil), s.snapshot.Data...)
	return out
}
