package raft

import "fmt"

// Storage 抽象 Raft 的持久化部分（currentTerm、votedFor、日志、快照）。
// 实现需要保证：进程/节点重启后这些状态仍然存在（测试中用可保留的
// 内存实现来模拟稳定磁盘，见 sim.Disk）。
type Storage interface {
	// HardState 返回当前任期与本任期投票。
	HardState() (term uint64, vote NodeID, err error)
	// SaveHardState 持久化任期与投票。
	SaveHardState(term uint64, vote NodeID) error

	// Entries 返回 [lo, hi) 范围内的日志条目；快照之前的位置返回 ErrCompacted。
	Entries(lo, hi uint64) ([]Entry, error)
	// Term 返回给定位置条目的任期；位置为 0 返回 0，nil。
	Term(index uint64) (uint64, error)
	// LastIndex 返回最后一条日志的位置。
	LastIndex() uint64
	// Append 按 Raft 追加语义写入条目：
	//   - 同位置任期相同：保留本地条目（幂等，重复消息安全）；
	//   - 同位置任期不同：发生冲突，截断该位置及之后的全部本地条目再追加
	//     （冲突被显式修正，绝不静默覆盖）；
	//   - 全新位置：紧接续写，出现空洞视为调用错误。
	Append(entries []Entry) error

	// Snapshot 返回当前快照（未生成时 Index 为 0）。
	Snapshot() Snapshot
	// ApplySnapshot 用对端快照整体替换压缩点：比本地旧的快照拒绝，
	// 快照覆盖范围内的本地日志被丢弃，之后的增量日志保留。
	ApplySnapshot(snap Snapshot) error
	// Compact 以给定快照为界压缩：保留 Index 之后的日志，
	// 快照点（含）之前的日志被丢弃。
	Compact(snap Snapshot) error

	// FirstIndex 返回压缩后仍保留的第一条日志位置（快照 Index+1）。
	FirstIndex() uint64
}

// MemoryStore 是 Storage 的内存实现。它在单个 goroutine 内使用，
// 由确定性模拟器驱动；测试里把它放进 sim.Disk，节点“重启”后依然保留。
type MemoryStore struct {
	term uint64
	vote NodeID

	// snap 为压缩点；snap.Index==0 表示尚未产生快照。
	snap Snapshot
	// entries 中第 i 个元素的日志位置为 snap.Index + 1 + i，
	// 因此日志位置在存储中始终连续、无空洞。
	entries []Entry
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (m *MemoryStore) HardState() (uint64, NodeID, error) {
	return m.term, m.vote, nil
}

func (m *MemoryStore) SaveHardState(term uint64, vote NodeID) error {
	m.term = term
	m.vote = vote
	return nil
}

func (m *MemoryStore) FirstIndex() uint64 { return m.snap.Index + 1 }

func (m *MemoryStore) LastIndex() uint64 {
	return m.snap.Index + uint64(len(m.entries))
}

func (m *MemoryStore) Term(i uint64) (uint64, error) {
	switch {
	case i == 0:
		return 0, nil // 哨兵位置：第一个真实条目前的“空任期”
	case i < m.snap.Index:
		return 0, ErrCompacted
	case i == m.snap.Index:
		return m.snap.Term, nil
	case i > m.LastIndex():
		return 0, ErrUnavailable
	}
	return m.entries[i-m.snap.Index-1].Term, nil
}

func (m *MemoryStore) Entries(lo, hi uint64) ([]Entry, error) {
	if lo > hi {
		return nil, fmt.Errorf("raft: invalid entries range [%d,%d)", lo, hi)
	}
	if lo < m.FirstIndex() {
		return nil, ErrCompacted
	}
	if hi > m.LastIndex()+1 {
		return nil, ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}
	first := m.FirstIndex()
	out := make([]Entry, hi-lo)
	copy(out, m.entries[lo-first:hi-first])
	return out, nil
}

func (m *MemoryStore) Append(in []Entry) error {
	if len(in) == 0 {
		return nil
	}
	first := m.FirstIndex()
	last := m.LastIndex()
	for _, e := range in {
		if e.Index <= m.snap.Index {
			// 已被快照覆盖的位置不能再被重写（幂等忽略）。
			continue
		}
		if e.Index > last+1 {
			return fmt.Errorf("raft: append creates gap at index %d (last %d)", e.Index, last)
		}
		pos := e.Index - first
		if e.Index <= last {
			old := m.entries[pos]
			if old.Term == e.Term && entryEqual(old, e) {
				// 同位置同任期且内容一致：重复投递，幂等保留，不覆盖。
				continue
			}
			// Raft 日志匹配性质保证“同位置同任期”的条目必然一致；
			// 内容不同说明历史在此分叉（例如新节点引导配置与领导者不同），
			// 必须按冲突处理：截断该位置及之后的全部本地条目再追加。
			m.entries = append([]Entry(nil), m.entries[:pos]...)
			last = e.Index - 1
		}
		cp := e
		if e.Data != nil {
			cp.Data = append([]byte(nil), e.Data...)
		}
		m.entries = append(m.entries, cp)
		last = e.Index
	}
	return nil
}

func (m *MemoryStore) Snapshot() Snapshot {
	out := m.snap
	if m.snap.Data != nil {
		out.Data = append([]byte(nil), m.snap.Data...)
	}
	out.Conf.Voters = append([]NodeID(nil), m.snap.Conf.Voters...)
	out.Conf.NewVoters = append([]NodeID(nil), m.snap.Conf.NewVoters...)
	return out
}

func (m *MemoryStore) ApplySnapshot(snap Snapshot) error {
	if snap.Index < m.snap.Index {
		return ErrCompacted // 比本地压缩点更旧，拒绝
	}
	if snap.Index == m.snap.Index {
		return nil // 重复快照，幂等
	}
	// 丢弃被新快照覆盖的日志，严格保留其后的增量。
	keepFrom := snap.Index - m.snap.Index
	if int(keepFrom) > len(m.entries) {
		keepFrom = uint64(len(m.entries))
	}
	m.entries = append([]Entry(nil), m.entries[keepFrom:]...)
	m.snap = cloneSnapshot(snap)
	return nil
}

func (m *MemoryStore) Compact(snap Snapshot) error {
	if snap.Index <= m.snap.Index {
		// 重复或更旧的压缩请求一律幂等忽略，绝不回退压缩点。
		return nil
	}
	if snap.Index > m.LastIndex() {
		return fmt.Errorf("raft: compact at %d beyond last index %d", snap.Index, m.LastIndex())
	}
	keepFrom := snap.Index - m.snap.Index
	m.entries = append([]Entry(nil), m.entries[keepFrom:]...)
	m.snap = cloneSnapshot(snap)
	return nil
}

func cloneSnapshot(s Snapshot) Snapshot {
	out := s
	if s.Data != nil {
		out.Data = append([]byte(nil), s.Data...)
	}
	out.Conf.Voters = append([]NodeID(nil), s.Conf.Voters...)
	out.Conf.NewVoters = append([]NodeID(nil), s.Conf.NewVoters...)
	return out
}

// entryEqual 判断同位置同任期的两条条目内容是否一致
// （类型与负载都比较，用于识别“形同实异”的分叉条目）。
func entryEqual(a, b Entry) bool {
	if a.Type != b.Type || len(a.Data) != len(b.Data) {
		return false
	}
	for i := range a.Data {
		if a.Data[i] != b.Data[i] {
			return false
		}
	}
	return true
}
