package raft

import "testing"

func TestAppendConflictTruncatesExplicitly(t *testing.T) {
	st := NewMemoryStore()
	mk := func(i uint64, term uint64, data string) Entry {
		return Entry{Index: i, Term: term, Type: EntryNormal, Data: []byte(data)}
	}
	if err := st.Append([]Entry{mk(1, 1, "a"), mk(2, 1, "b"), mk(3, 1, "c")}); err != nil {
		t.Fatal(err)
	}
	// 在 index=2 收到更高任期的分叉条目：2、3 必须被显式截断修正。
	if err := st.Append([]Entry{mk(2, 2, "B"), mk(3, 2, "C")}); err != nil {
		t.Fatal(err)
	}
	es, err := st.Entries(1, 4)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "B", "C"}
	for i, e := range es {
		if string(e.Data) != want[i] || e.Term != uint64(1+b2u(i >= 1)) {
			t.Fatalf("冲突截断结果错误: %+v", es)
		}
	}
	// 同位置同任期同内容重复追加必须幂等，不得报错或复制。
	if err := st.Append([]Entry{mk(2, 2, "B")}); err != nil {
		t.Fatal(err)
	}
	if st.LastIndex() != 3 {
		t.Fatalf("重复追加后 lastIndex=%d，期望 3", st.LastIndex())
	}
	// 同位置同任期但内容不同（引导配置分叉）：按冲突截断，绝不静默覆盖。
	if err := st.Append([]Entry{mk(1, 1, "X")}); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Term(2); got != 0 {
		t.Fatalf("分叉截断后 index2 应不存在，term=%d", got)
	}
}

func b2u(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestCompactAndSnapshotSemantics(t *testing.T) {
	st := NewMemoryStore()
	for i := uint64(1); i <= 5; i++ {
		if err := st.Append([]Entry{{Index: i, Term: 1, Type: EntryNormal}}); err != nil {
			t.Fatal(err)
		}
	}
	snap := Snapshot{Index: 3, Term: 1, Data: []byte("s")}
	if err := st.Compact(snap); err != nil {
		t.Fatal(err)
	}
	if st.FirstIndex() != 4 || st.LastIndex() != 5 {
		t.Fatalf("压缩后区间错误: [%d,%d]", st.FirstIndex(), st.LastIndex())
	}
	if _, err := st.Entries(1, 3); err != ErrCompacted {
		t.Fatalf("期望 ErrCompacted，得到 %v", err)
	}
	// 更旧的快照不得让压缩点回退。
	if err := st.ApplySnapshot(Snapshot{Index: 2, Term: 1}); err != ErrCompacted {
		t.Fatalf("旧快照期望拒绝 ErrCompacted，得到 %v", err)
	}
}

func TestQuorum(t *testing.T) {
	cases := map[int]int{1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 6: 4}
	for n, q := range cases {
		if got := Quorum(n); got != q {
			t.Fatalf("Quorum(%d)=%d 期望 %d", n, got, q)
		}
	}
}
