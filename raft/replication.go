package raft

// bcastAppend 向所有复制目标各发一轮 AppendEntries；
// 落后到快照点之前的节点改发 InstallSnapshot。
func (n *Node) bcastAppend() {
	for _, id := range n.replicaSet() {
		if id == n.id {
			continue
		}
		p, ok := n.pr[id]
		if !ok {
			p = &progress{nextIndex: n.lastIndex() + 1, matchIndex: 0}
			n.pr[id] = p
		}
		if p.nextIndex <= n.st.Snapshot().Index {
			n.sendSnapshot(id)
			continue
		}
		n.sendAppend(id)
	}
}

// sendAppend 按 nextIndex 组装发给单个跟随者的追加请求。
func (n *Node) sendAppend(to NodeID) {
	p := n.pr[to]
	prev := p.nextIndex - 1
	prevTerm, err := n.st.Term(prev)
	if err != nil {
		// prev 已被压缩：改走快照恢复。
		n.sendSnapshot(to)
		return
	}
	es, err := n.st.Entries(p.nextIndex, n.lastIndex()+1)
	if err != nil {
		n.sendSnapshot(to)
		return
	}
	m := Message{
		Type: MsgApp, To: to, Term: n.term,
		Index: prev, LogTerm: prevTerm, Commit: n.commitIndex,
		Entries: append([]Entry(nil), es...),
	}
	n.send(m)
}

// handleAppend 处理追加请求（跟随者/候选者路径）。
// 一致性检查基于 (prevLogIndex, prevLogTerm)；不匹配时显式截断冲突点
// 及之后的日志，而不是静默覆盖。
func (n *Node) handleAppend(m Message) {
	r := n.reply(m, MsgAppResp)

	// 同任期的合法心跳/复制使候选者放弃竞选（只有领导者会发 MsgApp）。
	if n.role == Candidate && m.Term == n.term {
		n.becomeFollower(m.Term, m.From)
	}
	if n.role == Follower {
		n.lead = m.From
		n.resetTimeout()
	}

	snapIndex := n.st.Snapshot().Index
	if m.Index < snapIndex {
		// 请求的前序位置我已经用快照覆盖了：明确拒绝，让领导者改发快照或新位置。
		r.Reject, r.RejectReason, r.RejectHint = true, RejectSnapshotStale, n.lastIndex()
		n.send(r)
		return
	}

	// 前序一致性检查。
	if m.Index > n.lastIndex() {
		r.Reject, r.RejectReason, r.RejectHint = true, RejectLogGap, n.lastIndex()
		n.send(r)
		return
	}
	localTerm, _ := n.st.Term(m.Index)
	if localTerm != m.LogTerm {
		// 冲突：找到冲突任期在本地的第一条位置作为回退提示，
		// 帮助领导者一次性跳过整个冲突任期。
		hint := conflictFirstIndex(n.st, m.Index, localTerm)
		r.Reject, r.RejectReason, r.RejectHint = true, RejectLogGap, hint
		n.log.Logf("[%s] term=%d 拒绝 %s 的追加：prev=(%d,%d) 与本地任期 %d 冲突，回退提示=%d",
			n.id, n.term, m.From, m.Index, m.LogTerm, localTerm, hint)
		n.send(r)
		return
	}

	// 追加成功：存储层负责对同位置不同任期条目做显式截断修正。
	if len(m.Entries) > 0 {
		// 过滤已被快照覆盖的位置，防止重写压缩点之前的日志。
		kept := m.Entries[:0]
		for _, e := range m.Entries {
			if e.Index > snapIndex {
				kept = append(kept, e)
			}
		}
		if err := n.st.Append(kept); err != nil {
			r.Reject, r.RejectReason = true, RejectLogGap
			n.send(r)
			return
		}
	}

	matched := m.Index
	if len(m.Entries) > 0 {
		matched = m.Entries[len(m.Entries)-1].Index
	}
	// 按最新日志重新激活配置（即使尚未提交），保证投票集合始终正确。
	if c, err := latestConfigAt(n.st, n.st.Snapshot(), n.lastIndex()); err == nil {
		n.conf = c
	}
	// 领导者提交位置不得让本地 commit 回退。
	if m.Commit > n.commitIndex {
		old := n.commitIndex
		if m.Commit < matched {
			n.commitIndex = m.Commit
		} else {
			n.commitIndex = matched
		}
		n.log.Logf("[%s] term=%d 跟随领导者 commit %d->%d（leaderCommit=%d, matched=%d）",
			n.id, n.term, old, n.commitIndex, m.Commit, matched)
	}
	r.Index = matched
	n.send(r)
}

// conflictFirstIndex 返回 termAt 任期在本地日志中最早出现的位置；
// 若该位置之前的条目任期更旧，则直接提示更靠前的位置。
func conflictFirstIndex(st Storage, index, termAt uint64) uint64 {
	i := index
	for i > st.Snapshot().Index {
		t, err := st.Term(i - 1)
		if err != nil || t != termAt {
			break
		}
		i--
	}
	return i
}

// handleAppendResp 处理追加响应（领导者路径）。
func (n *Node) handleAppendResp(m Message) {
	if n.role != Leader {
		return
	}
	p, ok := n.pr[m.From]
	if !ok {
		// 来源不在当前配置（例如已移除节点的迟到响应）：忽略。
		return
	}
	if m.Reject {
		// 任期原因已在 Step 入口统一处理；这里只处理日志缺口回退。
		hint := m.RejectHint
		if hint == 0 || hint >= p.nextIndex {
			hint = p.nextIndex - 1
		}
		p.nextIndex = hint
		if p.nextIndex < 1 {
			p.nextIndex = 1
		}
		p.matchIndex = 0
		n.log.Logf("[%s] term=%d 跟随者 %s 拒绝复制（%s），nextIndex 回退到 %d",
			n.id, n.term, m.From, m.RejectReason, p.nextIndex)
		// 快照点之前的回退直接走快照安装。
		if p.nextIndex <= n.st.Snapshot().Index {
			n.sendSnapshot(m.From)
		} else {
			n.sendAppend(m.From)
		}
		return
	}
	if m.Index <= p.matchIndex {
		return // 迟到的旧响应，幂等忽略
	}
	p.matchIndex = m.Index
	if p.nextIndex <= m.Index {
		p.nextIndex = m.Index + 1
	}
	// 推进多数派提交（内部已保证只提交本任期条目）。
	n.maybeCommit()
	// 还有未同步日志时立即补发一轮，加快收敛。
	if p.matchIndex < n.lastIndex() {
		n.sendAppend(m.From)
	}
}
