package raft

// sendSnapshot 由领导者向落后于压缩点的跟随者发送当前快照。
// 快照不改变任何提交位置，只帮助对方补齐“快照点之前”的状态。
func (n *Node) sendSnapshot(to NodeID) {
	snap := n.st.Snapshot()
	if snap.Index == 0 {
		// 尚无快照却需要快照：理论上不发生（压缩点为 0 时 nextIndex 必为 1）。
		return
	}
	p := n.pr[to]
	if p != nil {
		// 乐观推进：对方成功安装后再据响应确认。
		p.nextIndex = snap.Index + 1
	}
	m := Message{Type: MsgSnap, To: to, Term: n.term, Snapshot: snap}
	n.log.Logf("[%s] term=%d 向 %s 发送快照 index=%d term=%d（对方落后于压缩点）",
		n.id, n.term, to, snap.Index, snap.Term)
	n.send(m)
}

// handleSnapshot 是跟随者安装快照的路径。
// 规则：比本地快照更旧的必须拒绝（RejectSnapshotStale）；
// 安装后状态机整体恢复，快照点之后的增量日志保留以便继续追赶。
func (n *Node) handleSnapshot(m Message) {
	r := n.reply(m, MsgSnapResp)

	if n.role == Candidate && m.Term == n.term {
		n.becomeFollower(m.Term, m.From)
	}
	if n.role == Follower {
		n.lead = m.From
		n.resetTimeout()
	}
	snap := m.Snapshot
	cur := n.st.Snapshot()
	if snap.Index < cur.Index {
		r.Reject, r.RejectReason = true, RejectSnapshotStale
		n.log.Logf("[%s] term=%d 拒绝 %s 的快照：快照 index=%d 比本地 %d 更旧",
			n.id, n.term, m.From, snap.Index, cur.Index)
		n.send(r)
		return
	}
	if err := n.sm.Restore(append([]byte(nil), snap.Data...)); err != nil {
		r.Reject, r.RejectReason = true, RejectSnapshotStale
		n.send(r)
		return
	}
	if err := n.st.ApplySnapshot(snap); err != nil {
		r.Reject, r.RejectReason = true, RejectSnapshotStale
		n.send(r)
		return
	}
	// 快照恢复不影响提交语义：提交位置至少前进到快照点（绝不回退）。
	if snap.Index > n.commitIndex {
		n.commitIndex = snap.Index
	}
	n.conf = confClone(snap.Conf)
	n.log.Logf("[%s] term=%d 安装快照成功 index=%d term=%d 投票集=%v，随后可追增量 [%d,%d]",
		n.id, n.term, snap.Index, snap.Term, snap.Conf.Voters, n.st.FirstIndex(), n.st.LastIndex())
	r.Index = snap.Index
	n.send(r)
	n.applyCommitted()
}

// handleSnapshotResp 是领导者收到快照安装结果的路径。
func (n *Node) handleSnapshotResp(m Message) {
	if n.role != Leader {
		return
	}
	p, ok := n.pr[m.From]
	if !ok {
		return
	}
	if m.Reject {
		// 对方比快照还新（例如快照来自旧领导者）：重置 nextIndex 走常规复制探测。
		p.nextIndex = n.lastIndex() + 1
		n.sendAppend(m.From)
		return
	}
	if m.Index > p.matchIndex {
		p.matchIndex = m.Index
	}
	p.nextIndex = m.Index + 1
	n.log.Logf("[%s] term=%d %s 已安装快照到 index=%d，开始增量追赶",
		n.id, n.term, m.From, m.Index)
	if p.matchIndex < n.lastIndex() {
		n.sendAppend(m.From)
	}
	n.maybeCommit()
}
