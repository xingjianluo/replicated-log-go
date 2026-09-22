package raft

// campaign 发起一次新的选举：递增任期、自投、并行向所有投票成员拉票。
// 同一任期至多投一票且先持久化再发消息，保证“同一任期至多一个领导者”。
// 已不在最新配置投票集中的节点（被移除/降级）不得主动竞选，
// 避免局外节点用拉票请求不断推高在任节点任期、干扰集群。
func (n *Node) campaign() {
	if !inConfig(n.id, n.conf.Voters) {
		if n.role != Follower {
			n.becomeFollower(n.term, "")
		}
		n.log.Logf("[%s] term=%d 不在当前投票集 %v 中，放弃竞选（已被移除/降级）",
			n.id, n.term, n.conf.Voters)
		n.resetTimeout()
		return
	}
	n.role = Candidate
	n.term++
	n.vote = n.id
	n.lead = ""
	n.persist()
	n.resetTimeout()

	n.votes = map[NodeID]bool{n.id: true}
	li, lt := n.lastIndex(), n.lastTerm()
	n.log.Logf("[%s] term=%d 发起选举 last=(%d,%d) 多数派需要 %d/%d 票",
		n.id, n.term, li, lt, Quorum(len(n.conf.Voters)), len(n.conf.Voters))

	for _, id := range n.conf.Voters {
		if id == n.id {
			continue
		}
		m := Message{Type: MsgVote, To: id, Term: n.term, LastLogIndex: li, LastLogTerm: lt}
		n.send(m)
	}
	// 单节点集群：自投即多数派，立即当选。
	if HasMajority(len(n.votes), len(n.conf.Voters)) {
		n.becomeLeader()
	}
}

// handleVote 处理拉票请求，投票限制（Raft 5.2.1 与 5.4.1）：
//  1. 本任期尚未投票（或投的正是该候选者）；
//  2. 候选者日志至少与本地一样新：先比最后任期，再比最后位置。
func (n *Node) handleVote(m Message) {
	r := n.reply(m, MsgVoteResp)

	// 局外节点（不在本节点所知最新配置内）的拉票请求：明确拒绝，
	// 既不投票也不因其更高任期而放弃合法领导者。
	if !inConfig(m.From, n.conf.Voters) {
		r.Reject, r.RejectReason = true, RejectUnknownNode
		n.log.Logf("[%s] term=%d 拒绝 %s 的拉票：不在投票集 %v 中（%s）",
			n.id, n.term, m.From, n.conf.Voters, RejectUnknownNode)
		n.send(r)
		return
	}

	// 同任期内已存在领导者时，候选者拉票不被接受（防同任期双主）。
	if n.role == Leader && m.Term == n.term {
		r.Reject, r.RejectReason = true, RejectVotedAnother
		n.send(r)
		return
	}
	if n.vote != "" && n.vote != m.From {
		r.Reject, r.RejectReason = true, RejectVotedAnother
		n.log.Logf("[%s] term=%d 拒绝投票给 %s：本任期已投给 %s",
			n.id, n.term, m.From, n.vote)
		n.send(r)
		return
	}
	if !n.logAtLeast(m.LastLogTerm, m.LastLogIndex) {
		r.Reject, r.RejectReason = true, RejectLogBehind
		n.log.Logf("[%s] term=%d 拒绝投票给 %s：候选者日志 (%d,%d) 落后于本地 (%d,%d)",
			n.id, n.term, m.From, m.LastLogTerm, m.LastLogIndex, n.lastTerm(), n.lastIndex())
		n.send(r)
		return
	}
	n.vote = m.From
	n.persist()
	n.resetTimeout() // 投票后重置选举计时器，避免与被投候选者抢跑
	r.VoteGranted = true
	n.log.Logf("[%s] term=%d 投票给 %s（候选者日志 (%d,%d) >= 本地 (%d,%d)）",
		n.id, n.term, m.From, m.LastLogTerm, m.LastLogIndex, n.lastTerm(), n.lastIndex())
	n.send(r)
}

// logAtLeast 判断候选者日志 (cterm, cindex) 是否至少与本地一样新。
func (n *Node) logAtLeast(cterm, cindex uint64) bool {
	lterm := n.lastTerm()
	if cterm != lterm {
		return cterm > lterm
	}
	return cindex >= n.lastIndex()
}

func (n *Node) handleVoteResp(m Message) {
	if n.role != Candidate {
		return
	}
	if m.Reject {
		n.log.Logf("[%s] term=%d 拉票被 %s 拒绝：%s（当前票数 %d/%d）",
			n.id, n.term, m.From, m.RejectReason, len(n.votes), len(n.conf.Voters))
		return
	}
	n.votes[m.From] = true
	n.log.Logf("[%s] term=%d 获得 %s 选票：%d/%d，多数派线=%d",
		n.id, n.term, m.From, len(n.votes), len(n.conf.Voters), Quorum(len(n.conf.Voters)))
	if HasMajority(len(n.votes), len(n.conf.Voters)) {
		n.becomeLeader()
	}
}

// becomeLeader 完成当选：初始化所有复制进度，并立即广播一轮
// AppendEntries（无操作心跳），用于：
//   - 宣告领导权，压制同任期其他候选者；
//   - 让本任期的 noop 条目尽快复制，从而安全提交前任遗留日志。
func (n *Node) becomeLeader() {
	n.role = Leader
	n.lead = n.id
	n.pr = map[NodeID]*progress{}
	for _, id := range n.replicaSet() {
		p := &progress{nextIndex: n.lastIndex() + 1, matchIndex: 0}
		if id == n.id {
			p.matchIndex = n.lastIndex()
		}
		n.pr[id] = p
	}
	n.log.Logf("[%s] term=%d 赢得选举成为领导者，日志长度=%d，投票集=%v",
		n.id, n.term, n.lastIndex(), n.conf.Voters)

	// 当选即追加本任期 noop（空负载普通条目）。
	e := Entry{Term: n.term, Index: n.lastIndex() + 1, Type: EntryNormal}
	_ = n.st.Append([]Entry{e})
	n.pr[n.id] = &progress{nextIndex: e.Index + 1, matchIndex: e.Index}
	n.bcastAppend()
	n.maybeCommit()
}
