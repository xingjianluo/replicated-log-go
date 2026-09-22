package raft

func newTestNode(id NodeID, members Membership) *Node {
	log := NewLogger(nil, false)
	st := NewStorage(id, log)
	fsm := NewKVState()
	n := NewNode(id, members, st, fsm, log, 1)
	n.SetEmitter(func(OutMsg) {})
	return n
}
