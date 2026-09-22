package sim

import (
	"container/heap"
	"encoding/json"

	"github.com/xingjianluo/replicated-log-go/raft"
)

// nodeRuntime 绑定 Raft 节点、应用状态机与稳定磁盘。
type nodeRuntime struct {
	id    raft.NodeID
	node  *raft.Node
	sm    *raft.KVStateMachine
	disk  *Disk
	alive bool
}

// envEvent 是事件堆上的一个元素（消息投递）。
type envEvent struct {
	at  int64
	seq int64 // 同刻按入队顺序打破平局，保证确定性
	msg raft.Message
}

type eventHeap []envEvent

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(envEvent)) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// Env 是确定性执行环境：唯一的效果来源是 Run 对逻辑时钟的推进。
// 所有随机都来自种子，所有消息都经 Network 路由，因而同一脚本可复现。
type Env struct {
	log     *Logger
	net     *Network
	seed    int64
	seq     int64
	tickNow int64
	events  eventHeap
	nodes   map[raft.NodeID]*nodeRuntime
	order   []raft.NodeID // 稳定的节点处理顺序
}

// NewEnv 创建空环境。
func NewEnv(log *Logger, seed int64) *Env {
	e := &Env{
		log:    log,
		seed:   seed,
		net:    NewNetwork(seed+1, log),
		nodes:  map[raft.NodeID]*nodeRuntime{},
		events: eventHeap{},
	}
	heap.Init(&e.events)
	return e
}

// Network 暴露网络控制面（分区、丢包、重复、延迟）。
func (e *Env) Network() *Network { return e.net }

// AddNode 创建一个新节点（空白磁盘）。Bootstrap 前加入的节点互为初始成员。
func (e *Env) AddNode(id raft.NodeID) {
	if _, ok := e.nodes[id]; ok {
		return
	}
	rt := &nodeRuntime{id: id, sm: raft.NewKVStateMachine(), disk: NewDisk(), alive: true}
	e.nodes[id] = rt
	e.order = append(e.order, id)
	e.log.Infof("节点 %s 创建（空白磁盘）", id)
}

func (e *Env) createRaft(id raft.NodeID, peers []raft.NodeID) {
	rt := e.nodes[id]
	n, err := raft.NewNode(raft.Config{
		ID:            id,
		Peers:         peers,
		Storage:       rt.disk.Store(),
		StateMachine:  rt.sm,
		ElectionTick:  10,
		HeartbeatTick: 2,
		Seed:          e.seed,
		Logger:        e.log,
	})
	if err != nil {
		panic(err)
	}
	rt.node = n
	rt.alive = true
}

// Bootstrap 用已加入的全部节点初始化集群：所有节点写入同一份初始配置。
func (e *Env) Bootstrap(ids []raft.NodeID) {
	for _, id := range ids {
		e.AddNode(id)
	}
	for _, id := range ids {
		e.createRaft(id, append([]raft.NodeID(nil), ids...))
	}
	e.log.Infof("集群引导完成，初始投票集=%v，多数派=%d", ids, raft.Quorum(len(ids)))
}

// Tick 返回当前逻辑时钟。
func (e *Env) Tick() int64 { return e.tickNow }

// Run 推进 ticks 个逻辑时钟。每个 tick：先让所有存活节点走一次 Tick，
// 再投递所有到期消息（同一时刻按入队序，处理产生的新消息重新排队）。
func (e *Env) Run(ticks int) {
	for range make([]struct{}, ticks) {
		e.tickNow++
		e.log.setTick(e.tickNow)
		for _, id := range e.order {
			rt := e.nodes[id]
			if !rt.alive {
				continue
			}
			for _, m := range rt.node.Tick() {
				e.route(m)
			}
		}
		e.deliverDue()
	}
}

func (e *Env) route(m raft.Message) {
	for _, tm := range e.net.Route(m, e.tickNow) {
		e.seq++
		heap.Push(&e.events, envEvent{at: tm.DeliverAt, seq: e.seq, msg: tm.Msg})
	}
}

// deliverDue 投递时刻 <= 当前 tick 的全部消息；处理中新产生的消息
// 若延迟为 0，可能在同一轮继续被处理（递归深度受延迟下界自然限制）。
func (e *Env) deliverDue() {
	for e.events.Len() > 0 && e.events[0].at <= e.tickNow {
		ev := heap.Pop(&e.events).(envEvent)
		rt, ok := e.nodes[ev.msg.To]
		if !ok || !rt.alive {
			continue // 宕机期间消息被丢弃，模拟不可达内存态
		}
		out, err := rt.node.Step(ev.msg)
		if err != nil {
			continue
		}
		for _, m := range out {
			e.route(m)
		}
	}
}

// RunUntilLeader 推进时钟直到存在领导者（以多数派视图收敛为准），超时返回 false。
func (e *Env) RunUntilLeader(maxTicks int) bool {
	for range make([]struct{}, maxTicks) {
		if e.Leader() != "" {
			return true
		}
		e.Run(1)
	}
	return e.Leader() != ""
}

// Leader 返回领导者节点（要求领导者自认且 commit 已建立更好，这里以角色为准）。
func (e *Env) Leader() raft.NodeID {
	for _, id := range e.order {
		rt := e.nodes[id]
		if rt.alive && rt.node.Status().Role == raft.Leader {
			return id
		}
	}
	return ""
}

// IsLeader 报告指定节点当前是否为领导者。
func (e *Env) IsLeader(id raft.NodeID) bool {
	rt, ok := e.nodes[id]
	return ok && rt.alive && rt.node.Status().Role == raft.Leader
}

// Propose 由指定节点提案一条 set 命令；非领导者返回 raft.ErrNotLeader。
func (e *Env) Propose(from raft.NodeID, key, value string) error {
	rt := e.nodes[from]
	if rt == nil || !rt.alive {
		return raft.ErrNotLeader
	}
	cmd, _ := json.Marshal(raft.KVOp{Op: "set", Key: key, Value: value})
	if err := rt.node.Propose(cmd); err != nil {
		return err
	}
	for _, m := range rt.node.DrainMessages() {
		e.route(m)
	}
	e.deliverDue()
	return nil
}

// Delete 由指定节点提案一条 delete 命令。
func (e *Env) Delete(from raft.NodeID, key string) error {
	rt := e.nodes[from]
	if rt == nil || !rt.alive {
		return raft.ErrNotLeader
	}
	cmd, _ := json.Marshal(raft.KVOp{Op: "delete", Key: key})
	if err := rt.node.Propose(cmd); err != nil {
		return err
	}
	for _, m := range rt.node.DrainMessages() {
		e.route(m)
	}
	e.deliverDue()
	return nil
}

// ProposeConfChange 由指定节点发起单节点成员变更。
func (e *Env) ProposeConfChange(from raft.NodeID, cc raft.ConfChange) error {
	rt := e.nodes[from]
	if rt == nil || !rt.alive {
		return raft.ErrNotLeader
	}
	if err := rt.node.ProposeConfChange(cc); err != nil {
		return err
	}
	for _, m := range rt.node.DrainMessages() {
		e.route(m)
	}
	e.deliverDue()
	return nil
}

// CompactNode 要求节点以当前已应用位置生成快照、压缩日志。
func (e *Env) CompactNode(id raft.NodeID) (raft.Snapshot, error) {
	return e.nodes[id].node.TakeSnapshot()
}

// RestartNode 重启节点：磁盘保留（任期/投票/日志/快照），内存态重建。
// 状态机先用磁盘快照恢复，再由节点把 (快照点, 提交点] 的已提交日志重放；
// 重启后角色为跟随者，等待领导者心跳重新收敛。
func (e *Env) RestartNode(id raft.NodeID) error {
	rt, ok := e.nodes[id]
	if !ok {
		return raft.ErrNoPeer
	}
	if rt.node != nil {
		rt.node.Stop()
	}
	rt.sm = raft.NewKVStateMachine() // 内存状态机清空：靠快照+重放恢复
	// 先把磁盘上的快照恢复进状态机，保证应用位置不低于压缩点。
	if snap := rt.disk.Store().Snapshot(); snap.Index > 0 {
		if err := rt.sm.Restore(append([]byte(nil), snap.Data...)); err != nil {
			return err
		}
	}
	e.createRaft(id, nil) // 非空磁盘，Peers 被忽略
	rt.alive = true
	// 注入一条本地消息触发提交区间扫描与重放（不产生任何网络流量）。
	if _, err := rt.node.Step(raft.Message{Type: raft.MsgBeat, From: id, To: id}); err != nil {
		return err
	}
	snap := rt.disk.Store().Snapshot()
	e.log.Infof("节点 %s 重启：磁盘保留 lastIndex=%d，快照点=%d，重放到 applied=%d，角色=跟随者",
		id, rt.disk.Store().LastIndex(), snap.Index, rt.sm.AppliedIndex())
	return nil
}

// PrepareNewNode 为后续成员变更（ConfAddNode）准备一个空白节点：
// 磁盘与 Raft 节点以“现有成员集合 + 自己”为初始配置创建，但不参与投票，
// 直到领导者提交加入它的配置条目。这样它不会在加入前自行当选。
func (e *Env) PrepareNewNode(id raft.NodeID) {
	if _, ok := e.nodes[id]; ok {
		return
	}
	e.AddNode(id)
	var peers []raft.NodeID
	if lead := e.Leader(); lead != "" {
		peers = append(peers, e.Status(lead).Config.Voters...)
	}
	if !containsNode(peers, id) {
		peers = append(peers, id)
	}
	e.createRaft(id, peers)
}

func containsNode(ids []raft.NodeID, id raft.NodeID) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// StopNode 模拟宕机：停止处理时钟与消息（磁盘仍保留）。
func (e *Env) StopNode(id raft.NodeID) {
	if rt := e.nodes[id]; rt != nil {
		rt.alive = false
		if rt.node != nil {
			rt.node.Stop()
		}
		e.log.Infof("节点 %s 宕机", id)
	}
}

// StartNode 从宕机中恢复：在同一磁盘上重新创建 Raft 节点
// （旧对象已 Stop，必须重建；任期/日志/快照由磁盘保留）。
func (e *Env) StartNode(id raft.NodeID) {
	if rt := e.nodes[id]; rt != nil && !rt.alive {
		_ = e.RestartNode(id)
		e.log.Infof("节点 %s 恢复上线", id)
	}
}

// Get 读取节点状态机中的键值。
func (e *Env) Get(id raft.NodeID, key string) (string, bool) {
	return e.nodes[id].sm.Get(key)
}

// CommitIndex 返回节点的已提交位置。
func (e *Env) CommitIndex(id raft.NodeID) uint64 {
	return e.nodes[id].node.Status().Commit
}

// AppliedIndex 返回节点的已应用位置。
func (e *Env) AppliedIndex(id raft.NodeID) uint64 {
	return e.nodes[id].node.Status().Applied
}

// Status 返回节点的复制状态快照。
func (e *Env) Status(id raft.NodeID) raft.Status {
	return e.nodes[id].node.Status()
}

// LogMatch 检查所有在线节点状态机中该键的值与期望是否一致；
// 返回参与检查的节点数、实际值与是否全部一致。
func (e *Env) LogMatch(key string) (int, string, bool) {
	return e.LogMatchAmong(key, e.aliveOrder())
}

// LogMatchAmong 仅在给定节点集合上检查一致性（用于成员变更后排除已移除节点）。
func (e *Env) LogMatchAmong(key string, ids []raft.NodeID) (int, string, bool) {
	value, ok := "", false
	n := 0
	for _, id := range ids {
		rt := e.nodes[id]
		if rt == nil || !rt.alive {
			continue
		}
		n++
		v, present := rt.sm.Get(key)
		if n == 1 {
			value, ok = v, present
			continue
		}
		if present != ok || v != value {
			return n, v, false
		}
	}
	return n, value, ok
}

// aliveOrder 返回稳定顺序下的存活节点 ID。
func (e *Env) aliveOrder() []raft.NodeID {
	out := make([]raft.NodeID, 0, len(e.order))
	for _, id := range e.order {
		if e.nodes[id] != nil && e.nodes[id].alive {
			out = append(out, id)
		}
	}
	return out
}

// VotingMembers 返回指定节点当前所知配置中的投票成员。
func (e *Env) VotingMembers(from raft.NodeID) []raft.NodeID {
	if rt := e.nodes[from]; rt != nil && rt.node != nil {
		return append([]raft.NodeID(nil), rt.node.Status().Config.Voters...)
	}
	return nil
}
