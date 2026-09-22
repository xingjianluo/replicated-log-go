package raft

import "encoding/json"

// StateMachine 是被复制日志驱动的应用状态机。
// Apply 必须按日志顺序、幂等地被调用：节点重启重放已提交日志时，
// 同一位置可能被再次应用，实现需依据 index 去重。
type StateMachine interface {
	Apply(index uint64, term uint64, cmd []byte) []byte
	// Snapshot 返回当前状态机快照字节。
	Snapshot() ([]byte, error)
	// Restore 用快照字节整体恢复状态机。
	Restore(data []byte) error
	// AppliedIndex 返回最近一次成功应用的日志位置。
	AppliedIndex() uint64
}

// KVOp 是 KV 状态机支持的操作命令（JSON 编码后写入日志 Data）。
// Op 取值："set"（写入/覆盖）、"delete"（删除）。
type KVOp struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// kvSnapshot 是 KVStateMachine 的快照格式，同时携带应用位置，
// 保证恢复后重复 Apply 仍然幂等。
type kvSnapshot struct {
	Applied uint64            `json:"applied"`
	Data    map[string]string `json:"data"`
}

// KVStateMachine 是一个可重放、按位置幂等的内存键值状态机。
type KVStateMachine struct {
	applied uint64
	data    map[string]string
}

// NewKVStateMachine 创建空的 KV 状态机。
func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{data: map[string]string{}}
}

// Apply 按序应用一条命令。重复或过期的位置被幂等忽略：
// 节点重启重放已提交区间时不会二次生效，也不会回退 applied 位置。
func (k *KVStateMachine) Apply(index uint64, term uint64, cmd []byte) []byte {
	if index <= k.applied {
		return nil // 幂等：该位置已经应用过
	}
	if index != k.applied+1 {
		// 调用契约要求严格按序；乱序到达的应用视为缺陷而非静默跳过。
		panic("raft: StateMachine.Apply called out of order")
	}
	var op KVOp
	if len(cmd) > 0 {
		if err := json.Unmarshal(cmd, &op); err != nil {
			// 无法解析的命令同样推进 applied（它确实占用了一个日志位置），
			// 避免坏数据造成状态机永久卡住。
			k.applied = index
			return nil
		}
	}
	switch op.Op {
	case "set":
		k.data[op.Key] = op.Value
	case "delete":
		delete(k.data, op.Key)
	default:
		// 未知操作：忽略其效果，但位置仍然生效。
	}
	k.applied = index
	return nil
}

func (k *KVStateMachine) Snapshot() ([]byte, error) {
	cp := make(map[string]string, len(k.data))
	for key, v := range k.data {
		cp[key] = v
	}
	return json.Marshal(kvSnapshot{Applied: k.applied, Data: cp})
}

func (k *KVStateMachine) Restore(data []byte) error {
	if len(data) == 0 {
		k.applied = 0
		k.data = map[string]string{}
		return nil
	}
	var snap kvSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	k.applied = snap.Applied
	if snap.Data == nil {
		snap.Data = map[string]string{}
	}
	k.data = snap.Data
	return nil
}

func (k *KVStateMachine) AppliedIndex() uint64 { return k.applied }

// Get 返回键的当前值（只读旁路，不经过日志）。
func (k *KVStateMachine) Get(key string) (string, bool) {
	v, ok := k.data[key]
	return v, ok
}
