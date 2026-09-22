package sim

import "github.com/xingjianluo/replicated-log-go/raft"

// Disk 模拟节点的稳定存储：节点重启（重新创建 Raft 节点对象）后
// 同一个 Disk 仍被复用，因此 currentTerm/votedFor/日志/快照得以保留。
type Disk struct {
	store *raft.MemoryStore
}

// NewDisk 创建一块空白磁盘。
func NewDisk() *Disk { return &Disk{store: raft.NewMemoryStore()} }

// Store 返回底层 Raft 存储。
func (d *Disk) Store() raft.Storage { return d.store }
