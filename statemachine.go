package raft

import (
	"bytes"
	"fmt"
	"strconv"
)

// FSM 是确定性复制状态机接口。
// 所有节点在相同的已提交条目序列上必须得到相同的结果；
// Apply 必须幂等（重复以相同 index 应用不得改变结果）。
type FSM interface {
	// Apply 应用一条已提交命令。重复应用同一 index 必须无副作用。
	Apply(index int, term int, payload []byte) error
	// Snapshot 返回截至 lastApplied 的状态快照字节。
	Snapshot() []byte
	// Restore 从快照字节恢复状态；之后 Apply 只会收到更晚的索引。
	Restore(data []byte) error
	// LastIndex 返回最近应用的索引。
	LastIndex() int
}

// KVState 是演示/测试用的确定性追加型状态：
// 记录已应用命令的序列，天然支持按索引幂等与快照恢复。
type KVState struct {
	applied map[int]string
	order   []int
}

// NewKVState 创建空状态机。
func NewKVState() *KVState {
	return &KVState{applied: map[int]string{}}
}

// Apply 幂等应用命令：相同 index 重复调用直接命中；不同内容则报错。
func (s *KVState) Apply(index int, term int, payload []byte) error {
	if index <= 0 {
		return fmt.Errorf("apply: 非法索引 %d", index)
	}
	if existing, ok := s.applied[index]; ok {
		// 幂等命中：同一索引重复应用不得改变状态。
		if existing != string(payload) {
			return fmt.Errorf("apply: 索引 %d 内容冲突", index)
		}
		return nil
	}
	if index <= s.LastIndex() {
		// 命令必须按递增顺序应用；成员变更等非命令条目不进入 FSM，允许索引留空。
		return fmt.Errorf("apply: 索引回退或重复，lastApplied=%d 实际 %d", s.LastIndex(), index)
	}
	s.applied[index] = string(payload)
	s.order = append(s.order, index)
	return nil
}

// Snapshot 序列化当前状态。格式为每行 index:payload，payload 中的换行与反斜杠被转义。
func (s *KVState) Snapshot() []byte {
	var buf bytes.Buffer
	for _, idx := range s.order {
		cmd := escape(s.applied[idx])
		fmt.Fprintf(&buf, "%d:%s\n", idx, cmd)
	}
	return buf.Bytes()
}

// Restore 从快照恢复。
func (s *KVState) Restore(data []byte) error {
	s.applied = map[int]string{}
	s.order = nil
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		parts := bytes.SplitN(line, []byte{':'}, 2)
		if len(parts) != 2 {
			return fmt.Errorf("restore: 损坏的快照行 %q", line)
		}
		idx, err := strconv.Atoi(string(parts[0]))
		if err != nil {
			return fmt.Errorf("restore: 非法索引 %q", parts[0])
		}
		s.applied[idx] = unescape(string(parts[1]))
		s.order = append(s.order, idx)
	}
	return nil
}

// LastIndex 返回最近应用索引。
func (s *KVState) LastIndex() int {
	if len(s.order) == 0 {
		return 0
	}
	return s.order[len(s.order)-1]
}

// Commands 按应用顺序返回命令文本（测试断言用）。
func (s *KVState) Commands() []string {
	out := make([]string, 0, len(s.order))
	for _, idx := range s.order {
		out = append(out, s.applied[idx])
	}
	return out
}

func escape(s string) string {
	var buf bytes.Buffer
	for _, r := range s {
		switch r {
		case '\\':
			buf.WriteString(`\\`)
		case '\n':
			buf.WriteString(`\n`)
		default:
			buf.WriteRune(r)
		}
	}
	return buf.String()
}

func unescape(s string) string {
	var buf bytes.Buffer
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case '\\':
				buf.WriteByte('\\')
			case 'n':
				buf.WriteByte('\n')
			default:
				buf.WriteByte(s[i+1])
			}
			i++
			continue
		}
		buf.WriteByte(s[i])
	}
	return buf.String()
}
