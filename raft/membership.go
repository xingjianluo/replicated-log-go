package raft

import (
	"encoding/json"
)

// Quorum 返回 n 个成员形成多数派所需的最小同意票数：floor(n/2)+1。
func Quorum(n int) int {
	if n <= 0 {
		return 0
	}
	return n/2 + 1
}

// HasMajority 判断 votes 个同意是否构成 members 上的多数派。
func HasMajority(votes int, members int) bool {
	return votes >= Quorum(members)
}

// inConfig 判断节点是否在成员列表中。
func inConfig(id NodeID, members []NodeID) bool {
	for _, m := range members {
		if m == id {
			return true
		}
	}
	return false
}

// ackMajority 判断 ack 集合是否满足配置的提交多数派：
// 普通配置只需一个多数派；联合配置必须 Cold 与 Cnew 两个多数派同时满足。
func ackMajority(ack map[NodeID]bool, conf Configuration) bool {
	if len(conf.Voters) == 0 {
		return false
	}
	count := func(members []NodeID) int {
		n := 0
		for _, id := range members {
			if ack[id] {
				n++
			}
		}
		return n
	}
	if !HasMajority(count(conf.Voters), len(conf.Voters)) {
		return false
	}
	if conf.Joint {
		// 联合阶段 Cnew 是 Cold∪Cnew 的超集……此处约定：
		// Voters=Cold，NewVoters=Cnew，需分别在两个集合上取得多数派。
		return HasMajority(count(conf.NewVoters), len(conf.NewVoters))
	}
	return true
}

// confClone 返回配置的深拷贝（切片独立）。
func confClone(c Configuration) Configuration {
	out := Configuration{Joint: c.Joint}
	out.Voters = append([]NodeID(nil), c.Voters...)
	out.NewVoters = append([]NodeID(nil), c.NewVoters...)
	return out
}

// applyConfChange 在已提交的配置之上施加一次单成员变更，
// 返回变更后的新配置。添加已存在节点 / 移除不存在节点均返回 ErrNoChange；
// 单成员变更是“一次一个”规则，故无需进入联合配置。
func applyConfChange(conf Configuration, cc ConfChange) (Configuration, error) {
	out := confClone(conf)
	out.Joint = false
	out.NewVoters = nil
	switch cc.Type {
	case ConfAddNode:
		if inConfig(cc.Node, out.Voters) {
			return Configuration{}, ErrNoChange
		}
		out.Voters = append(out.Voters, cc.Node)
	case ConfRemoveNode:
		if !inConfig(cc.Node, out.Voters) {
			return Configuration{}, ErrNoChange
		}
		kept := out.Voters[:0]
		for _, id := range out.Voters {
			if id != cc.Node {
				kept = append(kept, id)
			}
		}
		out.Voters = kept
	default:
		return Configuration{}, ErrNoChange
	}
	return out, nil
}

// latestConfigAt 返回 index 位置生效的配置：以快照配置为基础，
// 重放其后所有已提交的成员变更条目。index==0 时返回空配置。
func latestConfigAt(st Storage, snap Snapshot, index uint64) (Configuration, error) {
	if index == 0 {
		return Configuration{}, nil
	}
	conf := confClone(snap.Conf)
	lo := snap.Index + 1
	if lo < 1 {
		lo = 1
	}
	if lo > index {
		return conf, nil
	}
	entries, err := st.Entries(lo, index+1)
	if err != nil {
		return Configuration{}, err
	}
	for _, e := range entries {
		if e.Type != EntryConfChange {
			continue
		}
		c, err := decodeConf(e.Data)
		if err != nil {
			return Configuration{}, err
		}
		conf = c
	}
	return conf, nil
}

// 序列化格式统一使用 JSON：字段固定，保证确定性与可读性。

func encodeConf(conf Configuration) []byte {
	b, _ := json.Marshal(configJSON{
		Joint:     conf.Joint,
		Voters:    conf.Voters,
		NewVoters: conf.NewVoters,
	})
	return b
}

func decodeConf(b []byte) (Configuration, error) {
	var cj configJSON
	if err := json.Unmarshal(b, &cj); err != nil {
		return Configuration{}, err
	}
	return Configuration{Joint: cj.Joint, Voters: cj.Voters, NewVoters: cj.NewVoters}, nil
}

type configJSON struct {
	Joint     bool     `json:"joint"`
	Voters    []NodeID `json:"voters"`
	NewVoters []NodeID `json:"new_voters,omitempty"`
}

func encodeConfChange(cc ConfChange) []byte {
	b, _ := json.Marshal(confChangeJSON{Type: int(cc.Type), Node: cc.Node})
	return b
}

func decodeConfChange(b []byte) (ConfChange, error) {
	var cj confChangeJSON
	if err := json.Unmarshal(b, &cj); err != nil {
		return ConfChange{}, err
	}
	return ConfChange{Type: ConfChangeType(cj.Type), Node: cj.Node}, nil
}

type confChangeJSON struct {
	Type int    `json:"type"`
	Node NodeID `json:"node"`
}
