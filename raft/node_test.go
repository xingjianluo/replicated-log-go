package raft

import (
	"testing"
)

type testCluster struct {
	nodes map[NodeID]*Node
	queue map[NodeID][]Message
}

func newTestCluster(t *testing.T, ids []NodeID) *testCluster {
	t.Helper()
	c := &testCluster{nodes: map[NodeID]*Node{}, queue: map[NodeID][]Message{}}
	for i, id := range ids {
		st := NewMemoryStore()
		n, err := NewNode(Config{
			ID: id, Peers: ids, Storage: st, StateMachine: NewKVStateMachine(),
			ElectionTick: 5, HeartbeatTick: 1, Seed: int64(i * 1000),
			Logger: testLogger{t},
		})
		if err != nil {
			t.Fatal(err)
		}
		c.nodes[id] = n
	}
	return c
}

type testLogger struct{ t *testing.T }

func (l testLogger) Logf(f string, a ...any) { l.t.Logf(f, a...) }

func (c *testCluster) deliver(m Message) {
	c.queue[m.To] = append(c.queue[m.To], m)
}

// pump 串行处理一轮所有排队消息（先到先处理；重复/乱序可由调用者手工制造）。
func (c *testCluster) pump() {
	for {
		progressed := false
		for id := range c.nodes {
			q := c.queue[id]
			if len(q) == 0 {
				continue
			}
			m := q[0]
			c.queue[id] = q[1:]
			out, err := c.nodes[id].Step(m)
			if err != nil {
				continue
			}
			for _, r := range out {
				c.deliver(r)
			}
			progressed = true
		}
		if !progressed {
			return
		}
	}
}

func (c *testCluster) tickN(n int) {
	for range make([]struct{}, n) {
		for id := range c.nodes {
			out := c.nodes[id].Tick()
			for _, m := range out {
				c.deliver(m)
			}
		}
		c.pump()
	}
}

func (c *testCluster) leader() NodeID {
	for id, n := range c.nodes {
		if n.Status().Role == Leader {
			return id
		}
	}
	return ""
}

func TestElectLeaderAndCommit(t *testing.T) {
	ids := []NodeID{"1", "2", "3"}
	c := newTestCluster(t, ids)
	c.tickN(20)
	lead := c.leader()
	if lead == "" {
		t.Fatalf("未选出领导者: %+v", func() map[NodeID]Status {
			m := map[NodeID]Status{}
			for id, n := range c.nodes {
				m[id] = n.Status()
			}
			return m
		}())
	}
	if err := c.nodes[lead].Propose([]byte(`{"op":"set","key":"k","value":"v"}`)); err != nil {
		t.Fatal(err)
	}
	for _, out := range c.nodes[lead].DrainMessages() {
		c.deliver(out)
	}
	c.pump()
	c.tickN(5)

	for _, id := range ids {
		st := c.nodes[id].Status()
		if st.Commit < 2 { // index1 引导配置 + noop(2) + 提案(3)
			t.Fatalf("节点 %s commit=%d, 期望 >=2; %+v", id, st.Commit, st)
		}
		v, ok := getSM(c.nodes[id]).Get("k")
		if !ok || v != "v" {
			t.Fatalf("节点 %s 状态机未收敛 k=v, got %q ok=%v", id, v, ok)
		}
	}
}

func getSM(n *Node) *KVStateMachine { return n.sm.(*KVStateMachine) }
