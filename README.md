# 复制日志一致性组件（可注入确定性执行环境）

一套可在本地运行、**无协程/无真实网络/无墙上时钟**的 Raft 复制日志教学与验证组件。
多个节点由确定性仿真器统一驱动，网络分区、节点重启、消息丢包/重复/乱序全部由
注入的随机种子决定，因此**同一种子的运行逐 tick 可复现**，便于稳定回归与原因排查。

## 目录结构

| 文件 | 职责 |
| --- | --- |
| `types.go` | 节点角色、日志条目、成员配置、快照与全部 RPC 消息的值对象；成员变更编解码 |
| `logger.go` | 结构化判定日志（任期 / 多数派 / 提交位置 / 可区分原因码） |
| `storage.go` | 单节点持久状态：当前任期、投票、日志、快照；显式冲突截断、日志压缩 |
| `statemachine.go` | 确定性状态机接口 `FSM` 与按索引幂等的演示实现 `KVState` |
| `node.go` | Raft 节点纯逻辑状态机：选举、复制、提交、快照、成员变更、重启恢复 |
| `network.go` | 确定性网络：分区、延迟、丢包、重复、乱序注入与可区分的故障统计 |
| `cluster.go` | 仿真编排：确定性 Tick 循环、消息分发、节点增删/重启、观察视图 |
| `cmd/demo/main.go` | 六个异常场景的可运行演示 |
| `*_test.go` | 20 个确定性场景测试 |

## 一致性协议要点

### 1. 领导者选举与多数派约束

- 时间被离散化为 Tick；选举超时在 `[8,15]` Tick 内按节点随机种子抖动，
  心跳间隔固定 2 Tick，避免活锁并保证同种子可复现。
- 候选人任期 +1、先给自己投票（持久化），再向其他成员拉票。
- 同一任期至多一个领导者：
  - 每个节点每个任期只能投一票（`already-voted`）；
  - 候选人日志必须至少与投票者一样新（先比末条任期，再比末条索引，`log-not-up-to-date`）；
  - 进入更高任期时清空本任期投票，使撞车选举能在下一任期化解；
  - 近期仍收到有效领导者心跳时拒绝拉票（`leader-lease`），防止被移除/陈旧节点干扰；
  - 获得 `⌈N/2⌉+1` 张赞成票才成为领导者。
- **少数派分区无法推进提交**：提交判定始终基于当前生效配置的多数派；
  分区中的旧领导者继续心跳也无法凑齐多数派，其本地未提交写入不会生效；
  网络恢复后发现更高任期立即退位（`deposed`），冲突后缀被显式截断修正。

### 2. 日志复制与提交语义

- 领导者为每个跟随者维护 `nextIndex / matchIndex`，通过 `AppendEntries`
  的 `PrevLogIndex/PrevLogTerm` 做一致性检查。
- 冲突处理是**显式**的：发现任期不匹配时记录 `log-conflict` 日志、
  按冲突任期快速回退，并物理删除本地冲突后缀（存储层打印删除区间），
  绝不静默覆盖。
- 提交规则遵循 Raft 图 8 安全性：只对**当前任期**的条目按多数派位置 N 提交，
  历史任期条目随 N 一并提交；跟随者提交点单调跟随 `min(leaderCommit, lastIndex)`，
  从不自行越过领导者提交。
- 已提交条目按序应用；`FSM.Apply` 必须按索引幂等（同索引重复应用无副作用、
  同索引不同内容报错），因此消息重复、领导者切换、节点重启都不会重复执行或回滚命令。
- 客户端应答区分“已落盘”和“已提交”：提案返回仅表示领导者落盘，
  组件提供 `WaitCommit` 等待多数派提交并应用后再判定成功。

### 3. 日志压缩（快照）与成员变更兼容

- 领导者可在任意 `index ≤ commitIndex` 处压缩：捕获 FSM 状态、
  记录快照点任期/索引与当时生效配置，丢弃旧日志；
  不允许压缩未提交位置（`snapshot-behind-commit`）。
- 严重落后节点（所需前缀已不在领导者日志中）通过 `InstallSnapshot` 恢复，
  快照点成为新的一致性锚点：下一条追加以快照点为 `PrevLogIndex`、快照任期为
  `PrevLogTerm`。已提交状态不受影响，提交位置不允许因旧快照回退。
- 成员变更采用**单节点变更**（一次只增/删一个投票成员）：
  - 上一次变更未提交时拒绝新变更（`membership-pending`），避免重叠配置；
  - 变更条目落盘后即按**新配置**计算多数派（Raft 单服务器变更安全性）；
  - 提交前领导者仍向“旧配置 ∪ 新配置”并集复制，
    保证被移除者也能收到“移除自己”的条目与最终提交点；
  - 节点仅在“移除自己”的变更**已提交**后才退出协议；
  - 新加入节点以空日志启动（不写本地引导配置），在被加入配置前不参与投票，
    通过日志重放（含 index=1 的引导配置条目）或快照获得一致状态；
  - 移除/降级节点不删除任何已提交数据：其历史保留在其余多数派成员中。

### 4. 确定性与可解释性

- `Cluster` 每 Tick 的顺序固定：节点计时 → 消息入队 → 网络到期投递（按序号稳定排序）
  → 逐消息处理并把响应排队到下一 Tick。节点按 NodeID 字典序遍历。
- 所有日志行都带 `term`（任期）、`quorum`（多数派阈值）、`commitIndex`（提交位置）
  与 `reason`（原因码），不同失败路径可区分，例如：
  `stale-term`、`already-voted`、`log-not-up-to-date`、`leader-lease`、
  `no-quorum`、`deposed`、`log-conflict`、`log-gap`、`membership-pending`、
  `unknown-voter`、`snapshot-behind-commit`、`partitioned`、`dropped`、
  `duplicate`、`shutdown`。

## 容错边界

| 故障 | 行为保证 |
| --- | --- |
| 少于半数节点失效/被分区 | 集群继续选主与提交，已提交数据不丢 |
| 领导者被隔离入少数派 | 无法提交新条目；多数派选出新领导者；恢复后旧领导者退位并修正冲突日志 |
| 节点崩溃重启 | 持久状态（任期/投票/日志/快照）保留，FSM 从快照恢复，随后追平日志 |
| 消息重复/乱序/丢包 | 幂等处理：重复确认不回退 matchIndex，重复应用无副作用；丢包靠心跳重传收敛 |
| 日志严重落后 | 经快照恢复，提交位置不回退，恢复后继续按日志追赶 |
| 成员增删 | 单节点变更串行生效；移除节点停止参与但不造成已提交数据丢失 |
| 达到或超过半数节点同时失效 | **不可用**（无法形成多数派），这是多数派协议的固有边界，组件不会错误地允许少数派提交 |

明确**不**覆盖：拜占庭故障（恶意/伪造消息）、跨集群地理位置时钟假设、
真实磁盘损坏（存储以内存模拟“磁盘不丢”）。

## 本地验证

要求 Go 1.25+。

```bash
# 1. 构建
go build ./...

# 2. 运行全部 20 个确定性场景测试
go test -v ./...

# 3. 运行六场景演示（打开详细判定日志可改 WithDebug(true)，demo 默认关闭 DEBUG）
go run ./cmd/demo
```

测试覆盖：

- `TestElectUniqueLeaderPerTerm`：同一任期唯一领导者；
- `TestBasicReplicationAndApply` / `TestIdempotentApply`：按序幂等复制与应用；
- `TestMinorityCannotCommit` / `TestPartitionThenConflictRepair`：
  网络分区下少数派不得提交、冲突条目被显式修正、恢复后收敛；
- `TestLeaderRestart` / `TestFollowerWipeAndRestart` /
  `TestElectionDuringSingleNodeDown`：崩溃重启与降级期间多数派可用性；
- `TestDuplicateAndReorder` / `TestDropThenRecover`：重复、乱序、丢包注入；
- `TestSnapshotLaggingNodeRecovery` / `TestSnapshotDoesNotRegressCommit` /
  `TestCompactRejectsUncommitted`：快照恢复与压缩安全；
- `TestAddNodeThenCommit` / `TestRemoveNodeThenCommit` /
  `TestPendingConfChangeBlocks` / `TestRemoveUnknownVoterRejected`：
  成员扩容、缩容、串行变更与非法变更拒绝；
- `TestChaosMixedFailures`：5 节点上叠加周期性分区、崩溃重启与丢包/重复/乱序，
  恢复后逐条校验所有存活成员的已提交日志任期与状态机命令完全一致；
- `TestDeterministic` / `TestDeterministicFaultRun`：相同种子两次运行结果（含故障统计）逐字节一致。

在代码中自定义场景：

```go
c := raft.NewCluster([]raft.NodeID{"n1", "n2", "n3"},
    raft.WithSeed(42),
    raft.WithFaultRates(0.05, 0.10, 0.10), // 丢包/重复/乱序概率
    raft.WithLogWriter(os.Stdout),
    raft.WithDebug(true))
_ = c.Start(200)
idx, reason := c.Propose("client", []byte("SET x=1"))
_ = c.WaitCommit(idx, 200)
c.Isolate("n1"); c.RunUntil(30); c.HealAll()
```
