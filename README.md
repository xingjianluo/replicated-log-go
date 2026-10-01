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
| `*_test.go` | 32 个确定性场景测试（含批量成员变更、变更中途故障、移除后再加入） |

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
- 成员变更采用 **Raft 联合共识（Joint Consensus）两阶段**，支持一次打包增删多个节点：
  - `Cluster.ProposeConfChangeBatch(client, add, remove)` 一次提交可同时包含
    多个新增、多个移除，甚至同一批里既加又删；旧接口
    `ProposeConfChange(ConfAddVoter/ConfRemoveVoter)` 完全保留，内部等价于
    单元素批次，**原有用法与返回语义不变**；
  - 一批变更在日志里是两条配置条目：
    1. **enter-joint**（`phase=1`）：条目落盘即进入联合配置 `C_old,new`，
       同时持有旧、新两份成员表；该条目必须**分别**获得旧集合多数派与
       新集合多数派才能提交；
    2. **leave-joint**（`phase=2`）：enter-joint 一经提交，由领导者
       **自动追加**；它落盘即切换到新配置 `C_new`，提交后整批变更最终落定。
    两阶段全部完成前拒绝发起下一批（`membership-pending`）；
  - **成员身份在生效前后的边界（投票/参选/提交计数）**：

    | 节点 | enter-joint 落盘前 | 联合阶段（enter 生效、leave 未生效） | leave-joint 提交后 |
    | --- | --- | --- | --- |
    | 新增节点 | 不属于配置，不投票、不能参选、不计入提交数；以空日志追赶 | 已属于 `C_new`，**可投票、可参选（需双方多数派）、计入新集合提交数** | 正式投票成员 |
    | 移除节点 | 仍是正式成员，一切如常 | 仍属于 `C_old`，**照常投票、参选、计入旧集合提交数**，因此能收到“移除自己”的条目与最终提交点 | 退出协议（`removed`），停止处理消息，已提交数据保留在其余节点 |
    | 提交所需多数派 | 单一多数派 | **旧、新两个多数派都要满足** | 单一多数派（按 `C_new`） |

  - 这套双多数派约束保证：变更前、变更中、变更完成后，集群任意时刻都不可能
    出现两个都能推进提交的领导者，也不可能出现“一部分节点认新表、一部分认旧表
    却各自能提交”的分裂；最终所有存活节点收敛到同一份成员表；
  - **变更期间不阻塞普通服务**：联合阶段普通命令照常提交，只是提交点同样需要
    旧∪新双方多数派（与配置条目一致）；
  - **故障与恢复**：enter-joint 尚未复制到存活多数派时领导者崩溃，该条目会被
    新日志安全截断——变更被**安全放弃**，可重新发起；enter-joint 已提交后
    领导者崩溃，新领导者依据日志重建联合状态并**自动补追加 leave-joint**，
    变更被**安全推到完成**。两种结果都不会停在“半生效”状态，已提交数据不丢、不回滚；
  - 领导者当选时写入一条 `noop` 空条目锚定当前任期提交点（图 8 安全性），
    并配合 **pre-vote 预投票**（不递增任期、不投自己的探测轮）防止陈旧或
    落后节点用高任期打断在任领导者；
  - 联合配置生效期间**不允许做日志压缩快照**（快照只保存一份成员表，
    无法携带 `C_old,new` 双集合），返回 `snapshot-joint`；等 leave-joint
    提交后再压缩即可；
  - 新加入节点以空日志启动（不写本地引导配置），在被加入配置前不参与投票，
    通过日志重放（含 index=1 的引导配置条目）或快照获得一致状态；
  - **移除后再加入**：同一节点 ID 重新加入时必须是全新空日志节点，
    经快照/日志重放追平当前成员表与全部历史；它被移除期间残留的旧成员表、
    旧日志随旧 Storage 一并丢弃，不会影响集群判断；重新加入后正常参与复制与选举；
  - 移除/降级节点不删除任何已提交数据：其历史保留在其余多数派成员中。

### 4. 确定性与可解释性

- `Cluster` 每 Tick 的顺序固定：节点计时 → 消息入队 → 网络到期投递（按序号稳定排序）
  → 逐消息处理并把响应排队到下一 Tick。节点按 NodeID 字典序遍历。
- 所有日志行都带 `term`（任期）、`quorum`（多数派阈值）、`commitIndex`（提交位置）
  与 `reason`（原因码），不同失败路径可区分，例如：
  `stale-term`、`already-voted`、`log-not-up-to-date`、`leader-lease`、
  `no-quorum`、`deposed`、`log-conflict`、`log-gap`、`membership-pending`、
  `unknown-voter`、`bad-conf-change`、`snapshot-joint`、
  `snapshot-behind-commit`、`partitioned`、`dropped`、
  `duplicate`、`shutdown`。

## 容错边界

| 故障 | 行为保证 |
| --- | --- |
| 少于半数节点失效/被分区 | 集群继续选主与提交，已提交数据不丢 |
| 领导者被隔离入少数派 | 无法提交新条目；多数派选出新领导者；恢复后旧领导者退位并修正冲突日志 |
| 节点崩溃重启 | 持久状态（任期/投票/日志/快照）保留，FSM 从快照恢复，随后追平日志 |
| 消息重复/乱序/丢包 | 幂等处理：重复确认不回退 matchIndex，重复应用无副作用；丢包靠心跳重传收敛 |
| 日志严重落后 | 经快照恢复，提交位置不回退，恢复后继续按日志追赶 |
| 成员增删 | 批量增删经联合共识两阶段原子生效；变更期间普通日志照常提交；故障后要么安全完成、要么安全放弃；移除节点停止参与但不造成已提交数据丢失；同节点移除后可空日志重新加入 |
| 达到或超过半数节点同时失效 | **不可用**（无法形成多数派），这是多数派协议的固有边界，组件不会错误地允许少数派提交 |

明确**不**覆盖：拜占庭故障（恶意/伪造消息）、跨集群地理位置时钟假设、
真实磁盘损坏（存储以内存模拟“磁盘不丢”）。

## 本地验证

要求 Go 1.25+。

```bash
# 1. 构建
go build ./...

# 2. 运行全部 32 个确定性场景测试
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
  成员扩容、缩容、串行变更与非法变更拒绝（单节点旧接口，经联合共识执行）；
- `TestBatchAddTwoVoters` / `TestBatchRemoveTwoVoters` /
  `TestBatchMixedAddAndRemove`：一次加两台、一次删两台、同批又加又删，
  落定后成员表全体一致且按新多数派继续提交，变更中途普通日志不中断；
- `TestBatchChangeValidation`：空批次、同批又加又删同一节点、删不存在成员、
  重复加入、删空集群等非法批量变更被显式拒绝（`bad-conf-change`/`unknown-voter`）；
- `TestBatchChangeLeaderCrash` / `TestBatchChangeEnterLeaderCrash` /
  `TestBatchChangeWithPartition`：变更中途领导者崩溃、恰在 enter-joint
  已提交时崩溃由新领导者接管补追加 leave-joint、以及联合阶段旧多数派被隔开时
  变更不落定且无分裂领导者，恢复后安全完成；
- `TestRemoveThenReaddVoter` / `TestBatchSwapNodes`：移除后以同一 ID 空日志
  重新加入并追平历史、反复进出集群，旧成员表/旧日志不影响集群判断；
- `TestBatchChangeDeterministic`：叠加故障注入的连续两批成员变更，
  相同种子两次运行（含故障统计）逐字节一致；
- `TestBatchChangeChaos`：连续批量增删成员的全程叠加丢包/重复/乱序、
  周期性分区与崩溃重启，断言任意时刻至多一个可提交领导者，恢复后成员表与
  已提交日志全体一致、不丢不重不回滚；
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

// 一次打包增删多个成员（联合共识两阶段，内部自动追加 leave-joint）。
_ = c.AddNode("n4")
_ = c.AddNode("n5")
cfgIdx, _ := c.ProposeConfChangeBatch("client",
    []raft.NodeID{"n4", "n5"}, []raft.NodeID{"n2"}) // 增 n4,n5；删 n2
// cfgIdx 是 enter-joint 索引；等待整批“最终落定”（离开联合阶段、成员表收敛）。
_ = c.WaitMembership([]raft.NodeID{"n1", "n3", "n4", "n5"}, 400)
// 旧的单节点接口仍然可用，内部等价于单元素批次。
_, _ = c.ProposeConfChange("client", raft.ConfChange{Op: raft.ConfAddVoter, Node: "n6"})
```
