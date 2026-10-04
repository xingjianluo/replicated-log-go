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
| `node.go` | Raft 节点纯逻辑状态机：选举、复制、提交、快照、单节点变更、批量变更（联合共识两阶段 + learner）、重启恢复 |
| `network.go` | 确定性网络：分区、延迟、丢包、重复、乱序注入与可区分的故障统计 |
| `cluster.go` | 仿真编排：确定性 Tick 循环、消息分发、节点增删/重启、learner 注册、批量变更编排、观察视图 |
| `cmd/demo/main.go` | 七个异常/运维场景的可运行演示 |
| `*_test.go` | 确定性场景测试（选举/复制/分区/重启/快照/成员变更/内容语义） |

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
- 成员变更支持两种方式，均为**串行生效**（上一次未落定前拒绝下一次，
  `membership-pending`）：
  - **单节点变更**（`ProposeConfChange`，原有用法保持不变）：一次增/删一个
    投票成员，变更条目落盘后即按新配置计算多数派（Raft 单服务器变更安全性），
    提交前向旧∪新并集复制。
  - **批量成员变更**（`ProposeBatchConfChange`，新增）：一次原子提交多个增删，
    内部用 Raft **联合共识（Joint Consensus）两阶段**完成：
    1. 新增节点先以 **learner（无投票权）**身份加入，立即接收日志/快照复制
       追平到领导者（不计多数派、不参选、不投票）；
    2. 追加 `enter-joint(C_old,new)` 条目：其提交必须**同时**拿到 C_old 与
       C_new 双方多数派，期间集群处于联合配置 C_old∪C_new，选举同样要求
       双多数派——因此变更前/中/后都不可能出现两个各自能推进提交的领导者，
       也不可能出现“一部分认新表、一部分认旧表却各自提交”的分裂；
    3. enter 提交后领导者自动追加 `leave-joint(C_new)`，再经双多数派提交，
       集群切换到最终配置；新领导者在联合中间态接管时会自动续推
       （必要时追加当前任期 no-op 以满足图 8 提交安全性）。
  - 变更窗口内普通命令照常提交（双多数派），不停摆；新增 learner 已先追平，
    联合阶段因此很短。
  - 被移除节点在联合期间仍凭 C_old 身份参与双多数派，直到 leave-joint 提交；
    领导者（含新当选接管者）会向其补发携带“leave 提交点”的最后心跳，
    节点确认自己的移除已提交后才干净退出，随后可被摘除并在日后重新加入。
  - 批量变更的**生效边界**（对每个被增/删节点，生效前 vs 生效后）：

    | 身份阶段 | 能否投票 | 能否拉票参选 | 计提交多数派 |
    | --- | --- | --- | --- |
    | learner（已加入编排、未入表） | 否 | 否 | 不计 |
    | 联合中间态且在 C_new 中（新加入者） | 是（仅新配置票） | 是（需双多数派） | 计入 C_new |
    | 联合中间态且仅在 C_old 中（将被删者） | 是（仅旧配置票） | 是（需双多数派） | 计入 C_old |
    | 联合中间态且同时在 C_old∩C_new | 是（两边票） | 是（需双多数派） | 两边均计 |
    | leave 提交后留在 C_new | 是 | 是 | 按 C_new |
    | leave 提交后被移出 | 否（确认提交后退出） | 否 | 不计 |

  - **移除后重新加入**：同名节点以全新空日志、learner 身份重新加入编排，
    旧持久状态被丢弃，完全由当前领导者经快照/日志补齐；其被移除期间的旧成员表、
    旧日志不会影响集群判断，加入生效后正常参与复制与选举。
  - 非法批量变更被显式拒绝：空批次（`bad-conf-change`）、同一节点重复增或
    既增又删（`bad-conf-change`）、添加已在表成员/删除非成员（`unknown-voter`）、
    删空整个集群（`bad-conf-change`）、learner 未追平（`learner-lagging`）。
  - 移除/降级节点不删除任何已提交数据：其历史保留在其余多数派成员中。

### 4. 确定性与可解释性

- `Cluster` 每 Tick 的顺序固定：节点计时 → 消息入队 → 网络到期投递（按序号稳定排序）
  → 逐消息处理并把响应排队到下一 Tick。节点按 NodeID 字典序遍历。
- 所有日志行都带 `term`（任期）、`quorum`（多数派阈值）、`commitIndex`（提交位置）
  与 `reason`（原因码），不同失败路径可区分，例如：
  `stale-term`、`already-voted`、`log-not-up-to-date`、`leader-lease`、
  `no-quorum`、`deposed`、`log-conflict`、`log-gap`、`membership-pending`、
  `unknown-voter`、`not-voter`、`bad-conf-change`、`learner-lagging`、
  `snapshot-behind-commit`、`partitioned`、`dropped`、
  `duplicate`、`shutdown`。

### 5. 客户端命令内容语义（无保留字）

- **命令正文没有保留字、没有禁止内容**：`Propose` 接受任意字节内容，
  包括与内部占位文本（联合共识续推用的 no-op 负载 `"joint-noop"`）
  完全相同的内容。客户端命令一律原样落盘、提交并应用到所有存活节点。
- **内部条目与客户端数据严格隔离**：集群自动追加的内部条目
  （联合续推 no-op）在落盘时以 `LogEntry.Internal` 标志标记，
  应用阶段只凭该标志跳过、绝不进入状态机；判别从不依据负载内容，
  因此内容相同的客户端命令不会被误当作内部条目吞掉，
  内部条目也不会混进状态机污染客户端数据。
- **“报告成功”的语义分两层**：`Propose` 返回成功仅表示条目已在领导者
  **落盘**（并给出日志索引）；只有经多数派（联合期间为双多数派）**提交**
  后才会按序应用到各节点状态机。`WaitCommit` 等待的正是“提交并应用”。
  因此判定一条命令最终生效，应以提交后各存活节点状态机内容为准；
  提交成功与状态机最终看到的内容严格对应，不会半途截断或回滚。
- 提交失败/被拒均有可区分原因码（如 `not-leader`、`no-quorum`、
  `membership-pending`、`shutdown`），与成功结果不会混淆。

## 容错边界

| 故障 | 行为保证 |
| --- | --- |
| 少于半数节点失效/被分区 | 集群继续选主与提交，已提交数据不丢 |
| 领导者被隔离入少数派 | 无法提交新条目；多数派选出新领导者；恢复后旧领导者退位并修正冲突日志 |
| 节点崩溃重启 | 持久状态（任期/投票/日志/快照）保留，FSM 从快照恢复，随后追平日志 |
| 消息重复/乱序/丢包 | 幂等处理：重复确认不回退 matchIndex，重复应用无副作用；丢包靠心跳重传收敛 |
| 日志严重落后 | 经快照恢复，提交位置不回退，恢复后继续按日志追赶 |
| 成员增删 | 单节点变更串行生效；批量变更经联合共识两阶段原子生效，双多数派保证全程不分裂；learner 先追平、窗口内普通命令不阻塞；移除节点干净退出后可重新加入，已提交数据不丢 |
| 达到或超过半数节点同时失效 | **不可用**（无法形成多数派），这是多数派协议的固有边界，组件不会错误地允许少数派提交 |

明确**不**覆盖：拜占庭故障（恶意/伪造消息）、跨集群地理位置时钟假设、
真实磁盘损坏（存储以内存模拟“磁盘不丢”）。

## 本地验证

要求 Go 1.25+。

```bash
# 1. 构建
go build ./...

# 2. 运行全部确定性场景测试（含批量变更、故障中途恢复、移除后再加入）
go test -v ./...

# 3. 运行七场景演示（打开详细判定日志可改 WithDebug(true)，demo 默认关闭 DEBUG）
go run ./cmd/demo
```

测试覆盖（原有场景全部保留，另新增批量成员变更套件与命令内容语义套件）：

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
  **单节点**成员扩容、缩容、串行变更与非法变更拒绝（原有用法，继续兼容）；
- `TestBatchAddTwoVoters` / `TestBatchRemoveTwoVoters` /
  `TestBatchMixedAddAndRemove`：一次加两个、一次删两个、同批既加又删，
  最终所有存活节点收敛到同一份成员表，learner 生效前后身份正确；
- `TestBatchChangeDoesNotBlockCommands`：两阶段窗口内普通命令持续提交不卡停；
- `TestBatchInvalidChangesRejected`：空批次/重复增/既增又删/删非成员/删空/
  learner 未追平/变更重叠等非法输入的显式原因码拒绝；
- `TestBatchLeaderCrashDuringJoint` / `TestBatchPartitionDuringJoint`：
  联合窗口领导者崩溃或被隔离，恢复后变更安全完成或安全放弃，成员表收敛、
  已提交数据不丢不回滚；
- `TestRemoveThenReaddNode`：节点移除后以同名空日志重新加入，旧成员表/旧日志
  不影响判断，重新加入后正常参与复制与选举；
- `TestBatchAddLearnerRecoversViaSnapshot` /
  `TestSnapshotDuringJointCommitted`：learner 经快照追平后批量加入、
  联合落定后快照配置正确；
- `TestBatchFuzzSeeds` / `TestBatchFuzzLeaderCrash` /
  `TestBatchFuzzLeaderPartition`：在注入丢包/重复/乱序的多种子下反复执行
  混合批量变更、联合窗口崩溃与分区，验证成员表收敛、命令序列一致、不丢数据；
- `TestChaosMixedFailures`：5 节点上叠加周期性分区、崩溃重启与丢包/重复/乱序，
  恢复后逐条校验所有存活成员的已提交日志任期与状态机命令完全一致；
- `TestDeterministic` / `TestDeterministicFaultRun`：相同种子两次运行结果（含故障统计）逐字节一致；
- `TestClientCommandEqualToInternalNoopPayload`：客户端命令正文恰好等于内部
  no-op 占位文本（`"joint-noop"`）时，仍被全部存活节点原样应用，
  不出现“报告成功却查无此命令”；
- `TestInternalNoopNeverReachesFSM`：联合窗口领导者崩溃触发内部 no-op 续推，
  内部条目被所有节点跳过、不进状态机（断言日志中确实存在内部条目，
  且状态机只有客户端命令）；
- `TestNoopPayloadCommandSurvivesJointCrashAndRestart`：联合变更进行中提交
  占位文本命令，随后领导者崩溃/重启、跟随者隔离/恢复，命令不丢不重，
  最终与状态机内容严格对应；
- `TestNoopPayloadCommandAcrossRemoveAndReadd`：节点移除前/移除后/重新加入后
  各提交一条占位文本命令，三条都按序落到全部节点（含重新加入者）的状态机。

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

批量成员变更（一次增删多个节点；learner 追平、enter/leave-joint 由集群自动完成）：

```go
c := raft.NewCluster([]raft.NodeID{"n1", "n2", "n3"},
    raft.WithSeed(42),
    raft.WithLogWriter(os.Stdout))
_ = c.Start(200)

// 1) 新节点先加入编排（无投票权 learner），立即开始日志/快照追赶。
_ = c.AddNode("n4")
_ = c.AddNode("n5")

// 2) 一次原子提交：同时加 n4/n5、删 n2（也支持只加或只删多个）。
//    内部自动走 enter-joint(C_old,new) 与 leave-joint(C_new)，
//    返回后变更已在全部存活节点落定并收敛到同一份成员表。
idx, reason := c.ProposeBatchConfChange("client", raft.BatchConfChange{
    Adds:    []raft.NodeID{"n4", "n5"},
    Removes: []raft.NodeID{"n2"},
})
if reason != raft.ReasonOK {
    log.Fatalf("批量变更失败: %s", reason)
}
_ = c.WaitMembershipConverged(
    []raft.NodeID{"n1", "n3", "n4", "n5"}, 300)
_ = c.WaitCommit(idx, 300)

// 3) 节点级 API（不等集群自动完成时）可用：
//    n.IsBatchConfComplete() 判断是否已离开联合中间态；
//    n.Joint() 判断是否处于 C_old,new；NodeView{Learner,Joint} 观察身份。
```
