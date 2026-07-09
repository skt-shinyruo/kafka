# Kafka Classic Group Rebalance 是谁发起的

结论：classic group 中，真正有权发起 rebalance 的是 broker 端
GroupCoordinator，当前源码中核心状态推进在 `GroupMetadataManager.prepareRebalance`。
它会把 classic group 转到 `PREPARING_REBALANCE`。

但这里的“Coordinator 观察到 consumer 变化”不能理解成 coordinator 主动轮询 consumer
本地状态。更准确的说法是：consumer 通过 `JoinGroup`、`LeaveGroup`、`Heartbeat` 等协议请求暴露状态变化，
或者 coordinator 通过定时器发现成员 heartbeat/session timeout，随后 coordinator 根据当前 group 状态和请求内容决定是否 rebalance。

## 更准确的触发链路

```text
consumer 本地状态变化、成员变化或故障
  -> consumer 发送 JoinGroup/LeaveGroup，或 coordinator 等到 heartbeat/session timeout
  -> coordinator 比较当前 group 元数据和请求/超时结果
  -> coordinator 决定是否 prepare rebalance
  -> group 进入 PREPARING_REBALANCE
  -> 后续 JoinGroup/SyncGroup/Heartbeat 返回新 generation 或 REBALANCE_IN_PROGRESS
  -> consumer 重新加入并完成新一轮 assignment
```

所以可以说：consumer 的变化通常是 rebalance 的外部原因，coordinator 是 rebalance 的决策者和状态机推进者。

## Classic Group 的主要触发场景

### 新成员加入

动态成员或静态成员加入时，coordinator 会添加成员，然后走：

```text
addMemberThenRebalanceOrCompleteJoin
  -> maybePrepareRebalanceOrCompleteJoin
  -> prepareRebalance
```

如果 group 当前状态允许进入 `PREPARING_REBALANCE`，coordinator 就会发起 rebalance。

### 已有成员重新 JoinGroup

已有成员重新加入时，coordinator 会比较成员协议和 metadata。

在 `STABLE` 状态下：

- leader 重新 `JoinGroup` 会触发 rebalance。
- follower 如果 metadata 或支持协议发生变化，会触发 rebalance。
- follower 如果 metadata 没变，coordinator 只返回当前 generation 信息，不触发 rebalance。

源码注释中特别说明，leader rejoin 可以用于触发那些会影响 assignment、但不一定改变 member metadata 的变化，
例如 consumer 发现 topic metadata 变化后重新加入 group。

### 成员显式离开

consumer 发送 `LeaveGroup`，或者 admin 按 `group.instance.id` 移除静态成员后，
coordinator 会移除成员。如果 group 处于 `STABLE` 或 `COMPLETING_REBALANCE`，会继续进入 rebalance。

### 心跳超时或 session 超时

如果成员不再正常 heartbeat，coordinator 的 timer 会触发 heartbeat expiration。
coordinator 发现成员没有满足 heartbeat 要求后，会移除该成员，并在必要时 rebalance。

这类场景对应的是 consumer crash、网络断开、长时间 stop-the-world、poll 线程卡住等问题在 coordinator 端的表现。

### SyncGroup 阶段异常

group 进入 `COMPLETING_REBALANCE` 后，coordinator 等 leader 提交 assignment，
并等成员完成 `SyncGroup`。如果有成员迟迟不发送 sync，`expirePendingSync` 会移除这些成员并重新 `prepareRebalance`。

如果 leader 提交 assignment 后，group metadata 写入 `__consumer_offsets` 失败，coordinator 也会传播错误并尝试重新 rebalance。

### 静态成员重入

静态成员由 `group.instance.id` 识别。它重入时可能替换旧的 `member.id`。

在 `STABLE` 状态下，如果静态成员重入不改变 selected protocol，coordinator 可以只持久化新的 member id，
不触发 rebalance。若 selected protocol 会变化，或者 group 正处于 `COMPLETING_REBALANCE`，
coordinator 会触发新的 rebalance。

## Topic Metadata 变化的注意点

classic group 下，topic metadata 变化通常不是 coordinator 在后台扫描后主动发起 rebalance。
常见链路是：

```text
topic/partition metadata 变化
  -> consumer 端 metadata 更新
  -> consumer 认为 assignment 可能需要变化
  -> consumer 重新 JoinGroup
  -> coordinator 看到 leader rejoin 或 member metadata 变化
  -> coordinator 触发 rebalance
```

也就是说，topic metadata 变化是外部诱因；classic coordinator 真正看到的是 consumer 的协议请求。

## 状态条件

`ClassicGroup.canRebalance()` 判断 group 能否进入 `PREPARING_REBALANCE`。
`ClassicGroupState` 中定义了：

```text
PREPARING_REBALANCE 的合法前置状态：
  - EMPTY
  - STABLE
  - COMPLETING_REBALANCE
```

如果 group 已经在 `PREPARING_REBALANCE`，很多入口不会再次 prepare rebalance，
而是尝试完成当前 join phase。

## 和新版 Consumer Group Protocol 的区别

新版 consumer group protocol 不再走 classic group 的 `JoinGroup -> SyncGroup`
两阶段状态机，也没有同样的 `PREPARING_REBALANCE/COMPLETING_REBALANCE` 语义。

新版路径中，consumer 通过 `ConsumerGroupHeartbeat` 汇报 member epoch、订阅、owned partitions 等信息。
coordinator 维护 group epoch、subscription metadata、target assignment，并通过 heartbeat response
让成员逐步 reconcile 到目标 assignment。

在新版协议里，成员加入/离开、订阅变化、regex 解析变化、metadata 过期并需要更新时，
coordinator 会在 heartbeat 处理路径中更新 metadata，必要时 bump group epoch，
然后计算新的 target assignment。

## 源码锚点

```text
group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java
  - prepareRebalance
  - maybePrepareRebalanceOrCompleteJoin
  - addMemberThenRebalanceOrCompleteJoin
  - updateMemberThenRebalanceOrCompleteJoin
  - updateStaticMemberThenRebalanceOrCompleteJoin
  - removeMemberAndUpdateClassicGroup
  - expireClassicGroupMemberHeartbeat
  - expirePendingSync
  - classicGroupLeaveToClassicGroup
  - classicGroupSyncToClassicGroup
  - consumerGroupHeartbeat

group-coordinator/src/main/java/org/apache/kafka/coordinator/group/classic/ClassicGroup.java
  - canRebalance

group-coordinator/src/main/java/org/apache/kafka/coordinator/group/classic/ClassicGroupState.java
  - EMPTY
  - PREPARING_REBALANCE
  - COMPLETING_REBALANCE
  - STABLE
  - DEAD
```
