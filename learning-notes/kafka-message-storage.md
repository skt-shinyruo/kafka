

# Kafka 消息是怎么存储的

Kafka 的消息本质上是以 **Topic-Partition 日志**的形式，按追加顺序存储在 Broker 磁盘上。

## 1. Topic 会被拆分成多个 Partition

例如：
```text
orders-0
orders-1
orders-2
```
每个 Partition 都是一条独立、有序、只追加的日志：
```text
offset 0 → offset 1 → offset 2 → offset 3 → ...
```
消息的顺序只在同一个 Partition 内保证，不同 Partition 之间没有全局顺序。

## 2. 消息通过 Offset 定位

每条消息在 Partition 中都有一个递增的 Offset：
```text
Partition 0:

offset 100   message A
offset 101   message B
offset 102   message C
```
Offset 不是全局唯一的，通常需要和 Topic、Partition 一起使用：
```text
(topic, partition, offset)
```
Kafka 不会因为 Consumer 读取了消息就删除它。Consumer 的消费位置通常由 Consumer Group 单独保存。

## 3. 磁盘上按 Segment 文件存储

一个 Partition 不会只对应一个无限增长的文件，而是会被拆分成多个 Segment：
```text
00000000000000000000.log
00000000000000000000.index
00000000000000000000.timeindex

00000000000000123456.log
00000000000000123456.index
00000000000000123456.timeindex
```
文件名前面的数字通常表示该 Segment 的起始 Offset。

### `.log`

存放实际的消息记录。Kafka 通常以 **RecordBatch** 的形式写入，而不是逐条写入。

一个 Batch 中可能包含多条消息：
```text
RecordBatch
├── message 1
├── message 2
└── message 3
```
Batch 还可能包含：

- Base Offset
- Timestamp
- Key
- Value
- Headers
- 压缩类型
- CRC 校验信息
- Producer ID 和事务信息等

如果 Producer 启用了压缩，通常是对整个 RecordBatch 进行压缩。

### `.index`

Offset 索引，用于快速定位消息在 `.log` 文件中的物理位置。

Kafka 使用的是**稀疏索引**，不是为每条消息都建立索引：
```text
offset 1000 → 文件位置 0
offset 1050 → 文件位置 4096
offset 1100 → 文件位置 8192
```
读取 offset 1070 时，Kafka 先找到最接近的索引位置，再顺序扫描少量数据。

### `.timeindex`

时间索引，用于根据消息时间戳查找大致对应的 Offset。

## 4. 消息写入过程

简化流程如下：
```text
Producer
↓
选择 Partition
↓
发送 RecordBatch
↓
Broker 追加到 Partition 当前 Active Segment
↓
写入 Page Cache
↓
根据配置刷盘
```
Kafka 主要采用**顺序追加写**，避免随机写，因此具有较高的吞吐量。

消息可能先写入操作系统 Page Cache，不一定在 `send` 返回的瞬间已经持久化到物理磁盘。可靠性还取决于：

- Producer 的 `acks`
- Broker 副本数量
- `min.insync.replicas`
- 是否执行磁盘 flush
- 副本是否同步完成

## 5. Broker 之间通过副本保存消息

每个 Partition 通常有多个副本：
```text
Partition 0
├── Broker 1：Leader
├── Broker 2：Follower
└── Broker 3：Follower
```
Producer 和 Consumer 通常与 Leader 交互。

Leader 将消息写入本地日志后，Follower 从 Leader 拉取并追加到自己的日志。已经同步完成的副本集合称为 **ISR（In-Sync Replicas）**。

高水位线（High Watermark）表示已经被足够多副本确认、Consumer 可以安全读取的范围：
```text
已追加数据：0 1 2 3 4 5 6
高水位线：            ↑
Consumer 通常只能读取已提交范围
```
因此，Kafka 不仅存储消息内容，也依赖 Offset、Leader Epoch、高水位线等元数据来管理副本一致性。

## 6. Consumer 读取时不会搬动消息

Consumer 通过 Offset 拉取消息：
```text
fetch(topic, partition, offset=100)
```
Broker 根据 `.index` 找到大致位置，再从 `.log` 中读取对应的 RecordBatch。

Consumer 读取后：

- 消息仍然保留在日志中；
- Consumer Group 只需要保存自己的消费 Offset；
- 不同 Consumer Group 可以独立重复消费同一批数据。

例如：
```text
消息日志：0 1 2 3 4 5 6 7

Group A 已消费到：4
Group B 已消费到：2
```
两组 Consumer 可以从各自的位置读取同一份数据。

## 7. 消息什么时候被删除

Kafka 通常根据 Topic 的保留策略删除旧 Segment，而不是逐条删除消息。

### 按时间保留
```
properties
retention.ms=604800000
```
例如保留 7 天，过期 Segment 会被删除。

### 按日志大小保留
```
properties
retention.bytes=10737418240
```
当 Partition 日志超过指定大小时，删除较旧的 Segment。

### 日志压缩

如果 Topic 配置：
```
properties
cleanup.policy=compact
```
Kafka 会根据 Key 保留较新的记录，适合保存状态：
```text
key=A, value=1
key=B, value=2
key=A, value=3
```
压缩后可能主要保留：
```text
key=A, value=3
key=B, value=2
```
注意，日志压缩不是普通的消息压缩：

- `gzip`、`lz4`、`snappy`、`zstd`：减少数据体积；
- `cleanup.policy=compact`：根据 Key 清理旧记录。

## 8. Kafka 还会保存哪些特殊日志

除了业务 Topic 的 Partition 日志，Kafka 还会保存一些内部 Topic：

- `__consumer_offsets`：保存 Consumer Group 的消费 Offset；
- `__transaction_state`：保存事务状态；
- Kafka Connect、Kafka Streams 等组件使用的内部 Topic。

在 KRaft 模式下，Kafka 还会有一个用于保存集群元数据的**元数据日志**。它采用 Raft 机制，由 Controller Quorum 复制和管理，主要存储：

- Topic 和 Partition 信息；
- Broker 注册信息；
- 配置变更；
- ACL 等集群元数据。

它与普通业务 Topic 的消息日志用途不同。

## 9. 总体结构

```text
Kafka Broker
└── Topic-Partition Replica
    └── UnifiedLog（完整逻辑日志）
        ├── Remote tier（可选的远程历史 Segment）
        └── LocalLog（当前 Broker 的本地 Segment）
            ├── Older LogSegment
            └── Active LogSegment
                ├── .log
                ├── .index
                ├── .timeindex
                └── .txnindex
```

## 10. LocalLog、UnifiedLog 与远程分层存储

### LocalLog 是什么

`LocalLog` 不是 log4j 这类运行诊断日志，而是某个 Topic-Partition 副本在当前
Broker 本地磁盘上的追加式消息日志。它管理一组按 base offset 排列的
`LogSegment`，并负责具体的本地文件操作：

- 向 active segment 追加 RecordBatch；
- 根据 offset 读取数据；
- 滚动创建新 Segment；
- 刷盘并维护 `recoveryPoint`；
- 维护 LEO（Log End Offset，下一条消息将写入的 offset）；
- 截断、删除、拆分和替换 Segment；
- 处理分区日志目录及磁盘 I/O 异常。

`LocalLog` 本身不是线程安全的，它依赖上层 `UnifiedLog` 的锁来保护修改操作。

### UnifiedLog 中的“远程”是什么

`UnifiedLog` 向上层提供一条完整的逻辑日志视图。未开启分层存储时，完整日志
都由它封装的 `LocalLog` 提供。开启 Tiered Storage 后，完整日志可以同时包含：

```text
完整逻辑日志 = 远程历史 Segment + 本地 Segment
```

这里的远程是 S3、HDFS 或其他由插件实现的外部存储系统，**不是其他 Kafka
Broker 上的 Follower 副本**。`RemoteLogManager` 协调远程 Segment 的复制、读取和清理，
`RemoteStorageManager` 插件执行实际的 copy、fetch 和 delete。远程 Segment 除了
`.log` 数据，也包括 offset、time、transaction、leader epoch 等辅助索引。

数据的典型流程是：

```text
写入消息
  ↓
追加到本地 active segment
  ↓
Segment 滚动后成为非 active segment
  ↓
复制到远程存储
  ↓
达到 local.retention.ms/bytes 后可删除本地副本
```

Active Segment 始终在本地。已上传的旧 Segment 也不会立即从本地删除，因此远程和
本地区间可以有重叠：

```text
offset: 0 ---------------- 700 -------- 1000
        |───── remote ───────|
                       |────── local ─────|
                                  ↑ active
```

当 Consumer 读取的 offset 已不在本地时，Consumer 仍然向 Kafka Broker 发送 Fetch
请求，由 Broker 从远程存储取回数据；Consumer 不会直接访问对象存储。

### 是否应该开启远程存储

远程分层存储不是 Kafka 正常运行的前提，也不是所有集群都应该默认开启的选项。
如果没有明确的长期保留需求，只使用本地存储通常更简单，且历史读取延迟更稳定。

适合只使用 `LocalLog` 对应的本地存储的情况：

- 数据只保留几小时或几天；
- Broker 磁盘容量和成本可接受；
- 希望维持稳定的低延迟和较小的运维复杂度；
- 没有经过生产验证的 `RemoteStorageManager` 插件。

适合开启远程分层存储的情况：

- 数据需要保留数周、数月或更久；
- 数据量很大，长期使用 Broker 本地 SSD 的成本过高；
- 经常需要回溯、重放或批量处理历史数据；
- 希望降低 Broker 扩缩容或故障恢复时需要迁移的历史数据量。

使用远程存储会引入更高的历史读取延迟、对象存储请求和流量成本，以及额外的插件、
监控、权限与故障处理复杂度。当前 Apache Kafka 定义了 `RemoteStorageManager` 接口，
但不内置生产级 S3/HDFS 实现，需要自行选择和验证插件。

最后，远程存储不能替代 Kafka 的副本机制。例如 `replication.factor=3` 时，分区的
Leader 和 Follower 仍然分别在多个 Broker 的 `LocalLog` 中保存和同步数据。远程存储
解决的主要是冷数据容量和成本问题，而副本机制解决的是实时可用性和一致性问题。

## 11. 总结

Kafka 将消息按 Topic-Partition 划分，以 Offset 作为顺序标识，按 RecordBatch 追加写入磁盘上的 Segment 文件，并通过索引快速定位、通过副本保证可靠性、通过保留策略或日志压缩清理旧数据。
