

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
└── Topic
└── Partition
├── Active Segment
│   ├── .log
│   ├── .index
│   └── .timeindex
├── Older Segment
├── Older Segment
└── Replica / Leader / Follower
```
## 10. 总结

Kafka 将消息按 Topic-Partition 划分，以 Offset 作为顺序标识，按 RecordBatch 追加写入磁盘上的 Segment 文件，并通过索引快速定位、通过副本保证可靠性、通过保留策略或日志压缩清理旧数据。
