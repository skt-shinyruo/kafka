# Kafka Producer 发送到 Broker 的消息布局

Kafka producer 发到 broker 的不是单条裸消息，而是一个 `ProduceRequest`。里面按 `topic -> partition` 分组，每个分区带一个 `MemoryRecords`，也就是一段 record batch 二进制数据。

代码路径上大致是：

```text
ProducerRecord -> ProducerBatch -> MemoryRecords -> ProduceRequest
```

在当前源码里可以看到 `Sender.sendProduceRequest()` 把每个 `ProducerBatch.records()` 放进：

```text
ProduceRequest
  acks
  timeoutMs
  transactionalId
  topicData[]
    topicName / topicId
    partitionData[]
      partitionIndex
      records = MemoryRecords
```

## Producer 端怎么构造 ProduceRequest

从应用调用 `send()` 到网络发送，中间大致经过这些步骤：

```text
KafkaProducer.send()
└─ KafkaProducer.doSend()
   ├─ Serializer
   │  ├─ key -> byte[]
   │  └─ value -> byte[]
   ├─ Partitioner
   │  └─ 决定 topic-partition
   ├─ RecordAccumulator.append()
   │  └─ 写入对应 topic-partition 的 ProducerBatch
   └─ Sender 线程
      ├─ 找出 ready 的 ProducerBatch
      ├─ ProducerBatch.records() -> MemoryRecords
      └─ Sender.sendProduceRequest() -> ProduceRequest
```

这条链路里有几个关键点：

| 阶段 | 作用 | 结果 |
| --- | --- | --- |
| `Serializer` | 把业务对象转成 bytes | `key`、`value` 变成二进制 |
| `Partitioner` | 决定写入哪个 partition | 得到 `TopicPartition` |
| `RecordAccumulator` | 按 `topic-partition` 缓冲消息 | 多条消息进入同一个 `ProducerBatch` |
| `MemoryRecordsBuilder` | 把 batch 写成 Kafka record batch 格式 | 得到 `MemoryRecords` |
| `Sender` | 按 broker 节点聚合 batch 并构造请求 | 得到 `ProduceRequest` |

注意：业务层看到的是 `ProducerRecord`；网络层发送的是 `ProduceRequest`；两者中间经过了序列化、分区、缓冲、合批和 record batch 编码。

## RecordAccumulator 和合批配置

Producer 不是每来一条消息就立刻发一个请求。它会把同一个 `topic-partition` 的消息放进同一个 `ProducerBatch`，再由 `Sender` 线程批量发送。

几个常见配置会影响 batch 怎么形成：

| 配置 | 影响 |
| --- | --- |
| `batch.size` | 单个 partition batch 的目标大小。消息积累到一定大小后更容易被发送。 |
| `linger.ms` | 为了等更多消息进入同一个 batch，producer 最多可以多等一小段时间。 |
| `buffer.memory` | producer 端用于缓冲待发送消息的总内存。 |
| `max.request.size` | 单个 Produce 请求允许的最大大小。 |
| `compression.type` | 是否压缩 batch，以及使用 `gzip`、`snappy`、`lz4`、`zstd` 还是 `none`。 |
| `acks` | producer 要等 broker 确认到什么程度才算发送完成。 |

合批可以这样理解：

```text
RecordAccumulator
├─ orders-2 deque
│  ├─ ProducerBatch A
│  │  ├─ Record(order-42)
│  │  ├─ Record(order-43)
│  │  └─ Record(order-44)
│  └─ ProducerBatch B
├─ orders-5 deque
│  └─ ProducerBatch C
└─ payments-0 deque
   └─ ProducerBatch D
```

`Sender` 线程发送请求时，会根据 metadata 找到每个 partition 的 leader broker，然后把发往同一个 broker 的多个 partition batch 合并进一个 `ProduceRequest`。

## 更完整的发送例子

假设应用短时间内发送了 6 条消息，分布在 2 个 topic、3 个 partition 上。

| 序号 | Topic | Partition | Key | Value | Headers | Timestamp |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `orders` | `2` | `order-42` | `{"status":"CREATED"}` | `traceId=abc` | `1720000000000` |
| 2 | `orders` | `2` | `order-43` | `{"status":"PAID"}` | `traceId=abc, source=app` | `1720000000500` |
| 3 | `orders` | `2` | `order-44` | `null` | `traceId=abc` | `1720000000900` |
| 4 | `orders` | `5` | `order-88` | `{"status":"CREATED"}` | `traceId=xyz` | `1720000001200` |
| 5 | `payments` | `0` | `pay-7` | `{"amount":99}` | `traceId=abc` | `1720000002000` |
| 6 | `payments` | `0` | `pay-8` | `{"amount":35}` | `traceId=def` | `1720000002300` |

Producer 会先按 `topic-partition` 分组。同一个 `topic-partition` 的多条消息可以进入同一个 `ProducerBatch`，然后构造成这个分区对应的 `MemoryRecords`。

发送到 broker 时大概长这样：

```text
ProduceRequest
├─ acks = 1
├─ timeoutMs = 30000
├─ transactionalId = null
└─ topicData
   ├─ topic = "orders"
   │  ├─ partitionData
   │  │  ├─ partition = 2
   │  │  └─ records = MemoryRecords
   │  │     └─ RecordBatch
   │  │        ├─ Record[0]: key="order-42", value="{\"status\":\"CREATED\"}"
   │  │        ├─ Record[1]: key="order-43", value="{\"status\":\"PAID\"}"
   │  │        └─ Record[2]: key="order-44", value=null
   │  └─ partitionData
   │     ├─ partition = 5
   │     └─ records = MemoryRecords
   │        └─ RecordBatch
   │           └─ Record[0]: key="order-88", value="{\"status\":\"CREATED\"}"
   └─ topic = "payments"
      └─ partitionData
         ├─ partition = 0
         └─ records = MemoryRecords
            └─ RecordBatch
               ├─ Record[0]: key="pay-7", value="{\"amount\":99}"
               └─ Record[1]: key="pay-8", value="{\"amount\":35}"
```

从网络请求的角度看，`ProduceRequest` 负责说明这些 `records` 属于哪个 topic 和 partition；真正的消息内容在每个 partition 的 `MemoryRecords` 里。

```text
ProduceRequest
└─ TopicProduceData[]
   ├─ "orders"
   │  ├─ PartitionProduceData(index=2, records=<orders-2 MemoryRecords>)
   │  └─ PartitionProduceData(index=5, records=<orders-5 MemoryRecords>)
   └─ "payments"
      └─ PartitionProduceData(index=0, records=<payments-0 MemoryRecords>)
```

## 一个 RecordBatch 内部怎么表示多条消息

下面用 `orders-2` 这个分区的 3 条消息画一个批次。为了说明布局，假设 broker 追加日志后，这个 batch 的 `BaseOffset = 120`；producer 发送时用 batch 内的 `OffsetDelta` 表示记录顺序，broker 写入日志时会确定最终日志 offset。示例里假设启用了幂等，所以 batch header 里有 `ProducerId`、`ProducerEpoch` 和 `BaseSequence`。

```text
orders-2 MemoryRecords
└─ RecordBatch
   ├─ Batch Header
   │  ├─ BaseOffset = 120
   │  ├─ LastOffsetDelta = 2
   │  ├─ BaseTimestamp = 1720000000000
   │  ├─ MaxTimestamp = 1720000000900
   │  ├─ RecordsCount = 3
   │  ├─ ProducerId = 9001
   │  ├─ ProducerEpoch = 3
   │  └─ BaseSequence = 15
   └─ Records
      ├─ Record[0]
      │  ├─ OffsetDelta = 0  => offset = 120 + 0 = 120
      │  ├─ TimestampDelta = 0  => timestamp = 1720000000000
      │  ├─ Key = "order-42"
      │  ├─ Value = "{\"status\":\"CREATED\"}"
      │  └─ Headers = {"traceId":"abc"}
      ├─ Record[1]
      │  ├─ OffsetDelta = 1  => offset = 120 + 1 = 121
      │  ├─ TimestampDelta = 500  => timestamp = 1720000000500
      │  ├─ Key = "order-43"
      │  ├─ Value = "{\"status\":\"PAID\"}"
      │  └─ Headers = {"traceId":"abc", "source":"app"}
      └─ Record[2]
         ├─ OffsetDelta = 2  => offset = 120 + 2 = 122
         ├─ TimestampDelta = 900  => timestamp = 1720000000900
         ├─ Key = "order-44"
         ├─ Value = null
         └─ Headers = {"traceId":"abc"}
```

对应关系可以压缩成一张表：

| Record | `OffsetDelta` | 还原后的 Offset | `TimestampDelta` | 还原后的 Timestamp | 说明 |
| --- | --- | --- | --- | --- | --- |
| `Record[0]` | `0` | `BaseOffset + 0 = 120` | `0` | `BaseTimestamp + 0 = 1720000000000` | 第一条订单消息 |
| `Record[1]` | `1` | `BaseOffset + 1 = 121` | `500` | `BaseTimestamp + 500 = 1720000000500` | 同 batch 第二条消息 |
| `Record[2]` | `2` | `BaseOffset + 2 = 122` | `900` | `BaseTimestamp + 900 = 1720000000900` | `value=null`，常用于 compacted topic 的 tombstone |

## 压缩前后对比

Kafka 的压缩发生在单个 `RecordBatch` 内。更准确地说，`RecordBatch` 的头部仍然按普通字段写入，压缩的是头部后面的 `Records` 区域。

未压缩时，`Records` 区域直接顺序保存每条 `Record` 的变长编码：

```text
orders-2 MemoryRecords
└─ RecordBatch
   ├─ Batch Header
   │  ├─ Attributes: compression = none
   │  ├─ BaseOffset = 120
   │  ├─ LastOffsetDelta = 2
   │  ├─ BaseTimestamp = 1720000000000
   │  ├─ MaxTimestamp = 1720000000900
   │  └─ RecordsCount = 3
   └─ Records
      ├─ Record[0] bytes
      │  ├─ Length
      │  ├─ Attributes
      │  ├─ TimestampDelta = 0
      │  ├─ OffsetDelta = 0
      │  ├─ KeyLength + Key
      │  ├─ ValueLength + Value
      │  └─ HeadersCount + Headers
      ├─ Record[1] bytes
      └─ Record[2] bytes
```

启用压缩后，`RecordBatch` 头部仍然在压缩数据外面，`Attributes` 里标记压缩算法，例如 `gzip`、`snappy`、`lz4` 或 `zstd`；`Records` 区域变成一段压缩后的 bytes。

```text
orders-2 MemoryRecords
└─ RecordBatch
   ├─ Batch Header
   │  ├─ Attributes: compression = zstd
   │  ├─ BaseOffset = 120
   │  ├─ LastOffsetDelta = 2
   │  ├─ BaseTimestamp = 1720000000000
   │  ├─ MaxTimestamp = 1720000000900
   │  └─ RecordsCount = 3
   └─ Records
      └─ CompressedBytes
         └─ zstd(
              Record[0] bytes
              Record[1] bytes
              Record[2] bytes
            )
```

解压后看到的内容仍然是同样的 `Record` 变长布局：

```text
CompressedBytes
└─ decompress
   ├─ Record[0]
   │  ├─ TimestampDelta = 0
   │  ├─ OffsetDelta = 0
   │  ├─ Key = "order-42"
   │  ├─ Value = "{\"status\":\"CREATED\"}"
   │  └─ Headers = {"traceId":"abc"}
   ├─ Record[1]
   │  ├─ TimestampDelta = 500
   │  ├─ OffsetDelta = 1
   │  ├─ Key = "order-43"
   │  ├─ Value = "{\"status\":\"PAID\"}"
   │  └─ Headers = {"traceId":"abc", "source":"app"}
   └─ Record[2]
      ├─ TimestampDelta = 900
      ├─ OffsetDelta = 2
      ├─ Key = "order-44"
      ├─ Value = null
      └─ Headers = {"traceId":"abc"}
```

压缩前后可以这样对比：

| 对比项 | 未压缩 | 压缩后 |
| --- | --- | --- |
| `ProduceRequest` 结构 | 不变 | 不变 |
| `TopicProduceData` / `PartitionProduceData` | 不变 | 不变 |
| `RecordBatch` 头部 | 明文保存 | 明文保存 |
| `Attributes` | `compression = none` | 标记具体压缩算法 |
| `Records` 区域 | 多条 `Record` 直接连续排列 | 一段压缩后的 bytes |
| 解压后内容 | 不需要解压 | 解压后仍是 `Record[0..n]` |
| `BaseOffset` / `OffsetDelta` 语义 | 不变 | 不变 |
| `BaseTimestamp` / `TimestampDelta` 语义 | 不变 | 不变 |

所以压缩不会改变 `ProduceRequest -> topic -> partition -> RecordBatch` 的外层组织方式，也不会改变单条 `Record` 的逻辑字段。它改变的是网络上传输和磁盘保存时 `Records` 区域的物理 bytes。压缩通常对多条消息更有意义，因为同一个 batch 内相似的 key、value、header 可以一起压缩。

## RecordBatch 布局

`RecordBatch` 布局，也就是 magic v2 之后的主要格式。

`RecordBatch` 头部固定 61 bytes。

| 字段 | 大小 |
| --- | --- |
| `BaseOffset` | `Int64` |
| `Length` | `Int32` |
| `PartitionLeaderEpoch` | `Int32` |
| `Magic` | `Int8` |
| `CRC` | `Uint32` |
| `Attributes` | `Int16` |
| `LastOffsetDelta` | `Int32` |
| `BaseTimestamp` | `Int64` |
| `MaxTimestamp` | `Int64` |
| `ProducerId` | `Int64` |
| `ProducerEpoch` | `Int16` |
| `BaseSequence` | `Int32` |
| `RecordsCount` | `Int32` |
| `Records` | `[Record]` |

按 byte offset 画出来：

```text
0         8    12   16 17    21 23   27       35       43       51 53   57   61
|---------|----|----|-|-----|--|----|--------|--------|--------|--|----|----|
 BaseOffset Len  Ep  M  CRC  Attr LastDelta BaseTs   MaxTs   ProdId PE Seq Count
                                                                            |
                                                                            v
                                                                      Records...
```

## Record 布局

单条 `Record` 的布局是变长的。

| 字段 | 类型 |
| --- | --- |
| `Length` | `Varint` |
| `Attributes` | `Int8` |
| `TimestampDelta` | `Varlong` |
| `OffsetDelta` | `Varint` |
| `KeyLength` | `Varint` |
| `Key` | `Bytes` |
| `ValueLength` | `Varint` |
| `Value` | `Bytes` |
| `HeadersCount` | `Varint` |
| `Headers` | `[Header]` |

## Header 布局

| 字段 | 类型 |
| --- | --- |
| `HeaderKeyLength` | `Varint` |
| `HeaderKey` | `UTF-8 String` |
| `HeaderValueLength` | `Varint` |
| `HeaderValue` | `Bytes` |

## Broker 收到后怎么处理

Broker 收到 `ProduceRequest` 后，会按 topic-partition 取出每个分区的 `records`，然后追加到对应分区 leader 的日志里。代码路径可以概括为：

```text
KafkaApis.handleProduceRequest()
└─ ReplicaManager.appendRecords()
   └─ ReplicaManager.appendRecordsToLeader()
      └─ Partition.appendRecordsToLeader()
         └─ UnifiedLog.appendAsLeader()
            └─ UnifiedLog.append()
               └─ 追加到 log segment / page cache
```

处理过程里会做几类事情：

| 阶段 | 主要动作 |
| --- | --- |
| 请求解析 | 从 `ProduceRequest` 里拿到 topic、partition、`MemoryRecords` |
| 权限和状态检查 | 检查 topic 是否存在、当前 broker 是否 leader、是否允许写入 |
| 记录校验 | 校验 record batch 格式、CRC、magic、压缩、幂等序列号等 |
| 日志追加 | 给 batch 分配最终 `BaseOffset`，追加到 leader 本地日志 |
| 副本复制 | follower 从 leader 拉取日志，进入 ISR 后参与高水位推进 |
| 生成响应 | 给每个 partition 生成 `ProduceResponse` 分区结果 |

这里有一个容易混淆的点：producer 发送的是 batch 结构，但最终日志 offset 是 broker append 时确定的。对于一个 batch：

```text
Producer 发送时:
Record[0].OffsetDelta = 0
Record[1].OffsetDelta = 1
Record[2].OffsetDelta = 2

Broker append 后:
Batch.BaseOffset = 120
Record[0].offset = 120 + 0 = 120
Record[1].offset = 120 + 1 = 121
Record[2].offset = 120 + 2 = 122
```

所以 producer 不直接决定最终日志 offset；producer 只把同一个 batch 内 record 的相对顺序编码进去。

## acks 和 ProduceResponse

`acks` 决定 producer 要等 broker 确认到什么程度。

| `acks` | 含义 | 响应特点 |
| --- | --- | --- |
| `0` | producer 不等 broker 响应 | 客户端发出请求后就认为完成，无法从响应里拿到 broker 写入结果 |
| `1` | leader 本地写入成功后响应 | leader append 成功即可返回 |
| `all` / `-1` | 等 ISR 副本满足要求后响应 | 可靠性更高，但延迟可能更高 |

普通成功响应大概长这样：

```text
ProduceResponse
└─ responses
   ├─ topic = "orders"
   │  ├─ partition = 2
   │  │  ├─ error = NONE
   │  │  ├─ baseOffset = 120
   │  │  ├─ logAppendTime = -1
   │  │  └─ recordErrors = []
   │  └─ partition = 5
   │     ├─ error = NONE
   │     ├─ baseOffset = 300
   │     └─ recordErrors = []
   └─ topic = "payments"
      └─ partition = 0
         ├─ error = NONE
         ├─ baseOffset = 81
         └─ recordErrors = []
```

如果某个 partition 失败，响应通常也是按 partition 返回错误；同一个 `ProduceRequest` 里可能一部分 partition 成功，另一部分失败。

```text
ProduceResponse
└─ responses
   └─ topic = "orders"
      ├─ partition = 2
      │  ├─ error = NONE
      │  └─ baseOffset = 120
      └─ partition = 5
         ├─ error = NOT_LEADER_OR_FOLLOWER
         └─ baseOffset = -1
```

Producer 根据响应决定是否完成 callback、是否刷新 metadata、是否重试、是否拆分 batch。

常见错误可以按下面理解：

| 错误 | 常见含义 | Producer 可能动作 |
| --- | --- | --- |
| `NOT_LEADER_OR_FOLLOWER` | metadata 过期，目标 broker 不是 leader | 刷新 metadata 后重试 |
| `NETWORK_EXCEPTION` | 网络连接异常 | 在重试窗口内重试 |
| `MESSAGE_TOO_LARGE` | 单条消息或 batch/request 太大 | 失败，或需要调小消息/调大限制 |
| `NOT_ENOUGH_REPLICAS` | `acks=all` 时 ISR 不满足要求 | 可重试或失败 |
| `OUT_OF_ORDER_SEQUENCE_NUMBER` | 幂等 producer 的 sequence 不符合 broker 预期 | 通常是严重顺序问题 |

## Request Header 和 Body 的关系

从 Kafka 协议层看，网络上发送的不是只有 `ProduceRequestData`，外面还有请求头。

```text
Request
├─ RequestHeader
│  ├─ ApiKey = Produce
│  ├─ ApiVersion
│  ├─ CorrelationId
│  └─ ClientId
└─ RequestBody
   └─ ProduceRequestData
      ├─ acks
      ├─ timeoutMs
      ├─ transactionalId
      └─ topicData[]
```

`CorrelationId` 用来把响应和请求对应起来；`ClientId` 用于标识客户端；`ApiVersion` 决定具体字段是否存在、如何编码。

## 重点理解

broker 收到的是协议请求 `ProduceRequest`；真正的业务消息在每个分区的 `records` 字段里，以 `RecordBatch + Record[]` 的二进制格式存在。一个 batch 可以包含多条同 `topic-partition` 的消息，压缩、幂等、事务、时间戳等信息主要体现在 batch 头部。

Producer 端负责序列化、分区、缓冲、合批和构造 `ProduceRequest`；broker 端负责校验、分配最终 offset、追加日志、复制并返回 `ProduceResponse`。网络上的 batch 和 broker 日志里的 batch 格式高度一致，区别在于 broker append 后会确定最终日志 offset 和追加结果。
