# Kafka Producer 幂等性

## 一句话定义

Kafka 的幂等通常指 **幂等生产者（idempotent producer）**：生产者因网络超时、
broker 切换等原因重试同一个发送请求时，Kafka 只会把这次发送写入分区日志一次。

它解决的是 producer 重试引入的重复消息问题，不等于整个业务系统自动具备
"exactly once"（恰好一次）语义。

## 它要解决什么问题

考虑一次发送的典型失败窗口：

```text
Producer                         Broker
   |  ProduceRequest(batch A)      |
   | ----------------------------> |
   |                               |  已将 A 追加到日志
   |        ProduceResponse        |
   | <---------------------------- |  响应在网络中丢失
   |                               |
   |  重试 ProduceRequest(batch A) |
   | ----------------------------> |
```

没有幂等时，broker 无法区分第二个请求是重试还是一条新消息，可能把 `batch A`
再次追加。开启幂等后，broker 能识别其为已成功写入的 batch，返回原结果而不重复追加。

因此，幂等将 producer 的发送语义从“重试时可能重复”的 at-least-once，收紧为
**一个 producer 会话内对 Kafka 分区的 exactly-once delivery**。

## 核心机制：PID、epoch 和 sequence

每个幂等 producer 会使用一个 `Producer ID`（PID）。对于它写入的每个
`topic-partition`，producer 还维护一条独立、单调递增的序列号流。

```text
producer PID = 9001, epoch = 3

orders-0: batch 1 [baseSequence=0, lastSequence=2]
          batch 2 [baseSequence=3, lastSequence=5]

orders-1: batch 1 [baseSequence=0, lastSequence=1]
```

一个 `RecordBatch` 的 header 会携带：

| 字段 | 作用 |
| --- | --- |
| `ProducerId` | 标识生产者身份。 |
| `ProducerEpoch` | 标识该 PID 的代次，隔离已经失效的旧 producer 实例。 |
| `BaseSequence` | 该 batch 第一条 record 的序列号。 |
| `LastSequence` | 由 `BaseSequence + recordCount - 1` 得出，用于标识 batch 覆盖的序列号范围。 |

broker 会按 producer 和分区保存 producer 状态，并保留最近 batch 的序列号范围和
offset 元数据。收到请求时，broker 会验证 epoch 和序列号：

- 与已写入 batch 的 epoch、序列号范围完全相同：认定为重试，返回先前结果，不重复追加。
- 序列号正好承接已确认的序列号：正常追加。
- 序列号跳跃或顺序不合法：以 `OutOfOrderSequence` 一类错误拒绝，避免消息乱序。
- epoch 过期：拒绝旧 producer，避免旧实例在网络恢复后继续写入。

例如 `orders-0` 的第一个 batch 已被写入，但响应丢失：

```text
第一次发送: PID=9001, epoch=3, baseSequence=0, lastSequence=2
  -> broker 追加到 orders-0，offset 为 120..122
  -> Producer 没有收到响应

重试发送:   PID=9001, epoch=3, baseSequence=0, lastSequence=2
  -> broker 查到同一序列号范围的已写入 batch
  -> 不再追加，返回原 batch 的结果
```

序列号是按 **topic-partition** 分开的，所以不同分区可以并行发送；它不提供跨分区的
原子性。

## 如何开启和配置

显式配置最清晰：

```java
Properties props = new Properties();
props.put("enable.idempotence", "true");
props.put("acks", "all");
props.put("retries", Integer.toString(Integer.MAX_VALUE));
props.put("max.in.flight.requests.per.connection", "5");
```

幂等 producer 的约束如下：

| 配置 | 要求 | 原因 |
| --- | --- | --- |
| `enable.idempotence` | `true` | 启用 PID、epoch 和序列号校验。 |
| `acks` | `all`（等价于 `-1`） | leader 必须等待 ISR 确认，才能安全确认该 batch。 |
| `retries` | 大于 `0` | 幂等的价值在于允许安全重试。 |
| `max.in.flight.requests.per.connection` | 不大于 `5` | broker 只保留有限数量的最近 producer batch 状态；该上限同时保证重试情况下的顺序。 |

当前源码的 `ProducerConfig` 中，`enable.idempotence` 默认值是 `true`；从 Kafka 3.0
开始，未显式覆盖时 `acks` 默认是 `all`，`retries` 默认是 `Integer.MAX_VALUE`。生产配置
仍建议显式写出幂等意图，并避免把这些依赖项改成冲突的值。

若显式设置 `enable.idempotence=true`，但同时设置 `acks=1`、`retries=0` 或
`max.in.flight.requests.per.connection>5`，客户端会因配置不兼容而失败。若没有显式开启
幂等，却给出不兼容的 `acks` 或 `retries`，客户端可能改为禁用幂等；因此不要依赖这种
隐式降级行为。

## 幂等保证的边界

幂等生产者保证的是“同一 producer 的同一次发送因重试被执行一次”，其范围有明确边界：

- 只处理 Kafka client 自动重试产生的重复发送。
- 只在单个 producer 会话内生效；普通幂等 producer 重启后不保留跨会话的业务去重语义。
- 保证以分区为单位，不保证多个 topic 或多个 partition 一起成功或一起失败。
- 不会识别应用主动执行两次 `send()` 的业务重复。两次调用会得到不同的序列号，Kafka 会把它们当作两条合法记录。
- 若 `send()` 最终报错，应用不能在不知道原消息是否已写入的情况下盲目重发；新调用不是原请求的自动重试，可能产生业务重复。
- 它不影响消费者“处理完成后再提交 offset”带来的重复消费，也不覆盖数据库、HTTP、邮件、支付等外部副作用。

因此，业务上仍应把可去重的业务键、数据库唯一约束、inbox/outbox 或去重表作为边界保护。

## 幂等与 Kafka 事务

Kafka 事务建立在幂等 producer 之上，但提供更大的原子范围：

| 能力 | 幂等生产者 | Kafka 事务 |
| --- | --- | --- |
| 消除 producer 自动重试的重复写入 | 是 | 是 |
| 原子写入多个 topic / partition | 否 | 是 |
| 原子提交“输出消息 + 已消费 offset” | 否 | 是 |
| 跨 producer 重启恢复同一个处理单元 | 否 | 通过稳定的 `transactional.id` 支持 |
| 覆盖外部数据库或 HTTP 调用 | 否 | 否 |

当应用需要“消费 Kafka -> 处理 -> 再写 Kafka”的端到端 exactly-once processing 时，
应该使用事务：

```java
props.put("transactional.id", "orders-processor-0");

producer.initTransactions();
producer.beginTransaction();
producer.send(outputRecord);
producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
producer.commitTransaction();
```

设置 `transactional.id` 会隐式开启幂等。`transactional.id` 应稳定且对运行中的
producer 实例唯一；新实例以相同 ID 启动时，会通过 epoch 隔离旧实例并完成或中止遗留事务。

要让下游只看到提交成功的事务消息，消费者还需要：

```properties
isolation.level=read_committed
```

`read_committed` 消费者只读取已提交事务中的消息，并跳过已中止事务的数据；它仍会读取
非事务消息。

## 选择方式

| 场景 | 推荐方式 |
| --- | --- |
| 只需避免 producer 网络重试造成的重复 | 幂等 producer。 |
| 需要向多个 Kafka 分区或 topic 原子写入 | Kafka 事务。 |
| 需要原子地提交输出消息和消费 offset | Kafka 事务 + `read_committed` 消费者。 |
| 处理结果会写数据库、调用 HTTP 或产生其他外部副作用 | Kafka 幂等/事务之外，再实现业务幂等。 |

## 当前源码中的对应位置

- `clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java`：定义并校验
  `enable.idempotence`、`acks`、`retries` 与 `max.in.flight.requests.per.connection` 的约束。
- `clients/src/main/java/org/apache/kafka/clients/producer/internals/TxnPartitionEntry.java`：维护每个
  partition 的 PID、epoch、下一个 sequence 以及 in-flight batch。
- `storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateEntry.java`：保存 broker
  端 producer 状态，并通过序列号范围查找重复 batch。
- `storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerAppendInfo.java`：在追加记录时
  校验 producer epoch 和 sequence，并更新 producer 状态。
- `clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java`：说明幂等 producer 与
  transactional producer 的对外语义和 API。

## 小结

幂等 producer 的本质是：用 `(PID, epoch, partition sequence)` 给每次 producer 发送建立可验证
的身份。这样同一个 batch 的重试不会在日志中留下第二份副本。它是可靠生产的基础能力，但
不是业务层去重，也不是跨 Kafka 与外部系统的通用 exactly-once 方案。
