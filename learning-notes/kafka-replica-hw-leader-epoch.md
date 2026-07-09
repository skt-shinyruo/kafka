# Kafka 副本复制中的 HW、LEO 和 Leader Epoch

Kafka 分区副本复制里，`HW`、`LEO` 和 `Leader Epoch` 解决的是不同问题。

先记住一句话：

```text
HW 是提交水位，不是日志一致性证明。
Leader Epoch 是日志历史，用来判断副本恢复时是否发生分叉。
```

更具体地说：

| 概念 | 全称 | 作用 |
| --- | --- | --- |
| `LEO` | Log End Offset | 当前副本下一条消息要写入的位置 |
| `HW` | High Watermark | 已提交消息的边界，消费者只能读取 `offset < HW` 的消息 |
| `Leader Epoch` | Leader 任期 | 标记每一任 Leader 从哪个 offset 开始写日志 |

这三者的分工是：

```text
正常复制时：
  Leader 根据 ISR 中各副本复制进度推进 HW。

消费者读取时：
  只能读取 offset < HW 的消息。

副本恢复时：
  通过 Leader Epoch 判断日志是否和当前 Leader 分叉，以及需要截断到哪里。
```

旧机制的问题是：副本恢复时曾经直接按本地 HW 截断日志。但 Follower 本地 HW
可能落后于它已经写入磁盘的日志，所以会把已经复制成功、且和 Leader 一致的消息误删。

## Offset 边界语义

Kafka 里的很多 offset 都是“下一条位置”。

如果日志里有：

```text
offset 0
offset 1
```

那么：

```text
LEO = 2
```

表示下一条消息会写到 offset `2`。

如果：

```text
HW = 1
```

表示：

```text
offset < 1 的消息已经提交
```

也就是只有 offset `0` 已提交。offset `1` 即使已经存在于本地日志，也还不能被消费者读取。

## 初始状态

假设一个分区有两个副本：

```text
副本 A：Leader
副本 B：Follower
```

一开始它们都只有一条消息：

```text
offset 0
```

状态是：

```text
A: LEO = 1, HW = 1
B: LEO = 1, HW = 1
```

含义是：

```text
LEO = 1：下一条消息写到 offset 1
HW  = 1：offset 0 已提交
```

## Producer 写入 offset 1

现在生产者发送一条新消息，Leader A 写入 offset `1`。

A 的日志变成：

```text
A: offset 0, offset 1
A.LEO = 2
A.HW  = 1
```

A 的 HW 还不能马上变成 `2`，因为此时只有 Leader A 自己写入了 offset `1`。
Leader 还不知道 Follower B 有没有复制成功。

Leader 推进 HW 时要看 ISR 中所有副本的复制进度：

```text
HW = min(Leader LEO, Follower LEO...)
```

此刻 Leader 视角里：

```text
A.LEO = 2
B.LEO = 1
```

所以：

```text
A.HW = min(2, 1) = 1
```

## B 第一次 Fetch：拉取 offset 1

B 向 A 发送 FetchRequest：

```text
FetchRequest(fetchOffset = 1)
```

这个请求的含义是：

```text
B 已经有 offset < 1 的数据。
请从 offset 1 开始给我。
```

Leader A 收到这个请求时，只能确认：

```text
B 已经复制到了 offset 0
```

也就是：

```text
Leader 眼中的 B 复制进度 = 1
```

所以此时 A 的 HW 仍然是：

```text
A.HW = 1
```

A 返回：

```text
FetchResponse:
  records  = offset 1
  leaderHW = 1
```

这里非常关键：

```text
A 返回给 B 的 leaderHW 还是 1。
```

原因是：A 不能在发送响应时就假设 B 一定已经成功写入 offset `1`。
响应可能还没到 B，B 可能还没落盘，B 也可能马上宕机。

## B 写入 offset 1，但本地 HW 仍然是 1

B 收到 FetchResponse 后，把 offset `1` 写入自己的日志。

B 的日志变成：

```text
B: offset 0, offset 1
B.LEO = 2
```

但 B 的本地 HW 怎么更新？

Follower 的本地 HW 来自 Leader 在 FetchResponse 中带回来的 `leaderHW`。
刚才 A 返回的是：

```text
leaderHW = 1
```

所以 B 处理这次响应后仍然是：

```text
B.HW = 1
```

于是出现一个正常的中间状态：

```text
A: offset 0, 1    LEO = 2, HW = 1
B: offset 0, 1    LEO = 2, HW = 1
```

重点是：

```text
B 已经写入了 offset 1。
B 的 LEO 已经是 2。
B 的本地 HW 仍然是 1。
```

这不是异常，而是复制协议里的时间差。

## B 第二次 Fetch：告诉 A 自己已经到 offset 2

B 写完 offset `1` 后，会继续向 A 拉取数据，于是发送下一次 FetchRequest：

```text
FetchRequest(fetchOffset = 2)
```

这个请求的含义是：

```text
B 已经拥有 offset < 2 的数据。
```

换句话说，B 通过这个请求告诉 Leader A：

```text
offset 1 我已经复制成功了。
```

A 收到这个请求后，才更新自己眼中 B 的复制进度：

```text
B.LEO = 2
```

此时 Leader A 看到：

```text
A.LEO = 2
B.LEO = 2
```

所以 A 可以推进 HW：

```text
A.HW = min(2, 2) = 2
```

然后 A 返回给 B：

```text
FetchResponse:
  records  = 空
  leaderHW = 2
```

如果 B 正常收到并处理这个响应，B 才会把自己的本地 HW 更新成：

```text
B.HW = 2
```

## Follower HW 滞后的危险窗口

危险窗口出现在这里：

```text
B 已经写入 offset 1
B 已经发送 FetchRequest(fetchOffset = 2)
A 可能已经把 HW 推进到 2
B 还没收到或还没处理 leaderHW = 2 的 FetchResponse
```

这时可能出现：

```text
A: offset 0, 1    LEO = 2, HW = 2
B: offset 0, 1    LEO = 2, HW = 1
```

也就是说：

```text
Leader A 的 HW 已经更新到 2。
Follower B 的日志也已经有 offset 1。
但 Follower B 本地记录的 HW 仍然是 1。
```

这就是“Follower 端 HW 更新滞后”的完整原因。

## 旧机制为什么会误删数据

假设 B 在这个窗口里宕机。

B 重启后，旧机制会做：

```text
truncateTo(localHW)
```

B 本地 HW 是：

```text
B.HW = 1
```

所以执行：

```text
truncateTo(1)
```

这里容易误解。`truncateTo(1)` 不是“保留 offset 1”，而是：

```text
保留 offset < 1
删除 offset >= 1
```

所以 B 从：

```text
offset 0, offset 1
LEO = 2
```

变成：

```text
offset 0
LEO = 1
```

offset `1` 被删掉了。

但 offset `1` 原本已经复制到了 B，只是 B 的本地 HW 落后。因此这是误删。

## 误删为什么会变成真正的数据丢失

如果 A 一直活着，B 删除 offset `1` 后还可以重新从 A 拉取：

```text
FetchRequest(fetchOffset = 1)
```

A 仍然有 offset `1`，可以再发给 B。

真正危险的是连续故障：

```text
1. B 因为本地 HW=1，把 offset 1 截掉
2. B 还没来得及从 A 重新拉回 offset 1
3. A 宕机
4. Kafka 只能让 B 成为新 Leader
```

此时 B 的日志是：

```text
B: offset 0
B.LEO = 1
```

B 成为新 Leader 后，集群的权威日志就变成 B 的日志。

等 A 恢复后，A 需要向新 Leader B 对齐。A 原本有：

```text
A: offset 0, offset 1
```

但新 Leader B 只有：

```text
B: offset 0
```

于是 A 也要截断：

```text
A 删除 offset 1
```

最终：

```text
A 没有 offset 1
B 没有 offset 1
```

这条消息永久丢失。

完整链条是：

```text
B 已复制 offset 1
B 本地 HW 仍是 1
B 重启后按 HW 截断
B 删除 offset 1
A 在 B 补回 offset 1 前宕机
B 成为新 Leader
A 恢复后向 B 对齐，也删除 offset 1
```

## Leader Epoch 机制如何改变恢复逻辑

Leader Epoch 不是用来替代 HW 判断提交状态的。

它解决的是：

```text
副本恢复时，到底该不该截断日志？
如果要截断，截断到哪里？
```

每一任 Leader 都有一个 epoch。

开始时：

```text
A 是 Leader
Leader Epoch = 0
```

Kafka 记录：

```text
[epoch = 0, startOffset = 0]
```

含义是：

```text
从 offset 0 开始，这段日志属于 epoch 0 这任 Leader。
```

此时 A 和 B 都有：

```text
offset 0, offset 1
LEO = 2
```

它们的日志历史是：

```text
offset: 0  1
epoch:  0  0
```

B 重启后，新机制不会直接按本地 HW 截断。

B 会先问当前 Leader A：

```text
我本地最后的 epoch 是 0。
请问 epoch 0 在你这里结束到哪个 offset？
```

A 当前仍然是 epoch `0` 的 Leader，并且 A 的 LEO 是 `2`。

所以 A 回答：

```text
epoch 0 endOffset = 2
```

B 拿到结果后比较：

```text
Leader A 说：epoch 0 到 offset 2 结束
B 自己：LEO 也是 2
```

这说明：

```text
B 的日志没有超过 Leader。
B 的日志也没有和 Leader 分叉。
B 不需要截断。
```

所以 B 保留：

```text
offset 0, offset 1
```

即使此时：

```text
B.HW = 1
```

B 也不会因为 HW 落后而删除 offset `1`。

这就是 Leader Epoch 解决问题的关键：

```text
恢复时不再把本地 HW 当成日志一致性的依据。
恢复时先和当前 Leader 比较日志历史。
```

## Leader Epoch 什么时候会要求截断

再看一个真的需要截断的场景。

A 是旧 Leader，epoch `0`，它写了：

```text
offset 0, 1, 2
```

但 offset `2` 没有复制到 B。随后 A 宕机，B 成为新 Leader，epoch 变成 `1`。

B 从 offset `2` 开始写入新的消息：

```text
B:
offset 0, 1 属于 epoch 0
offset 2    属于 epoch 1
```

B 的 epoch 缓存类似：

```text
[epoch = 0, startOffset = 0]
[epoch = 1, startOffset = 2]
```

现在 A 恢复了。

A 本地有：

```text
offset 0, 1, 2
```

但 A 的 offset `2` 是旧 Leader A 在 epoch `0` 写的。
当前 Leader B 的 offset `2` 是新 Leader B 在 epoch `1` 写的。

也就是说：

```text
A 的 offset 2 和 B 的 offset 2 不是同一条消息。
日志发生了分叉。
```

A 会问 B：

```text
epoch 0 在你这里结束到哪里？
```

B 回答：

```text
epoch 0 endOffset = 2
```

于是 A 必须截断到 offset `2`：

```text
保留 offset < 2
删除 offset >= 2
```

也就是：

```text
保留 offset 0, 1
删除自己的 offset 2
```

然后 A 再从 B 拉取新的 offset `2`。

这个截断是正确的，因为 A 的 offset `2` 确实和当前 Leader B 的日志冲突。

## HW 和 Leader Epoch 的最终分工

把两者放在一起看：

```text
HW 负责提交语义：
  消费者最多能读到哪里？
  哪些消息算 committed？

Leader Epoch 负责恢复语义：
  副本重启后，日志有没有和当前 Leader 分叉？
  要不要截断？
  截断到哪里？
```

旧机制的问题是：

```text
用 HW 去做恢复截断判断。
```

但 HW 只适合表示提交进度，不适合判断日志历史是否分叉。

因为 Follower 的 HW 可能落后于它的 LEO：

```text
B.LEO = 2
B.HW  = 1
```

这时 B 的 offset `1` 明明存在，而且和 Leader 一致，但 HW 看起来却说它还没有提交。
按 HW 截断就会误删。

Leader Epoch 的改进是：

```text
恢复时不看本地 HW 是否落后。
恢复时问当前 Leader：我这段 epoch 的日志在你那里结束到哪里？
```

如果本地日志没有超过 Leader 返回的 epoch end offset，就不截断。

如果本地日志超过了，说明有多余日志或分叉日志，才截断。

## 源码锚点

当前源码中可以关注这些位置：

```text
storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java
  - maybeIncrementHighWatermark
    HW 只能单调推进，并且不能超过本地 LEO。

storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java
  - endOffsetFor
    根据请求的 leader epoch 返回该 epoch 在 leader 视角下的结束 offset。

core/src/main/scala/kafka/server/AbstractFetcherThread.scala
  - truncateToHighWatermark
    epoch 信息不可用时，才退回按本地 high watermark 截断。

  - maybeTruncateToEpochEndOffsets
    根据 leader 返回的 epoch end offset 决定是否截断。

  - getOffsetTruncationState
    计算实际截断 offset。正常有 leader epoch 信息时，会基于 epoch end offset
    和本地 log end offset 做判断；leader epoch 信息不可用时才使用 high watermark。
```

## 总结

整个机制可以压缩成三句话：

```text
HW 是提交水位，控制消费者能读到哪里。

Follower 的本地 HW 可能落后于它已经写入磁盘的 LEO，
所以恢复时只按本地 HW 截断可能误删数据。

Leader Epoch 给日志加上 Leader 任期历史，
恢复时按日志历史对齐当前 Leader，只有真正分叉的部分才会被截断。
```
