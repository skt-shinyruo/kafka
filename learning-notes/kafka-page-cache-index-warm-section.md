# Kafka 为什么这么依赖 Page Cache：从日志写入到索引 Warm Section

很多人第一次看 Kafka 的存储路径时，会自然地把它想象成这样：Broker
收到消息后，先放进 JVM 堆内的一套大缓存，再由后台线程慢慢刷盘。

Kafka 的核心路径不是这样。Kafka 更接近于把操作系统的 page cache
当成主要缓存层：消息追加到日志文件，索引通过 `mmap` 访问，消费时尽量从
page cache 直接送到 socket。这个选择不仅影响了日志读写路径，也影响了索引查找算法本身。

换句话说，Kafka 的日志设计有意保持简单：只追加写入、顺序读取。它不在 JVM
堆内维护一套完整的消息对象缓存，而是把空闲内存交给操作系统作为日志文件的读缓存。
这样既避免了 JVM 对象头和重复缓存带来的内存开销，也避免了为大量缓存对象付出额外的
垃圾回收成本。Kafka 设计文档所说的“立即写入持久化日志”表示写入文件系统，
不代表每次写入都立即 `fsync` 到物理磁盘。

本文用 Kafka 当前源码里的几个关键路径说明三件事：

1. Kafka 写入日志时并没有先维护一套大的堆内消息缓存。
2. 消费追得比较近时，读取通常命中 OS page cache，并可走零拷贝路径。
3. Kafka 的索引查找为了减少 page fault，专门引入了尾部 warm section 优化。

## 写入路径：MemoryRecords 直接追加到日志文件

Kafka 的设计文档在持久化章节里明确说明：数据会立即写入文件系统上的持久日志，
但不一定立刻 `fsync` 到物理磁盘；实际效果是数据先进入内核的 page cache。
参考 `docs/design/design.md` 中关于 pagecache-centric design 的说明。

从代码路径看，Broker 日志追加大致是：

```text
LocalLog.append(...)
  -> LogSegment.append(...)
    -> FileRecords.append(...)
      -> MemoryRecords.writeFullyTo(FileChannel)
```

对应源码位置：

```text
storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java
storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java
clients/src/main/java/org/apache/kafka/common/record/internal/FileRecords.java
```

`LocalLog.append` 负责把写入转交给当前 active segment，并更新 log end offset。
`LogSegment.append` 负责 segment 级别的组织工作：记录物理位置、追加 records、
按 `indexIntervalBytes` 维护 offset index 和 time index。真正把消息写入日志文件的是
`FileRecords.append`，它调用 `records.writeFullyTo(channel)`，目标是日志文件对应的
`FileChannel`。

这条路径的关键点是：Kafka 没有先把消息复制进一个长期存在的 JVM 堆内消息缓存，
再异步刷到文件。写入 `FileChannel` 之后，数据由操作系统接管，通常先落在 page cache，
之后由内核根据自身策略回写到磁盘。Kafka 的 `flush` 则会调用 `channel.force(true)`，
用于把已经写入的文件数据强制提交到物理存储。

这个设计有几个直接收益：

1. 避免 JVM 堆内大对象缓存带来的额外复制和 GC 压力。
2. 避免同一份数据同时存在于 JVM cache 和 OS page cache，减少重复缓存。
3. Broker 重启后，OS page cache 中仍可能保留热数据；进程内缓存则一定丢失。
4. 顺序追加更适合磁盘和文件系统，也更容易被内核做 write-behind 优化。

## 读取路径：Page Cache 加 Sendfile

Kafka 的读取路径同样围绕 page cache 设计。

`FileRecords.writeTo` 会调用目标传输通道的 `transferFrom(channel, position, count)`。
在明文网络层，`PlaintextTransportLayer.transferFrom` 最终调用的是：

```java
fileChannel.transferTo(position, count, socketChannel)
```

也就是 Java 对底层零拷贝能力的封装。在 Linux 上，这类路径通常对应
`sendfile` 风格的数据传输：文件数据已经在内核 page cache 中时，内核可以把它直接发送到
socket，避免先复制到用户态缓冲区，再从用户态复制回内核网络缓冲区。

数据路径可以简化为：

| 步骤 | 不使用零拷贝 | 使用 `sendfile` / `transferTo` |
| --- | --- | --- |
| 1 | 磁盘 → 页缓存（内核空间） | 磁盘 → 页缓存（内核空间） |
| 2 | 页缓存 → 用户缓冲区 | 跳过 |
| 3 | 用户缓冲区 → 套接字缓冲区 | 跳过 |
| 4 | 套接字缓冲区 → 网卡缓冲区 | 页缓存 → 网卡缓冲区 |

因此，应用层不需要先把文件内容读入用户空间再写回内核网络栈；在数据已经位于页缓存时，
主要工作变成内核中的高效转发。实际是否走完整零拷贝路径仍取决于传输协议和操作系统，
例如 TLS 加密通常需要在用户空间处理数据。

Kafka 设计文档在 `docs/design/design.md` 的效率章节也强调了这一点：Kafka 依赖现代
Unix 系统提供的 page cache 到 socket 的高效传输路径。对于追得比较紧的消费者，常见情况是：

```text
producer append -> log file -> page cache
consumer fetch  -> same page cache -> socket
```

如果消费者大多 caught up，读请求往往直接命中 page cache，而不是每次都从磁盘重新读取。
这就是 Kafka 能在持久化日志之上仍然维持高吞吐消费的重要原因。

需要注意的是，零拷贝路径和明文传输关系更直接。启用 SSL/TLS 时，Kafka 不能简单地把
page cache 中的数据直接交给 socket，因为加密处理发生在用户态库中，数据路径会不同。
设计文档中也明确说明 Kafka 当前不使用 in-kernel `SSL_sendfile`。

## 索引文件：看似内存访问，实际仍受 Page Cache 影响

Kafka 的日志段不只有 `.log` 数据文件，还有 offset index、time index 等索引文件。
这些索引文件同样是文件系统上的文件，但 Kafka 通过 `mmap` 把它们映射到内存地址空间。

`AbstractIndex` 中的注释说得很直接：Kafka 会把 index 文件 mmap 到内存中，index 的读写都通过
OS page cache 完成，这在多数情况下可以避免阻塞式磁盘 I/O。

这句话容易被误解。`mmap` 之后，Java 代码读 `MappedByteBuffer` 看起来像普通内存访问，
但它不是 JVM 堆内数组。如果访问的那一页索引文件不在 page cache 中，CPU 访问该虚拟地址时会触发
page fault，内核必须把对应文件页从磁盘读入内存，然后当前线程才能继续执行。

也就是说，索引查找虽然写起来像内存查找，但性能仍然取决于页面是否在 OS page cache 中。
这也是后面 warm section 优化的背景。

## 标准二分查找为什么不够 Cache Friendly

Offset index 和 time index 都是有序结构。直觉上，用二分查找就可以了，复杂度是
`O(log n)`。但这里的问题不在比较次数，而在访问了哪些文件页。

标准二分查找会从整个索引区间的中点开始跳跃式访问。假设一个索引已经占用 13 个 page，
目标 entry 在最后一页，也就是 page 12。标准二分可能访问这些 page：

```text
page number: |0|1|2|3|4|5|6|7|8|9|10|11|12|
steps:       |1| | | | | |3| | |4|  |5 |2/6|
```

虽然目标在尾部，但查找过程仍然碰到了 page 0、6、9、11、12。

Kafka 的典型访问模式很特殊：

1. 写入总是追加到索引尾部。
2. in-sync follower 的复制请求通常靠近 log end。
3. 大多数在线消费者如果没有明显落后，也会读接近 log end 的位置。

因此，从访问局部性看，索引尾部才是真正的热区。但标准二分查找会稳定访问一些历史中间页。
更麻烦的是，当索引增长到新 page 时，二分路径里的中间页集合会变化。

例如索引从 13 页增长到 14 页后，查找尾部 entry 可能变成访问：

```text
page number: |0|1|2|3|4|5|6|7|8|9|10|11|12|13|
steps:       |1| | | | | | |3| | | 4|5 | 6|2/7|
```

这时 page 7 和 page 10 可能很久没被访问过。操作系统的 page cache 通常基于 LRU 或近似 LRU
策略管理内存，这些长期未被访问的页更可能已经被淘汰。索引刚进入新 page 后的第一次尾部查找，
就可能因为访问 page 7、10 而触发 page fault。

Kafka 源码注释里提到，这种 page fault 在测试中会把 at-least-once produce latency
从几毫秒拉高到约一秒。这不是因为二分查找比较次数太多，而是因为一次冷 page fault
可能把请求线程阻塞在磁盘 I/O 上。

## Warm Section：先判断目标是否在索引尾部

Kafka 的改法很直接：不要一上来就在整个索引文件上做二分。先把索引尾部最近的 N 个 entry
看作 warm section。如果目标落在这段尾部热区，就只在 warm section 内二分；否则才去旧区间查找。

源码里的伪代码是：

```text
if (target > indexEntry[end - N])
    binarySearch(end - N, end)
else
    binarySearch(begin, end - N)
```

实际实现位于 `AbstractIndex.indexSlotRangeFor`：

```java
int firstHotEntry = Math.max(0, entries - 1 - warmEntries());
if (compareIndexEntry(parseEntry(idx, firstHotEntry), target, searchEntity) < 0) {
    return binarySearch(idx, target, searchEntity, searchResultType, firstHotEntry, entries - 1);
}

return binarySearch(idx, target, searchEntity, searchResultType, 0, firstHotEntry);
```

这里的 `firstHotEntry` 就是 warm section 的起点。对于追得很近的 follower 或 consumer，
目标 offset 大概率大于这个 entry，于是查找被限制在索引尾部的小范围内。

这不是改变索引的数据结构，也不是引入复杂缓存，而是改变查找范围，让常见路径更符合
Kafka 的访问模式。

## 为什么 N 选择 8192 字节

`AbstractIndex.warmEntries()` 的实现是：

```java
protected final int warmEntries() {
    return 8192 / entrySize();
}
```

这里的 8192 不是 8192 个 entry，而是约 8KB 的索引空间。

Offset index 的 entry size 是 8 字节，所以 warm section 大约是 1024 个 entry。
Time index 的 entry size 是 12 字节，所以 warm section 大约是 682 个 entry。

Kafka 选择这个大小是为了平衡两个目标。

第一，warm section 要足够小。典型系统 page size 至少是 4KB，8KB 左右的索引热区通常只覆盖少数几个
page。一次 warm-section 二分会稳定触碰尾部、热区起点和中间位置等关键 entry，这样这些 page
会在频繁查询中持续保持“最近使用”，不容易被 LRU 淘汰。

第二，warm section 又要足够大。按照 Kafka 默认索引间隔，8KB 的索引大致对应几 MB 的日志数据。
源码注释中给出的量级是：offset index 约 4MB 日志消息，time index 约 2.7MB 日志消息。
这足以覆盖大多数 follower 和在线 consumer 的追尾读取场景。

如果 N 设得更大，理论上可以覆盖更大的尾部范围，但会带来新问题：warm section 跨越更多 page 后，
一次查找未必能稳定触碰所有 page，也就不能保证整个热区真的保持 warm。

## 这个优化到底减少了什么

Warm section 优化减少的不是算法复杂度。标准二分和 warm-section 二分本质上都是二分。
它减少的是冷文件页访问，也就是降低 `mmap` 索引查找触发 major page fault 的概率。

可以把二者的差别理解为：

```text
标准二分：
  每次查尾部，也可能跳到历史中间页。

Warm section 二分：
  常见的追尾查找只在最近追加的少数 page 内跳转。
```

对于 Kafka 这种 append-only、读多靠近尾部的系统，后者更贴合真实工作负载。它让 OS page cache
的 LRU 策略更容易发挥作用：频繁访问的尾部索引页持续保持热状态，而不是被标准二分带到一批
偶尔才访问的历史中间页上。

这也是 Kafka 工程设计里一个很有代表性的点：它没有只停留在“使用 page cache”这个抽象层面，
而是把 page cache 的行为继续下沉到索引查找算法里考虑。

## 小结

Kafka 的高吞吐不是来自单一技巧，而是来自一组互相配合的工程选择：

1. 日志写入走顺序追加，数据进入文件系统和 OS page cache。
2. 消费读取尽量复用 page cache，并在明文网络路径上使用零拷贝传输。
3. 索引文件通过 `mmap` 访问，避免维护大型 JVM 堆内索引对象。
4. 索引查找针对 Kafka 的“尾部热”访问模式做 warm section 优化，减少冷页访问和 page fault。

所以，Kafka 的 page cache 不是“顺手用了操作系统缓存”这么简单。它是 Kafka 存储、网络和索引路径共同围绕的核心设计假设。
理解这一点后，再看 Kafka 为什么不维护一套大的 JVM 堆内消息缓存、为什么强调顺序 I/O、
为什么索引查找要避免不必要 page fault，就会更连贯。

## 关键源码位置

```text
docs/design/design.md
storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java
storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java
storage/src/main/java/org/apache/kafka/storage/internals/log/AbstractIndex.java
storage/src/main/java/org/apache/kafka/storage/internals/log/OffsetIndex.java
storage/src/main/java/org/apache/kafka/storage/internals/log/TimeIndex.java
clients/src/main/java/org/apache/kafka/common/record/internal/FileRecords.java
clients/src/main/java/org/apache/kafka/common/network/PlaintextTransportLayer.java
```
