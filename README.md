## platodb
platodb 是一款高性能的基于 LSM-Tree 的键值数据库，采用模块化架构设计，支持快速的读写性能和崩溃恢复能力。适用于高吞吐量和低延迟需求的存储场景。

## 架构

```
+---------------------+
|   Network Server    | (处理客户端请求)
+---------------------+
           |
           ▼
+---------------------+
|  Command Processor  | (解析命令)
+---------------------+
           |
           ▼
+-------------------------------------------------------+
|                         DB                            |
|  +---------------------+   +------------------------+ |
|  |  Memory Table       |   |  Write-Ahead Log (WAL) | |
|  |  (可变内存存储)      |   |  (崩溃恢复)             | |
|  +---------------------+   +------------------------+ |
|                 |                         |           |
|                 ▼                         ▼           |
|  +---------------------+   +------------------------+ |
|  |  Flusher (持久化)    |   |        SSTable         | |
|  +---------------------+   | (持久化块存储)          | |
|                            +------------------------+ |
+-------------------------------------------------------+
```
- 网络服务器：负责接收客户端请求，通过 TCP 协议与客户端通信，解析请求后交由命令处理器处理。
- 命令处理器：负责解析客户端发送的命令，如 Get、Set、Delete，并将其映射为具体的数据库操作。
- 数据库引擎 (DB)
  - MemoryTable：活跃的数据存储，所有写入操作优先存储到内存表中。当内存表达到一定阈值时，会触发持久化。
  - WAL：在写入内存表前记录日志，确保在系统崩溃后可以恢复未持久化的数据。
  - SSTable：磁盘上的持久化存储结构，采用分段存储，支持二分查找、布隆过滤器加速查询，以及块缓存和快照机制提升读取性能。

## 数据格式

```aiignore

# sstable 分段
┌────────────┐      ┌────────────┐
│ 000001.seg │      │ 000002.seg │
└────────────┘      └────────────┘ 

# segment文件分块
┌──────────┬──────────┬───────────┬────────────┐
│ block-1  │ block-2  |  block-3  │  block-... │
└──────────┴──────────┴───────────┴────────────┘

# block块内包含多个记录
┌──────────┬──────────┬───────────┬────────────┐
│ chunk-1  │ chunk-2  |  chunk-3  │  chunk-... │
└──────────┴──────────┴───────────┴────────────┘

# 正常记录的字节格式
┌──────────┬──────┬─────────┬──────────┬─────────┬────────┐
│ tombstone│ crc  | KeyLen  │   Key    │ ValueLen│  Value │
└──────────┴──────┴─────────┴──────────┴─────────┴────────┘

# 已删除记录的字节格式
┌───────────┬───────┬─────────┬─────────┐
│ tombstone |  crc  │ KeyLen  │   Key   │
└───────────┴───────┴─────────┴─────────┘
```

## 性能表现

### 写

```
goos: windows
goarch: amd64
cpu: Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz
BenchmarkSet-16    	  224626	      5283 ns/op	    1204 B/op	      14 allocs/op
```

### 读

```
goos: windows
goarch: amd64
cpu: Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz
BenchmarkGet-16    	 1023334	      1083 ns/op	       0 B/op	       0 allocs/op
```