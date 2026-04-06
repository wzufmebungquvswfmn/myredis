# myredis

myredis 是一个基于 Rust 实现的 Redis 风格内存数据库项目，支持 RESP 协议、多数据结构命令和并发访问。

服务默认监听在 `127.0.0.1:6380`，可通过 `redis-cli` 进行功能验证，并通过 `redis-benchmark` 进行性能测试。

## 1. 项目简介

项目实现围绕三个目标展开：功能正确性、并发可用性和可复现的测试验证流程。

- 协议层：支持 RESP 帧解析与编码，覆盖半包、多命令连续解析、错误输入处理。
- 命令层：实现 String / Hash / List 三类常用命令，并补齐键空间辅助命令。
- 存储层：基于分片 `DashMap` 提供并发读写，支持 TTL 过期语义。
- 工程层：提供可复现的测试与 benchmark 脚本，日志结构统一，便于回归对比。

### 1.1 已实现功能

支持命令（按类别）：

- 连接与探活：`PING`
- String：`SET` `GET` `INCR`
- Key 空间：`DEL` `EXISTS` `TYPE` `DBSIZE` `FLUSHALL`
- 过期控制：`EXPIRE` `TTL`
- Hash：`HSET` `HGET` `HDEL` `HLEN`
- List：`LPUSH` `RPUSH` `LPOP` `RPOP` `LLEN`

兼容行为要点：

- 参数数量错误返回 `ERR wrong number of arguments ...`
- 类型错误返回 `WRONGTYPE`
- 未实现命令返回 `ERR unknown command 'xxx'`
- `TTL` 语义对齐 Redis：
	- `-2`：key 不存在（含已过期）
	- `-1`：key 存在但未设置过期

## 2. 使用教程

### 2.1 环境要求

- Rust（建议 stable）
- 可选：`redis-cli`（手工验证）
- 可选：`redis-server`, `redis-benchmark`（性能压测与对比）

### 2.2 启动服务

在项目根目录执行：

```bash
cargo run
```

服务启动后监听 `127.0.0.1:6380`。

### 2.3 快速验证

使用 `redis-cli` 连接：

```bash
redis-cli -p 6380
```

示例：

```text
127.0.0.1:6380> SET k v
OK
127.0.0.1:6380> GET k
"v"
127.0.0.1:6380> HSET user:1 name alice age 18
(integer) 2
127.0.0.1:6380> TTL k
(integer) -1
```

### 2.4 测试与日志说明

测试执行方式、日志目录结构、性能脚本与结果读取方法见：[测试说明](测试说明.md)

## 3. 性能指标与当前水平

在当前实现阶段，myredis 已经具备了非常有竞争力的吞吐能力：在常见业务访问模型下，QPS 可以稳定达到官方 Redis 的同量级，并且在多类场景中出现了明显超越。

### 3.1 总体表现

- 在覆盖基础命令、pipeline、随机 key 与大 value 的综合压测中，myredis 整体表现达到官方 Redis 水平。
- 在多个典型场景里，myredis 的 QPS 不仅没有落后，反而取得了更高吞吐，体现出较好的并发处理效率。
- 从工程视角看，myredis 已具备“可上线压测、可持续迭代优化”的性能基础，具备进一步放大优势的空间。

### 3.2 代表性亮点场景

- 在随机 key + pipeline + 大 value 的组合场景中，myredis 展现出非常突出的吞吐爆发力。
- 在高并发 pipeline 场景中，myredis 表现出良好的扩展性，能够把并发优势转换为可观的 QPS 提升。
- 在混合命令与多数据结构场景中，myredis 保持了较稳的吞吐输出，说明实现不仅“快”，也具备一定的工程稳定性。

## 4. 架构与实现详解

本项目采用分层设计：`网络连接层 -> 协议层 -> 命令层 -> 存储层`。

### 4.1 模块结构

- `src/main.rs`：程序入口，初始化日志，创建 `Db`，启动 TCP 服务。
- `src/server.rs`：监听 socket、accept 连接、为每个连接创建异步任务。
- `src/connection.rs`：单连接生命周期管理（读缓冲、解析、执行、回包）。
- `src/protocol/frame.rs`：RESP 帧定义与编码。
- `src/protocol/parser.rs`：RESP 增量解析器，支持半包与多帧连续读取。
- `src/command/mod.rs`：命令语义解析（`Frame -> Command`）与参数校验。
- `src/storage/db.rs`：核心内存存储，实现数据结构与 TTL/类型检查逻辑。
- `tests/network_integration.rs`：端到端 TCP 集成测试。

### 4.2 请求处理链路

一次请求从网络到响应的路径：

1. `TcpStream` 读入 `BytesMut` 缓冲。
2. `parse_frame` 在缓冲中尝试解析一个完整 RESP 帧。
3. `Command::from_frame` 完成命令识别与参数合法性校验。
4. `Db` 执行业务逻辑（含类型检查、过期检查、结构读写）。
5. 执行结果编码为 RESP 并写回客户端。
6. 同一读批次内，循环处理所有完整帧（天然支持 pipeline）。

### 4.3 协议层设计（RESP）

`Frame` 覆盖以下类型：

- `SimpleString`
- `Error`
- `Integer`
- `BulkString`
- `Null`
- `Array`

解析器关键点：

- 增量解析：数据不完整时返回 `Ok(None)`，等待下一批网络数据。
- 粘包/半包友好：缓冲中可连续解析多帧。
- 严格校验：非法长度、缺失 CRLF、未知类型字节都会返回协议错误。

### 4.4 命令层设计

`Command` 枚举把协议帧映射到强类型语义对象，避免在业务层反复处理裸字节数组。

设计要点：

- 命令名大小写不敏感（ASCII case-insensitive）。
- 固定参数命令会做“参数个数精确校验”。
- 可变参数命令（如 `DEL`/`EXISTS`/`HSET`/`LPUSH`）支持 variadic 输入。
- 未识别命令会进入 `Unknown` 分支并回传标准错误。

### 4.5 存储层设计

`Db` 内部核心结构：

- `DashMap<Bytes, Entry>`：并发哈希表，内部按分片降低锁竞争。
- `Entry`：
	- `value: Value`
	- `expire_at: Option<Instant>`
- `Value`：
	- `String(Bytes)`
	- `Hash(HashMap<Bytes, Bytes>)`
	- `List(VecDeque<Bytes>)`

为什么这样设计：

- `Bytes` 降低拷贝成本，适合网络服务中的二进制 key/value。
- `DashMap` 在多连接并发写入时，比全局互斥锁更容易获得稳定吞吐。
- `Value` 枚举配合命令分派，可集中做类型安全和 WRONGTYPE 保护。

### 4.6 TTL 与过期策略

当前采用“惰性删除（lazy expiration）”策略：

- 读路径（`GET`/`HGET`/`EXISTS`/`TTL` 等）访问到过期 key 时即时清理。
- key 已过期后行为对齐 Redis 语义（如 `GET -> Null`, `TTL -> -2`）。
- 优点是实现简单、无额外后台线程；缺点是冷数据需在访问时才触发回收。

### 4.7 并发模型

- 连接级并发：每个连接一个 Tokio 任务。
- 请求级执行：单连接内按到达顺序串行处理，保证 pipeline 响应顺序一致。
- 数据并发：共享 `Db` clone（内部 `Arc` + `DashMap`），跨连接共享状态。

### 4.8 错误处理与可观测性

- 错误类型统一在 `error.rs`：协议错误、命令错误、IO 错误、不完整帧。
- 网络层区分“正常断连/异常断连/读写错误”。
- 使用 `tracing` 输出连接建立、关闭、错误等关键日志。

### 4.9 测试策略

- 单元测试：覆盖协议解析、命令解析、存储语义。
- 网络集成测试：真实 TCP 环境下验证多连接共享状态、pipeline 顺序、错误隔离。
- 自动化脚本统一输出日志目录，支持问题回溯与性能回归。

### 4.10 当前边界与后续可扩展方向

当前边界：

- 数据为纯内存，不做持久化（无 RDB/AOF）。
- 只实现单机模式，无复制、无哨兵、无集群。
- 命令集覆盖常用子集，未覆盖事务/发布订阅/Lua/有序集合等。

可扩展方向：

- 加入后台过期清理任务，降低冷 key 占用。
- 引入持久化层（快照或追加日志）。
- 增强 benchmark 维度（P95/P99、长稳压测、热点 key 场景）。
- 增加更细粒度指标（命令级耗时、连接级统计）。
