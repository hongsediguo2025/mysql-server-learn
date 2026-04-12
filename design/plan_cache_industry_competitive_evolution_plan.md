# Plan Cache 业界高竞争力演进规划（Draft）

> 面向当前 `ps_point_plan_cache` 实现的后续总体规划  
> 工作树基线时间：2026-04-10

## 1. 文档目的

本文档不讨论当前 v1/v1.2 的细节实现，而是回答一个更大的问题：

> 从现在这版 `per-Prepared_statement` 的点查快路径出发，怎样演进到一个具有 Oracle / SQL Server / PolarDB 级别竞争力的综合 plan cache 能力？

本文的目标是：

- 给出“业界高竞争力”的能力定义，而不是只看单点 benchmark
- 明确当前实现的真实定位与差距
- 提出可落地的分层架构与阶段路线图
- 控制节奏，避免一步做成过重系统
- 为后续的专项设计文档提供总纲

### 1.1 背景知识：先把概念讲清楚

本小节不是实现设计，而是背景知识。

目的只有一个：

> 先把 plan cache 相关术语讲清楚，再去看 Oracle / SQL Server / PolarDB 的方案，以及本文后面的架构与路线图。

如果没有这层背景，后面很容易把这些词混在一起：

- plan cache
- plan history
- baseline
- outline
- force plan
- parameterization
- parameter sensitive plans
- Query Store

#### 1.1.1 先用一个统一比喻理解 plan cache

可以把数据库执行 SQL 想成“餐馆后厨做菜”。

- SQL 文本：顾客下单
- parse / optimize / compile：后厨决定这道菜怎么做
- execution plan：菜谱
- execute：真正开火做菜
- plan cache：把用过的菜谱留下来，下次相似订单直接复用

这个比喻下，有几件事特别容易想明白：

1. `plan cache` 不是结果缓存
  它缓存的是“怎么做”，不是“做出来的结果”。
2. `plan cache` 也不是执行状态缓存
  它通常缓存的是“可复用的计划信息”，不是某次执行临时打开的表句柄、游标状态、行迭代器等执行期对象。
3. 一条 SQL 不一定永远只该有一张菜谱
  点一份菜和点一千份菜，最优做法可能不同。  
   这就是为什么成熟数据库会支持“同一条 SQL 有多个计划变体”。

#### 1.1.2 三件事要分开：plan cache、plan 治理、plan 观测

这三件事经常被混成一件事，但其实完全不同。

`plan cache` 关注的是：

> 下次遇到相似 SQL，能不能不要从头重新优化？

它更关注：

- 命中率
- 重编译次数
- optimizer CPU
- 共享范围

`plan 治理` 关注的是：

> 如果自动选出来的计划不稳定，DBA 能不能干预？

它更关注：

- baseline
- force plan
- outline / hint
- accepted / rejected
- evolve / verify / rollback

`plan 观测` 关注的是：

> 出问题时，我能不能看明白为什么？

它更关注：

- 命中/未命中原因
- 失效原因
- 当前有哪些计划变体
- 哪些 SQL 最容易抖动
- 某个计划是什么时候切换的

很多业界成熟方案厉害的地方，不只是“命中后更快”，而是把这三件事一起做完整。

#### 1.1.3 `hard parse`、`soft parse`、`recompile` 到底是什么

这是 plan cache 领域最基础的三个词。

`hard parse` / `hard compile` 的意思是：

- 缓存里没有可直接复用的计划
- 或者原计划已失效
- 或者当前环境与已缓存计划不兼容

于是数据库只能重新：

- 解析
- 语义分析
- 优化
- 生成计划

通俗说法：

> 后厨没现成菜谱，只能重想一遍。

`soft parse` 的意思是：

- 已经有可复用计划
- 不必完整重走优化流程

通俗说法：

> 直接拿旧菜谱开做。

`recompile` 的意思是：

- 原先有计划
- 但因为各种原因，这次又得重新编译

常见原因有：

- DDL
- 统计信息变化
- 参数敏感导致需要不同计划
- 执行环境变化

通俗说法：

> 不是第一次做，但旧菜谱现在不适用了。

#### 1.1.4 `bind variable`、`parameterization`、`bind peeking`

这些概念都和“相似 SQL 能不能共用一个缓存入口”有关。

`bind variable` / `parameter` 的意思是把具体常量抽成参数。

例如：

```sql
SELECT * FROM t WHERE id = 123;
```

如果把常量 `123` 抽成参数，就变成：

```sql
SELECT * FROM t WHERE id = ?;
```

这时：

- SQL 形状更稳定
- 更容易共享计划
- 也更容易触发参数敏感问题

`parameterization` 的意思是：

> 把字面量 SQL 归一化成参数化 SQL，让更多查询共享同一个 plan cache 入口。

通俗说法：

> 把“id=1”“id=2”“id=3”都当成“id=?” 这一类。

这解决的是：

- “这些 SQL 能不能共用一个缓存入口？”

它还没有解决：

- “共用入口之后，里面到底该放一个计划还是多个计划？”

`bind peeking` 的意思是：

> 第一次编译带参数 SQL 时，优化器偷看一下本次参数值，再据此选计划。

它的优点是：

- 计划可能更贴近第一次实际执行

它的问题是：

- 第一次看到的参数，未必代表之后的大多数参数

这正是参数敏感计划问题的根源之一。

#### 1.1.5 `plan family`、`variant`、`parent/child cursor`、`dispatcher`

成熟数据库都逐渐走向一个共同结论：

> 同一条归一化 SQL，往往不该只有一个计划。

只是各家的表达方式不同。

`plan family` 可以通俗理解为：

> 同一条 normalized SQL 下面的一组计划。

它是一个“计划家族”。

`plan variant` 可以通俗理解为：

> 计划家族中的一个具体版本。

比如：

- variant A：适合高选择性参数
- variant B：适合低选择性参数

`parent cursor` / `child cursor` 是 Oracle 的说法。

可以粗略理解成：

- `parent cursor`：这条 SQL 的大类入口
- `child cursor`：这条 SQL 在某种条件下的一个具体执行版本

也就是：

> Oracle 把“多计划”这件事放在 child cursor 上表达。

`dispatcher plan` 是 SQL Server PSP 的说法。

它不是负责真正执行的计划，而是先做判断：

- 当前参数属于哪一类
- 应该把请求分发给哪个 variant

通俗说法：

> 它像一个分诊台，不是门诊医生本身。

#### 1.1.6 `baseline`、`accepted plan`、`force plan`、`outline`、`plan guide`、`Query Store`

这些术语都和“计划稳定化”有关，但不是一回事。

`baseline` 可以通俗理解为：

> 一条 SQL 允许使用的一组计划白名单。

它强调的是：

- 哪些计划可以用
- 哪些计划不应该直接上生产

`accepted plan` 可以通俗理解为：

> 已经验证过、允许正式使用的 baseline 成员。

也就是说：

- baseline 是“白名单集合”
- accepted plan 是“白名单里已经转正的成员”

`force plan` 可以通俗理解为：

> 直接指定这条 SQL 用某一个计划。

它比 baseline 更强，也更直接。

`outline` 可以通俗理解为：

> 不是把计划对象本身存起来，而是给优化器附加 hint，诱导它重新生成期望计划。

也就是说：

- baseline 更像“只允许用这些计划”
- outline 更像“告诉优化器该往哪个方向想”

`plan guide` 是 SQL Server 的术语，作用和定向规则类似。

可以通俗理解为：

> 针对某一类 SQL 的局部覆盖规则，可以改变参数化方式或 hint 行为。

`Query Store` 是 SQL Server 的一个非常重要的概念。

一定要记住：

> `Query Store` 不是普通意义上的 plan cache。

它更像：

- 飞行记录仪
- 历史档案馆
- 计划变化审计系统

它会保存：

- query 文本/归一化信息
- 历史计划
- runtime stats
- 时间维度变化
- 强制计划状态

所以它解决的是：

- 过去发生了什么
- 某条 SQL 是什么时候变慢的
- 是否发生了计划切换

而普通 plan cache 更关注：

- 现在这次能不能直接复用计划

#### 1.1.7 Oracle：通俗理解它在做什么

Oracle 的基础模型是：

- `shared SQL area`
- `private SQL area`

可以把它理解成：

- `shared SQL area`：所有会话共享的“公共菜谱区”
- `private SQL area`：每个会话自己这次执行的“临时做菜状态”

这意味着 Oracle 的一个核心原则是：

> 共享计划，不共享执行状态。

随后 Oracle 用：

- `parent cursor`
- `child cursor`

来承载“同一条 SQL 的多个具体执行版本”。

当同一条带 bind 的 SQL 在不同参数下表现差异很大时，Oracle 会通过：

- `bind-sensitive`
- `bind-aware`
- `Adaptive Cursor Sharing (ACS)`

逐步学习不同参数区间该用哪个 child cursor。

通俗说法：

> Oracle 会先怀疑“这条 SQL 可能对参数敏感”，再逐步学会“这类参数该走这张菜谱”。

Oracle 在计划稳定化上还有 `SQL Plan Management, SPM`：

- 新计划不一定直接可用
- 可以先作为候选
- 验证后再进入 accepted baseline

这套机制特别适合线上稳定性管理。

Oracle 还有一个非常值得学习的点：

- `V$SQL_SHARED_CURSOR`

它会解释：

> 为什么这个 child cursor 不能和已有 child cursor 共享？

也就是把“不共享”的原因显式做成 reason code。

#### 1.1.8 SQL Server：通俗理解它在做什么

SQL Server 的基础模型是：

- 全局 `plan cache`
- 辅助性的 `Query Store`

`plan cache` 是在线热缓存，用于直接复用计划。  
`Query Store` 是历史系统，用于记录计划变化和运行表现。

SQL Server 很强的一点是，它把“多计划”做得非常显式。

在 `Parameter Sensitive Plan optimization, PSP` 中，会有：

- `dispatcher plan`
- `query variant`

它们的关系可以理解成：

- `dispatcher plan`：先判断本次参数属于哪一类
- `query variant`：真正负责执行的具体计划

也就是说：

> SQL Server 把“多计划选择”设计成了一个显式分发模型。

SQL Server 另一大强项是运维闭环：

- `Query Store` 保存历史计划
- 可做 `force plan`
- 还可以做 `optimized plan forcing`

通俗说法：

> 它不只记住哪个计划好，还尽量把“怎么更快地重新得到这个计划”也一起保存下来。

另外，SQL Server 对 plan cache 污染控制也很重视。

典型例子是：

- `optimize for ad hoc workloads`

它会让第一次出现的 ad hoc SQL 只留下一个很小的 stub，而不是完整计划，避免单次 SQL 把缓存打爆。

#### 1.1.9 PolarDB MySQL：通俗理解它在做什么

PolarDB MySQL 的文档体现出非常强的工程化思路：

> 不是所有 SQL 都值得缓存计划。

因此它在 `Auto Plan Cache` 里提供多种模式：

- `OFF`
- `AUTO`
- `DEMAND`
- `ENFORCE`

可以通俗理解成：

- `OFF`：不用
- `AUTO`：系统判断值不值得缓存
- `DEMAND`：你指定要缓存
- `ENFORCE`：强制都缓存

它最有特色的是 `AUTO` 模式的 admission policy：

- 看优化时间是否足够高
- 看优化时间占总时间比重是否足够高
- 看执行次数是否达到阈值
- 看长期不命中时是否应回收

也就是说：

> PolarDB MySQL 很强调“先判断这条 SQL 是否值得进缓存”。

同时，PolarDB MySQL 通过 `SQL Sharing` 提供 digest 级共享与可观测。

可以通俗理解为：

> `SQL Sharing` 更像它承载共享 SQL 信息的平台层；  
> `Auto Plan Cache` 是平台层上的一个能力。

它在计划稳定化上主要用的是 `Statement Outline`。

这不是直接缓存计划对象，而是：

- 对某个 digest 建规则
- 在 SQL 进优化器前自动补 hint
- 让优化器更容易生成稳定目标计划

所以它更像：

> 用 hint/outline 做稳定化治理。

#### 1.1.10 这对本文后续章节意味着什么

通过上面的背景知识，可以把三家的核心模式压缩成下面三句话：

- Oracle：更像 `shared cursor + child cursor + baseline`
- SQL Server：更像 `global plan cache + dispatcher/variant + Query Store`
- PolarDB MySQL：更像 `SQL Sharing + admission policy + outline`

这也是为什么本文后续的规划，不会只讨论“再支持几个 query shape”，而是会同时讨论：

- 共享范围
- 多计划
- 参数敏感选择
- 计划治理
- 失效模型
- 可观测性

只有这几层一起考虑，后续方案才会更接近真正“业界高竞争力”的综合 plan cache 平台。

## 2. 当前实现的真实定位

### 2.1 当前实现是什么

当前仓库中的 `ps_point_plan_cache` 本质上是一个：

- `per-Prepared_statement`
- `per-connection`
- `single-slot`
- `single-plan`
- `classic optimizer only`
- 以单表唯一键等值点查为核心目标的 fast path 模板缓存

其核心机制是：

1. `PREPARE` 阶段做静态 shape 分类
2. 第一次正常 `EXECUTE` 后，根据优化器实际产出的 plan 做 admission
3. 后续 HOT 命中时，在 `JOIN::optimize()` 入口旁路大部分优化流程
4. 基于当前执行态对象快速重建一个最小 fresh plan，而不是复用旧 `JOIN`

这是一条非常正确的起步路线，因为它：

- 避开了 MySQL 执行期对象生命周期最难的部分
- 把风险收敛在一个很窄的 workload 上
- 已经验证了“跳过优化器开销”这条路线是可行的

### 2.2 当前实现不是什么

当前实现还不是一个“综合 plan cache”，原因主要有 6 点：

1. 共享范围非常窄
  只在单个 `Prepared_statement` 内生效，不能跨连接、跨 PS 共享。
2. 计划数是单槽位
  同一条语句永远只有一个 plan template，没有 plan family 的概念。
3. 参数敏感性能力缺失
  无法像 Oracle ACS 或 SQL Server PSP 一样对不同参数分布保留多个变体。
4. 查询覆盖仍然很窄
  当前主要覆盖单表唯一键等值；`RANGE_PK_BETWEEN` 仅有结构预留，尚未真正形成完整分类 + admission + fast path 链路。
5. 治理能力不足
  还没有 baseline / outline / force / evolve / ban plan 等治理能力。
6. 可观测性和运维能力不足
  目前只有 sysvar 和少量 status counter，无法回答 DBA 最关心的问题：
  - 哪些 SQL 在用 plan cache
  - 命中率是多少
  - 为什么失效
  - 是否出现 plan thrashing
  - 某条 SQL 当前有哪些计划变体

### 2.3 当前阶段最重要的判断

因此，当前能力更准确的命名应该是：

> **PS point-query plan fast path / plan template cache**

而不是：

> 通用 SQL 级 plan cache

这个判断非常重要，因为它决定了后续演进顺序：

- 不能只继续“扩 shape”
- 必须先补“平台层能力”
- 否则系统会快速演变成一组越来越多的特判，而不是一个可运营的 plan cache 平台

## 3. 业界高竞争力到底意味着什么

要实现业界高竞争力，目标不能只定义成：

- `oltp_point_select` QPS 再涨一点

而应定义为下面 7 个维度的综合能力。

### 3.1 维度 A：共享范围广

高竞争力 plan cache 不能只在单个 PS 对象里缓存，而需要支持：

- 同连接内不同 PS 共享
- 跨连接共享
- 文本 SQL 与 binary PS 的统一归一化入口
- 按 digest / normalized SQL 组织 plan family

### 3.2 维度 B：参数敏感多计划

高竞争力 plan cache 不能假设“一条 SQL 永远只有一个最优计划”。

必须支持：

- 同一 normalized SQL 下存在多个变体计划
- 根据 bind/selectivity/统计信息做路由
- 参数分布漂移时自动重学习
- 避免 parameter sniffing 导致的全局错误计划

### 3.3 维度 C：计划稳定化与治理

成熟数据库的 plan cache 都不只解决“快”，还解决“稳”。

必须支持：

- baseline / accepted plan
- force plan / ban plan
- outline / hint 注入
- 新计划 canary / evolve
- rollback 到历史稳定计划

### 3.4 维度 D：精确失效与低抖动

必须有明确的 invalidation 模型，覆盖：

- DDL
- statistics/histogram 变化
- metadata version 变化
- optimizer context 变化
- charset / collation / sql_mode 变化
- privilege / RLS / security context 变化

并且要尽量做到：

- 该失效的失效
- 不该失效的不抖动

### 3.5 维度 E：自动准入、回收与预算控制

高竞争力 plan cache 不是“能缓存的都缓存”，而是有节制地缓存。

必须具备：

- admission gate
- memory budget
- TTL / heat / hit-based promotion
- 淘汰与回收策略
- 防止 cache pollution

### 3.6 维度 F：强可观测性

必须能在系统层回答：

- cache 总大小、使用率、淘汰量
- 每条 digest 的命中、miss、replan、失效原因
- 每个 variant 的选择条件与命中次数
- 回退原因分布
- top SQL / top plan family / top invalidation source

### 3.7 维度 G：混合 workload 下默认安全

计划缓存真正的竞争力，不在最理想点查场景，而在：

- mixed read/write
- point + range + join
- stats 持续变化
- schema 持续演进
- 线上会话环境不完全一致

系统必须做到：

- 热点 workload 有正收益
- 非目标 workload 不明显回退
- 默认开启时可灰度、可观测、可回滚

## 4. 对标业界能力的抽象结论

虽然各家的实现细节不同，但 Oracle、SQL Server、PolarDB 这些成熟方案，抽象后基本都包含下面 5 层能力：

1. **归一化与共享层**
  以 normalized SQL / digest 作为组织入口，而不只是单个 statement 对象。
2. **多变体计划层**
  同一 SQL 可以挂多个 plan variant，而不是单 plan。
3. **运行时选择层**
  根据参数特征、统计信息和上下文为本次执行选择合适变体。
4. **治理控制层**
  DBA 可以接受、强制、冻结、淘汰、演进计划。
5. **运营观测层**
  可以监控、诊断、灰度、回滚。

这说明后续演进方向不应只是“支持更多 query shape”，而应按这 5 层逐步搭平台。

## 5. 目标架构：从单槽位模板到 Plan Cache 平台

## 5.1 总体目标

建议将长期目标定义为：

> 建立一个以 normalized SQL 为中心、支持 plan family、多变体选择、计划治理、强可观测、默认安全的综合 plan cache 平台。

### 5.2 建议的分层模型

```text
Layer 0: Local fast-path template
  - 当前 ps_point_plan_cache 所在层
  - 面向单 PS / 单计划 / 单形状

Layer 1: Shared template cache
  - 以 normalized SQL digest 为 key
  - 跨连接共享稳定模板和 plan family 元数据

Layer 2: Plan family / variant cache
  - 同一 digest 下多个 plan variant
  - 记录参数区间、选择性特征、运行统计

Layer 3: Runtime selector
  - 本次执行根据 bind profile / stats / context 选择 variant

Layer 4: Governance
  - baseline / outline / force / evolve / ban

Layer 5: Observability & operations
  - P_S / I_S / SHOW / trace / digest-level introspection
```

### 5.3 为什么不能直接跨执行缓存 JOIN / TABLE / Iterator

这个原则应继续保持：

- `JOIN *`
- `TABLE *`
- `QEP_TAB *`
- `AccessPath *`
- `RowIterator *`

这些对象都与当次执行上下文强耦合，不适合作为跨连接共享缓存对象。

后续共享层应缓存的是：

- normalized SQL key
- 语句形状摘要
- 关键 plan metadata
- 运行时 guard descriptor
- variant 选择条件
- baseline / governance 元数据

换句话说，后续的共享层应缓存：

> **稳定描述信息**

而不是：

> **执行态对象本体**

## 6. 当前到目标之间的核心差距

建议把差距拆成 8 个课题。

### 6.1 课题 1：从 per-PS 到 digest 级共享

当前问题：

- 相同 SQL 在不同连接上会重复学习、重复 admission、重复缓存

目标：

- 同一 normalized SQL 在 server 级形成共享入口

关键设计点：

- cache key 的规范化
- schema/version 参与 key 的方式
- 文本 SQL 与 binary PS 的统一归一化模型

### 6.2 课题 2：从 single-slot 到 plan family

当前问题：

- 一条 SQL 只能缓存一个计划

目标：

- 一条 SQL 可以有多个 plan variant

关键设计点：

- variant 的创建条件
- 选择条件
- 上限控制
- 淘汰策略

### 6.3 课题 3：参数敏感计划选择

当前问题：

- 类型漂移只能退回重学，无法形成多变体收敛

目标：

- 支持 bind-sensitive / bind-aware 的变体学习与选择

关键设计点：

- selectivity bucket
- histogram/cardinality feedback
- 参数 profile 的抽象方式

### 6.4 课题 4：支持文本 SQL

当前问题：

- 当前主要围绕 prepared statement 生命周期设计

目标：

- 对文本 SQL 也可按 normalized form 走 shared plan/template cache

关键设计点：

- 文本 SQL 的参数化策略
- digest 归一化与 literal handling
- 是否支持 simple/forced parameterization 模式

### 6.5 课题 5：计划治理

当前问题：

- 计划完全由运行时自动学习，DBA 无法干预

目标：

- baseline
- outline
- force plan
- ban variant
- evolve / verify

### 6.6 课题 6：失效与再学习模型

当前问题：

- 当前 invalidation 主要服务于局部 correctness

目标：

- 建立 server 级统一 invalidation / refresh 模型

关键设计点：

- metadata invalidation
- stats invalidation
- context invalidation
- retryable demotion 与 hard invalidation 的边界

### 6.7 课题 7：预算与淘汰

当前问题：

- 当前 per-PS 单槽位几乎不需要 eviction

目标：

- server 级 plan cache 必须有 budget 和 eviction

关键设计点：

- 内存预算
- family 与 variant 的大小评估
- LRU/LFU/heat-based 淘汰
- 冷缓存污染防治

### 6.8 课题 8：可观测与灰度

当前问题：

- 目前只有 hits/admissions/invalidations/fallback 等简单计数

目标：

- 增加 digest 级、family 级、variant 级的可观测能力

关键设计点：

- `performance_schema` 视图
- 原因码枚举
- top-N 统计
- explain / trace 联动

## 7. 演进原则

## 7.1 原则一：先做平台层，再扩查询覆盖

如果先扩 shape，再补治理和共享，系统会迅速碎片化。

推荐顺序：

1. 稳定底座
2. 共享层
3. 多变体
4. 治理
5. 继续扩 shape

## 7.2 原则二：先共享“稳定描述”，不要共享执行态对象

这是和当前实现完全一致、并且应该长期坚持的方向。

## 7.3 原则三：先支持 classic optimizer 的完整平台，再考虑 hypergraph

当前系统已经明确绕开 hypergraph optimizer。后续要扩展时，也应当是：

- 先把 classic 路径的平台能力做扎实
- 再设计 hypergraph 的 metadata 表达与兼容模型

而不是两条线同时重做

## 7.4 原则四：默认安全比极限收益更重要

如果 plan cache 想默认开启，必须优先保证：

- 可回滚
- 可灰度
- 可解释
- 不因为少数 bad case 伤害整体信誉

## 7.5 原则五：所有自动学习都必须能被观测和覆盖

自动学习不是黑盒。

必须允许：

- 看见学到了什么
- 看见为什么选它
- 看见为什么失效
- 手工覆盖它

## 8. 分阶段路线图

下面给出建议的五阶段演进。

### Phase A：把当前底座做成“可运营的稳定特性”

**目标**

- 把当前 `ps_point_plan_cache` 从“workload 优化点”升级为“可灰度特性”

**范围**

- 完善 reason code
- 完善测试矩阵
- 增加 digest 级统计
- 增加 per-shape 命中统计
- 增加失效原因细分
- 解决现有 benchmark 中的收益抖动与回退问题

**新增能力**

- fallback reason enum
- invalidation reason enum
- `performance_schema` 表或 instrumentation
- 计划命中链路 trace
- 更完整的 mixed workload benchmark

**退出标准**

- 默认 ON 下，目标 workload 稳定正收益
- 非目标 workload 回退受控
- DBA 能定位一条 SQL 为什么没命中

**说明**

这是所有后续工作的前提。  
如果这一步没有完成，就不建议继续把范围扩到 server 级共享。

### Phase B：引入 SQL digest 级共享模板缓存

**目标**

- 从 `per-PS` 升级到 `server-level shared template cache`

**范围**

- 设计 normalized SQL key
- 引入 shared family entry
- PS 与 text SQL 共用 shared lookup 入口
- 本地 `Prepared_statement` 保留 local shortcut，但优先挂接 shared family

**建议缓存内容**

- digest / normalized SQL
- schema identity
- plan type
- shape metadata
- chosen key / range metadata
- guard descriptor
- stats/context fingerprint

**不建议缓存内容**

- `JOIN` *
- `TABLE` *
- `QEP_TAB` *
- `AccessPath` *
- `Iterator `*

**退出标准**

- 相同 SQL 跨连接执行时，不再重复冷启动学习
- shared hit ratio 有稳定提升
- 内存预算与淘汰机制初步可用

### Phase C：引入参数敏感计划家族（PSP/ACS 风格）

**目标**

- 让“一条 SQL 多个计划”成为一等能力

**范围**

- 引入 `PlanFamily`
- 同一 family 下维护多个 `PlanVariant`
- runtime selector 根据 bind profile 选择变体
- 统计与反馈驱动新变体的生成和老变体的衰退

**关键设计**

- variant key 不直接等于参数值，而应等于参数特征
- 可按选择性 bucket / histogram region / range width 分类
- 支持 bind-sensitive -> bind-aware 的升级路径

**退出标准**

- 对典型参数敏感 SQL，能避免“一个坏计划污染全局”
- 在 mixed workload 下优于单计划策略

### Phase D：引入计划治理能力

**目标**

- 从“自动优化特性”升级为“可管理能力”

**范围**

- baseline
- outline
- force plan
- ban plan
- evolve / verify / canary

**建议形态**

- 系统表存储 baseline / outline 元数据
- digest + variant id 作为治理对象
- 支持 DBA 指定 accepted / preferred / rejected

**退出标准**

- 某条核心 SQL 出现抖动时，DBA 能稳定住它
- 自动学习产生的新计划可灰度验证后再放量

### Phase E：扩查询覆盖并形成默认开启能力

**目标**

- 在平台层完整后，逐步扩充查询覆盖

**建议扩展顺序**

1. 单表 PK/UK range
2. `IN (...)` / 多点查
3. `ORDER BY ... LIMIT` 热点窄场景
4. 简单聚合
5. 简单等值 join
6. classic optimizer 下更广形状
7. hypergraph optimizer 对接

**退出标准**

- cache 平台能力不因 shape 扩展而变脆
- 新场景的治理、可观测、失效模型同步到位

## 9. 建议的数据结构演进方向

下面不是最终设计，只是推荐方向。

### 9.1 从 `PsPointPlanTemplate` 到 `PlanFamily`

建议后续把当前 `PsPointPlanTemplate` 视为：

- local variant descriptor

再向上引入：

```text
PlanCache
  -> SharedFamilyMap
      -> PlanFamily
          -> PlanVariant[0..N]
```

### 9.2 关键对象建议

#### `PlanFamilyKey`

建议包含：

- normalized SQL digest
- default schema identity
- object resolution fingerprint
- protocol/statement class
- optimizer mode fingerprint

#### `PlanVariant`

建议包含：

- shape / plan type
- chosen access path descriptor
- parameter profile descriptor
- compatibility guard
- hit/miss/replan stats
- last validated metadata version
- governance state

#### `RuntimeSelector`

负责：

- family lookup
- variant choice
- fallback to optimize
- feedback write-back

## 10. 建议的准入、降级与淘汰模型

### 10.1 准入

建议从“只要能学就缓存”升级为“满足阈值才缓存”。

准入维度可包括：

- 语句频度
- 优化耗时占比
- 执行总耗时占比
- 重复次数
- 稳定性评分

### 10.2 降级

建议明确区分两类降级：

- **retryable demotion**
  - 参数画像漂移
  - optimizer context 轻微变化
  - runtime guard 不满足但结构未损坏
- **hard invalidation**
  - DDL
  - metadata version 变化
  - key layout 变化
  - 权限/解析语义变化

### 10.3 淘汰

server 级缓存需要同时支持：

- family 淘汰
- family 内 variant 淘汰

建议规则：

- 热度优先
- 大小感知
- 最近使用
- 低收益变体优先淘汰

## 11. 可观测性路线图

建议尽早规划以下观测接口。

### 11.1 系统级

- 当前 cache memory
- family 数
- variant 数
- admission rate
- hit rate
- replan rate
- invalidation rate
- eviction rate

### 11.2 digest 级

- normalized SQL
- family id
- hit / miss / fallback
- 当前有效 variant 数
- 上次失效原因
- baseline / force 状态

### 11.3 variant 级

- variant id
- 适用参数画像
- chosen key / range / join shape
- 命中次数
- 近期开销
- 最后验证时间
- 是否 accepted / forced / rejected

### 11.4 explain / trace 联动

建议支持：

- `EXPLAIN` 显示是否命中 plan cache
- optimizer trace 中显示 family lookup / selector / fallback reason

## 12. KPI 建议

长期目标不建议只写一个 benchmark 数字，而建议拆成四类。

### 12.1 收益 KPI

- 目标热点场景优化 CPU 显著下降
- point / range / 参数敏感 workload 获得稳定正收益

### 12.2 稳定性 KPI

- 默认 ON 下 mixed workload 无明显系统性回退
- 计划抖动可被检测、解释、收敛

### 12.3 运营 KPI

- DBA 能定位 top family / top invalidation / top fallback
- 关键 SQL 能手工冻结或回滚计划

### 12.4 平台 KPI

- 跨连接共享命中率提升
- 计划重复学习显著下降
- 内存预算与淘汰行为可控

## 13. 风险与误区

### 13.1 风险一：过早追求“缓存整棵计划”

这会直接撞上执行态对象生命周期问题，是高风险方向。

### 13.2 风险二：过早扩 shape

如果在没有共享层、治理层之前快速扩 shape，系统会很快失控。

### 13.3 风险三：把 plan cache 做成黑盒

没有强可观测性，线上出了问题就只能整体关闭，特性价值会迅速下降。

### 13.4 风险四：没有 variant 上限控制

多变体系统最容易失控的地方，就是参数敏感带来的 plan explosion。

### 13.5 风险五：治理能力滞后于自动学习

自动学习跑得越快，没有治理兜底时越危险。

## 14. 推荐的近期落地顺序

结合当前代码基线，建议按下面顺序推进：

1. **先完成 Phase A 设计与收口**
  - 补齐 observability
  - 补齐 reason code
  - 稳定 benchmark
  - 收敛现有收益抖动
2. **再设计 shared family cache**
  - 先只支持 classic optimizer
  - 先只承载当前已有 point-query 模板
  - 不急于一次支持所有 shape
3. **随后设计 parameter-sensitive variant**
  - 从最典型的 point/range 参数敏感 SQL 入手
  - 小步引入 family 内多变体
4. **最后接治理层**
  - baseline / force / outline
  - evolve / verify
5. **平台稳住后继续扩 shape**

## 15. 建议的后续专项文档列表

本文是总纲。建议后续拆成下面几份专项设计：

- `plan_cache_phaseA_observability_and_stability.md`
- `plan_cache_shared_family_design.md`
- `plan_cache_parameter_sensitive_variants.md`
- `plan_cache_governance_and_baseline.md`
- `plan_cache_invalidation_and_refresh_model.md`
- `plan_cache_text_sql_parameterization_design.md`
- `plan_cache_hypergraph_support_analysis.md`

## 16. 最终建议

对当前项目，最合理的战略不是：

> 继续把 `ps_point_plan_cache` 做成越来越深的局部优化特性

而是：

> 把它当作 Layer 0，本地 fast-path 底座；然后围绕 shared family、variant、治理、观测，逐步搭出完整 plan cache 平台

一句话总结：

> **先把“命中后更快”做稳，再把“同一 SQL 的计划学习、共享、选择、治理、演进”做成平台。**

只有做到这一步，才真正具备与业界成熟方案正面竞争的能力。

## 附录 A：术语解释速查与官方文档出处

本附录的目标是让读者在阅读正文时，可以快速回查：

- 这个术语到底是什么意思
- 它在 Oracle / SQL Server / PolarDB 里的语境是什么
- 官方文档出处在哪里

说明：

- 本附录优先列出与本文规划直接相关的术语
- 文档链接尽量选用官方手册首页级或主题页级链接
- Microsoft Learn 某些页面可能因地区或语言设置跳转到本地化页面，但仍属于官方文档

### A.1 通用概念

#### `plan cache`

通俗解释：

> 数据库为了避免重复优化/编译，把“怎么执行”的计划信息缓存起来，供后续相似 SQL 复用。

它缓存的通常是：

- 计划描述
- 编译结果
- 相关元数据

它通常不缓存：

- 执行结果
- 每次执行的会话状态
- 每次执行临时打开的表对象

官方资料：

- Oracle: [Memory Architecture](https://docs.oracle.com/en/database/oracle/oracle-database/19/cncpt/memory-architecture.html)
- SQL Server: [Query Processing Architecture Guide](https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver17)
- PolarDB MySQL: [Auto Plan Cache](https://www.alibabacloud.com/help/en/polardb/polardb-for-mysql/user-guide/auto-plan-cache)

#### `hard parse` / `hard compile`

通俗解释：

> 这次执行不能直接复用已有计划，只能重新解析、优化并生成计划。

常见触发原因：

- 没有可用缓存
- 计划失效
- 当前环境和缓存计划不兼容

官方资料：

- Oracle: [Memory Architecture](https://docs.oracle.com/en/database/oracle/oracle-database/19/cncpt/memory-architecture.html)
- Oracle: [Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/improving-rwp-cursor-sharing.html)
- SQL Server: [Query Processing Architecture Guide](https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver17)

#### `soft parse`

通俗解释：

> 这次执行可以直接复用已有计划，不必完整重走优化流程。

官方资料：

- Oracle: [Memory Architecture](https://docs.oracle.com/en/database/oracle/oracle-database/19/cncpt/memory-architecture.html)
- Oracle: [Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/improving-rwp-cursor-sharing.html)

#### `recompile`

通俗解释：

> 原来有计划，但因为对象、统计信息或执行环境变化，这次又必须重新编译。

官方资料：

- Oracle: [Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/improving-rwp-cursor-sharing.html)
- SQL Server: [Query Processing Architecture Guide](https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver17)

#### `bind variable` / `parameter`

通俗解释：

> 把 SQL 中的具体常量抽成参数位，让语句形状更稳定，更容易共享计划。

示例：

```sql
SELECT * FROM t WHERE id = ?;
```

官方资料：

- Oracle: [Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/improving-rwp-cursor-sharing.html)
- SQL Server: [Query Processing Architecture Guide](https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver17)

#### `parameterization`

通俗解释：

> 把字面量 SQL 归一化成参数化 SQL，好让更多语句共享同一个缓存入口。

它解决的是：

- “这些 SQL 能不能共用一个入口？”

它不自动解决的是：

- “这个入口下面到底保留一个计划还是多个计划？”

官方资料：

- SQL Server: [Specify Query Parameterization Behavior by Using Plan Guides](https://learn.microsoft.com/en-us/sql/relational-databases/performance/specify-query-parameterization-behavior-by-using-plan-guides?view=sql-server-ver17)
- SQL Server: [Query Processing Architecture Guide](https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver17)
- Oracle: [CURSOR_SHARING](https://docs.oracle.com/en/database/oracle/oracle-database/26/refrn/CURSOR_SHARING.html)

#### `bind peeking`

通俗解释：

> 优化器在第一次编译带参数 SQL 时，会看一下当前参数值，再据此选计划。

它的优点是第一次执行可能更准。  
它的问题是第一次参数不一定代表之后的主流参数。

官方资料：

- Oracle: [Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/improving-rwp-cursor-sharing.html)
- SQL Server: [Query Processing Architecture Guide](https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver17)

#### `parameter sensitivity` / `parameter sniffing`

通俗解释：

> 同一条参数化 SQL，在不同参数值下，最优计划可能完全不同。

如果系统长期只保留一个计划，就可能出现：

- 小结果集适合索引
- 大结果集适合扫描
- 但缓存里只剩下一种做法

官方资料：

- SQL Server: [Query Processing Architecture Guide](https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver17)
- SQL Server: [Parameter Sensitive Plan optimization](https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization?view=sql-server-ver17)
- Oracle: [Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/improving-rwp-cursor-sharing.html)

#### `plan family`

通俗解释：

> 同一条 normalized SQL 下面的一组计划集合。

它不是单个计划，而是“这条 SQL 的计划家族”。

正文中的长期目标里，`PlanFamily` 就是这个概念的工程化表达。

官方资料：

- Oracle: [Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/improving-rwp-cursor-sharing.html)
- SQL Server: [Parameter Sensitive Plan optimization](https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization?view=sql-server-ver17)

#### `plan variant`

通俗解释：

> 计划家族中的某一个具体计划版本。

例如：

- 高选择性参数用 variant A
- 低选择性参数用 variant B

官方资料：

- SQL Server: [Parameter Sensitive Plan optimization](https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization?view=sql-server-ver17)

### A.2 Oracle 相关概念

#### `shared SQL area`

通俗解释：

> Oracle 中所有会话可共享的“公共计划区”，里面存 parse tree 和执行计划。

官方资料：

- Oracle: [Memory Architecture](https://docs.oracle.com/en/database/oracle/oracle-database/19/cncpt/memory-architecture.html)

#### `private SQL area`

通俗解释：

> Oracle 中每个会话自己的执行状态区，里面有 bind 值、执行状态、运行时信息。

这也是为什么 Oracle 能做到：

> 共享计划，不共享每次执行状态。

官方资料：

- Oracle: [Memory Architecture](https://docs.oracle.com/en/database/oracle/oracle-database/19/cncpt/memory-architecture.html)

#### `cursor`

通俗解释：

> 可以把 cursor 粗略理解成“指向某条 SQL 执行状态与共享信息的句柄”。

官方资料：

- Oracle: [Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/improving-rwp-cursor-sharing.html)
- Oracle: [Memory Architecture](https://docs.oracle.com/en/database/oracle/oracle-database/19/cncpt/memory-architecture.html)

#### `parent cursor`

通俗解释：

> 表示 SQL 文本这一级别的入口。

如果 SQL 文本不同，通常会形成不同 parent cursor。

官方资料：

- Oracle: [Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/improving-rwp-cursor-sharing.html)

#### `child cursor`

通俗解释：

> 表示这条 SQL 在某种 optimizer 环境、bind 特征或元数据条件下的一个具体执行版本。

这也是 Oracle 表达“同一 SQL 有多个计划”的主要机制。

官方资料：

- Oracle: [Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/improving-rwp-cursor-sharing.html)
- Oracle: [V$SQL_SHARED_CURSOR](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-SQL_SHARED_CURSOR.html)

#### `Adaptive Cursor Sharing (ACS)`

通俗解释：

> Oracle 会根据不同 bind 值下的实际执行表现，逐步学习是否需要为同一条 SQL 维护多个 child cursor。

相关状态通常会涉及：

- `bind-sensitive`
- `bind-aware`

官方资料：

- Oracle: [Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/improving-rwp-cursor-sharing.html)

#### `CURSOR_SHARING`

通俗解释：

> 控制 Oracle 允许哪类 SQL 共享同一个 cursor。

常见值：

- `EXACT`
- `FORCE`

其中 `FORCE` 更像临时止血工具，不适合作为长期万能解法。

官方资料：

- Oracle: [CURSOR_SHARING](https://docs.oracle.com/en/database/oracle/oracle-database/26/refrn/CURSOR_SHARING.html)
- Oracle: [Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/improving-rwp-cursor-sharing.html)

#### `SQL Plan Management (SPM)`

通俗解释：

> Oracle 的计划治理框架，用来防止计划回归。

核心思想是：

- 新计划先发现
- 不一定立即可用
- 经过验证后再进入正式可用集合

官方资料：

- Oracle: [Managing SQL Plan Baselines](https://docs.oracle.com/en/database/oracle/oracle-database/26/tdppt/managing-sql-plan-baselines.html)
- Oracle: [DBMS_SPM](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_SPM.html)

#### `SQL plan baseline`

通俗解释：

> 一条 SQL 被允许使用的一组计划白名单。

官方资料：

- Oracle: [Managing SQL Plan Baselines](https://docs.oracle.com/en/database/oracle/oracle-database/26/tdppt/managing-sql-plan-baselines.html)
- Oracle: [DBMS_SPM](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_SPM.html)

#### `accepted plan`

通俗解释：

> 已经验证过、允许正式使用的 baseline 成员。

官方资料：

- Oracle: [Managing SQL Plan Baselines](https://docs.oracle.com/en/database/oracle/oracle-database/26/tdppt/managing-sql-plan-baselines.html)

#### `V$SQL_SHARED_CURSOR`

通俗解释：

> Oracle 用来解释“为什么某个 child cursor 不能和已有 child cursor 共享”的诊断视图。

它非常值得借鉴，因为它把“非共享原因”显式做成了 reason code。

官方资料：

- Oracle: [V$SQL_SHARED_CURSOR](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-SQL_SHARED_CURSOR.html)

### A.3 SQL Server 相关概念

#### `plan cache`

通俗解释：

> SQL Server 用来保存在线热计划的内存区域。

官方资料：

- SQL Server: [Query Processing Architecture Guide](https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver17)

#### `OBJCP`

通俗解释：

> SQL Server plan cache 中用于存放对象型计划的 cache store。

通常对应：

- stored procedure
- function
- trigger

官方资料：

- SQL Server: [Query Processing Architecture Guide](https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver17)

#### `SQLCP`

通俗解释：

> SQL Server plan cache 中用于存放动态 SQL、自动参数化 SQL、prepared query 计划的 cache store。

官方资料：

- SQL Server: [Query Processing Architecture Guide](https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver17)

#### `execution context`

通俗解释：

> SQL Server 中每次执行自己的运行时上下文，例如当前参数值等。

它和 compiled plan 分离，这也是计划能被多人共享的重要前提。

官方资料：

- SQL Server: [Query Processing Architecture Guide](https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver17)

#### `Query Store`

通俗解释：

> SQL Server 的计划历史与运行统计仓库，不是普通的热缓存。

它用来回答：

- 哪些计划变过
- 什么时候变的
- 变了之后性能如何
- 是否需要 force plan

官方资料：

- SQL Server: [Monitor performance by using the Query Store](https://learn.microsoft.com/en-us/sql/relational-databases/performance/monitoring-performance-by-using-the-query-store?view=sql-server-ver17)

#### `plan forcing`

通俗解释：

> 通过 Query Store 或相关接口，要求某条 SQL 使用某个指定计划。

官方资料：

- SQL Server: [Monitor performance by using the Query Store](https://learn.microsoft.com/en-us/sql/relational-databases/performance/monitoring-performance-by-using-the-query-store?view=sql-server-ver17)
- SQL Server: [Optimized plan forcing with Query Store](https://learn.microsoft.com/en-us/sql/relational-databases/performance/optimized-plan-forcing-query-store?view=sql-server-ver16)

#### `optimized plan forcing`

通俗解释：

> SQL Server 2022 引入的增强能力。  
> 不只是强制计划，还会把一部分“如何更快复现该计划的编译步骤”保存下来，减少重复 forced query 的编译开销。

官方资料：

- SQL Server: [Optimized plan forcing with Query Store](https://learn.microsoft.com/en-us/sql/relational-databases/performance/optimized-plan-forcing-query-store?view=sql-server-ver16)

#### `Parameter Sensitive Plan optimization (PSP)`

通俗解释：

> SQL Server 用于解决“同一条参数化 SQL，一个缓存计划不适合所有参数值”的官方多计划机制。

其核心模型是：

- `dispatcher plan`
- `query variant`

官方资料：

- SQL Server: [Parameter Sensitive Plan optimization](https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization?view=sql-server-ver17)
- SQL Server: [ALTER DATABASE SCOPED CONFIGURATION](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-database-scoped-configuration-transact-sql?view=sql-server-ver17)

#### `dispatcher plan`

通俗解释：

> PSP 中负责“先看参数落在哪个区间、再把请求分发给具体 variant”的计划。

它更像“分诊台”，不是最终执行计划本身。

官方资料：

- SQL Server: [Parameter Sensitive Plan optimization](https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization?view=sql-server-ver17)

#### `query variant`

通俗解释：

> PSP 中真正执行的具体计划版本。

同一条 SQL 可以有多个 variant。

官方资料：

- SQL Server: [Parameter Sensitive Plan optimization](https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization?view=sql-server-ver17)

#### `simple parameterization`

通俗解释：

> SQL Server 自动、有限度地把字面量 SQL 转成参数化 SQL。

官方资料：

- SQL Server: [Specify Query Parameterization Behavior by Using Plan Guides](https://learn.microsoft.com/en-us/sql/relational-databases/performance/specify-query-parameterization-behavior-by-using-plan-guides?view=sql-server-ver17)

#### `forced parameterization`

通俗解释：

> SQL Server 更激进地把更多 SQL 做参数化，以提升计划复用率。

官方资料：

- SQL Server: [Specify Query Parameterization Behavior by Using Plan Guides](https://learn.microsoft.com/en-us/sql/relational-databases/performance/specify-query-parameterization-behavior-by-using-plan-guides?view=sql-server-ver17)

#### `plan guide`

通俗解释：

> SQL Server 对某一类 SQL 的局部覆盖规则，可以影响参数化行为或附加 hint/固定计划。

官方资料：

- SQL Server: [Plan Guides](https://learn.microsoft.com/en-us/sql/relational-databases/performance/plan-guides?view=sql-server-ver17)
- SQL Server: [Specify Query Parameterization Behavior by Using Plan Guides](https://learn.microsoft.com/en-us/sql/relational-databases/performance/specify-query-parameterization-behavior-by-using-plan-guides?view=sql-server-ver17)

#### `optimize for ad hoc workloads`

通俗解释：

> SQL Server 用来缓解大量 single-use ad hoc SQL 污染 plan cache 的机制。

它第一次只存：

- `compiled plan stub`

不是完整计划。

官方资料：

- SQL Server: [Server configuration: optimize for ad hoc workloads](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/optimize-for-ad-hoc-workloads-server-configuration-option?view=sql-server-ver17)

#### `compiled plan stub`

通俗解释：

> 一个很小的“占位计划记录”，只用于表明这条 ad hoc SQL 曾经编译过一次。

如果后续再次执行，才升级成完整计划。

官方资料：

- SQL Server: [Server configuration: optimize for ad hoc workloads](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/optimize-for-ad-hoc-workloads-server-configuration-option?view=sql-server-ver17)

### A.4 PolarDB MySQL 相关概念

#### `Auto Plan Cache`

通俗解释：

> PolarDB MySQL 提供的自动计划缓存能力，用于减少优化时间，但不会无脑缓存所有 SQL。

它特别强调：

- 有些 SQL 优化时间很长，值得缓存
- 有些 SQL 对参数敏感，固定计划可能反而退化

官方资料：

- PolarDB MySQL: [Auto Plan Cache](https://www.alibabacloud.com/help/en/polardb/polardb-for-mysql/user-guide/auto-plan-cache)

#### `AUTO / DEMAND / ENFORCE`

通俗解释：

> PolarDB MySQL Auto Plan Cache 的几种工作模式。

可以粗略理解为：

- `AUTO`：系统判断值不值得缓存
- `DEMAND`：你点名要缓存
- `ENFORCE`：强制缓存

官方资料：

- PolarDB MySQL: [Auto Plan Cache](https://www.alibabacloud.com/help/en/polardb/polardb-for-mysql/user-guide/auto-plan-cache)

#### `admission policy`

通俗解释：

> 一条 SQL 不是“能缓存就缓存”，而是要先满足一定条件才进缓存。

PolarDB MySQL 的官方文档里，这些条件包括：

- 总执行时间阈值
- 优化时间占比阈值
- 计数阈值
- 过期时间

官方资料：

- PolarDB MySQL: [Auto Plan Cache](https://www.alibabacloud.com/help/en/polardb/polardb-for-mysql/user-guide/auto-plan-cache)

#### `SQL Sharing`

通俗解释：

> PolarDB MySQL 中承载共享 SQL/计划信息的平台层。  
> Auto Plan Cache 的计划信息存放在这个模块中，并可通过系统表查看。

官方资料：

- PolarDB MySQL: [Auto Plan Cache](https://www.alibabacloud.com/help/en/polardb/polardb-for-mysql/user-guide/auto-plan-cache)

#### `Statement Outline`

通俗解释：

> 通过 optimizer hints 或 index hints，稳定一条 SQL 的执行计划。

它不是直接缓存计划对象，而是：

- 对 SQL 建规则
- 在 SQL 进入优化器前自动补 hint
- 让优化器更稳定地产生目标计划

官方资料：

- PolarDB MySQL: [Statement Outline](https://www.alibabacloud.com/help/doc-detail/172533.html)

#### `DBMS_OUTLN`

通俗解释：

> PolarDB MySQL 用于创建、预览、展示、删除 Statement Outline 的工具包。

官方资料：

- PolarDB MySQL: [Statement Outline](https://www.alibabacloud.com/help/doc-detail/172533.html)

## 附录 B：主要官方资料索引

下面按厂商给出本文最重要的官方参考资料，便于后续继续深入阅读。

### B.1 Oracle

- [Memory Architecture](https://docs.oracle.com/en/database/oracle/oracle-database/19/cncpt/memory-architecture.html)
- [Improving Real-World Performance Through Cursor Sharing](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgsql/improving-rwp-cursor-sharing.html)
- [CURSOR_SHARING](https://docs.oracle.com/en/database/oracle/oracle-database/26/refrn/CURSOR_SHARING.html)
- [V$SQL_SHARED_CURSOR](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-SQL_SHARED_CURSOR.html)
- [Managing SQL Plan Baselines](https://docs.oracle.com/en/database/oracle/oracle-database/26/tdppt/managing-sql-plan-baselines.html)
- [DBMS_SPM](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_SPM.html)

### B.2 SQL Server

- [Query Processing Architecture Guide](https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver17)
- [Monitor performance by using the Query Store](https://learn.microsoft.com/en-us/sql/relational-databases/performance/monitoring-performance-by-using-the-query-store?view=sql-server-ver17)
- [Parameter Sensitive Plan optimization](https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization?view=sql-server-ver17)
- [Optimized plan forcing with Query Store](https://learn.microsoft.com/en-us/sql/relational-databases/performance/optimized-plan-forcing-query-store?view=sql-server-ver16)
- [Plan Guides](https://learn.microsoft.com/en-us/sql/relational-databases/performance/plan-guides?view=sql-server-ver17)
- [Specify Query Parameterization Behavior by Using Plan Guides](https://learn.microsoft.com/en-us/sql/relational-databases/performance/specify-query-parameterization-behavior-by-using-plan-guides?view=sql-server-ver17)
- [Server configuration: optimize for ad hoc workloads](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/optimize-for-ad-hoc-workloads-server-configuration-option?view=sql-server-ver17)
- [ALTER DATABASE SCOPED CONFIGURATION](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-database-scoped-configuration-transact-sql?view=sql-server-ver17)

### B.3 PolarDB MySQL

- [Auto Plan Cache](https://www.alibabacloud.com/help/en/polardb/polardb-for-mysql/user-guide/auto-plan-cache)
- [Statement Outline](https://www.alibabacloud.com/help/doc-detail/172533.html)

### B.4 附注

- 本附录中的链接均为官方文档链接。
- Microsoft Learn 某些页面可能因地区语言自动跳转到本地化 URL，但属于同一官方主题页。
- 对于具体实现细节，后续专项设计文档仍应以最新官方页面与当前内核代码为准。

## 附录 C：竞品能力对比表

本附录把 Oracle、SQL Server、PolarDB MySQL 与当前仓库实现放在一张表里看。

目的不是评判谁“最好”，而是帮助回答两个更实际的问题：

1. 业界成熟方案到底强在哪些层面？
2. 我们当前最缺的是哪几层能力？

说明：

- “当前实现”指本文对应时间点仓库中的 `ps_point_plan_cache`
- “公开文档未明确”表示官方文档没有把内部实现机制展开到可以下结论的程度
- 某些格子里的表述是对官方文档的工程化归纳，不等于厂商原话

### C.1 综合能力对比


| 维度                | Oracle                                             | SQL Server                                               | PolarDB MySQL                                   | 当前实现                                        |
| ----------------- | -------------------------------------------------- | -------------------------------------------------------- | ----------------------------------------------- | ------------------------------------------- |
| 核心组织单位            | `parent cursor + child cursor`                     | `plan cache + dispatcher/variant + Query Store`          | `SQL Sharing + plan cache/outline`              | `Prepared_statement + single-slot template` |
| 共享范围              | 强。shared SQL area 跨 session 共享                     | 强。global plan cache 跨 session 共享                         | 强于单连接，本质是 digest/SQL sharing 级共享                | 弱。仅 per-PS、per-connection                   |
| 主要缓存对象            | shared cursor / child cursor / plan metadata       | compiled plan / execution context 分离 / Query Store 历史    | 共享 SQL 信息、计划信息、outline 规则                       | point-query 模板与部分 arena 缓存对象                |
| 文本 SQL 支持         | 强                                                  | 强                                                        | 强                                               | 弱，当前主要围绕 PS 生命周期                            |
| 参数化能力             | 有 `CURSOR_SHARING`，但 `FORCE` 不是长期主路径               | strong，simple/forced parameterization + plan guides      | 有 digest/共享入口，自动计划缓存更偏准入控制                      | 当前主要依赖 PS 参数化，本身不做文本 SQL 参数化                |
| 参数敏感多计划           | 强。ACS 支持多个 child cursor                            | 强。PSP 用 dispatcher + variants                            | 公开文档未明确等价于 ACS/PSP 的自动多变体机制                     | 无。single-slot 单计划                           |
| 多计划表达方式           | child cursor                                       | query variant                                            | 公开文档更偏 plan cache + outline，而非显式 variant family | 无                                           |
| 计划稳定化             | 强。SPM / SQL plan baseline / accepted plan / evolve | 强。Query Store force plan / optimized plan forcing        | 中强。Statement Outline 稳定计划                       | 很弱。当前无 baseline / force / outline           |
| 计划回滚能力            | 强。baseline 白名单与 evolve 机制                          | 强。可 force 回历史计划                                          | 中。通过 outline 定向稳定                               | 无内建治理面                                      |
| admission policy  | 有，但公开文档更强调共享、ACS、SPM                               | 有 plan cache 生命周期和内存回收控制，但 admission 不是 PolarDB 那种显式阈值模型 | 强。`AUTO/DEMAND/ENFORCE` + 阈值 + 计数 + 过期时间        | 很弱。当前基本是 shape + first execute admission    |
| eviction / budget | 有 shared pool / library cache 资源竞争与老化              | 强。plan cache 有内存压力回收；ad hoc stub 控污染                     | 有过期、阈值与共享模块治理                                   | 几乎没有，per-PS 单槽位不涉及 server 级淘汰               |
| invalidation      | 强。DDL / stats / env 变化触发；有 child 级非共享原因诊断          | 强。schema/stats/compile context 变化会重编译                    | 有表版本、模式与 outline 规则关联；公开文档对内部 invalidation 细节较少 | 有 correctness 导向 invalidation，但主要服务于本地快路径   |
| “为什么没共享/没命中”的解释能力 | 很强。`V$SQL_SHARED_CURSOR`                           | 中强。可借助 Query Store、DMV、compile/recompile 信息排查            | 中。系统表可见 digest/plan 信息，但官方公开的 reason-code 体系较弱  | 弱。只有少量 status counter，reason code 仍待建设      |
| 历史计划追踪            | 中强。可配合 AWR/SPM/cursor 视图                           | 很强。Query Store 是完整历史面                                    | 中。共享表与 outline 能看到当前信息，历史面不如 Query Store 完整     | 弱。当前几乎无历史面                                  |
| 典型优势场景            | 高并发 OLTP、bind-heavy、需要计划治理的核心系统                    | 复杂混合负载、参数敏感 SQL、运维闭环要求高的系统                               | MySQL 兼容场景下的自动计划缓存与定向稳定化                        | 单表热点点查、PS 场景下减少优化器 CPU                      |
| 主要短板/代价           | 体系复杂，child cursor 与 shared pool 问题定位门槛高            | 体系大，运维面丰富但也更复杂                                           | 自动多计划能力公开程度不如 Oracle/SQL Server                 | 不是综合 plan cache，只是 Layer 0 fast-path 能力     |


### C.2 关键术语映射表

这张表用于把三家的术语映射到一套更统一的工程语言里。


| 统一理解     | Oracle                          | SQL Server                                  | PolarDB MySQL               | 对我们方案的映射                               |
| -------- | ------------------------------- | ------------------------------------------- | --------------------------- | -------------------------------------- |
| 共享入口     | parent cursor / shared SQL area | normalized query + plan cache entry         | SQL Sharing / digest        | `PlanFamilyKey` / shared family entry  |
| 多计划家族    | child cursor 集合                 | dispatcher + query variants                 | 公开文档未强调显式 family            | `PlanFamily`                           |
| 具体计划版本   | child cursor                    | query variant                               | 单个 plan / outline 目标计划      | `PlanVariant`                          |
| 运行时选择器   | ACS / bind-aware 逻辑             | dispatcher plan                             | 文档侧更偏 AUTO 准入，而非显式 selector | `RuntimeSelector`                      |
| 计划白名单    | SQL plan baseline               | Query Store forced/selected plan（能力接近但模型不同） | outline 规则集合（更偏 hint 治理）    | baseline / accepted plan               |
| 强制使用某计划  | baseline + force / accepted     | force plan                                  | outline / hint 注入           | force / preferred variant              |
| 历史档案与回溯  | AWR/SPM/cursor 视图               | Query Store                                 | 部分系统表与管理接口                  | future history / observability layer   |
| 不共享/失效诊断 | `V$SQL_SHARED_CURSOR`           | Query Store + DMV + recompile 信息            | 系统表可见性有限                    | reason code / invalidation diagnostics |


### C.3 对我们的直接启发


| 厂商            | 最值得借鉴的点                                                                                   | 不建议照搬的点                               | 对我们当前阶段最相关的动作                                                   |
| ------------- | ----------------------------------------------------------------------------------------- | ------------------------------------- | --------------------------------------------------------------- |
| Oracle        | `parent/child` 心智模型、ACS、多 child cursor、`V$SQL_SHARED_CURSOR` 原因码体系、SPM baseline           | `CURSOR_SHARING=FORCE` 这种过于粗暴的全局参数化手段 | 建立 family/variant 模型和 reason code 体系                            |
| SQL Server    | dispatcher + variants 的显式多计划架构、Query Store 历史面、optimized plan forcing、ad hoc pollution 控制 | 过早照搬庞大的完整运维体系，可能超过当前阶段可承受复杂度          | 设计 shared family cache 时同步规划 history / forcing 接口               |
| PolarDB MySQL | `AUTO/DEMAND/ENFORCE` 思路、admission policy、digest 级系统表、outline 稳定化                         | 在没有参数敏感多计划能力前，就用强制计划缓存覆盖更广 workload   | 先把准入阈值、系统表可观测和 outline 风格治理入口设计出来                               |
| 当前实现          | correctness 优先、本地 fast-path 模板、避免跨执行复用执行态对象的原则                                            | 把当前 local fast-path 误当成综合 plan cache  | 把当前实现定位为 Layer 0，然后向 shared family / variants / governance 逐层演进 |


### C.4 一句话结论

如果把三家竞品的精华压缩成一句话：

- Oracle 最强在：**共享 cursor 体系 + 参数敏感多 child cursor + baseline 治理**
- SQL Server 最强在：**显式多计划分发模型 + Query Store 运维闭环**
- PolarDB MySQL 最强在：**自动准入策略 + MySQL 语境下的 digest 级共享与 outline 稳定化**

而当前实现最应该补的，不是单纯再加几个 query shape，而是：

1. shared family 层
2. parameter-sensitive variant 层
3. governance 层
4. observability/history 层

这四层补齐之后，才有资格谈“综合 plan cache 竞品能力”。