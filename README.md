# 多副本离线写入核对 API

接收 2～8 个副本离线产生的 1～120 条 put/delete 事件（可乱序），校验
向量时钟因果关系，并按向量时钟偏序解析出每个 key 的有效版本：

- 唯一极大事件是 put → 返回其值
- 唯一极大事件是 delete → 标记为已删除
- 存在多个极大事件（互不可比，即使值相同）→ 报告冲突

此外，`/api/catchup` 接口在同一批事件之上，为目标副本生成**可直接重放的
增量补传计划**：传入目标对各来源已连续接收的最高 seq（`known_clock`），
缺失事件按依赖分轮返回。

`/api/resolve-conflict` 接口把**已确认的冲突处理结果**写成一条新的、
可重放的事件：在写入副本上追加 put/delete，其时钟并入所有冲突版本及该副本
此前见过的历史；返回新事件与追加后该 key 的裁决。

上述三个接口完全无状态，**请求数据不落库**。

另外还提供一组**持久化历史**接口（`/api/histories`）：多个调用方先凭一份
合法批次创建历史并获得 ID，之后只按 ID 读取、汇入新的离线事件、或按 ID 提交
冲突处理参数（不必再传整个批次）。汇入按 `(replica_id, seq)` 合并：内容完全
相同的事件忽略，同标识异内容返回 **409**；冲突提交时会重新核对当前极大版本，
过期返回 **409**。历史保存在本地 SQLite（Docker 下位于命名卷），容器重启后
仍可读取。

持久历史还提供一个**键迁移**接口
（`POST /api/histories/{id}/migrate-key`）：把一个 key 当前的唯一 put 值
搬到另一个空闲 key，并生成**两条可重放事件**。请求体含写入副本、源键、
目标键和两键当前观察到的极大版本（各自为 `{replica_id, seq}` 列表，目标键
无事件时为空列表）。服务端在**同一个串行化事务**内核对两键：源键当前必须是
唯一 put，目标键必须没有事件或当前裁决为唯一 delete，且两个版本列表必须分别
**等于**该键当前的全部极大事件；状态或版本不符返回 **409**，两键相同、写入/
版本副本不存在、版本重复或 120 条上限不足两个名额返回 **422**。通过后给写入
副本接续分配两个 seq：先在源键写一条 delete，再在目标键写一条 put（值取源值）；
第一条事件的时钟覆盖两键当前版本与写入副本此前历史，第二条覆盖第一条（因此
后一条依赖前一条）。两条事件都通过原批次校验、一起生效；源键判为已删除、
目标键保留源值、其他键裁决不变，失败则历史不改。暂存会话既不参与也不被改动。
返回的两条原格式事件可直接用于 `/api/events` 汇入或 `/api/catchup` 增量补传。

此外还有一个**只读的撤除预演**接口：给定已提交历史的 ID 与一条事件的
`replica_id`、`seq`，预演把该离线事件撤除会波及哪些写入与 key——目标事件及
所有直接或间接依赖它的事件（跨 key）都会计入，无关并发事件保留；只返回预演
结果，**不实际删除、不重排序号或时钟、不改动已提交历史**。

最后是一组**导入会话**接口（`/api/histories/{id}/import-sessions`）：离线事件
分几次到达时，先按历史 ID 开一个空会话，把每批事件**暂存**进去（暂存对**新
标识**事件只校验单事件本身，允许前序或跨副本依赖尚未到达；对已暂存及同批
重复的 `(replica_id, seq)` 重传不做这些校验，而按 `key`、操作、值与**完整
clock**（键序无关）判等——同内容忽略，异内容 **409**，缺分量、多出零分量或
本分量改变都算异内容，不会误判为 422），收齐后一次**提交**——提交把暂存事件
与历史最新已提交状态按原规则合并（完整判等、因果校验、120 条总上限），成功即
封闭会话。会话容量按**已有暂存加本批去重后的不同标识**计数：一批发来 121 份
完全相同的合法事件只存一条；已满 120 条的会话重传已有事件仍成功，新标识超限
才 **422**。提交失败（缺依赖等）时开放会话与已暂存数据原样保留，补齐后可直接
重试。会话同样持久化在 SQLite 中，进程/容器重启后可继续暂存与提交。

会话还提供一个**只读缺口查询**接口
（`/api/histories/{id}/import-sessions/{sid}/missing-ranges`）：在**同一已提交
快照**上合并历史已提交事件与会话暂存事件，先按标识比对——暂存事件与已提交事件
同 `(replica_id, seq)` 但原完整内容不同即返回 **409**，一次性列出**全部**冲突
的 `replica_id`、`seq`（按二者排序），不返回缺口清单；无冲突时，对每个副本取
其本副本合并最大 seq 与所有事件 clock 对该副本分量的最大值作为需求上界，把
`1..上界` 中尚缺的序号合成**最大的连续闭区间**，按 `replica_id`、`start_seq`
排序返回。缺失序号**绝不逐个展开**——暂存 seq 为 `10^12` 也只返回一个区间；
上界为 0 的副本不产生区间。返回空列表只表示标识齐全，**不保证提交通过**（时钟
因果等仍在提交时校验）。封闭会话也可查询；查询不提交会话，也不改动历史、暂存
与状态。

另外还有一个**只读因果证据批次**接口
（`POST /api/histories/{id}/evidence-batch`）：给定持久历史 ID 与一组**非空、
无重复**的 key，从同一已提交历史中取出这些 key 的**全部**事件——包括后来被
覆盖的 put 与 delete——再递归纳入每条事件的**本副本上一事件**与**其他副本
clock 非零分量所指事件**；依赖会跨 key 延伸，因此闭包中可能出现只作为依赖
证据的**辅助 key**，闭包之外的事件一律不加入。返回原 `replica_ids` 与原线格式
`events`，按 `replica_id`、`seq` 排序，**不重排序号、不改写时钟**，因此整批
原样通过原完整校验，可**直接**作为 `/api/resolve`、`/api/catchup` 与
`/api/resolve-conflict` 的请求体重放；所选 key 在证据批次上的裁决与在原历史上
**完全一致**，辅助 key 的裁决允许不同。历史不存在或任一所选 key 无事件返回
**404**，非法 `keys`（缺失、为空、含空串/非字符串或有重复）返回 **422**；查询
不改动历史、导入会话与任何旧接口。

最后还有一个**只读事件因果关系**接口
（`POST /api/histories/{id}/event-relation`）：解释同一份已提交历史中**任意两条
事件**的因果关系，**允许跨 key** 比较。请求体携带 `first`、`second` 两个
`{replica_id, seq}` 标识（`replica_id` 非空、`seq` 为严格正整数、拒绝布尔值），
响应 `relation` 以 **first 相对 second** 的方向为准：`before`/`after`（因果
先/后）、`concurrent`（互不可比）或 `same`（同一事件）。先后关系附 `chain`：
从**较早到较晚**的事件标识链，链上后一事件**直接依赖**前一事件（本副本
`seq - 1` 或其他副本 clock 非零分量所指事件），取**边数最少**、再按
`(replica_id, seq)` 序列**字典序最小**（`seq` 按数值比较）的链；`same` 返回
单节点链；`concurrent` 时链为空，并各给出一个**自身 clock 大于对方**的分量及
双方数值（多个候选取副本 ID 最小者）。关系与证据取自**同一已提交历史**（不含
会话暂存），查询不改动历史或会话；历史或事件不存在返回 **404**，标识非法返回
**422**。

持久历史还支持**在线接纳新副本**（`POST /api/histories/{id}/replicas`）：已创建
的历史可以再加入一个副本，**已有导入会话无需先封闭**（开放与封闭会话都保留）。
请求体只带新副本 ID；新副本追加到 `replica_ids` **末尾**，并为**已提交事件**与
该历史**所有开放、封闭会话的暂存事件**的 clock **补入该副本的零分量**——原
`seq`、`key`、操作、值、已有 clock 分量以及会话 ID 与状态全部不变，因此已有
键的裁决与值不变。暂存允许缺依赖，接入**不跑完整批次校验**，会话没收齐也不
阻止接入。历史行与全部会话行在**同一个串行化事务**中更新，读取要么看到接入
前、要么看到接入后的完整副本集合（历史与会话取自同一快照，不会混用），与旧
汇入、暂存及提交并发不会丢事件，失败一律回滚。成功后返回**扩容后的原历史读取
结构**；新副本可从 `seq=1` 起经旧接口写入，之后所有写入仍按**完整 clock**
规则校验（clock 必须恰好覆盖全部副本，服务端**不自动补**请求中缺失的分量）；
接入结果随 SQLite 持久化，重启后保留；其他历史不受影响；**只支持加入，不支持
移除或改名**。历史不存在 **404**，副本已在集合中 **409**，空副本 ID 或加入后
超过 **8** 个副本 **422**。

最后还有一组**接收回执与稳定水位**接口
（`/api/histories/{id}/receipts`）：每个**接收副本**上报它已确认收到的**因果
前缀**（`known_clock`），服务端逐副本持久保存；查询时返回按接收副本 ID 排序
的全部回执，以及所有副本**共同接收**的 `stable_clock`——每个分量取**全部
接收方**（当前 `replica_ids` 的每一个成员）该分量的最小值，**从未上报的副本
按全零计**（因此它不出现在 `receipts` 列表中，但会把水位压成全零）。上报的
`known_clock` 必须**恰好覆盖当前副本集合**（无缺、无多），分量为非负整数且
**拒绝布尔值**，每个分量不超过该来源在**已提交事件**中的最大 seq；任一
**已接收事件**（自身 seq 不超过上报的本副本分量）的 clock 都不得超出该前缀，
否则 **422**。再次上报逐分量**不得倒退**，倒退返回 **409**
（`receipt_regressed`，列出倒退分量的已存/新值）；与已存回执**完全相同**的
重传视为幂等重发，**200** 成功。历史或接收副本不存在返回 **404**
（`history_not_found` / `receipt_replica_not_found`）。校验与保存基于**同一个
已加锁的已提交状态**，并发上报串行化、不会用较小进度覆盖较大进度。**暂存会话
中的事件不计入可接收范围**，失败不改任何数据。新副本接入时：全部旧回执
**补上新副本的零分量**，新接收方按全零计，扩容历史行、会话行与回执行在**同一
个串行化事务**中提交——查询永远不会混用扩容前后的副本集合与回执，因此扩容后
**水位可以下降**。回执持久化在同一个 SQLite 数据库/命名卷中，旧数据卷（不含
回执表）直接挂载时旧接口与数据完全不变，回执查询视为无上报。

## 启动

需要 Docker 与 Docker Compose v2（`docker compose` 子命令）。

```bash
docker compose up --build -d
```

停止：

```bash
docker compose down
```

## 访问地址与端口

| 用途 | 地址 |
| --- | --- |
| 健康检查 | <http://localhost:8000/health> |
| 裁决接口 | `POST http://localhost:8000/api/resolve` |
| 冲突处理事件接口 | `POST http://localhost:8000/api/resolve-conflict` |
| 增量补传接口 | `POST http://localhost:8000/api/catchup` |
| 创建持久历史 | `POST http://localhost:8000/api/histories` |
| 读取历史（批次+裁决） | `GET http://localhost:8000/api/histories/{id}` |
| 汇入离线事件 | `POST http://localhost:8000/api/histories/{id}/events` |
| 提交冲突处理 | `POST http://localhost:8000/api/histories/{id}/resolve-conflict` |
| 键迁移（同事务删除源键、写入目标键） | `POST http://localhost:8000/api/histories/{id}/migrate-key` |
| 撤除预演（只读，不落任何写入） | `GET http://localhost:8000/api/histories/{id}/withdraw-preview?replica_id=r1&seq=1` |
| 创建导入会话 | `POST http://localhost:8000/api/histories/{id}/import-sessions` |
| 查询会话状态与暂存事件 | `GET http://localhost:8000/api/histories/{id}/import-sessions/{sid}` |
| 暂存一批事件 | `POST http://localhost:8000/api/histories/{id}/import-sessions/{sid}/events` |
| 提交会话（汇入并封闭） | `POST http://localhost:8000/api/histories/{id}/import-sessions/{sid}/commit` |
| 查询仍需补取的缺口区间（只读，不提交） | `GET http://localhost:8000/api/histories/{id}/import-sessions/{sid}/missing-ranges` |
| 提取指定键的因果证据批次（只读，可直接重放） | `POST http://localhost:8000/api/histories/{id}/evidence-batch` |
| 解释两条事件的因果关系（只读，可跨 key） | `POST http://localhost:8000/api/histories/{id}/event-relation` |
| 接纳新副本（追加到 replica_ids，扩展全部 clock 与会话暂存） | `POST http://localhost:8000/api/histories/{id}/replicas` |
| 上报接收回执（已确认的因果前缀，持久保存） | `POST http://localhost:8000/api/histories/{id}/receipts` |
| 查询全部回执与共同稳定水位（只读） | `GET http://localhost:8000/api/histories/{id}/receipts` |
| Swagger UI | <http://localhost:8000/docs> |
| OpenAPI 文档 | <http://localhost:8000/openapi.json> |

容器内监听 `0.0.0.0:8000`，映射到宿主机 **8000** 端口。

## 请求格式

```json
{
  "replica_ids": ["r1", "r2"],
  "events": [
    {
      "replica_id": "r1",
      "seq": 1,
      "key": "name",
      "put": "alice",
      "clock": {"r1": 1, "r2": 0}
    },
    {
      "replica_id": "r2",
      "seq": 1,
      "key": "name",
      "delete": "delete",
      "clock": {"r1": 0, "r2": 1}
    }
  ]
}
```

约束：

- `replica_ids`：2～8 个非空且唯一的副本 ID。
- 每个事件属于其中某个副本；`seq` 从 1 开始，同一副本内不重复且必须连续
  （事件在请求中可乱序）。缺口诊断只按**实际提交的事件**进行，缺失序号以
  **闭区间概述**（如 `[2, 999999999999]`），绝不逐个分配、遍历或列出
  `1..最大 seq`，因此稀疏的 `10^12` 大序号也只产生一条轻量
  `non_contiguous_seq` 错误（422），不会耗尽内存；不设 seq 上限。
- `put`（字符串，允许空串）与 `delete`（值必须是字符串 `"delete"`）二选一。
- `clock` 必须覆盖**全部**副本且无多余分量，分量为非负整数；
  本副本分量必须等于本事件 `seq`。
- 时钟任何分量都不得指向不存在的事件（0 表示尚不知道该副本的事件）。
- 本副本上一事件（`seq - 1`）以及其他副本分量所指的最新事件，其时钟必须
  逐分量 `<=` 当前时钟且至少一个分量严格更小。
- 任何违例整批返回 **HTTP 422**，错误详情中带 `event_index`（请求 `events`
  数组下标）指出具体事件。

## 可执行 HTTP 示例

### 1. 冲突：两个副本并发改同一个 key（值相同也冲突）

```bash
curl -sS -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }'
```

响应（结果按 key 排序；冲突事件按 `replica_id`、`seq` 排序）：

```json
{
  "results": [
    {
      "key": "name",
      "state": "conflict",
      "events": [
        {"replica_id": "r1", "seq": 1, "op": "put", "value": "alice",
         "clock": {"r1": 1, "r2": 0}},
        {"replica_id": "r2", "seq": 1, "op": "put", "value": "alice",
         "clock": {"r1": 0, "r2": 1}}
      ]
    }
  ]
}
```

### 2. 同步后再写入：put 生效、旧 delete 被遮蔽

```bash
curl -sS -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r1", "seq": 2, "key": "session", "delete": "delete",
       "clock": {"r1": 2, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r1", "seq": 3, "key": "name", "put": "alice2",
       "clock": {"r1": 3, "r2": 1}}
    ]
  }'
```

响应：

```json
{
  "results": [
    {
      "key": "name",
      "state": "put",
      "value": "alice2",
      "event": {"replica_id": "r1", "seq": 3, "op": "put", "value": "alice2",
                "clock": {"r1": 3, "r2": 1}}
    },
    {
      "key": "session",
      "state": "deleted",
      "event": {"replica_id": "r1", "seq": 2, "op": "delete", "value": null,
                "clock": {"r1": 2, "r2": 0}}
    }
  ]
}
```

### 3. 非法批次：时钟本副本分量不等于 seq → 422

```bash
curl -sS -i -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "k", "put": "v",
       "clock": {"r1": 2, "r2": 0}}
    ]
  }'
```

返回 `422 Unprocessable Entity`，body 例如：

```json
{
  "detail": [
    {
      "code": "clock_self_component",
      "message": "clock['r1'] = 2 must equal seq = 1",
      "event_index": 0
    }
  ]
}
```

## 冲突处理事件接口 `POST /api/resolve-conflict`

把确认过的冲突处理结果写成一条**可重放事件**。请求沿用裁决接口的
`replica_ids`、`events`（先按原规则整批校验），并额外携带：

- `key`：要消解的冲突 key（非空字符串）；
- `replica_id`：写入副本，必须属于 `replica_ids`；
- `versions`：当时看到的冲突版本列表，每项含 `replica_id`、`seq`；
  列表须无重复且**恰好等于**该 key 当前全部极大事件，且至少两项
  （列表顺序任意）；
- 处理结果二选一：`put`（字符串，允许空串）或 `delete`（值必须为
  字符串 `"delete"`）。

接口行为：

- 新事件的 `seq` 接续写入副本现有末尾（该副本此前无事件时为 `1`）；
- 新事件时钟逐分量取所有冲突版本时钟与写入副本此前历史的最大值，
  再把写入副本自身分量推进到新 `seq`——因此新事件严格支配全部冲突版本；
- 响应包含完整新事件 `event`（与事件请求线格式一致：`put` 或
  `delete` 二选一），以及追加后的该 key 裁决 `result`
  （形状与 `/api/resolve` 的单条结果一致）；
- 即使各冲突版本的值相同，冲突也必须经此接口显式消解；其他 key 的
  裁决不受影响；
- 新事件并入 `events` 后可直接通过原批次校验，并能由 `/api/catchup`
  按依赖安排补传。

状态码：

- **200**：冲突已按确认结果消解；
- **409**：`versions` 已过期/不完整（`conflict_versions_mismatch`），
  或该 key 当前已不冲突/批次中不存在（`key_not_conflicted`）；
  返回 409 时不生成事件；
- **422**：原批次校验失败，或写入副本不在 `replica_ids`
  （`unknown_replica_id`）、版本列表重复
  （`duplicate_conflict_version`）、原批次已满 120 条（`batch_full`）
  等结构性错误。

### 4. 消解并发冲突：追加支配两分支的 put

`r1/1` 与 `r2/1` 并发写入 `name`（两分支互不可比），确认采用 `r1` 写入
的 `"alice2"`；版本列表可以乱序：

```bash
curl -sS -X POST http://localhost:8000/api/resolve-conflict \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ],
    "key": "name",
    "replica_id": "r1",
    "versions": [
      {"replica_id": "r2", "seq": 1},
      {"replica_id": "r1", "seq": 1}
    ],
    "put": "alice2"
  }'
```

响应（新事件为 `r1/2`，时钟并入两个冲突分支）：

```json
{
  "event": {
    "replica_id": "r1",
    "seq": 2,
    "key": "name",
    "put": "alice2",
    "clock": {"r1": 2, "r2": 1}
  },
  "result": {
    "key": "name",
    "state": "put",
    "value": "alice2",
    "event": {"replica_id": "r1", "seq": 2, "op": "put", "value": "alice2",
              "clock": {"r1": 2, "r2": 1}}
  }
}
```

把返回的 `event` 追加到原批次后再请求 `/api/resolve`，`name` 即变为
普通 `put`；请求 `/api/catchup` 时它会排在所依赖的冲突版本之后补传。

### 5. 冲突已被新写入遮蔽（版本列表过期）→ 409

批次中 `r1/2` 因果上支配 `r2/1`，`name` 已不再冲突。仍按旧冲突提交时
返回 `409 Conflict`，且不生成事件：

```bash
curl -sS -i -X POST http://localhost:8000/api/resolve-conflict \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "other", "put": "x",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice",
       "clock": {"r1": 2, "r2": 1}}
    ],
    "key": "name",
    "replica_id": "r1",
    "versions": [
      {"replica_id": "r1", "seq": 2},
      {"replica_id": "r2", "seq": 1}
    ],
    "put": "alice2"
  }'
```

```json
{
  "detail": [
    {
      "code": "key_not_conflicted",
      "message": "key 'name' is no longer in conflict"
    }
  ]
}
```

若 key 仍冲突但列表缺少某个极大版本、列入了非极大版本，或指向不存在的
`(replica_id, seq)`，返回 `409` 且错误码为 `conflict_versions_mismatch`。

## 增量补传接口 `POST /api/catchup`

请求沿用裁决接口的 `replica_ids`、`events`（按同样规则整批校验），额外携带
`known_clock`：目标对**每个**来源已连续接收的最高 seq。

```json
{
  "replica_ids": ["r1", "r2"],
  "known_clock": {"r1": 1, "r2": 0},
  "events": [ ... ]
}
```

`known_clock` 规则（违例返回 **422**）：

- 必须**恰好覆盖**所有 `replica_ids`，无缺失、无多余键；
- 每个分量为非负整数；
- 分量不得超过该来源在本批次中的最高 seq（批次中没有事件的来源只能填 `0`）；
- 若某个**已接收事件**（自身 `seq <= known_clock[本副本]`）的时钟任一分量
  超过对应的 `known_clock`，返回 `clock_exceeds_known_clock` 并附该事件的
  `event_index`。

缺失事件（`seq > known_clock[本副本]`）按**轮次**返回：

- 每一轮纳入当时所有依赖已满足的事件——其本副本上一事件（`seq - 1`）以及
  其他副本时钟分量所指事件，均已在目标端或在更早轮次中传输；
- 轮内事件按 `replica_id`、`seq` 排序；
- 事件保留原操作（`op`）、`value`（delete 时为 `null`）、`key` 和 `clock`；
- `progress` 为补传完成后目标对各来源的进度（即批次中各来源最高 seq）；
- 全部事件均已接收时 `rounds` 为空数组；
- 并发写入即使值相同也分别补传，不做去重或冲突合并。

### 6. 分轮补传：跨副本依赖逐轮解锁

目标已接收 `r1/1`（时钟 `{r1:1, r2:0}`），缺失 `r2/1`（依赖 `r1/1`）与
`r1/2`（依赖 `r2/1`），因此分为两轮：

```bash
curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 1, "r2": 0},
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice2",
       "clock": {"r1": 2, "r2": 1}}
    ]
  }'
```

响应（第 1 轮先补 `r2/1`，第 2 轮再补依赖它的 `r1/2`）：

```json
{
  "rounds": [
    [
      {"replica_id": "r2", "seq": 1, "key": "name", "op": "put",
       "value": "alice", "clock": {"r1": 1, "r2": 1}}
    ],
    [
      {"replica_id": "r1", "seq": 2, "key": "name", "op": "put",
       "value": "alice2", "clock": {"r1": 2, "r2": 1}}
    ]
  ],
  "progress": {"r1": 2, "r2": 1}
}
```

### 7. 并发同值写入也分别补传

```bash
curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 0, "r2": 0},
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }'
```

两个事件互不依赖，进入同一轮（按 `replica_id` 排序），即使值相同也都补传：

```json
{
  "rounds": [
    [
      {"replica_id": "r1", "seq": 1, "key": "name", "op": "put",
       "value": "alice", "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "op": "put",
       "value": "alice", "clock": {"r1": 0, "r2": 1}}
    ]
  ],
  "progress": {"r1": 1, "r2": 1}
}
```

### 8. 全部已接收：轮次为空

```bash
curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 1, "r2": 1},
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}}
    ]
  }'
```

```json
{
  "rounds": [],
  "progress": {"r1": 1, "r2": 1}
}
```

### 9. 非法请求：已接收事件的时钟超出 known_clock → 422

目标声称只连续收到 `r1/1` 且不知道 `r2` 的任何事件，但该事件时钟里
`r2 = 1`，与目标认知矛盾：

```bash
curl -sS -i -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 1, "r2": 0},
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}}
    ]
  }'
```

返回 `422 Unprocessable Entity`，body：

```json
{
  "detail": [
    {
      "code": "clock_exceeds_known_clock",
      "message": "event already received (replica 'r1', seq 1 <= known_clock 1) but its clock {'r1': 1, 'r2': 1} exceeds known_clock; components beyond target knowledge: {'r2': 1}",
      "event_index": 0
    }
  ]
}
```

其余 `known_clock` 违例的错误码：`known_clock_coverage`（缺失/多余分量）、
`known_clock_ahead_of_batch`（分量超过批次最高 seq）。

## 持久化历史接口 `/api/histories`

让多个调用方在**同一份持久历史**上汇入离线事件、提交冲突处理。数据库为本地
SQLite 文件，路径由环境变量 `HISTORY_DB_PATH` 决定；`docker compose` 启动时
固定为容器内 `/data/history.db` 并挂载命名卷 `history-data`，因此
`docker compose restart` / 容器重建后历史仍可读取。原三个无状态接口与
`docker compose up --build -d` 启动方式保持不变。

接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/histories` | 用合法的 `replica_ids` + `events` 创建历史，返回 `{"id": ...}`（201） |
| `GET` | `/api/histories/{id}` | 读取完整原格式批次（事件按 `replica_id`、`seq` 排序）及原裁决，二者来自同一已提交状态 |
| `POST` | `/api/histories/{id}/events` | 按 ID 汇入一批事件（请求体只有 `events`） |
| `POST` | `/api/histories/{id}/resolve-conflict` | 按 ID 提交原冲突处理参数（`key`、`replica_id`、`versions`、`put`/`delete`），不再传整个批次 |
| `GET` | `/api/histories/{id}/withdraw-preview?replica_id=...&seq=...` | **只读**预演撤除一条事件的波及范围（撤除集合、剩余事件、前后裁决），不改动历史 |
| `POST` | `/api/histories/{id}/evidence-batch` | **只读**提取指定 key 列表的完整写入记录及其递归因果闭包，返回可直接重放的原格式批次 |
| `POST` | `/api/histories/{id}/event-relation` | **只读**解释两条事件的因果关系（`before`/`after`/`concurrent`/`same`）及证据链，可跨 key |

汇入规则：

- 按 `(replica_id, seq)` 合并：同标识且内容完全相同的事件忽略（可重复发送，
  `clock` 键的顺序不影响相等）；同标识但 `key`、操作、值或**完整 `clock`**
  任一不同，返回 **409**（`event_content_mismatch`，错误中带汇入批次的
  `event_index`）——`clock` 缺少分量、多出分量（即使多出的分量为 `0`）、或
  任一分量值不同，都算异内容；
- 合并后的整份历史重新执行原校验（因果、连续性、clock 覆盖等），失败
  **422**；因此**未重复的新事件**若 `clock` 覆盖不合法（缺/多分量等），
  仍返回 **422** 而非 409；事件总数上限仍为 **120**（超出为 `batch_full`，422）；
- 每次写入**全部生效或全部不生效**（单事务），失败时历史保持原样。

冲突提交规则：

- 服务端在写锁内重新读取当前历史，`versions` 必须**恰好等于**该 key 当前全部
  极大事件；过期/不完整/key 已不冲突 → **409**（与无状态接口同一套错误码），
  且不生成事件；
- 新事件 `seq` 接续写入副本现有末尾（此前无事件时为 1），时钟并入全部冲突
  版本与写入副本历史，随后随整份历史一起原子写入；
- 并发提交同一冲突时只有一个能成功，其余拿到 409，不会重复占用序号。

不存在的 ID 在四个按 ID 访问的接口上都返回 **404**（`history_not_found`）；
撤除预演在历史存在但目标事件不存在（含 `replica_id` 不属于该历史）时另返回
`event_not_found`，同样是 **404**。

### 10. 创建历史并获得 ID

```bash
curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }'
```

```json
{ "id": "f82cdf528c99461c9a6becef9edddd1f" }
```

创建请求的约束与 `/api/resolve` 完全一致；非法批次返回 **422**，不会生成
历史。

### 11. 按 ID 读取：完整原格式批次 + 原裁决（同一已提交状态）

```bash
curl -sS http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f
```

事件在响应中按 `replica_id`、`seq` 排序（与创建时的提交顺序无关）：

```json
{
  "id": "f82cdf528c99461c9a6becef9edddd1f",
  "replica_ids": ["r1", "r2"],
  "events": [
    {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
     "clock": {"r1": 1, "r2": 0}},
    {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
     "clock": {"r1": 0, "r2": 1}}
  ],
  "results": [
    {
      "key": "name",
      "state": "conflict",
      "events": [
        {"replica_id": "r1", "seq": 1, "op": "put", "value": "alice",
         "clock": {"r1": 1, "r2": 0}},
        {"replica_id": "r2", "seq": 1, "op": "put", "value": "bob",
         "clock": {"r1": 0, "r2": 1}}
      ]
    }
  ]
}
```

### 12. 汇入离线事件：重复忽略、新事件合并

请求体只需要 `events`（不再传 `replica_ids` 或整个批次）。下面把已有的
`r1/1`（内容相同，忽略）与新事件 `r2/2` 一起汇入：

```bash
curl -sS -X POST \
  http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f/events \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 2, "key": "other", "put": "x",
       "clock": {"r1": 0, "r2": 2}}
    ]
  }'
```

响应与 `GET` 形状一致（`id`、`replica_ids`、排序后的完整 `events`、
`results`）。同标识异内容返回 **409**，且整批（含批次内其他合法事件）都不
写入：

```json
{
  "detail": [
    {
      "code": "event_content_mismatch",
      "message": "event with replica_id 'r1', seq 1 already exists in this history with different content; ...",
      "event_index": 0
    }
  ]
}
```

合并后违反因果/连续性等原校验，或超出 120 条上限时返回 **422**。

### 13. 汇入重复事件：完整 clock 逐字相等才忽略，否则 409 且整批不写入

判等比较的是事件的**完整内容**（`key`、操作、值与完整 `clock`）。先看一次
完全相同的重发——仅 `clock` 键的书写顺序不同仍然相等，事件被忽略：

```bash
curl -sS -X POST \
  http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f/events \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r2": 0, "r1": 1}}
    ]
  }'
```

返回 200，历史事件数不变。但同一 `(replica_id, seq)` 的重发只要 `clock`
多出一个分量——**即使多出的分量值为 `0`**——就算异内容。下例把异内容的
`r1/1` 与一条同批合法新事件 `r1/2` 一起汇入：

```bash
curl -sS -i -X POST \
  http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f/events \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0, "r3": 0}},
      {"replica_id": "r1", "seq": 2, "key": "other", "put": "x",
       "clock": {"r1": 2, "r2": 1}}
    ]
  }'
```

返回 `409 Conflict`，整批（包括同批合法的 `r1/2`）都不写入：

```json
{
  "detail": [
    {
      "code": "event_content_mismatch",
      "message": "event with replica_id 'r1', seq 1 already exists in this history with different content; stored {'replica_id': 'r1', 'seq': 1, 'key': 'name', 'put': 'alice', 'clock': {'r1': 1, 'r2': 0}}, incoming {'replica_id': 'r1', 'seq': 1, 'key': 'name', 'put': 'alice', 'clock': {'r1': 1, 'r2': 0, 'r3': 0}}",
      "event_index": 0
    }
  ]
}
```

同样地，重发事件的 `clock` **缺少**某个分量、或某个分量值与已存事件不同
（例如已存为 `{"r1": 0, "r2": 1}`，重发为 `{"r1": 1, "r2": 1}`），也返回
同样的 **409** 与 `event_index`，不会因缺键而报 500。

注意：覆盖校验只针对**未重复的新事件**。若新身份的事件自身 `clock` 覆盖
不合法（缺少或多出副本分量），仍由合并后的整批校验返回 **422**
（`clock_replica_coverage`，带该事件的 `event_index`），而不是 409。

### 14. 按 ID 提交冲突处理：核对当前 versions、接续序号

请求体就是原冲突处理参数（与 `/api/resolve-conflict` 相比少了
`replica_ids`、`events`）：

```bash
curl -sS -X POST \
  http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f/resolve-conflict \
  -H 'Content-Type: application/json' \
  -d '{
    "key": "name",
    "replica_id": "r1",
    "versions": [
      {"replica_id": "r2", "seq": 1},
      {"replica_id": "r1", "seq": 1}
    ],
    "put": "chosen"
  }'
```

```json
{
  "id": "f82cdf528c99461c9a6becef9edddd1f",
  "event": {
    "replica_id": "r1",
    "seq": 2,
    "key": "name",
    "put": "chosen",
    "clock": {"r1": 2, "r2": 1}
  },
  "result": {
    "key": "name",
    "state": "put",
    "value": "chosen",
    "event": {"replica_id": "r1", "seq": 2, "op": "put", "value": "chosen",
              "clock": {"r1": 2, "r2": 1}}
  }
}
```

生成的事件已持久化到历史中（`GET` 即可看到，事件按 `replica_id`、`seq`
排序）。再用旧的 `versions` 提交会返回 **409**（`key_not_conflicted` 或
`conflict_versions_mismatch`），不生成事件、不占用 `r2/2` 序号。

### 15. 重启容器后历史仍在

数据在命名卷 `history-data` 中，重启/重建容器（不改卷）后按原 ID 仍可读取：

```bash
docker compose restart
curl -sS http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f
```

如需连数据一起清除：`docker compose down -v`。

## 键迁移接口 `POST /api/histories/{id}/migrate-key`

把持久历史中一个 key 的当前 put 值搬到另一个空闲 key，并生成两条原格式、
可独立重放的事件。

请求体：

- `replica_id`：写入副本（必须属于该历史的副本集合）；
- `source_key`：源键，非空；其当前裁决必须是**唯一 put**；
- `target_key`：目标键，非空且与源键不同；其当前裁决必须是**无事件**或
  **唯一 delete**；
- `source_versions`：调用方观察到的源键当前全部极大事件，每项
  `{replica_id, seq}`，不可重复；
- `target_versions`：调用方观察到的目标键当前全部极大事件；目标键无事件时
  为 `[]`，唯一 delete 时为单元素列表。

两键在**同一个 `BEGIN IMMEDIATE` 事务**内、针对最新已提交状态一起核对：

- 先 delete 源键（seq = 写入副本当前最大 seq + 1），时钟 = 两键当前极大事件
  时钟与写入副本前序事件时钟的逐分量最大，再把本副本分量推进到新 seq；
- 再 put 目标键（seq 再 +1），值取源键唯一 put 的值，时钟在前者基础上推进，
  因此后一条依赖前一条，补传时必须分两轮。

两条事件一起通过原批次校验（副本/seq 连续、时钟覆盖与因果）并一起写入；成功后
源键裁决为 `deleted`、目标键裁决为带源值的 `put`，其他键裁决不变。失败一律
回滚，历史与暂存会话都不改动。

状态码：

- **200**：迁移成功，返回两条原格式事件与两键裁决（按 key 排序）；
- **404**：历史不存在（`history_not_found`）；
- **422**：两键相同（`source_target_key_equal`）、写入或版本副本不在副本集合
  （`unknown_replica_id`）、版本列表内有重复（`duplicate_conflict_version`）、
  或剩余事件名额不足两个（`batch_full`）；字段缺失/空键/seq 非正整数等同为
  schema 422；
- **409**：源键不是唯一 put（`source_key_not_movable`）、目标键不空且非唯一
  delete（`target_key_not_idle`）、任一版本列表与当前全部极大事件不一致
  （`migration_versions_mismatch`，包括空目标却给了版本、漏给或多给版本）。
  两键状态同时不符时两个错误码一次返回。

### 15a. 迁移 key：同一事务删源、写目标，再用返回事件补传

先创建历史：`r1/1` 写 `name = alice`，`r2/1` 已同步 `r1/1` 后写
`other = x`。

```bash
curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "other", "put": "x",
       "clock": {"r1": 1, "r2": 1}}
    ]
  }'
# {"id":"f82cdf528c99461c9a6becef9edddd1f"}
```

把 `name` 迁移到空闲键 `alias`，由副本 `r2` 写入。源键当前唯一极大事件是
`r1/1`；目标键没有任何事件，版本列表为空：

```bash
curl -sS -X POST \
  http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f/migrate-key \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_id": "r2",
    "source_key": "name",
    "target_key": "alias",
    "source_versions": [{"replica_id": "r1", "seq": 1}],
    "target_versions": []
  }'
```

```json
{
  "id": "f82cdf528c99461c9a6becef9edddd1f",
  "events": [
    {"replica_id": "r2", "seq": 2, "key": "name",
     "delete": "delete", "clock": {"r1": 1, "r2": 2}},
    {"replica_id": "r2", "seq": 3, "key": "alias",
     "put": "alice", "clock": {"r1": 1, "r2": 3}}
  ],
  "results": [
    {"key": "alias", "state": "put", "value": "alice",
     "event": {"replica_id": "r2", "seq": 3, "op": "put", "value": "alice",
               "clock": {"r1": 1, "r2": 3}}},
    {"key": "name", "state": "deleted",
     "event": {"replica_id": "r2", "seq": 2, "op": "delete", "value": null,
               "clock": {"r1": 1, "r2": 2}}}
  ]
}
```

`GET /api/histories/{id}` 此时可看到全部四条事件：`name` 已删除、`alias`
持有 `alice`，无关键 `other` 裁决不变。

**补传到另一副本。** 某副本只接收到迁移前的状态
（`known_clock = {"r1": 1, "r2": 1}`），把历史丢给 `/api/catchup` 即可得到
两轮计划——第一条 delete 先就绪，第二条 put 依赖同副本的 delete，下一轮才
就绪：

```bash
curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 1, "r2": 1},
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "other", "put": "x",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r2", "seq": 2, "key": "name",
       "delete": "delete", "clock": {"r1": 1, "r2": 2}},
      {"replica_id": "r2", "seq": 3, "key": "alias", "put": "alice",
       "clock": {"r1": 1, "r2": 3}}
    ]
  }'
```

```json
{
  "rounds": [
    [{"replica_id": "r2", "seq": 2, "key": "name", "op": "delete",
      "value": null, "clock": {"r1": 1, "r2": 2}}],
    [{"replica_id": "r2", "seq": 3, "key": "alias", "op": "put",
      "value": "alice", "clock": {"r1": 1, "r2": 3}}]
  ],
  "progress": {"r1": 1, "r2": 3}
}
```

若对端是另一份**持久历史**（同 `replica_ids`），也可以直接把响应中的两条
事件汇入；按 `(replica_id, seq)` 幂等合并，重发安全：

```bash
curl -sS -X POST \
  http://localhost:8000/api/histories/<对端历史ID>/events \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r2", "seq": 2, "key": "name",
     "delete": "delete", "clock": {"r1": 1, "r2": 2}},
    {"replica_id": "r2", "seq": 3, "key": "alias", "put": "alice",
     "clock": {"r1": 1, "r2": 3}}
  ]}'
```

注意两条事件必须与它们所覆盖的前序（示例中的 `r1/1`）一起到达；前序缺失时
汇入返回 422（`clock_points_to_missing_event`），补齐后重试即可，已汇入的
事件重发会被忽略。

**常见失败。** 迁移后再用同样的源键版本重试，源键已删除 → **409**
`source_key_not_movable`；目标键当前是 put → **409** `target_key_not_idle`；
调用方持有的版本过期（如源键之后又被并发写入）→ **409**
`migration_versions_mismatch`，这些失败都不占用 seq、不改历史，重新读取
两键当前极大版本后重试即可。

## 撤除预演接口 `GET /api/histories/{id}/withdraw-preview`

在**已提交的持久历史**上预演：如果撤除某一条离线事件，会连带波及哪些事件与
key。接口为**只读**：不删除任何事件、不修改、不重排序号或时钟，前后结果取自
同一次读取的同一已提交历史；旧接口与库中数据均不受影响。

查询参数：

- `replica_id`：目标事件所属副本（非空字符串）；
- `seq`：目标事件在该副本的序号（正整数）。

波及范围按与批次校验相同的**直接依赖边**反向闭包计算。一条事件的直接依赖是：

- 本副本的上一事件（`seq - 1`，存在时）；
- 时钟中**其他副本**的非零分量所指向的事件（分量为 `0` 表示不依赖该副本）。

撤除集合 = 目标事件 + 所有直接或间接依赖它的事件，**跨 key 也计入**；
与其无因果关系的**并发事件一律保留**（即使写的是同一个 key）。保留事件的
`seq`、`clock` 原样不变，不做重编。

响应：

- `id`：历史 ID；
- `withdrawn`：将被撤除事件的 `replica_id`、`seq`，按 `replica_id`、`seq`
  排序（目标本身包含在内）；
- `events`：剩余事件的**原线格式**（`put`/`delete` 二选一、原始 `clock`），
  顺序仍按 `replica_id`、`seq`；
- `changes`：裁决发生变化的 key，按 key 排序，每项含完整的前后裁决结构
  `before`/`after`（形状与 `/api/resolve` 的单条结果一致，按完整结构判变）；
  某 key 没有剩余事件时 `after` 为 `null`（表示该 key 无事件，**不等同于
  `deleted`**）；裁决结构不变的 key 不出现在 `changes` 中。

即使所有事件都被撤除，也返回 **200**，此时 `events` 为空数组。

状态码：

- **200**：预演完成（历史或 key 被完全撤空同样是 200）；
- **404**：历史不存在（`history_not_found`），或历史存在但没有该
  `replica_id`/`seq` 的事件（`event_not_found`，包括该 `replica_id` 不属于
  本历史的副本集合）；
- **422**：参数非法，如缺少/非数字/非正整数的 `seq`、空的 `replica_id`。

### 16. 创建历史拿到 ID，预演撤除，再读取确认历史未改动

先创建一份含三个副本、因果成链的历史：`r1/1` 写 `a`；`r2/1` 同步过 `r1/1`
后写 `b`（时钟 `r1 = 1`）；`r1/2` 依赖本副本上一事件和 `r2/1`，删除 `a`；
`r3/1` 完全离线并发写 `c`（各分量为 0），与前三者无因果关系。

```bash
curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2", "r3"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "a", "put": "x",
       "clock": {"r1": 1, "r2": 0, "r3": 0}},
      {"replica_id": "r2", "seq": 1, "key": "b", "put": "y",
       "clock": {"r1": 1, "r2": 1, "r3": 0}},
      {"replica_id": "r1", "seq": 2, "key": "a", "delete": "delete",
       "clock": {"r1": 2, "r2": 1, "r3": 0}},
      {"replica_id": "r3", "seq": 1, "key": "c", "put": "z",
       "clock": {"r1": 0, "r2": 0, "r3": 1}}
    ]
  }'
```

```json
{ "id": "0df8277d554f49629f627be82ea06322" }
```

用返回的 ID 预演撤除 `r1/1`（以下示例需要 `python3`；也可直接把上一步的 ID
手工填入 URL）：

```bash
HID=0df8277d554f49629f627be82ea06322
curl -sS -G "http://localhost:8000/api/histories/$HID/withdraw-preview" \
  --data-urlencode 'replica_id=r1' \
  --data-urlencode 'seq=1'
```

`r1/1`、依赖它的 `r2/1`、以及再依赖二者的 `r1/2` 全部计入撤除（跨 `a`、`b`
两个 key）；无关并发的 `r3/1` 原样保留。`a`、`b` 无剩余事件，`after` 为
`null`；`c` 的裁决不变，故不出现：

```json
{
  "id": "0df8277d554f49629f627be82ea06322",
  "withdrawn": [
    {"replica_id": "r1", "seq": 1},
    {"replica_id": "r1", "seq": 2},
    {"replica_id": "r2", "seq": 1}
  ],
  "events": [
    {"replica_id": "r3", "seq": 1, "key": "c", "put": "z",
     "clock": {"r1": 0, "r2": 0, "r3": 1}}
  ],
  "changes": [
    {
      "key": "a",
      "before": {
        "key": "a", "state": "deleted",
        "event": {"replica_id": "r1", "seq": 2, "op": "delete", "value": null,
                  "clock": {"r1": 2, "r2": 1, "r3": 0}}
      },
      "after": null
    },
    {
      "key": "b",
      "before": {
        "key": "b", "state": "put", "value": "y",
        "event": {"replica_id": "r2", "seq": 1, "op": "put", "value": "y",
                  "clock": {"r1": 1, "r2": 1, "r3": 0}}
      },
      "after": null
    }
  ]
}
```

预演不落任何写入——再读一次历史，4 个事件、裁决均与预演前完全一致：

```bash
curl -sS "http://localhost:8000/api/histories/$HID"
```

```json
{
  "id": "0df8277d554f49629f627be82ea06322",
  "replica_ids": ["r1", "r2", "r3"],
  "events": [
    {"replica_id": "r1", "seq": 1, "key": "a", "put": "x",
     "clock": {"r1": 1, "r2": 0, "r3": 0}},
    {"replica_id": "r1", "seq": 2, "key": "a", "delete": "delete",
     "clock": {"r1": 2, "r2": 1, "r3": 0}},
    {"replica_id": "r2", "seq": 1, "key": "b", "put": "y",
     "clock": {"r1": 1, "r2": 1, "r3": 0}},
    {"replica_id": "r3", "seq": 1, "key": "c", "put": "z",
     "clock": {"r1": 0, "r2": 0, "r3": 1}}
  ],
  "results": [ "……与创建时相同，未被修改……" ]
}
```

并发分支的对照：同一历史预演撤除 `r3/1` 只会撤掉它自己，`a`、`b` 不受影响；
若某 key 的两个极大事件互不可比（冲突），撤除其中一支只会让另一支成为唯一
裁决，另一支**不会**进入 `withdrawn`。错误示例：

```bash
# 历史不存在 -> 404 history_not_found
curl -sS -i -G "http://localhost:8000/api/histories/00000000000000000000000000000000/withdraw-preview" \
  --data-urlencode 'replica_id=r1' --data-urlencode 'seq=1'

# 事件不存在（r1 没有 seq 9）-> 404 event_not_found
curl -sS -i -G "http://localhost:8000/api/histories/$HID/withdraw-preview" \
  --data-urlencode 'replica_id=r1' --data-urlencode 'seq=9'

# seq 非正整数 -> 422
curl -sS -i -G "http://localhost:8000/api/histories/$HID/withdraw-preview" \
  --data-urlencode 'replica_id=r1' --data-urlencode 'seq=0'
```

## 导入会话接口 `/api/histories/{id}/import-sessions`

离线事件**分几次到达**时，不必每批都直接汇入历史：先按历史 ID 开一个**导入
会话**，把各批事件暂存进去，收齐后一次提交。会话与历史一样持久化在 SQLite
中，进程/容器重启后可继续暂存与提交；暂存中的事件**不影响**历史的读取、裁决
与撤除预演，只有提交成功才会改变已提交历史。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/histories/{id}/import-sessions` | 在该历史上创建空会话，返回 `{"id": ...}`（201） |
| `GET` | `/api/histories/{id}/import-sessions/{sid}` | 查询会话状态（`open`/`closed`）与暂存事件（原格式，按 `replica_id`、`seq` 排序） |
| `POST` | `/api/histories/{id}/import-sessions/{sid}/events` | 暂存一批事件（请求体只有 `events`；同标识重传按完整内容判等去重，原始条数可超过 120），返回更新后的会话状态 |
| `POST` | `/api/histories/{id}/import-sessions/{sid}/commit` | 把暂存事件一次汇入历史并封闭会话，返回与 `GET /api/histories/{id}` 相同的结构 |
| `GET` | `/api/histories/{id}/import-sessions/{sid}/missing-ranges` | **只读**查询仍需从来源补取的 `(replica_id, seq)` 缺口，合成连续闭区间返回；不提交、不改任何数据，封闭会话也可查 |

暂存规则（每次追加**全部生效或全部不生效**，失败不改暂存）：

- **新标识**事件（本批第一次出现、且尚未暂存的 `(replica_id, seq)`）只做
  **单事件**校验：字段合法（`put`/`delete` 二选一等）、副本属于该历史、
  `clock` 恰好覆盖全部副本、本副本分量等于 `seq`；违例 **422**；
- **允许前序事件或跨副本依赖尚未到达**（连续性、因果、依赖存在性都留到提交时
  检查），事件可按任意顺序、任意批次追加；
- **重传**事件（标识已暂存，或本批更早出现过）**不做**上面的单事件校验，而是
  按 `(replica_id, seq)` 与先到的那份做**完整内容**判等：比较 `key`、操作
  （put/delete）、值与**完整 `clock`**（键序无关）。内容完全相同的重复追加被
  忽略；`key`/操作/值不同，或 `clock` **缺少分量、多出分量（即使多出的分量为
  `0`）、任一分量值不同（含本副本分量不等于 `seq`）**，都算异内容，返回
  **409**（`event_content_mismatch`，带追加批次的 `event_index`）而**非 422**；
  因此重传一个缺/多分量的时钟不会再被误判为时钟覆盖错误；
- 每个会话至多暂存 **120 个不同标识**，容量只计**已暂存标识**加上**本批去重后
  的新标识**：121 份完全相同的合法事件只暂存一条；已满 120 条的会话再重传已有
  事件（哪怕一批超过 120 条原始数据）仍然成功；再加入任何新标识才超出容量，
  返回 **422**（`batch_full`）。注意：每次追加请求本身仍要求 `events` 至少
  1 条，但**没有**旧汇入接口那种单批 120 条的上限（上限按去重后的标识计）。

提交规则：

- 在写锁内读取历史**最新已提交**状态，把暂存事件按原汇入规则合并：与已提交
  事件同标识同内容的忽略，异内容 **409**；合并后重新执行原整批校验（连续性、
  因果、依赖存在性、clock 覆盖等）与 **120 条总上限**，违例 **422**。连续性
  校验只处理实际事件：暂存一个缺依赖的稀疏大序号（如 `10^12`）本身仍被接受，
  但提交时返回 **422** `non_contiguous_seq`，错误中以闭区间概述缺口
  （`[2, 999999999999]`），**不会**展开 `1..10^12` 耗尽内存，也不设 seq 上限；
- **失败时开放会话与已暂存数据原样保留**，补齐缺失事件后可对同一会话重试；
- 成功时汇入与封闭**在同一事务中生效**（与旧写接口并发也不会丢事件），响应
  就是历史的读取结构（`id`、`replica_ids`、排序后的完整 `events`、`results`）；
- 封闭后的会话再追加或再提交返回 **409**（`session_closed`），会话本身仍可
  查询；
- 历史不存在（`history_not_found`）或会话不存在（`session_not_found`，含会话
  属于其他历史）都返回 **404**。

### 17. 乱序分批暂存，收齐后一次汇入

历史上已提交 `r1/1`（`name = alice`）。`r1/3` 比它的前序 `r1/2` 先到达——
直接汇入历史会被拒（缺 `r1/2`），但可以先暂存：

```bash
# 创建历史（已有 r1/1）
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

# 开导入会话
SID=$(curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

# 第一批：r1/3 先到（它的前序 r1/2 还没到）——暂存接受
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 3, "key": "name", "put": "alice3",
       "clock": {"r1": 3, "r2": 0}}
    ]
  }'

# 第二批：前序 r1/2 到达
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice2",
       "clock": {"r1": 2, "r2": 0}}
    ]
  }'
```

每次追加都返回更新后的会话状态；也可以随时查询（暂存事件按 `replica_id`、
`seq` 排序，与到达顺序无关）：

```bash
curl -sS "http://localhost:8000/api/histories/$HID/import-sessions/$SID"
```

```json
{
  "id": "9c1f…",
  "history_id": "f82c…",
  "status": "open",
  "events": [
    {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice2",
     "clock": {"r1": 2, "r2": 0}},
    {"replica_id": "r1", "seq": 3, "key": "name", "put": "alice3",
     "clock": {"r1": 3, "r2": 0}}
  ]
}
```

收齐后一次提交，响应就是历史的读取结构，会话随之封闭：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/commit"
```

```json
{
  "id": "f82c…",
  "replica_ids": ["r1", "r2"],
  "events": [
    {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
     "clock": {"r1": 1, "r2": 0}},
    {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice2",
     "clock": {"r1": 2, "r2": 0}},
    {"replica_id": "r1", "seq": 3, "key": "name", "put": "alice3",
     "clock": {"r1": 3, "r2": 0}}
  ],
  "results": [
    {
      "key": "name",
      "state": "put",
      "value": "alice3",
      "event": {"replica_id": "r1", "seq": 3, "op": "put", "value": "alice3",
                "clock": {"r1": 3, "r2": 0}}
    }
  ]
}
```

### 18. 缺依赖提交失败（422），补齐后对同一会话重试

暂存只校验单事件，依赖是否齐全要到**提交**时才检查。下面暂存的 `r2/1` 时钟
指向 `r1 = 4`，但 `r1/4` 既不在历史里也不在会话中：

```bash
SID2=$(curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID2/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "other", "put": "x",
       "clock": {"r1": 4, "r2": 1}}
    ]
  }'

# 提交：依赖的 r1/4 不存在 -> 422
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID2/commit"
```

返回 `422 Unprocessable Entity`，且**开放会话与暂存数据原样保留**：

```json
{
  "detail": [
    {
      "code": "clock_points_to_missing_event",
      "message": "clock['r1'] = 4, but replica 'r1' has no event with seq 4",
      "event_index": 3
    }
  ]
}
```

（`event_index` 是合并后整批——已提交事件加暂存事件——中的下标。）

把缺失的 `r1/4` 补进**同一会话**，重新提交即成功（`r2/1` 依赖的 `r1/4` 随
同一批汇入）：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID2/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 4, "key": "name", "put": "alice4",
       "clock": {"r1": 4, "r2": 0}}
    ]
  }'

# 重试提交 -> 200，会话封闭
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID2/commit"

# 封闭后再追加或再提交 -> 409
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID2/commit"
```

```json
{
  "detail": [
    {
      "code": "session_closed",
      "message": "import session '…' is already closed; it accepts no further appends or commits"
    }
  ]
}
```

暂存期间历史的读取与裁决完全不受会话影响；只有提交成功后，
`GET /api/histories/{id}` 才会包含汇入的事件。会话与历史一样落在 SQLite
中，`docker compose restart` 后开放会话可继续暂存与提交。

### 19. 暂存重传：按完整内容判等，缺/多零分量或本分量改变是 409；121 份相同只存一条

暂存对**已暂存及同批重复的标识**只做完整内容判等，不再套用时钟覆盖等单事件
校验。先准备一份历史（已提交 `r1/1`）并开会话、暂存一条合法的 `r1/2`：

```bash
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "k", "put": "a",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
SID=$(curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 2, "key": "k", "put": "b",
       "clock": {"r1": 2, "r2": 1}}
    ]
  }'
```

**完全相同的重传被忽略**（仅 `clock` 键的书写顺序不同也算相同），暂存条数
不变：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 2, "key": "k", "put": "b",
       "clock": {"r2": 1, "r1": 2}}
    ]
  }'
```

**同一标识的重传只要 `clock` 多出一个零分量、缺少分量，或本副本分量被改写，
都算异内容 → 409（而不是时钟覆盖/本分量的 422）**：

```bash
# 多出零分量 r3=0
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 2, "key": "k", "put": "b",
       "clock": {"r1": 2, "r2": 1, "r3": 0}}
    ]
  }'

# 缺少 r2 分量
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r1", "seq": 2, "key": "k", "put": "b",
     "clock": {"r1": 2}}
  ]}'

# 本副本分量被改成 3（!= seq 2）
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r1", "seq": 2, "key": "k", "put": "b",
     "clock": {"r1": 3, "r2": 1}}
  ]}'
```

三次都返回 `409 Conflict`，错误形状相同，并指出是追加批次的第 0 条：

```json
{
  "detail": [
    {
      "code": "event_content_mismatch",
      "message": "event with replica_id 'r1', seq 2 is already present in this session with different content; prior {...}, incoming {...}",
      "event_index": 0
    }
  ]
}
```

对比：**新标识**事件的时钟缺/多分量仍是 **422**（`clock_replica_coverage`），
因为单事件校验只对新标识生效：

```bash
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r1", "seq": 3, "key": "k", "put": "c",
     "clock": {"r1": 3}}
  ]}'
# 422 Unprocessable Entity，code=clock_replica_coverage
```

**121 份完全相同的合法新事件只暂存一条**（暂存容量按去重后的不同标识计数，
追加请求本身没有单批 120 条上限；下面用 `python3` 生成 121 份的请求体）：

```bash
python3 - <<'EOF' > /tmp/dup121.json
import json
one = {"replica_id": "r2", "seq": 1, "key": "q", "put": "z",
       "clock": {"r1": 1, "r2": 1}}
print(json.dumps({"events": [one] * 121}))
EOF

curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' -d @/tmp/dup121.json
```

返回 200，会话中只有此前暂存的 `r1/2` 与这一条 `r2/1`，共 **2** 条（不是 122
条）。同理，已经存满 120 个不同标识的会话，再重传这 120 条（哪怕一批 121 份）
依然返回 200；只有再出现一个**新标识**才返回 **422** `batch_full`。任一追加
失败时，本次批次（连同其中合法的其他事件）全部不落，已暂存事件、会话状态与
历史都保持原样。

## 导入缺口查询接口 `GET /api/histories/{id}/import-sessions/{sid}/missing-ranges`

分批暂存时，可以**只读**地查询：相对来源，这个会话还缺哪些 `(replica_id, seq)`
事件需要补取。接口**不提交会话**、不封闭会话，也不改动历史、暂存与状态；开放
与**已封闭**的会话都可以查询。

计算在**同一已提交快照**上进行：

1. 先把历史的已提交事件与会话的暂存事件按 `(replica_id, seq)` 合并，逐份取
   同一已提交状态。同一标识在历史和暂存中都存在时，按**原完整内容**
   （`key`、操作、值与完整 `clock`，键序无关）判等；内容相同视为同一条，
   内容不同则返回 **409**（`event_content_mismatch`），一次性列出**全部**冲突
   的 `replica_id`、`seq`（按二者排序），此时**不返回缺口清单**。
2. 没有冲突时，对每个副本计算需求**上界** = 合并事件中该副本自身 `seq` 的
   最大值，与所有合并事件 `clock` 对该副本分量的最大值，两者取大。
3. 把闭区间 `1..上界` 中**已有标识之外**的序号合成为**最大的连续区间**
   （`start_seq`、`end_seq` 均为闭区间端点），按 `replica_id`、`start_seq`
   排序后放在 `missing_ranges` 中返回。

缺口序号**绝不逐个展开**：即使暂存事件的 `seq`/clock 分量为 `10^12`，响应也
只有一个区间。没有任何事件引用、自身也没有事件的副本上界为 `0`，不产生区间。
**无缺口时返回 200 与空列表——这只表示标识齐全，不保证随后提交通过**（时钟
因果、连续性、120 条上限等仍在提交时检查）。

状态码：

- **200**：返回 `{"missing_ranges": [...]}`（无缺口时为空数组）；
- **409**：暂存与已提交事件存在同标识异内容，`detail` 列出全部冲突标识，
  不含缺口清单；
- **404**：历史不存在（`history_not_found`）或会话不存在
  （`session_not_found`，含会话属于其他历史）。

### 20. 查缺口 → 补齐 → 再查为空 → 提交

历史上已提交 `r1/1`（clock `{r1:1, r2:0}`）。开一个导入会话，先暂存乱序到达的
`r1/3` 与 `r2/1`，其中 `r2/1` 的时钟声称已知道 `r1` 到 `4`：

```bash
# 创建历史（已提交 r1/1）
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "k", "put": "a",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

# 开导入会话
SID=$(curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

# 暂存 r1/3 与 r2/1（r2/1 的 clock 引用了 r1/4）
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 3, "key": "k", "put": "c",
       "clock": {"r1": 3, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "k", "put": "b",
       "clock": {"r1": 4, "r2": 1}}
    ]
  }' > /dev/null

# 第一次查缺口
curl -sS "http://localhost:8000/api/histories/$HID/import-sessions/$SID/missing-ranges"
```

`r1` 的上界取自身最大 seq `3` 与 clock 分量最大值 `4` 的较大者 `4`；`1..4` 中
已有 `1`、`3`，故缺 `2` 与 `4`，合成两个闭区间；`r2` 上界为 `1` 且已有 `1`，
无缺口。区间按 `replica_id`、`start_seq` 排序：

```json
{
  "missing_ranges": [
    {"replica_id": "r1", "start_seq": 2, "end_seq": 2},
    {"replica_id": "r1", "start_seq": 4, "end_seq": 4}
  ]
}
```

把缺的 `r1/2`、`r1/4` 补进**同一会话**后再查，得到空列表（仅表示标识齐全）：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 2, "key": "k", "put": "b2",
       "clock": {"r1": 2, "r2": 0}},
      {"replica_id": "r1", "seq": 4, "key": "k", "put": "d",
       "clock": {"r1": 4, "r2": 1}}
    ]
  }' > /dev/null

curl -sS "http://localhost:8000/api/histories/$HID/import-sessions/$SID/missing-ranges"
```

```json
{"missing_ranges": []}
```

整个过程中查询不产生任何写入（可随时再 `GET` 历史或会话核对）。标识齐全后再
正常提交；空列表本身不保证提交成功——若补齐事件的时钟违反因果，提交仍会返回
**422**，开放会话与暂存原样保留。

### 21. 大序号不展开：`10^12` 也只是一个区间

缺口只以区间返回，响应大小与缺口长度无关。暂存一条引用 `r1 = 1000000000000`
的事件，缺口仍是单个闭区间 `[2, 1000000000000]`，而不是一长串序号：

```bash
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "k", "put": "a",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
SID=$(curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r2", "seq": 1, "key": "k", "put": "b",
     "clock": {"r1": 1000000000000, "r2": 1}}
  ]}' > /dev/null

curl -sS "http://localhost:8000/api/histories/$HID/import-sessions/$SID/missing-ranges"
```

```json
{
  "missing_ranges": [
    {"replica_id": "r1", "start_seq": 2, "end_seq": 1000000000000}
  ]
}
```

### 21a. 稀疏大序号提交：`10^12` 也只产生一条 422，区间概述缺口

暂存允许缺依赖的大序号——即使暂存的 `r1/10^12` 与已提交的 `r1/1` 之间缺了
`10^12 - 2` 个序号，暂存仍然成功；但**提交**时整批连续性校验必须拒绝它。
缺口只以**闭区间**概述（`[2, 999999999999]`），服务端绝不逐个分配、遍历或
列出 `1..10^12`，因此请求瞬间返回一条 `non_contiguous_seq` 422，而不会耗尽
内存；系统**不设 seq 上限**。

```bash
# 历史含 r1/1（clock (r1,r2)=(1,0)）
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "k", "put": "a",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
SID=$(curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

# 暂存稀疏的 r1/10^12（缺依赖）——暂存接受
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r1", "seq": 1000000000000, "key": "k", "put": "b",
     "clock": {"r1": 1000000000000, "r2": 0}}
  ]}' > /dev/null

# 提交 -> 422，缺口只概述为一个闭区间
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/commit"
```

```json
{
  "detail": [
    {
      "code": "non_contiguous_seq",
      "message": "seqs for replica 'r1' must be unique and start at 1 with no gaps; got sorted seqs [1, 1000000000000] (missing closed interval(s): [2, 999999999999])"
    }
  ]
}
```

失败提交不改任何状态：历史仍只有 `r1/1`、会话仍为 `open` 且仍暂存着
`r1/10^12`，可随时用缺口查询核对（返回的也只是区间 `[2, 999999999999]`）。
共用同一份整批校验的其他接口（`/api/resolve`、`/api/catchup`、
`/api/resolve-conflict`、`POST /api/histories` 与
`POST /api/histories/{id}/events`）对同类稀疏输入同样返回这条 422。

**小缺口对照**：只缺一个序号时，区间是单点闭区间。历史含 `r1/1`、暂存
`r1/3`，提交返回的 `non_contiguous_seq` 消息中带 `missing closed interval(s):
[2, 2]`；把缺失的 `r1/2` 补进同一会话后再提交即成功封闭（见例 17/18）。

### 22. 同标识异内容：409 列出全部冲突，不返回清单

暂存了一条与已提交事件同 `(replica_id, seq)` 但值不同的事件时，缺口查询返回
**409**；若同时存在多处冲突（甚至伴随其他合法暂存事件），`detail` 会按
`replica_id`、`seq` 排序列出**全部**冲突标识，且响应中没有 `missing_ranges`：

```bash
curl -sS -i "http://localhost:8000/api/histories/$HID/import-sessions/$SID/missing-ranges"
```

```json
{
  "detail": [
    {
      "code": "event_content_mismatch",
      "message": "staged event with replica_id 'r1', seq 1 differs in content from the event with the same identity already committed in this history; resolve the conflicting retransmission before the session can be completed",
      "replica_id": "r1",
      "seq": 1
    }
  ]
}
```

封闭后的会话仍可查询其缺口；历史或会话不存在时分别返回 **404**
`history_not_found` / `session_not_found`（会话属于其他历史同样按不存在处理）。

## 因果证据批次接口 `POST /api/histories/{id}/evidence-batch`

有时只需把**某几个 key 的完整写入记录**交给另一方核对，而不必整份历史外传。
本接口从一份**已提交的持久历史**中提取这些 key 的事件及其**因果证据**，返回一
份**可独立重放**的原格式批次。接口为**只读**：不改动历史、不改动导入会话、不
影响任何旧接口，可重复查询。

请求体只含 `keys`：一个**非空数组**，元素为**非空字符串且无重复**：

```json
{ "keys": ["a"] }
```

提取规则：

1. 取所选 key 的**全部**事件作为种子——包括后来在该 key 上被覆盖的 put 与
   delete（不是只取当前极大版本）。
2. 对种子集合按与批次校验相同的**直接依赖边**递归求闭包，每条纳入事件的：
   - **本副本上一事件**（`seq - 1`，存在时）；
   - 时钟中**其他副本非零分量**所指事件；

   都一并纳入。依赖**跨 key 也保留**：依赖事件若写的是别的 key，该 key 就作为
   **辅助 key**随批次带出；闭包之外的事件（例如因果无关的并发写入）一律不加入。
3. 因为每条纳入事件的本副本前缀与跨副本分量所指事件都在批次内，事件可保持
   **原始 `seq` 与 `clock`**，无需重编；闭包是每个副本的因果前缀，天然连续。

响应就是原批次形状（`{"replica_ids": ..., "events": ...}`）：

- `replica_ids` 为历史的**原副本列表**（即使闭包中某副本没有事件，其位置与
  clock 中的零分量也原样保留）；
- `events` 为**原线格式**事件（`put`/`delete` 二选一、原始 `clock`），按
  `replica_id`、`seq` 排序；
- 响应体本身通过原完整校验，可**直接** POST 给 `/api/resolve`、
  `/api/catchup`（再补一个 `known_clock`）和 `/api/resolve-conflict` 重放；
- 每个**所选 key** 在证据批次上的裁决与在**原完整历史**上的裁决完全一致
  （所选 key 的事件一个不少）；**辅助 key** 只提供依赖证据，其在证据批次上的
  裁决**允许不同**（与该 key 因果无关、闭包之外的事件不会被带出）。

状态码：

- **200**：返回证据批次；
- **404**：历史不存在（`history_not_found`），或任一**所选 key** 在该历史中
  没有任何事件（`key_not_found`，错误项带 `key` 字段；多个 key 都不存在时一次
  性按 key 排序列出全部）；
- **422**：`keys` 非法——字段缺失、不是数组、数组为空、元素为空串或非字符串
  （请求体校验错误），或元素有重复（`duplicate_key`）。参数语义校验先于历史
  查询：即使历史 ID 不存在，重复 key 仍返回 422。

### 23. 跨 key 依赖：提取后直接调用原裁决接口

先创建一份三副本历史：`r1/1` 写 key `a`；`r2/1` 同步过 `r1/1` 后写**另一个
key** `dep`（clock `r1 = 1`，这是跨 key 依赖）；`r1/2` 依赖本副本上一事件与
`r2/1`，把 `a` 改成 `"a2"`；`r3/1` 在 key `other` 上完全离线并发写入，与前
三者无因果关系。

```bash
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2", "r3"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "a", "put": "a1",
       "clock": {"r1": 1, "r2": 0, "r3": 0}},
      {"replica_id": "r2", "seq": 1, "key": "dep", "put": "d1",
       "clock": {"r1": 1, "r2": 1, "r3": 0}},
      {"replica_id": "r1", "seq": 2, "key": "a", "put": "a2",
       "clock": {"r1": 2, "r2": 1, "r3": 0}},
      {"replica_id": "r3", "seq": 1, "key": "other", "put": "z",
       "clock": {"r1": 0, "r2": 0, "r3": 1}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
```

只提取 key `a`：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/evidence-batch" \
  -H 'Content-Type: application/json' \
  -d '{"keys": ["a"]}'
```

返回的批次含 `a` 的两条完整记录（包括被覆盖的 `r1/1` put `"a1"`）、跨 key
依赖事件 `r2/1`（辅助 key `dep`，只作证据）；无关并发的 `r3/1` **不**出现：

```json
{
  "replica_ids": ["r1", "r2", "r3"],
  "events": [
    {"replica_id": "r1", "seq": 1, "key": "a", "put": "a1",
     "clock": {"r1": 1, "r2": 0, "r3": 0}},
    {"replica_id": "r1", "seq": 2, "key": "a", "put": "a2",
     "clock": {"r1": 2, "r2": 1, "r3": 0}},
    {"replica_id": "r2", "seq": 1, "key": "dep", "put": "d1",
     "clock": {"r1": 1, "r2": 1, "r3": 0}}
  ]
}
```

响应体本身就是合法的原裁决请求，保存后可**直接**调用原裁决接口（所选 key `a`
的裁决与原历史一致，为 put `"a2"`；辅助 key `dep` 仅提供依赖证据）：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/evidence-batch" \
  -H 'Content-Type: application/json' \
  -d '{"keys": ["a"]}' > /tmp/evidence.json

curl -sS -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  --data @/tmp/evidence.json
```

```json
{
  "results": [
    {"key": "a", "state": "put", "value": "a2",
     "event": {"replica_id": "r1", "seq": 2, "op": "put", "value": "a2",
               "clock": {"r1": 2, "r2": 1, "r3": 0}}},
    {"key": "dep", "state": "put", "value": "d1",
     "event": {"replica_id": "r2", "seq": 1, "op": "put", "value": "d1",
               "clock": {"r1": 1, "r2": 1, "r3": 0}}}
  ]
}
```

同一批事件也能直接喂给补传接口——只需在原批次上补一个 `known_clock`，依赖按
轮解锁（`r1/1` → `r2/1` → `r1/2`）：

```bash
python3 - <<'EOF'
import json
batch = json.load(open("/tmp/evidence.json"))
batch["known_clock"] = {"r1": 0, "r2": 0, "r3": 0}
json.dump(batch, open("/tmp/evidence-catchup.json", "w"))
EOF

curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  --data @/tmp/evidence-catchup.json
```

若所选 key 在原历史中处于冲突，证据批次同样可以直接交给
`/api/resolve-conflict`（补上 `key`、`replica_id`、`versions` 与处理结果即可）。
辅助 key `dep` 在证据批次中的裁决只随依赖证据计算：原历史中任何与
`r2/1` 并发、闭包之外的 `dep` 写入都不会被带出，因此其批次裁决可能与原历史
不同，这不影响所选 key `a` 的裁决一致性。

错误示例：

```bash
# 历史不存在 -> 404 history_not_found
curl -sS -i -X POST \
  http://localhost:8000/api/histories/00000000000000000000000000000000/evidence-batch \
  -H 'Content-Type: application/json' -d '{"keys": ["a"]}'

# 所选 key 在历史中无任何事件 -> 404 key_not_found（带 key 字段）
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/evidence-batch" \
  -H 'Content-Type: application/json' -d '{"keys": ["nope"]}'

# keys 有重复 -> 422 duplicate_key（空数组、空串、非字符串同为 422）
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/evidence-batch" \
  -H 'Content-Type: application/json' -d '{"keys": ["a", "a"]}'
```

```json
{
  "detail": [
    {
      "code": "key_not_found",
      "message": "this history has no event with key 'nope'",
      "key": "nope"
    }
  ]
}
```

整个查询过程不产生任何写入：再 `GET` 历史，事件与裁决均与提取前一致；已有的
导入会话（开放或封闭）及其暂存内容也不受影响。

## 事件因果关系接口 `POST /api/histories/{id}/event-relation`

解释**同一份已提交持久历史**中任意两条事件的因果关系，**允许跨 key** 比较。
接口为**只读**：关系与证据取自同一次读取的已提交历史（**不含**导入会话中尚未
提交的暂存事件），不改动历史、导入会话与任何旧接口，可重复查询。

请求体携带两个事件标识 `first`、`second`，各为 `{replica_id, seq}`：

```json
{
  "first": {"replica_id": "r2", "seq": 1},
  "second": {"replica_id": "r1", "seq": 2}
}
```

- `replica_id`：非空字符串；
- `seq`：正整数（严格校验：拒绝布尔值、字符串、浮点数、0 与负数）。

响应的 `relation` 以 **first 相对 second** 的方向为准：

- `before` / `after`：first 在因果上先于 / 后于 second（向量时钟严格偏序）。
  响应附 `chain`：从**较早事件到较晚事件**的事件标识链（与方向无关——`after`
  时同样从 second 到 first）。链上后一事件**直接依赖**前一事件，即前一事件是
  后一事件**本副本的 `seq - 1`**，或是后一事件 clock 中**其他副本非零分量**
  所指的事件。存在多条链时取**边数最少**者；仍并列时取标识 `(replica_id, seq)`
  序列**字典序最小**者（`replica_id` 按字符串、`seq` 按数值比较）。
- `concurrent`：两者互不可比。`chain` 为空数组；`first_ahead` 与
  `second_ahead` 各给出一个**自身 clock 大于对方**的分量及双方在该分量上的
  数值（`first_value`/`second_value`）；有多个候选分量时取**副本 ID 最小**者。
- `same`：两个标识指向同一事件，`chain` 为单节点链。

状态码：

- **200**：返回 `{"id", "relation", "chain", ...}`；
- **404**：历史不存在（`history_not_found`），或任一标识在历史中没有对应事件
  （`event_not_found`，先检查 `first` 再检查 `second`）；
- **422**：标识非法（缺字段、空 `replica_id`、`seq` 为布尔/字符串/非正整数、
  多余字段等）；请求体校验先于历史查询，历史不存在时非法请求体仍返回 422。

### 24. 跨键先后：依赖链作为证据

先创建一份三副本历史：`r1/1` 写 key `a`；`r2/1` 同步过 `r1/1` 后写**另一个
key** `dep`；`r1/2` 依赖本副本上一事件与 `r2/1`，把 `a` 改成 `"a2"`；`r2/2`
再依赖 `r1/2` 写 `dep`；`r3/1` 在 key `other` 上完全离线并发写入：

```bash
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2", "r3"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "a", "put": "a1",
       "clock": {"r1": 1, "r2": 0, "r3": 0}},
      {"replica_id": "r2", "seq": 1, "key": "dep", "put": "d1",
       "clock": {"r1": 1, "r2": 1, "r3": 0}},
      {"replica_id": "r1", "seq": 2, "key": "a", "put": "a2",
       "clock": {"r1": 2, "r2": 1, "r3": 0}},
      {"replica_id": "r2", "seq": 2, "key": "dep", "put": "d2",
       "clock": {"r1": 2, "r2": 2, "r3": 0}},
      {"replica_id": "r3", "seq": 1, "key": "other", "put": "z",
       "clock": {"r1": 0, "r2": 0, "r3": 1}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
```

比较 `dep` 上的 `r2/1` 与 `a` 上的 `r1/2`——**跨 key** 也有因果关系：
`r1/2` 的 clock 中 `r2 = 1` 直接指向 `r2/1`，因此 `r2/1` 先于 `r1/2`，证据链
就是这条直接依赖边：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/event-relation" \
  -H 'Content-Type: application/json' \
  -d '{"first": {"replica_id": "r2", "seq": 1},
       "second": {"replica_id": "r1", "seq": 2}}'
```

```json
{
  "id": "0df8…",
  "relation": "before",
  "chain": [
    {"replica_id": "r2", "seq": 1},
    {"replica_id": "r1", "seq": 2}
  ]
}
```

方向以 `first` 为准：交换两个标识得到 `after`，但链仍从**较早**的 `r2/1` 到
**较晚**的 `r1/2`：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/event-relation" \
  -H 'Content-Type: application/json' \
  -d '{"first": {"replica_id": "r1", "seq": 2},
       "second": {"replica_id": "r2", "seq": 1}}'
```

```json
{
  "id": "0df8…",
  "relation": "after",
  "chain": [
    {"replica_id": "r2", "seq": 1},
    {"replica_id": "r1", "seq": 2}
  ]
}
```

从 `r1/1` 到 `r2/2` 有两条等长（2 条边）的依赖链——经 `r1/2` 或经 `r2/1`；
取边数最少后仍并列，按标识序列字典序取较小者（第二节点 `("r1", 2)` 小于
`("r2", 1)`）：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/event-relation" \
  -H 'Content-Type: application/json' \
  -d '{"first": {"replica_id": "r1", "seq": 1},
       "second": {"replica_id": "r2", "seq": 2}}'
```

```json
{
  "id": "0df8…",
  "relation": "before",
  "chain": [
    {"replica_id": "r1", "seq": 1},
    {"replica_id": "r1", "seq": 2},
    {"replica_id": "r2", "seq": 2}
  ]
}
```

同一事件返回 `same` 与单节点链：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/event-relation" \
  -H 'Content-Type: application/json' \
  -d '{"first": {"replica_id": "r3", "seq": 1},
       "second": {"replica_id": "r3", "seq": 1}}'
```

```json
{
  "id": "0df8…",
  "relation": "same",
  "chain": [{"replica_id": "r3", "seq": 1}]
}
```

### 25. 长短路径汇合：始终取直接依赖边数最少的链

长短两条依赖链可能在中间节点汇合，此时答案**不得**随事件集合的遍历顺序
（哈希种子）漂移。以 `(r1, r2)` 记 clock，下面五条合法 put 事件为
`r1/1=(1,0)`、`r2/1=(1,1)`、`r1/2=(2,1)`、`r2/2=(2,2)`、`r2/3=(2,3)`：

```bash
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "k", "put": "a",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "k", "put": "b",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r1", "seq": 2, "key": "k", "put": "c",
       "clock": {"r1": 2, "r2": 1}},
      {"replica_id": "r2", "seq": 2, "key": "k", "put": "d",
       "clock": {"r1": 2, "r2": 2}},
      {"replica_id": "r2", "seq": 3, "key": "k", "put": "e",
       "clock": {"r1": 2, "r2": 3}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
```

从 `r1/1` 到 `r2/3` 同时存在绕行长链 `r1/1 → r2/1 → r1/2 → r2/2 → r2/3`
（4 条边，`r1/2` 与 `r2/3` 都是汇合点）和短链 `r1/1 → r1/2 → r2/3`
（2 条边）。`first=r1/1`、`second=r2/3` 必须返回 `before` 与这条短链：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/event-relation" \
  -H 'Content-Type: application/json' \
  -d '{"first": {"replica_id": "r1", "seq": 1},
       "second": {"replica_id": "r2", "seq": 3}}'
```

```json
{
  "id": "1a7e…",
  "relation": "before",
  "chain": [
    {"replica_id": "r1", "seq": 1},
    {"replica_id": "r1", "seq": 2},
    {"replica_id": "r2", "seq": 3}
  ]
}
```

交换两个标识返回 `after`，链仍然从较早事件排到较晚事件：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/event-relation" \
  -H 'Content-Type: application/json' \
  -d '{"first": {"replica_id": "r2", "seq": 3},
       "second": {"replica_id": "r1", "seq": 1}}'
```

```json
{
  "id": "1a7e…",
  "relation": "after",
  "chain": [
    {"replica_id": "r1", "seq": 1},
    {"replica_id": "r1", "seq": 2},
    {"replica_id": "r2", "seq": 3}
  ]
}
```

最短边数由反向 BFS 保证（首次到达即最短距离），与集合遍历顺序无关；边数
并列时再在每一步选择距终点少一条边的后继里 `(replica_id, seq)` 最小者
（`seq` 按数值比较），因此同一历史的答案不受 Python 哈希种子影响。

### 26. 同值并发：空链与双方各自领先的分量

两个副本并发地把同一个 key 写成**相同的值**（裁决上仍是冲突），因果关系为
`concurrent`：

```bash
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl -sS -X POST "http://localhost:8000/api/histories/$HID/event-relation" \
  -H 'Content-Type: application/json' \
  -d '{"first": {"replica_id": "r1", "seq": 1},
       "second": {"replica_id": "r2", "seq": 1}}'
```

`chain` 为空；`first_ahead`/`second_ahead` 各给出一个自身 clock 严格大于对方
的分量及双方数值（`first` 在 `r1` 分量上 1 > 0，`second` 在 `r2` 分量上
1 > 0）：

```json
{
  "id": "f82c…",
  "relation": "concurrent",
  "chain": [],
  "first_ahead": {"replica_id": "r1", "first_value": 1, "second_value": 0},
  "second_ahead": {"replica_id": "r2", "first_value": 0, "second_value": 1}
}
```

错误示例：

```bash
# 历史不存在 -> 404 history_not_found
curl -sS -i -X POST \
  http://localhost:8000/api/histories/00000000000000000000000000000000/event-relation \
  -H 'Content-Type: application/json' \
  -d '{"first": {"replica_id": "r1", "seq": 1},
       "second": {"replica_id": "r2", "seq": 1}}'

# 事件不存在（r1 没有 seq 9）-> 404 event_not_found
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/event-relation" \
  -H 'Content-Type: application/json' \
  -d '{"first": {"replica_id": "r1", "seq": 9},
       "second": {"replica_id": "r2", "seq": 1}}'

# seq 为布尔值（或 0、负数、字符串、空 replica_id）-> 422
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/event-relation" \
  -H 'Content-Type: application/json' \
  -d '{"first": {"replica_id": "r1", "seq": true},
       "second": {"replica_id": "r2", "seq": 1}}'
```

整个查询过程不产生任何写入：再 `GET` 历史，事件与裁决均与查询前一致；暂存在
导入会话中、尚未提交的事件不参与比较（按暂存事件的标识查询会得到 404
`event_not_found`）。

## 新副本接入接口 `POST /api/histories/{id}/replicas`

已创建的持久历史可以**在线再接纳一个副本**，接入时**不必封闭任何导入会话**。
请求体只含新副本 ID：

```json
{ "replica_id": "r3" }
```

接入在**同一个 `BEGIN IMMEDIATE` 事务**内完成下列全部改动，要么一起生效、要么
全部回滚：

- 新副本 ID 追加到历史 `replica_ids` 的**末尾**（顺序影响 clock 分量的返回顺序，
  不影响语义）；
- 历史的**每条已提交事件**的 clock 补入 `新副本: 0` 分量；
- 该历史**每个导入会话**——无论 `open` 还是 `closed`——的**每条暂存事件**同样
  补入 `新副本: 0` 分量；会话 ID、`status`、暂存条数都不变。
- 原 `seq`、`key`、操作（put/delete）、值与 clock 的**已有分量原样不变**；
  补入的全是零分量，不改变任何已有事件之间的偏序关系，因此**已有键的状态与值
  不变**（`results` 只多出一个零分量）。

接入**不做**完整批次校验：暂存本来就允许缺前序/跨副本依赖，会话没收齐不能阻止
接入。成功响应就是**扩容后的原历史读取结构**（`id`、`replica_ids`、排序后的
完整 `events`、`results`）。

接入之后：

- 新副本可从 **`seq=1`** 起经原有写接口（`/events` 汇入、`/resolve-conflict`、
  `/migrate-key`，或导入会话暂存/提交）写入；
- 该历史**后续所有写入仍按完整 clock 规则校验**：clock 必须恰好覆盖扩容后的
  全部副本，服务端**不会**替请求自动补缺失的分量（缺分量仍是
  `clock_replica_coverage`，422）；
- 接入结果落在同一 SQLite 文件中，容器重启后保留；**其他历史完全不受影响**；
- 只支持**加入**，不提供移除或改名。

状态码：

- **200**：接入成功，返回扩容后的历史读取结构；
- **404**：历史不存在（`history_not_found`）；
- **409**：该副本已在 `replica_ids` 中（`replica_already_exists`，错误项带
  `replica_id`）；
- **422**：`replica_id` 为空/缺失/非字符串（请求体 schema 校验），或接入后
  副本数超过 **8**（`replica_limit_exceeded`）。

并发与一致性：历史行与全部会话行在同一串行化事务内更新，与旧汇入、冲突提交、
会话暂存/提交互斥，不会丢事件、不会分配重复 seq；读取端历史与会话使用**同一
已提交快照**，只能看到接入前或接入后的完整状态，不会出现"历史已含新副本、
暂存 clock 却没有新分量"（或反过来）的混用状态。

### 27. 接入新副本：开放/封闭会话都不关闭，已提交与暂存事件统一补零分量

先创建一份两副本冲突历史：`r1/1` 与 `r2/1` 并发写 `name`：

```bash
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name",
       "delete": "delete", "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
```

开两个会话：`SID1` 暂存一条**缺前序也没关系**的事件并保持开放；`SID2` 暂存一条
事件后提交（提交后会话封闭，用于观察封闭会话同样被补分量）：

```bash
SID1=$(curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
SID2=$(curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

# 开放会话：暂存 r1/2（暂存允许依赖暂不完整）
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID1/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r1", "seq": 2, "key": "other", "put": "p",
     "clock": {"r1": 2, "r2": 1}}
  ]}' > /dev/null

# 第二个会话暂存 r2/2 后提交 -> 封闭，r2/2 进入已提交历史
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID2/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r2", "seq": 2, "key": "other", "put": "q",
     "clock": {"r1": 1, "r2": 2}}
  ]}' > /dev/null
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID2/commit" \
  > /dev/null
```

接入新副本 `r3`——两个会话都**不需要**关闭：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/replicas" \
  -H 'Content-Type: application/json' \
  -d '{"replica_id": "r3"}'
```

响应是扩容后的原历史读取结构：`replica_ids` 末尾追加 `r3`；三条已提交事件的
clock 全部多出 `"r3": 0`，其余字段一字未动；`name` 仍是原来的冲突裁决（只是
裁决事件的 clock 多了零分量），`other` 仍是 put：

```json
{
  "id": "…",
  "replica_ids": ["r1", "r2", "r3"],
  "events": [
    {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
     "clock": {"r1": 1, "r2": 0, "r3": 0}},
    {"replica_id": "r2", "seq": 1, "key": "name",
     "delete": "delete", "clock": {"r1": 0, "r2": 1, "r3": 0}},
    {"replica_id": "r2", "seq": 2, "key": "other", "put": "q",
     "clock": {"r1": 1, "r2": 2, "r3": 0}}
  ],
  "results": [ "……键状态与值同接入前完全一致，仅 clock 多 r3:0……" ]
}
```

`r1/2` 当时只在开放会话 `SID1` 中暂存、并未提交，因此它**不在**上面的已提交
`events` 中；它只出现在会话读取结果里（同样已补 `r3:0`）。

会话侧也一起补了分量，状态不变——开放会话仍开放、封闭会话仍封闭：

```bash
# 开放会话：暂存事件已带 r3:0，status 仍为 open
curl -sS "http://localhost:8000/api/histories/$HID/import-sessions/$SID1"
# 封闭会话：暂存事件同样已带 r3:0，status 仍为 closed
curl -sS "http://localhost:8000/api/histories/$HID/import-sessions/$SID2"
```

### 28. 续提同一个开放会话：新事件必须带完整 clock，随后提交

接入后继续向 `SID1` 暂存事件。补齐此前缺失的依赖后，新事件的 clock 必须
**恰好覆盖三个副本**（服务端不替请求补分量）：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID1/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r1", "seq": 3, "key": "other", "put": "p3",
     "clock": {"r1": 3, "r2": 2, "r3": 0}}
  ]}'
```

提交同一个会话即可把暂存事件（接入前暂存的 `r1/2` 已自动带 `r3:0`）汇入历史，
会话随之封闭：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID1/commit"
```

```json
{
  "id": "…",
  "replica_ids": ["r1", "r2", "r3"],
  "events": [
    {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
     "clock": {"r1": 1, "r2": 0, "r3": 0}},
    {"replica_id": "r1", "seq": 2, "key": "other", "put": "p",
     "clock": {"r1": 2, "r2": 1, "r3": 0}},
    {"replica_id": "r1", "seq": 3, "key": "other", "put": "p3",
     "clock": {"r1": 3, "r2": 2, "r3": 0}},
    {"replica_id": "r2", "seq": 1, "key": "name",
     "delete": "delete", "clock": {"r1": 0, "r2": 1, "r3": 0}},
    {"replica_id": "r2", "seq": 2, "key": "other", "put": "q",
     "clock": {"r1": 1, "r2": 2, "r3": 0}}
  ],
  "results": [ "……" ]
}
```

注意接入后若再用**旧的两分量 clock** 重传一个已暂存/已提交标识，按完整内容
判等会因缺少 `r3` 分量而得到 **409** `event_content_mismatch`（重传必须使用
接入后的三分量形式）；这与既有重传规则一致。

### 29. 新副本从 seq=1 写入；clock 缺分量仍是 422，服务端不自动补

`r3` 从 `seq=1` 开始写入，经原有汇入接口即可（clock 覆盖全部三个副本）：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r3", "seq": 1, "key": "name", "put": "carol",
     "clock": {"r1": 3, "r2": 2, "r3": 1}}
  ]}'
```

返回 200，历史读取结构中出现 `r3/1`。接入**不会**放宽后续写入的 clock 规则——
请求里漏掉任一副本分量仍是 **422**（不会被当作 0 自动补入）：

```bash
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r3", "seq": 2, "key": "other", "put": "x",
     "clock": {"r1": 3, "r3": 2}}
  ]}'
# 422 Unprocessable Entity，code=clock_replica_coverage（缺少 r2 分量）
```

冲突提交与键迁移同样适用：写入副本可直接指定为 `r3`，其 `versions`/生成事件的
clock 都按扩容后的三副本计算（例如
`POST /api/histories/{id}/resolve-conflict`，body 中
`"replica_id": "r3"`）。

### 30. 失败情形：404 / 409 / 422，失败不改动；重启后保留

```bash
# 历史不存在 -> 404 history_not_found
curl -sS -i -X POST \
  http://localhost:8000/api/histories/00000000000000000000000000000000/replicas \
  -H 'Content-Type: application/json' -d '{"replica_id": "r9"}'

# r3 已是成员 -> 409 replica_already_exists，且不改动任何数据
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/replicas" \
  -H 'Content-Type: application/json' -d '{"replica_id": "r3"}'

# 空副本 ID -> 422（请求体校验）
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/replicas" \
  -H 'Content-Type: application/json' -d '{"replica_id": ""}'

# 8 副本上限：建一份已有 8 个副本的历史，再接入第 9 个
# -> 422 replica_limit_exceeded
HID8=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{"replica_ids": ["a","b","c","d","e","f","g","h"],
       "events": [
         {"replica_id": "a", "seq": 1, "key": "k", "put": "v",
          "clock": {"a":1,"b":0,"c":0,"d":0,"e":0,"f":0,"g":0,"h":0}}
       ]}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID8/replicas" \
  -H 'Content-Type: application/json' -d '{"replica_id": "i"}'
```

```json
{
  "detail": [
    {
      "code": "replica_already_exists",
      "message": "replica 'r3' is already a member of this history's replica_ids; a replica can only be admitted once",
      "replica_id": "r3"
    }
  ]
}
```

任一失败都在事务内回滚：再 `GET` 历史，`replica_ids`、事件、裁决及全部会话
（含暂存内容与状态）都与请求前一致。

接入结果与历史、会话一样持久化在 SQLite 命名卷中，`docker compose restart`
后按原 ID 读取仍是扩容后的三副本历史；接入前后创建的**其他历史**的
`replica_ids` 与 clock 完全不变。系统只提供加入能力，没有移除或改名副本的
接口。

## 接收回执与稳定水位接口 `/api/histories/{id}/receipts`

每个接收副本可以上报它**已确认接收的因果前缀**；服务端按
`(history_id, replica_id)` 持久保存每个接收方的最新回执，并可查询所有副本
**共同接收**的稳定水位。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/histories/{id}/receipts` | 上报一个接收副本的 `known_clock` 回执；幂等重传成功，倒退 409，非法 422，历史/副本不存在 404 |
| `GET` | `/api/histories/{id}/receipts` | **只读**返回按接收副本 ID 排序的回执列表与 `stable_clock` |

上报请求体：

```json
{
  "replica_id": "r1",
  "known_clock": {"r1": 1, "r2": 2}
}
```

上报规则：

- `replica_id`：非空字符串，且必须是该历史**当前** `replica_ids` 的成员，否则
  **404**（`receipt_replica_not_found`）；历史不存在同样 **404**
  （`history_not_found`）；
- `known_clock` 必须**恰好覆盖当前副本集合**（缺分量或多分量均为
  `known_clock_coverage`，**422**）；每个分量为非负整数，**布尔值被拒绝**
  （schema 422）；
- 每个分量不得超过该来源在**已提交事件**中的最大 seq
  （`known_clock_ahead_of_batch`，422）；任一已接收事件（自身 seq 不超过上报
  的本副本分量）的 clock 任一分量超出该前缀，为
  `clock_exceeds_known_clock`（422）。**暂存会话中的事件不计入**可接收范围；
- 回执逐分量**只许持平或前进**：任一分量小于已存值即为倒退，返回 **409**
  `receipt_regressed`，错误中带 `regressed` 映射列出每个倒退分量的
  `stored`/`incoming`，且已存数据不变；与已存回执**完全相同**的重传是幂等
  重发，返回 **200**；
- 校验与保存发生在**同一个 `BEGIN IMMEDIATE` 事务**内、针对同一已提交状态；
  并发上报因此串行化，较大的已确认进度永远不会被较小上报覆盖；
- 失败（422/409/404）一律不写入任何数据。

查询响应：

- `receipts`：**已上报过**的接收方回执（从未上报的副本不在其中），按
  `replica_id` 排序，每项为 `{replica_id, known_clock}`；
- `stable_clock`：每个分量取**全部当前接收方**（`replica_ids` 的每个成员）
  该分量的最小值；**从未上报的副本按全零计**，所以只要有一个当前副本没上报，
  水位对应分量即为 0。历史、回执取自同一读快照，扩容前后的集合与回执绝不混用。

新副本接入与回执的联动（与历史行、全部会话行在**同一事务**生效）：

- 每份**旧回执**补上**新副本的零分量**，旧分量原值不变；
- **新接收方**没有回执行，按**全零**计入稳定水位；
- 因此扩容后即使旧回执都没倒退，`stable_clock` 也**可能下降**（新副本分量为
  0）；扩容后旧的两分分量 clock 再上报会是 422（缺分量），须按新集合上报。

### 31. 上报回执、查询稳定水位

先建一份三事件历史（`r1` 最大 seq 为 1，`r2` 最大 seq 为 2；`r2/2` 的 clock
为 `{"r1":1,"r2":2}`）：

```bash
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "delete": "delete",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r2", "seq": 2, "key": "other", "put": "q",
       "clock": {"r1": 1, "r2": 2}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
```

任何上报之前查询：没有回执，所有接收方都按全零，水位是全零：

```bash
curl -sS "http://localhost:8000/api/histories/$HID/receipts"
```

```json
{
  "id": "……",
  "replica_ids": ["r1", "r2"],
  "receipts": [],
  "stable_clock": {"r1": 0, "r2": 0}
}
```

接收副本 `r1` 上报它已确认的因果前缀（收到了自己的 1 和 `r2/2`，而 `r2/2`
clock 正是 `{r1:1, r2:2}`，未越界）：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/receipts" \
  -H 'Content-Type: application/json' \
  -d '{"replica_id": "r1", "known_clock": {"r1": 1, "r2": 2}}'
```

此时只有 `r1` 上报过；`r2` 从未上报、按全零计，所以共同水位仍是全零：

```json
{
  "id": "……",
  "replica_ids": ["r1", "r2"],
  "receipt": {"replica_id": "r1", "known_clock": {"r1": 1, "r2": 2}},
  "receipts": [
    {"replica_id": "r1", "known_clock": {"r1": 1, "r2": 2}}
  ],
  "stable_clock": {"r1": 0, "r2": 0}
}
```

`r2` 也上报同一前缀后，逐分量最小值就是双方共同接收的水位：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/receipts" \
  -H 'Content-Type: application/json' \
  -d '{"replica_id": "r2", "known_clock": {"r1": 1, "r2": 2}}'

curl -sS "http://localhost:8000/api/histories/$HID/receipts"
```

```json
{
  "id": "……",
  "replica_ids": ["r1", "r2"],
  "receipts": [
    {"replica_id": "r1", "known_clock": {"r1": 1, "r2": 2}},
    {"replica_id": "r2", "known_clock": {"r1": 1, "r2": 2}}
  ],
  "stable_clock": {"r1": 1, "r2": 2}
}
```

**幂等重传与倒退。** 把完全相同的回执再发一次仍然 **200**（只保留一行）；
但只要任一分量变小（下面把 `r2` 从 2 退到 1），即返回 **409** 且已存回执不变：

```bash
# 相同重传 -> 200
curl -sS -o /dev/null -w '%{http_code}\n' -X POST \
  "http://localhost:8000/api/histories/$HID/receipts" \
  -H 'Content-Type: application/json' \
  -d '{"replica_id": "r1", "known_clock": {"r1": 1, "r2": 2}}'

# 分量倒退 -> 409
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/receipts" \
  -H 'Content-Type: application/json' \
  -d '{"replica_id": "r1", "known_clock": {"r1": 1, "r2": 1}}'
```

```json
{
  "detail": [
    {
      "code": "receipt_regressed",
      "message": "receipt reported by replica 'r1' moves at least one component backwards; each component may stay equal or grow, never shrink: {'r2': {'stored': 2, 'incoming': 1}}",
      "replica_id": "r1",
      "regressed": {"r2": {"stored": 2, "incoming": 1}}
    }
  ]
}
```

**非法上报（均为 422，不写入）：**

```bash
# clock 缺/多分量 -> 422 known_clock_coverage
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/receipts" \
  -H 'Content-Type: application/json' \
  -d '{"replica_id": "r1", "known_clock": {"r1": 1}}'

# 分量超过已提交最大 seq（r1 只有 seq 1）-> 422 known_clock_ahead_of_batch
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/receipts" \
  -H 'Content-Type: application/json' \
  -d '{"replica_id": "r1", "known_clock": {"r1": 2, "r2": 2}}'

# 已接收事件 clock 超出前缀：声称收到 r2/2（r2 分量=2）却 r1=0，
# 而 r2/2 的 clock 为 {r1:1,r2:2} -> 422 clock_exceeds_known_clock
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/receipts" \
  -H 'Content-Type: application/json' \
  -d '{"replica_id": "r2", "known_clock": {"r1": 0, "r2": 2}}'

# 布尔值不作为整数分量 -> 422（请求体校验）
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/receipts" \
  -H 'Content-Type: application/json' \
  -d '{"replica_id": "r1", "known_clock": {"r1": true, "r2": 0}}'

# 接收副本不在集合中 -> 404 receipt_replica_not_found
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/receipts" \
  -H 'Content-Type: application/json' \
  -d '{"replica_id": "r9", "known_clock": {"r1": 0, "r2": 0}}'
```

### 32. 扩容后重新确认：旧回执补零、新接收方全零、水位可下降

沿用上一例：`r1`、`r2` 都已确认到 `{"r1":1,"r2":2}`，共同水位也是
`{"r1":1,"r2":2}`。接入第三个副本 `r3`（与历史、会话同一事务生效）：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/replicas" \
  -H 'Content-Type: application/json' -d '{"replica_id": "r3"}'
```

再查回执：旧回执各自补了 `r3: 0`，`r3` 从未上报按全零计，三个分量的最小值
因此全部回落到 0——**水位下降了，但没有任何旧回执倒退**：

```bash
curl -sS "http://localhost:8000/api/histories/$HID/receipts"
```

```json
{
  "id": "……",
  "replica_ids": ["r1", "r2", "r3"],
  "receipts": [
    {"replica_id": "r1", "known_clock": {"r1": 1, "r2": 2, "r3": 0}},
    {"replica_id": "r2", "known_clock": {"r1": 1, "r2": 2, "r3": 0}}
  ],
  "stable_clock": {"r1": 0, "r2": 0, "r3": 0}
}
```

扩容后旧的两分分量 clock 不再合法（缺 `r3` 分量，422）；按**新集合**重新
确认即可。让 `r3` 上报它也收到了此前全部事件：

```bash
# 旧形状 -> 422 known_clock_coverage
curl -sS -o /dev/null -w '%{http_code}\n' -X POST \
  "http://localhost:8000/api/histories/$HID/receipts" \
  -H 'Content-Type: application/json' \
  -d '{"replica_id": "r1", "known_clock": {"r1": 1, "r2": 2}}'

# r3 按三分量上报
curl -sS -X POST "http://localhost:8000/api/histories/$HID/receipts" \
  -H 'Content-Type: application/json' \
  -d '{"replica_id": "r3", "known_clock": {"r1": 1, "r2": 2, "r3": 0}}'
```

此时三个接收方共同的稳定水位恢复到扩容前的前缀（`r3` 自身尚无事件）：

```json
{
  "receipts": [
    {"replica_id": "r1", "known_clock": {"r1": 1, "r2": 2, "r3": 0}},
    {"replica_id": "r2", "known_clock": {"r1": 1, "r2": 2, "r3": 0}},
    {"replica_id": "r3", "known_clock": {"r1": 1, "r2": 2, "r3": 0}}
  ],
  "stable_clock": {"r1": 1, "r2": 2, "r3": 0}
}
```

回执随 SQLite 命名卷持久化，`docker compose restart` 后仍可查询；挂入旧版本
数据卷（无回执表）时旧接口与数据完全不变，回执查询等价于尚无任何上报，第一次
上报时自动建表。

## 本地开发（不使用 Docker）

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

本地运行时持久化历史默认写入当前目录的 `history.db`（可用环境变量
`HISTORY_DB_PATH` 指定其他路径，例如 `HISTORY_DB_PATH=/tmp/history.db`）；
三个无状态接口不依赖数据库。
