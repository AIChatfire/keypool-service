# keypool

把 new-api 的**多 key 管理能力**暴露为一个独立 HTTP 薄服务：渠道内多 key 的
选取（轮询/随机/用量均衡 + 时间带批次轮换）、失败自动禁用、恢复自动启用、
用量计数与手动启停，全部复用 new-api 的 MySQL/PostgreSQL/SQLite
（`channels` / `abilities` / `options` 表）与 Redis，与 new-api 并存运行。

## 架构

```
                 +-------------------+
   caller  ----> |  keypool (本服务)  |
   (HTTP)        |                   |
                 |  internal/api     |  Go1.22 ServeMux + Bearer 鉴权 + 统一包络
                 |    |        |     |
                 | selector   state  |  选 key / 禁用·启用执行
                 |    |        |     |
                 |  store   redisx   |  gorm 读穿 + 行锁事务写穿 / 锁·游标·用量·幂等·事件
                 +----|---------|----+
                      |         |
              +-------v--+  +---v--------+
              |  new-api |  |   Redis    |
              |  共享 DB |  | keypool:*  |
              | channels |  | cursor/    |
              | abilities|  | usage/lock/|
              | options  |  | idem/events|
              +----------+  +------------+

                 (new-api 本体继续运行，二者共用 DB/Redis)
```

- `store`：读 `channels`/`abilities`，事务写穿 `channel_info`/`status`/`other_info`
  与 `abilities.enabled`（**单写者纪律**：只写这四列，见 SPEC §2.3）；
  写事务内对渠道行加 `SELECT ... FOR UPDATE`（MySQL/PostgreSQL），与 new-api
  自身的渠道写串行化，避免 read-modify-write 丢失更新。
  `OptionsPoller` 每 `SYNC_INTERVAL_SEC`（默认 60s）轮询 `options` 表生成配置快照。
- `selector`：渠道确定（cid 直达或 abilities priority 分档 + weight 加权）→
  候选集（启用 key ∩ 当前轮换批次，空则 look-ahead）→ Lua 单 RTT 选 key。
  传 `key_index` 时走精确直达短路：只校验索引范围与 key 启用状态，跳过候选集
  与 Lua（零 Redis RTT）。
- `state`：上报处理（幂等 → epoch 校验 → 用量校正 → 渠道锁 → classifier 判定 →
  事务写穿 → 事件 XADD），禁用/启用语义逐字节对齐 new-api。
- `redisx`：`keypool:` 前缀的游标/用量 hash/衰减元数据/渠道锁/幂等键/事件 Stream，
  与 new-api 自身键空间零重叠。

## 快速开始

```bash
cp .env.example .env   # 修改 AUTH_TOKEN / DATABASE_DSN / REDIS_ADDR
export $(grep -v '^#' .env | xargs)

go build ./cmd/keypool
./keypool              # 监听 :$PORT（默认 8080）

# 或 Docker
docker build -t keypool .
docker run --env-file .env -p 8080:8080 keypool
```

## 接口约定

- 统一响应包络：`{"code":0,"message":"ok","data":...,"request_id":"<16位hex>"}`。
- 除 `GET /healthz` 外全部接口要求 `Authorization: Bearer $AUTH_TOKEN`（失败 401/`40100`）。
- 错误包络 code：

| code | HTTP | 含义 |
|------|------|------|
| `40001` | 503 | 无可用 key（`data.retry_after_ms` 给出重试建议） |
| `40002` | 404 | 渠道不存在 |
| `40003` | 409 | 幂等冲突（重复上报） |
| `40010` | 400 | 参数错误 |
| `40100` | 401 | 未鉴权 / token 错误 |
| `50001` | 500/503 | 依赖故障（DB/Redis）或内部错误 |

## 接口一览

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/keys/select` | 选取一个可用 key（可附带渠道元数据） |
| POST | `/v1/keys/report` | 上报调用结果（用量/成功/失败，驱动自动禁启） |
| GET | `/v1/channels/{id}` | 渠道元数据（new-api channels 表投影） |
| GET | `/v1/channels/{id}/keys` | 渠道 key 列表（状态/用量/轮换状态/脱敏） |
| PATCH | `/v1/channels/{id}/keys/{idx}` | 手动启用/禁用某个 key |
| GET/PUT | `/v1/channels/{id}/balance` | 用量均衡配置（读/写） |
| GET/PUT | `/v1/channels/{id}/rotation` | 时间带批次轮换配置（读/写） |
| GET | `/v1/channels/{id}/usage` | 渠道用量计数 |
| POST | `/v1/settings/reload` | 立即重建 options 配置快照 |
| GET | `/healthz` | 健康检查（裸 JSON，无鉴权） |
| GET | `/metrics` | Prometheus 指标（需鉴权） |

以下示例假设 `TOKEN=$AUTH_TOKEN`、`BASE=http://127.0.0.1:8080`、`H="Authorization: Bearer $TOKEN"`。

---

### POST /v1/keys/select — 选 key

渠道定位二选一：`channel_id` 直达，或 `group`+`model` 经 abilities 表
priority 分档 + weight 加权选择。再叠加可选的 `key_index` 做**单 key 精确直达**。

请求体：

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `channel_id` | int | - | 渠道直达（与 group+model 二选一） |
| `key_index` | int | - | **精确直达指定 key，必须搭配 `channel_id`**（详见下节） |
| `group` / `model` | string | - | 按分组+模型选渠道 |
| `retry` | int | 0 | 重试次数，影响 abilities 档位下探 |
| `exclude` | array | - | 排除项 `[{"channel_id":7,"key_index":1}]` |
| `mode` | string | 渠道配置 | `polling`\|`random`\|`usage` 覆盖 |
| `est_tokens` | float | 0 | usage 模式预估预扣量（>0 时签发租约） |
| `advance_cursor` | bool | true | false=测活 peek，不推轮询游标 |
| `include_channel` | bool | false | true 时响应附带 `channel` 渠道元数据 |

```bash
# 渠道直达
curl -X POST $BASE/v1/keys/select -H "$H" -H 'Content-Type: application/json' -d '{
  "channel_id": 7
}'

# channel_id + key_index 精确直达（测活/复现/定向压测）
curl -X POST $BASE/v1/keys/select -H "$H" -H 'Content-Type: application/json' -d '{
  "channel_id": 7, "key_index": 3
}'

# 分组+模型，排除已失败 key，usage 均衡 + 预扣，附带渠道元数据
curl -X POST $BASE/v1/keys/select -H "$H" -H 'Content-Type: application/json' -d '{
  "group": "default", "model": "gpt-4o", "retry": 0,
  "exclude": [{"channel_id": 7, "key_index": 1}],
  "mode": "usage", "est_tokens": 512, "advance_cursor": true,
  "include_channel": true
}'
```

响应 `data`：

```json
{
  "channel_id": 7,
  "key_index": 0,
  "key": "sk-...",
  "base_url": "https://api.upstream.example",
  "mode": "usage",
  "epoch": "a1b2c3d4",
  "band": {"index": 2, "ends_at": 1735689900},
  "lease_id": "0123...cdef",
  "channel": {
    "id": 7, "name": "upstream-a", "type": 1, "status": 1,
    "group": "default", "tag": "paid", "remark": "主用上游",
    "models": ["gpt-4o", "gpt-4o-mini"],
    "base_url": "https://api.upstream.example",
    "priority": 0, "weight": 0, "auto_ban": true,
    "multi_key": true, "multi_key_mode": "polling",
    "openai_organization": "org-xxx", "test_model": "gpt-4o-mini",
    "model_mapping": {"gpt-4o": "gpt-4o-2024-08-06"},
    "status_code_mapping": {"503": "500"},
    "header_override": {"X-Custom-Header": "v"},
    "param_override": {"temperature": 0.5},
    "setting": {"proxy": "http://127.0.0.1:7890"},
    "settings": {"azure_api_version": "2024-08-01-preview"},
    "other": {"region": "us"}
  }
}
```

`channel`（ChannelMeta）只透出 **new-api 后台 Web 端可配置的信息**（渠道编辑表单项）：

| 分组 | 字段 |
|------|------|
| 基础 | `id` `name` `type` `status` `group` `tag` `remark` `models` `base_url` `priority` `weight` `auto_ban` |
| 多 key | `multi_key` `multi_key_mode` |
| 上游参数 | `openai_organization` `test_model`（测活模型） |
| 覆盖/映射 | `model_mapping`（模型重定向）`status_code_mapping`（状态码覆盖）`header_override`（自定义请求标头）`param_override`（请求体参数覆盖）`setting`（渠道额外设置/代理）`settings`（azure 版本等）`other`（Vertex 部署地区等） |

运行态/统计字段（`created_time` `test_time` `response_time` `balance`
`used_quota` `other_info` `key_count` `epoch` 等）**不返回**；
`epoch` 仍由 select 响应顶层携带。JSON 字符串列均解析为对象透出，
为空或解析失败时该字段省略；其中 `setting`/`settings`/`other` 只透出
**非默认值**的配置项——new-api 落库时会把这两列按类型化 struct 全量
序列化（带满 `false`/`""`/`0` 默认值），投影层已剔除零值噪声，剔除后
无剩余项则字段整体省略。`param_override` 为用户手填 JSON，显式
`0`/`false` 有语义（如 `"temperature":0`），原样透出。
**注意**：`header_override`/`param_override` 等可能含敏感配置，接口由 `AUTH_TOKEN` 保护，按需授权。

- `band` 仅启用批次轮换时出现；`lease_id` 仅 usage 模式且 `est_tokens>0` 时出现；
  `channel` 仅 `include_channel=true` 时出现。
- 无可用 key → 503 `code=40001`，`data.retry_after_ms=1000`；渠道不存在 → 404 `code=40002`。
- **`epoch` 是 key 集合指纹**：拿到 key 后渠道 key 集合若被改动，携带旧 epoch
  的 report 会被忽略（`stale_epoch_ignored`），调用方应重新 select。

#### key_index 精确直达

传 `key_index` 即锁定渠道内第 N 个 key（0 起），**跳过所有选取算法**：不查
轮换批次、不推轮询游标、不做 usage 打分与预扣，也**不访问 Redis**（Redis
降级期间仍可用）。适用于测活、故障复现、定向压测等需要"就要这一把 key"的场景。

| 约束 | 说明 |
|------|------|
| **必须搭配 `channel_id`** | 只传 `key_index`（走 `group`+`model`）→ 400 `40010`：`key_index requires channel_id`。渠道由加权算法动态选出，索引无从对应 |
| 索引从 0 起，须 `>= 0` | 负数 → 400 `40010` |
| `key_index` 越界 | → 400 `40010`（渠道 key 数不足，属永久性错误，重试无意义） |
| 该 key 已被禁用 | → 503 `40001`（与常规无可用 key 一致，可能被重新启用故语义可重试） |
| 渠道不存在 | → 404 `40002`（渠道校验先于索引校验） |
| 渠道被禁用 | → 503 `40001` |

响应差异：`mode` 固定返回 `"direct"`（表示未走任何调度算法），不返回
`band`、不返回 `lease_id`；`key`/`base_url`/`epoch`/`channel` 与常规一致。
同时传入的 `mode`、`est_tokens`、`advance_cursor`、`exclude` **一律被忽略**。

指标：直达命中额外累加 `keypool_select_direct_total`。

### POST /v1/keys/report — 上报调用结果

驱动用量计数与自动禁用/自动启用。幂等键：`Idempotency-Key` 请求头优先于
body 的 `idempotency_key`；重复上报 → 409 `code=40003`。

请求体：

| 字段 | 类型 | 说明 |
|------|------|------|
| `channel_id` | int | 必填 |
| `key_index` | int | 与 `key` 二选一定位；同时传则必须一致 |
| `key` | string | 按 key 字符串精确匹配定位 |
| `epoch` | string | select 返回的指纹；不匹配则整单忽略 |
| `success` | bool | 是否成功 |
| `status_code` / `error_code` / `error_message` | - | 失败时供禁用分类器判定 |
| `usage` | object | `{"prompt_tokens":100,"completion_tokens":50,"cost":0.002}` |
| `lease_id` | string | select 签发的租约，按 actual−est 校正用量 |
| `idempotency_key` | string | 幂等键（头部优先） |

```bash
curl -X POST $BASE/v1/keys/report -H "$H" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: req-123' -d '{
  "channel_id": 7, "key_index": 0, "epoch": "a1b2c3d4",
  "success": false, "status_code": 401, "error_message": "Permission denied: ...",
  "usage": {"prompt_tokens": 100, "completion_tokens": 50, "cost": 0.002}
}'
```

响应 `data.action` ∈ `none` | `cooldown` | `key_disabled` | `channel_disabled` |
`enabled` | `stale_epoch_ignored` | `duplicate`；`channel_status` 为动作后的渠道状态。

### GET /v1/channels/{id} — 渠道元数据

返回 `data` = 上文 `channel` 对象同构的 ChannelMeta（仅 Web 端可配置字段：
基础信息 + 多 key 模式 + 上游参数 + model_mapping/status_code_mapping/
header_override/param_override/setting/settings/other 等覆盖配置）。
渠道不存在 → 404 `code=40002`。

```bash
curl $BASE/v1/channels/7 -H "$H"
```

### GET /v1/channels/{id}/keys — key 列表

```bash
curl $BASE/v1/channels/7/keys -H "$H"
```

```json
{
  "epoch": "a1b2c3d4", "mode": "polling",
  "keys": [
    {"index": 0, "status": 1, "usage": 1234.5, "rotation_state": "active", "key_mask": "sk-a****bbbb"},
    {"index": 1, "status": 3, "reason": "quota", "disabled_time": 1735689600,
     "usage": 0, "rotation_state": "standby", "key_mask": "sk-c****dddd"}
  ]
}
```

`status`：1=启用 2=手动禁用 3=自动禁用；`usage` 在 Redis 降级时整体省略；
`rotation_state` 未配置轮换时为 `""`。

### PATCH /v1/channels/{id}/keys/{idx} — 手动启停 key

```bash
curl -X PATCH $BASE/v1/channels/7/keys/2 -H "$H" -H 'Content-Type: application/json' -d '{
  "status": "disabled", "reason": "人工下线"
}'
curl -X PATCH $BASE/v1/channels/7/keys/2 -H "$H" -d '{"status": "enabled"}'
```

`status` ∈ `enabled`|`disabled`（禁用写 status=2 及 disabled_reason/time）。
响应同 report：`action` ∈ `enabled`|`key_disabled`|`channel_disabled`
（禁用后全 key 灭 → 渠道联动禁用）。

### GET/PUT /v1/channels/{id}/balance — 用量均衡配置

写入 options 表 `keypool.balance.{cid}` 并立即刷新快照。

```bash
curl -X PUT $BASE/v1/channels/7/balance -H "$H" -d '{
  "mode": "usage", "metric": "tokens", "decay_interval": 3600, "decay_factor": 0.5
}'
curl $BASE/v1/channels/7/balance -H "$H"     # 无配置返回默认值
```

`mode` ∈ `usage`|`request`|`auto`；`metric` ∈ `tokens`|`cost`。
默认值：`{"mode":"auto","metric":"tokens","decay_interval":3600,"decay_factor":0.5}`。

### GET/PUT /v1/channels/{id}/rotation — 批次轮换配置

```bash
curl -X PUT $BASE/v1/channels/7/rotation -H "$H" -d '{
  "band_seconds": 600, "active_count": 2, "overlap_bands": 0, "order": "index"
}'
curl $BASE/v1/channels/7/rotation -H "$H"
```

约束：`band_seconds>=30`、`active_count>=1`、`overlap_bands>=0`、
`order` ∈ `index`|`shuffle`。默认值：`{"band_seconds":3600,"active_count":1,"overlap_bands":0,"order":"index"}`。

### GET /v1/channels/{id}/usage — 用量计数

```bash
curl $BASE/v1/channels/7/usage -H "$H"
# → {"cid":7,"metric":"tokens","counters":{"0":1234.5},"last_decay":1735689600}
```

`metric` 取自 balance 配置（缺省 `tokens`）；Redis 降级 → 503/`50001`。

### POST /v1/settings/reload — 立即重建配置快照

不等 60s 轮询，立即重读 options 表（含 new-api 管理台改的开关与
keypool 的 balance/rotation 配置）。

```bash
curl -X POST $BASE/v1/settings/reload -H "$H"     # → {"reloaded":true}
```

### GET /metrics — Prometheus 指标

```bash
curl $BASE/metrics -H "$H"
# keypool_select_total{cid,idx} / keypool_report_total{action} /
# keypool_band_lookahead_total / keypool_select_direct_total /
# keypool_process_uptime_seconds
```

## 与 new-api 共存（防冲突设计）

**写隔离：**
- **单写者纪律**：keypool 只写 `channels.channel_info` / `channels.status` /
  `channels.other_info` 与 `abilities.enabled` 四列，以及 `options` 表的
  `keypool.*` 扩展键（写入路径强制 `keypool.` 前缀校验，物理上无法误写
  new-api 原生配置项）。不要再用其他脚本/服务直接改这些列。
- **行锁写穿**：禁用/启用事务内对渠道行 `SELECT ... FOR UPDATE`
  （MySQL/PostgreSQL）后再 read-modify-write，与 new-api 自身的渠道写
  串行化，杜绝交错丢失更新；SQLite 由单文件写锁天然串行。
- **Redis 键空间**：全部 `keypool:` 前缀，与 new-api 自身键零重叠。

**开关协调：**
- [ ] **关闭 new-api 的自动禁用/自动启用**：二者同时做自动禁用会双写
  `channel_info`。由 keypool 接管后，把 new-api 侧
  `AutomaticDisableChannelEnabled`/`AutomaticEnableChannelEnabled` 关掉
  （keypool 读同两个 options，60s 轮询同步，接管后语义不变）。
- [ ] **60s 同步窗口**：new-api 管理台改的 options（开关、禁用码表、关键词）
  最迟 `SYNC_INTERVAL_SEC`（默认 60s）生效；需要立即生效时调
  `POST /v1/settings/reload`。
- [ ] key 集合变更（new-api 后台编辑渠道 key）会改变 epoch：携带旧 epoch 的
  report 返回 `stale_epoch_ignored`，不做任何写，调用方重新 select 即可。

## 轮换与用量均衡

**批次轮换**（`keypool.rotation.{cid}`）：把渠道全部 key 按索引连续切成
`active_count` 个/批（`order=shuffle` 时用 `sha1(cid)` 做种子确定性洗牌，
所有实例视角一致）。当前批次号 = `floor(now/band_seconds) % 批数`；
`overlap_bands>0` 时把前 N 个时间带的批次并入候选。候选集 =
启用 key ∩ 当前批次，为空则向后续批次 look-ahead（并计入
`keypool_band_lookahead_total`）。每个 band 内选取退化为轮询/随机/usage。
典型用途：某些渠道商限制“单 key 连续使用时长”，按时间带均匀摊开。

**用量均衡**（`keypool.balance.{cid}`）：`mode=usage` 时在 Redis
`keypool:usage:{cid}` hash 上按“计数最小者优先”（含 5% 抖动破并列）选 key，
`metric=tokens|cost` 决定计数口径；`decay_interval`/`decay_factor` 控制
周期性衰减（Lua 内惰性执行），保证长期均衡而不永久惩罚历史用量。
`mode=request` 退化为渠道自身的 `multi_key_mode`（polling|random）；
`mode=auto` 当前按 request 处理。选 key 时 `est_tokens` 预扣、
report 时按实际用量校正。

## 降级行为

- **Redis 不可达**：启动时 PING 失败仅记日志，服务以 degraded 模式继续运行。
  此时 `keys/select` / `keys/report` / 手动启停因锁与游标不可用而返回
  `50001`（503）；`GET /v1/channels/{id}` 与 `GET /v1/channels/{id}/keys`
  正常返回（后者省略 `usage` 字段）；`GET .../usage` 返回 503/50001。
  Redis 恢复后无需重启（go-redis 自动重连，锁/游标/用量路径随之恢复）。
- **DB 不可达**：`store.Open` 失败即退出（fatal）；运行期 DB 错误映射为
  `50001`。
- **panic**：由 recover 中间件兜底为 500/50001，不会打挂进程。

## 开发

```bash
go build ./... && go vet ./... && go test ./...
```

目录契约见 `SPEC.md`：`cmd/keypool`（装配）、`internal/{config,store,redisx,
classifier,selector,state,api}`，禁止反向依赖。

## E2E 自测（真实 sqlite + Redis）

```bash
# 建议使用独立 Redis 实例，避免 flush 本地开发库
redis-server --port 16399 --daemonize yes

go build -o keypool ./cmd/keypool
python3 scripts/e2e/seed.py /tmp/keypool-e2e.db 127.0.0.1:16399
PORT=18099 AUTH_TOKEN=e2e-token DATABASE_TYPE=sqlite \
  DATABASE_DSN=/tmp/keypool-e2e.db REDIS_ADDR=127.0.0.1:16399 ./keypool &
python3 scripts/e2e/e2e_test.py   # 35 项断言：轮询均匀/禁用写穿/全灭联动/恢复/幂等/epoch/轮换/usage 均衡/渠道元数据（仅 Web 可配置字段）/PATCH 启停
```

## Docker Compose 部署（只跑 keypool 薄服务）

DB/Redis 复用 new-api 现有实例，环境变量与 new-api 同款（`SQL_DSN` / `REDIS_CONN_STRING`）：

```bash
# 1) 修改 docker-compose.yml 里的 SQL_DSN / REDIS_CONN_STRING / AUTH_TOKEN（或写 .env）
docker compose up -d          # 直接拉取 ghcr.io/aichatfire/keypool-service:latest
curl http://localhost:8080/healthz
# 本地自构：把 compose 中 image 换成 build: .，再 docker compose up -d --build
```

- DB/Redis 在**宿主机**：默认 compose 已带 `host.docker.internal:host-gateway` 映射，示例 DSN 即用宿主机地址；
  也可用 host 网络模式：`docker compose -f docker-compose.yml -f docker-compose.host.yml up -d`（DSN 用 127.0.0.1）。
- DB/Redis 在**内网其它机器**：直接把 DSN 里的 host 改成内网 IP 即可。
- 支持 PostgreSQL：`SQL_DSN=postgres://...`（scheme 自动识别）；SQLite：`SQL_DSN=/path/to.db`（容器内需挂载文件）。

## Docker Compose 一体化部署（推荐）

```bash
docker compose up -d --build          # mysql + redis + new-api(管理平面) + keypool
# new-api 后台  http://localhost:3000   首次 root/123456（登录后改密）
# keypool API   http://localhost:8080   Authorization: Bearer $KEYPOOL_AUTH_TOKEN
```

- 两服务共享同一 MySQL/Redis；`MYSQL_ROOT_PASSWORD`、`KEYPOOL_AUTH_TOKEN` 用环境变量或 `.env` 覆盖，生产必改。
- 已有外部 MySQL/Redis/new-api：`docker compose up -d --build keypool`，并在 compose 里把 `DATABASE_DSN`/`REDIS_ADDR` 指向外部实例。
- new-api 侧建议：后台「运营设置」开"成功请求后自动启用通道"（测活成功自动恢复 key）；本场景 new-api 不转发流量，"自动禁用通道"开关无影响，若它同时转发其它渠道建议关闭以避免双写。
