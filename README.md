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
                 |  store   redisx   |  gorm 读穿 + 事务写穿 / 锁·游标·用量·幂等·事件
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
  `OptionsPoller` 每 `SYNC_INTERVAL_SEC`（默认 60s）轮询 `options` 表生成配置快照。
- `selector`：渠道确定（cid 直达或 abilities priority 分档 + weight 加权）→
  候选集（启用 key ∩ 当前轮换批次，空则 look-ahead）→ Lua 单 RTT 选 key。
- `state`：上报处理（幂等 → epoch 校验 → 用量校正 → 渠道锁 → classifier 判定 →
  事务写穿 → 事件 XADD），禁用/启用语义逐字节对齐 new-api。
- `redisx`：`keypool:` 前缀的游标/用量 hash/衰减元数据/渠道锁/幂等键/事件 Stream。

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

统一响应包络：`{"code":0,"message":"ok","data":...,"request_id":"<8字节hex>"}`。
除 `/healthz` 外全部接口要求 `Authorization: Bearer $AUTH_TOKEN`（失败 401/40100）。

## 接口示例

以下假设 `TOKEN=$AUTH_TOKEN`，`BASE=http://127.0.0.1:8080`。

```bash
H="Authorization: Bearer $TOKEN"

# 健康检查（无鉴权）
curl $BASE/healthz

# 选 key：按渠道直达，或按 group+model 经 abilities 分档加权
curl -X POST $BASE/v1/key:get -H "$H" -H 'Content-Type: application/json' -d '{
  "channel_id": 7
}'
curl -X POST $BASE/v1/key:get -H "$H" -H 'Content-Type: application/json' -d '{
  "group": "default", "model": "gpt-4o", "retry": 0,
  "exclude": [{"channel_id": 7, "key_index": 1}],
  "mode": "usage", "est_tokens": 512, "advance_cursor": true
}'
# → {"code":0,"message":"ok","data":{"channel_id":7,"key_index":0,"key":"sk-...",
#    "base_url":"...","mode":"usage","epoch":"a1b2c3d4","band":{"index":2,"ends_at":...}},...}
# 无可用 key → 503 code=40001 data.retry_after_ms=1000；渠道不存在 → 404 code=40002

# 用量/错误上报（Idempotency-Key 头部优先于 body idempotency_key）
curl -X POST $BASE/v1/key:report -H "$H" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: req-123' -d '{
  "channel_id": 7, "key_index": 0, "epoch": "a1b2c3d4",
  "success": false, "status_code": 401, "error_message": "Permission denied: ...",
  "usage": {"prompt_tokens": 100, "completion_tokens": 50, "cost": 0.002}
}'
# action ∈ none|key_disabled|channel_disabled|enabled|stale_epoch_ignored
# 幂等冲突 → 409 code=40003

# 渠道 key 列表（状态/禁用原因/用量/轮换状态/脱敏）
curl $BASE/v1/channels/7/keys -H "$H"

# 手动禁用 / 启用某个 key（禁用 status=2，会写 disabled_reason/time）
curl -X POST $BASE/v1/channels/7/keys/2:disable -H "$H" -d '{"reason":"人工下线"}'
curl -X POST $BASE/v1/channels/7/keys/2:enable  -H "$H" -d '{}'

# 用量均衡配置（写入 options 表 keypool.balance.7，立即刷新快照）
curl -X PUT $BASE/v1/channels/7/balance -H "$H" -d '{
  "mode": "usage", "metric": "tokens", "decay_interval": 3600, "decay_factor": 0.5
}'
curl $BASE/v1/channels/7/balance -H "$H"     # 无配置返回默认值

# 批次轮换配置（band_seconds>=30, active_count>=1）
curl -X PUT $BASE/v1/channels/7/rotation -H "$H" -d '{
  "band_seconds": 600, "active_count": 2, "overlap_bands": 0, "order": "index"
}'
curl $BASE/v1/channels/7/rotation -H "$H"

# 渠道用量计数（metric 取自 balance 配置，缺省 tokens）
curl $BASE/v1/channels/7/usage -H "$H"

# 立即重建 options 快照（不等 60s 轮询）
curl -X POST $BASE/v1/cache:invalidate -H "$H"

# Prometheus 指标（需鉴权）
curl $BASE/metrics -H "$H"
# keypool_select_total{cid,idx} / keypool_report_total{action} /
# keypool_band_lookahead_total / keypool_process_uptime_seconds
```

错误包络 code：`40001` 无可用 key、`40002` 渠道不存在、`40003` 幂等冲突、
`40010` 参数错误、`50001` 依赖故障、`40100` 未鉴权。

## 与 new-api 共存 checklist

- [ ] **关闭 new-api 的自动禁用/自动启用**：二者同时做自动禁用会双写
  `channel_info`。由 keypool 接管后，把 new-api 侧
  `AutomaticDisableChannelEnabled`/`AutomaticEnableChannelEnabled` 关掉，
  或保证只有 keypool 依据这些开关动作（keypool 同样读这两个 options，
  60s 轮询同步）。
- [ ] **单写者纪律**：keypool 只写 `channels.channel_info` / `channels.status` /
  `channels.other_info` 与 `abilities.enabled`，以及 `options` 表的
  `keypool.*` 扩展键。不要再用其他脚本/服务直接改这些列。
- [ ] **60s 同步窗口**：new-api 管理台改的 options（开关、禁用码表、关键词）
  与 keypool 的 balance/rotation 配置最迟 `SYNC_INTERVAL_SEC`（默认 60s）
  生效；需要立即生效时调 `POST /v1/cache:invalidate`。
- [ ] key 集合变更（new-api 后台编辑渠道 key）会改变 epoch：携带旧 epoch 的
  report 返回 `stale_epoch_ignored`，不做任何写，调用方重新 `key:get` 即可。
- [ ] Redis 键空间为 `keypool:` 前缀，与 new-api 自身键不冲突。

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
  此时 `key:get` / `key:report` / 手动启停因锁与游标不可用而返回
  `50001`（503）；`GET /v1/channels/{id}/keys` 正常返回但省略 `usage` 字段；
  `GET .../usage` 返回 503/50001。Redis 恢复后无需重启（连接由 go-redis
  自动重连，锁/游标/用量路径随之恢复）。
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
python3 scripts/e2e/seed.py /tmp/keypool-e2e.db          # 造 new-api 兼容种子库 + 清 Redis
PORT=18099 AUTH_TOKEN=e2e-token DATABASE_TYPE=sqlite \
  DATABASE_DSN=/tmp/keypool-e2e.db REDIS_ADDR=127.0.0.1:6379 ./keypool &
python3 scripts/e2e/e2e_test.py                          # 24 个场景：轮询均匀/禁用写穿/全灭联动/恢复/幂等/epoch/轮换/usage 均衡
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

## E2E 自测（真实 sqlite + Redis）
```bash
python3 scripts/e2e/seed.py /tmp/keypool-e2e.db          # 造 new-api 兼容种子库 + 清 Redis
PORT=18099 AUTH_TOKEN=e2e-token DATABASE_TYPE=sqlite \
  DATABASE_DSN=/tmp/keypool-e2e.db REDIS_ADDR=127.0.0.1:6379 ./keypool &
python3 scripts/e2e/e2e_test.py                          # 24 个场景：轮询均匀/禁用写穿/全灭联动/恢复/幂等/epoch/轮换/usage 均衡
```

## Docker Compose 一体化部署（推荐）
```bash
docker compose up -d --build          # mysql + redis + new-api(管理平面) + keypool
# new-api 后台  http://localhost:3000   首次 root/123456（登录后改密）
# keypool API   http://localhost:8080   Authorization: Bearer $KEYPOOL_AUTH_TOKEN
```
- 两服务共享同一 MySQL/Redis；`MYSQL_ROOT_PASSWORD`、`KEYPOOL_AUTH_TOKEN` 用环境变量或 `.env` 覆盖，生产必改。
- 已有外部 MySQL/Redis/new-api：`docker compose up -d --build keypool`，并在 compose 里把 `DATABASE_DSN`/`REDIS_ADDR` 指向外部实例。
- new-api 侧建议：后台「运营设置」开"成功请求后自动启用通道"（测活成功自动恢复 key）；本场景 new-api 不转发流量，"自动禁用通道"开关无影响，若它同时转发其它渠道建议关闭以避免双写。
