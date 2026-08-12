# keypool 落地计划 — new-api 多 key 管理薄服务

## 目标
交付可编译运行的 Go 项目 keypool：把 new-api 多 key 轮询能力暴露为 HTTP 接口，共用 new-api 的 DB（channels/abilities/options 表）与 Redis。

## 已确认的设计基线（前三轮结论，subagent 必须严格遵守）
- key 拆分复刻 new-api `GetKeys()`：`[` 开头按 JSON 数组，否则按 `\n` 切分
- 轮询复刻 `GetNextEnabledKey()` 语义：status_list 缺省=启用；游标取模扫描
- 禁用回写复刻 `handlerMultiKeyUpdate()`：key 状态(3=自动禁用/2=手动)+reason/time；全灭才 channels.status=3 + other_info + abilities.enabled=false；恢复反向
- 禁用判定复刻 `ShouldDisableChannel`：总开关 + auto_ban + 状态码区间(默认401) + 关键词小写匹配
- options 表 60s 轮询：AutomaticDisableChannelEnabled / AutomaticDisableStatusCodes / AutomaticDisableKeywords / AutomaticEnableChannelEnabled / keypool.* 扩展
- Redis：每渠道锁 keypool:lock:{cid}、游标 keypool:cursor:{cid}、用量 keypool:usage:{cid}、幂等 keypool:idem:{key}
- 增强：usage 均衡（预扣+校正+衰减+min入场）、时间带轮换（band=floor(t/T) 纯函数，候选集=启用∩批次，不写DB状态）
- epoch=sha1(key列表)[:8]，report 校验，防索引漂移

## 接口
- POST /v1/key:get（channel_id 或 group+model；mode; est_tokens; exclude; advance_cursor）
- POST /v1/key:report（lease/epoch + usage + 错误信息；Idempotency-Key）
- GET /v1/channels/{id}/keys（key 脱敏）
- POST /v1/channels/{id}/keys/{idx}:enable | :disable
- PUT/GET /v1/channels/{id}/balance、/v1/channels/{id}/rotation
- GET /v1/channels/{id}/usage
- POST /v1/cache:invalidate
- GET /healthz /metrics

## 阶段
- Stage 1：读 vibecoding-general-swarm 技能，按规范编排
- Stage 2：脚手架 + 领域层（model/store：Channel/ChannelInfo/Ability/Option + GetKeys + 回写语义）
- Stage 3：Redis 层（Lua 选 key、锁、游标、用量、幂等）
- Stage 4：业务层（selector 渠道/批次/策略、classifier 禁用判定、state 状态机写穿）
- Stage 5：HTTP API + main 装配 + Dockerfile + README
- Stage 6：review + go build/vet 通过 + 交付 zip

## 目录
/mnt/agents/output/keypool/ （go module: keypool）
