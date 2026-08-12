# keypool SPEC — 单一事实源（所有 subagent 严格按此实现，不得擅自改动接口）

## 0. 项目定位
把 new-api 的多 key 管理能力暴露为 HTTP 薄服务。与 new-api 共用 MySQL/PostgreSQL/SQLite（channels、abilities、options 表）与 Redis。Go module 名：`keypool`。Go 1.22。仅允许使用 go.mod 已声明依赖 + 标准库（HTTP 用 net/http + Go1.22 ServeMux 路径参数，不引 web 框架）。

## 1. 目录与包契约
```
cmd/keypool/main.go            package main      # 装配：env→config→store→redisx→selector/state→api
internal/config/config.go      package config    # env 配置
internal/store/types.go        package store     # Channel/ChannelInfo/Ability/Option 类型与 JSON 语义
internal/store/store.go        package store     # DB 访问 + channel_info 写穿 + abilities 联动
internal/store/poller.go       package store     # options 表 60s 轮询 → ConfigSnapshot
internal/redisx/redisx.go      package redisx    # Redis client、锁、游标、用量、幂等、事件、Lua
internal/classifier/classifier.go package classifier # 禁用/启用判定（new-api 语义）
internal/selector/selector.go  package selector  # 渠道选择 + 批次轮换 + 选 key（调 redisx.Lua）
internal/state/state.go        package state     # 禁用/启用执行器（锁+事务写穿+事件）
internal/api/api.go            package api       # HTTP handlers + 鉴权 + 错误包络
Dockerfile  README.md  .env.example
```
包间依赖：api → {selector, state, store, redisx, config}；selector → {store, redisx}；state → {store, redisx, classifier}；classifier → {store(仅类型)}；redisx、config 不依赖任何内部包。**禁止反向依赖。**

## 2. new-api 语义基线（必须逐字节对齐）
### 2.1 channels 表用到的列（其余列不读不写）
`id, type, key, status, name, weight, base_url, models, `+"`group`"+`, priority, auto_ban, other_info, channel_info`
- status：1=Enabled 2=ManuallyDisabled 3=AutoDisabled（常量名同此）
- `key` 列含多 key：`[` 开头按 JSON 数组解析，否则按 `\n` 切分（Trim 末尾换行）
### 2.2 channel_info JSON（gorm `type:json`，字段名完全一致）
```json
{"is_multi_key":true,"multi_key_size":3,"multi_key_status_list":{"0":3},
 "multi_key_disabled_reason":{"0":"..."},"multi_key_disabled_time":{"0":1735689600},
 "multi_key_polling_index":0,"multi_key_mode":"polling"}
```
multi_key_mode ∈ {"random","polling"}；status_list 中**缺失索引视为启用(1)**；key 状态值域同渠道 status。
### 2.3 禁用/启用写穿语义（复刻 handlerMultiKeyUpdate + UpdateChannelStatus）
- 定位 key：优先 key_index；否则按 key 字符串在 GetKeys() 结果中精确匹配取首个
- 禁用 idx：status_list[idx]=3(自动)或2(手动)；写 disabled_reason/disabled_time(秒级时间戳)
- 启用 idx：从 status_list **删除** idx（同时删 reason/time）
- 全灭判定：GetKeys() 范围内无启用 key → channels.status=3 且 other_info 合并写入 `{"status_reason":"All keys are disabled","status_time":<ts>}`（other_info 为 JSON object，合并而非覆盖）；同时 `UPDATE abilities SET enabled=false WHERE channel_id=?`
- 恢复：存在启用 key 且操作=启用 → channels.status=1，abilities.enabled=true
- 非多 key 渠道：直接改 channels.status + other_info（reason/time）
- 单写者纪律：本服务只写 channels 的 `channel_info,status,other_info` 三列与 abilities.enabled
### 2.4 禁用判定（复刻 ShouldDisableChannel）
依次：全局开关 off→false；auto_ban=0→false；status_code 命中区间(默认"401")→true；error_message 小写化后包含任一关键词(默认 7 条见下)→true；否则 false。
默认关键词：Your credit balance is too low / This organization has been disabled. / You exceeded your current quota / Permission denied / The security token included in the request is invalid / Operation not allowed / Your account is not authorized
### 2.5 options 表（key/value 两列，PK=key）
复用 key：`AutomaticDisableChannelEnabled`(bool) `AutomaticEnableChannelEnabled`(bool) `AutomaticDisableStatusCodes`(`401,500-503`语法) `AutomaticDisableKeywords`(换行分隔,小写化) `ChannelDisableThreshold`(float 可忽略)。扩展 key 前缀 `keypool.`（balance/rotation 配置，JSON）。
### 2.6 abilities 表列：group, model, channel_id, enabled, priority, weight, tag

## 3. Redis 契约（统一前缀 keypool:）
| Key | 类型 | 说明 |
|---|---|---|
| keypool:cursor:{cid} | String | INCR 轮询游标 |
| keypool:usage:{cid} | Hash {idx:float} | token/cost 用量计数（衰减） |
| keypool:usage_meta:{cid} | Hash {last_decay:ts} | 衰减元数据 |
| keypool:lock:{cid} | String SET NX PX 5000 | 每渠道串行锁，value=随机 token，Lua 校验释放 |
| keypool:idem:{key} | String SET NX PX 600000 | report 幂等 |
| keypool:events | Stream XADD maxlen~10000 | 事件 {type,cid,idx,from,to,reason,ts} |
| keypool:cfg:{name} | String | keypool.* 配置镜像缓存(可选) |

### 3.1 select_key.lua（ARGV 契约，Go 计算候选集后单 RTT）
KEYS: [1]=cursor [2]=usage [3]=usage_meta
ARGV: mode(polling|random|usage), candidates(JSON int 数组，已按轮换过滤), est(float), decay_interval_sec, decay_factor, now_ts, jitter_pct(如0.05)
逻辑：①usage 模式且 now-last_decay>interval → 全部计数*=factor,写 last_decay；②候选为空→return -1；③polling: i=INCR(cursor)%len；random: rand；usage: 取计数最小者(并列取首个, 加 jitter 扰动: 有效计数=计数*(1+rand*jitter))；④usage 模式 HINCRBYFLOAT usage[idx] est；⑤return idx（candidates 中的真实 key 索引，非下标）
### 3.2 锁：Lock(cid) (token,ok,err) / Unlock(cid,token)；幂等：IdemSet(key) bool

## 4. 内部 Go 接口契约（签名一字不改）
```go
// config
type Config struct {
  Port int; AuthToken string; DatabaseType string; DatabaseDSN string // mysql|postgres|sqlite
  RedisAddr, RedisPass string; RedisDB int; SyncIntervalSec int      // env 见 §7
}
func Load() Config

// store
type ChannelInfo struct {
  IsMultiKey bool `json:"is_multi_key"`
  MultiKeySize int `json:"multi_key_size"`
  MultiKeyStatusList map[int]int `json:"multi_key_status_list,omitempty"`
  MultiKeyDisabledReason map[int]string `json:"multi_key_disabled_reason,omitempty"`
  MultiKeyDisabledTime map[int]int64 `json:"multi_key_disabled_time,omitempty"`
  MultiKeyPollingIndex int `json:"multi_key_polling_index"`
  MultiKeyMode string `json:"multi_key_mode"`
}
type Channel struct { /* §2.1 列, gorm tags 与 new-api 一致 */ }
func (c *Channel) GetKeys() []string            // §2.1 拆分语义
func (c *Channel) Epoch() string                // sha1(strings.Join(GetKeys(),"\x00"))[:8]
func (c *Channel) EnabledKeyIndexes() []int
type Store struct{ /* gorm.DB */ }
func Open(cfg config.Config) (*Store, error)    // 按 DatabaseType 选 driver, 表名复用 new-api
func (s *Store) GetChannel(id int) (*Channel, error)
func (s *Store) Abilities(group, model string) ([]Ability, error) // enabled=true
func (s *Store) GetChannelsByIDs(ids []int) ([]*Channel, error)
// 事务写穿：锁内被调用；语义=§2.3
func (s *Store) ApplyKeyStatus(cid, idx, status int, reason string) (channelStatus int, allDead bool, err error)
func (s *Store) PollOptions() (map[string]string, error)          // options 表全量
// 配置快照（原子替换）
type Settings struct {
  AutoDisableOn, AutoEnableOn bool; DisableCodeRanges []CodeRange
  DisableKeywords []string; Balance map[int]BalanceCfg; Rotation map[int]RotationCfg // key=cid
}
type CodeRange struct{ Start, End int }
func ParseCodeRanges(s string) ([]CodeRange, error)               // "401,500-503"
type BalanceCfg struct{ Mode, Metric string; DecayInterval, DecayFactor, Catchup float64 } // Mode: usage|request|auto; Metric: tokens|cost
type RotationCfg struct{ BandSeconds, ActiveCount, OverlapBands int; Order string }        // Order: index|shuffle
type SettingsProvider interface{ Get() *Settings }

// classifier
func ShouldDisable(st *store.Settings, autoBan bool, statusCode int, errMsg string) bool
func ShouldEnable(st *store.Settings, prevStatus int) bool        // AutoEnableOn && prevStatus==3

// selector
type SelectReq struct {
  ChannelID int; Group, Model string; Retry int
  Exclude []KeyRef; Mode string            // ""|polling|random|usage 覆盖渠道配置
  EstTokens float64; AdvanceCursor bool    // 默认 true；false=测活不推游标
}
type KeyRef struct{ ChannelID, KeyIndex int }
type SelectResp struct {
  ChannelID, KeyIndex int; Key, BaseURL, Mode, Epoch string
  Band *BandInfo `json:"band,omitempty"`
}
type BandInfo struct{ Index int; EndsAt int64 }
type Selector struct{ /* store, redisx, settingsProvider */ }
func (sl *Selector) Select(ctx, req SelectReq) (*SelectResp, error) // ErrNoKey(40001)/ErrNoChannel(40002)
// 内部规则：cid 直达→该渠道；否则 abilities 分层(priority desc 去重档位, retry 越界取最低档)+档内 weight 加权随机(weight+10)
// 候选集=EnabledKeyIndexes ∩ 轮换批次(无配置=全集)；空→look-ahead 后续批次；全空→ErrNoKey
// 批次划分：按 key 索引连续切分 ActiveCount 个/批；Order=shuffle 时用 sha1(cid) 做种子确定性洗牌
// band=floor(now/BandSeconds)%批数；OverlapBands>0 并入前 N 带批次
// AdvanceCursor=false → polling 模式只读游标不 INCR（Lua ARGV mode 传 "peek"）

// state
type ReportReq struct {
  LeaseID string; ChannelID, KeyIndex int; Key, Epoch string
  Success bool; StatusCode int; ErrorCode, ErrorMessage string
  Usage *Usage; IdempotencyKey string
}
type Usage struct{ PromptTokens, CompletionTokens float64; Cost float64 }
type ReportResp struct{ Action string; ChannelStatus int } // none|cooldown|key_disabled|channel_disabled|enabled|stale_epoch_ignored|duplicate
type Manager struct{ /* store, redisx, classifier, settingsProvider */ }
func (m *Manager) Report(ctx, r ReportReq) (*ReportResp, error)
func (m *Manager) SetKeyStatus(ctx, cid, idx, status int, reason string) (*ReportResp, error) // 管理端手动启停(status 1|2)
// 流程：幂等→epoch 校验(不一致 stale_epoch_ignored)→用量校正(HINCRBYFLOAT actual-est 由 api 传入 est)→
// 锁(cid)→成功&自动启用→ApplyKeyStatus(enable)；失败→ShouldDisable→ApplyKeyStatus(3,errMsg截断256)→解锁→XADD 事件

// api（统一包络 {"code":0,"message":"ok","data":...,"request_id":"..."}；鉴权 Bearer AuthToken；错误码 §5）
func NewRouter(cfg config.Config, sl *selector.Selector, m *state.Manager, s *store.Store, sp store.SettingsProvider, rdb *redisx.Client) http.Handler
```

## 5. HTTP 接口与错误码
- POST /v1/key:get ← SelectReq → SelectResp（503 code=40001 无可用 key；404 code=40002 渠道不存在）
- POST /v1/key:report ← ReportReq（头部 Idempotency-Key 或 body 同名字段）→ ReportResp；409 code=40003 幂等冲突
- GET /v1/channels/{id}/keys → {epoch, mode, keys:[{index, status, reason, disabled_time, usage, rotation_state, key_mask}]}（key 脱敏：前4后4）
- POST /v1/channels/{id}/keys/{idx}:enable | :disable（body {reason}）→ ReportResp
- PUT /v1/channels/{id}/balance | /v1/channels/{id}/rotation（body=BalanceCfg|RotationCfg JSON，写入 options 表 keypool.balance.{cid} / keypool.rotation.{cid}，并刷新快照）；GET 同路径读取
- GET /v1/channels/{id}/usage → {cid, metric, counters:{idx:val}, last_decay}
- POST /v1/cache:invalidate → 快照重建（重新 PollOptions + 清理本地缓存）
- GET /healthz → {status:"ok"}；GET /metrics → Prometheus 文本（keypool_select_total{cid,idx}、keypool_report_total{action}、keypool_band_lookahead_total）
- 错误包络 code：40001/40002/40003/40010(参数)/50001(依赖故障)

## 6. 组装顺序（main.go）
Load env → redisx.NewClient(PING 失败则 degraded=true，仅 log)→ store.Open（gorm, 禁用默认事务嵌套, 连接池 20）→ 启动 OptionsPoller(60s)→ Selector/Manager → api.NewRouter → http.Server(:PORT)。优雅退出 10s。

## 7. env（.env.example 完整列出）
PORT=8080 AUTH_TOKEN=change-me DATABASE_TYPE=mysql DATABASE_DSN="user:pass@tcp(127.0.0.1:3306)/newapi?parseTime=true" REDIS_ADDR=127.0.0.1:6379 REDIS_PASS= REDIS_DB=0 SYNC_INTERVAL_SEC=60

## 8. 验收
- go build ./... 与 go vet ./... 零错误
- 每包至少 1 个单测（拆分/区间解析/批次划分/Lua ARGV 构造/禁用判定），go test ./... 通过（不依赖真实 DB/Redis，用接口fake）
- README：架构图(ascii)、接口 curl 示例、与 new-api 共存 checklist（关 new-api 自动禁用、单写者）
