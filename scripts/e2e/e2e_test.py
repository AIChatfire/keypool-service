#!/usr/bin/env python3
"""keypool E2E test suite — real sqlite DB + real redis."""
import json, os, sqlite3, time, urllib.request, urllib.error, uuid

BASE = os.environ.get("E2E_BASE", "http://127.0.0.1:18099")
TOK = "e2e-token"
DB = os.environ.get("E2E_DB", "/tmp/keypool-e2e.db")
results = []

def call(method, path, body=None, token=TOK, idem=None, raw=False):
    req = urllib.request.Request(BASE + path, method=method)
    if token: req.add_header("Authorization", "Bearer " + token)
    if idem: req.add_header("Idempotency-Key", idem)
    data = None
    if body is not None:
        req.add_header("Content-Type", "application/json")
        data = json.dumps(body).encode()
    try:
        with urllib.request.urlopen(req, data, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read())
        except Exception: return e.code, {}

def db_channel(cid):
    con = sqlite3.connect(DB); con.row_factory = sqlite3.Row
    r = con.execute("SELECT status, channel_info, other_info FROM channels WHERE id=?", (cid,)).fetchone()
    con.close()
    return r["status"], json.loads(r["channel_info"]), r["other_info"]

def db_ability(cid):
    con = sqlite3.connect(DB)
    rows = con.execute("SELECT enabled FROM abilities WHERE channel_id=?", (cid,)).fetchall()
    con.close()
    return [r[0] for r in rows]

def db_retry(fetch, pred, tries=15, delay=0.2):
    # keypool 跑在容器、本脚本在宿主机直读绑定挂载的 sqlite 文件时，
    # Docker Desktop 文件共享对容器内提交偶发延迟可见（写路径本身
    # 同步提交，由 API 响应与单元测试保证）。对宿主机侧 DB 断言做
    # 短重试，消除环境性 flake，不改变被测语义。
    val = fetch()
    for _ in range(tries):
        if pred(val):
            return val
        time.sleep(delay)
        val = fetch()
    return val

def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))
    print(("PASS" if cond else "FAIL"), name, ("| " + str(detail)[:200] if detail else ""))

# T1 healthz 裸 JSON
s, b = call("GET", "/healthz", token=None)
check("T1 healthz bare json", s == 200 and b == {"status": "ok"}, b)

# T2 鉴权
s, b = call("POST", "/v1/keys/select", {"channel_id": 12}, token=None)
check("T2 no-token -> 401", s == 401, s)

# T2b 参数校验
s, b = call("POST", "/v1/keys/select", {"channel_id": 12, "mode": "bogus"})
check("T2b bad mode -> 40010", s == 400 and b.get("code") == 40010, (s, b.get("code")))
s, b = call("POST", "/v1/keys/select", {})
check("T2c empty params -> 40010", s == 400 and b.get("code") == 40010, (s, b.get("code")))

# T3 轮询均匀性: 25 次, 5 把 key 应各 5 次
dist = {}
first_epoch = None
for _ in range(25):
    s, b = call("POST", "/v1/keys/select", {"channel_id": 12})
    d = b["data"]; first_epoch = first_epoch or d["epoch"]
    dist[d["key_index"]] = dist.get(d["key_index"], 0) + 1
check("T3 polling uniform 25/5", sorted(dist) == [0,1,2,3,4] and len(set(dist.values())) == 1, dist)

# T4 上报 401 -> key_disabled, DB 验证
s, b = call("POST", "/v1/keys/select", {"channel_id": 12})
victim = b["data"]["key_index"]
s, b = call("POST", "/v1/keys/report", {"channel_id": 12, "key_index": victim,
    "success": False, "status_code": 401, "error_message": "Invalid API key provided"})
check("T4 report 401 -> key_disabled", b["data"]["action"] == "key_disabled", b["data"])
st, ci, _ = db_retry(lambda: db_channel(12),
                     lambda r: str(victim) in r[1].get("multi_key_status_list", {}))
check("T4b db status_list[idx]=3 + reason/time", ci["multi_key_status_list"][str(victim)] == 3
      and str(victim) in ci.get("multi_key_disabled_reason", {}), ci)

# T5 禁用后不再被选中
got = set()
for _ in range(12):
    _, b = call("POST", "/v1/keys/select", {"channel_id": 12})
    got.add(b["data"]["key_index"])
check("T5 disabled key skipped", victim not in got and len(got) == 4, got)

# T6 全部禁用 -> channel_disabled + abilities 关闭 + keys/select 40001
acts = []
for _ in range(4):
    _, b = call("POST", "/v1/keys/select", {"channel_id": 12})
    idx = b["data"]["key_index"]
    _, rb = call("POST", "/v1/keys/report", {"channel_id": 12, "key_index": idx,
        "success": False, "status_code": 401, "error_message": "Invalid API key"})
    acts.append(rb["data"]["action"])
st, ci, oi = db_retry(lambda: db_channel(12), lambda r: r[0] == 3)
ab12 = db_retry(lambda: db_ability(12), lambda a: all(e == 0 for e in a))
check("T6 last disable -> channel_disabled", acts[-1] == "channel_disabled", acts)
check("T6b channel status=3 + abilities off + reason", st == 3 and all(e == 0 for e in ab12)
      and "All keys are disabled" in oi, (st, ab12, oi))
s, b = call("POST", "/v1/keys/select", {"channel_id": 12})
check("T6c keys/select -> 503/40001", s == 503 and b.get("code") == 40001, (s, b.get("code")))

# T7 group+model 选渠道 (12 全灭 -> 13)
s, b = call("POST", "/v1/keys/select", {"group": "default", "model": "gpt-4o"})
check("T7 group+model picks ch13", b["data"]["channel_id"] == 13 and b["data"]["key"] == "sk-single", b["data"])

# T8 逐个启用 -> 渠道与 abilities 恢复
for i in range(5):
    call("PATCH", f"/v1/channels/12/keys/{i}", {"status": "enabled", "reason": "recover"})
st, ci, _ = db_retry(lambda: db_channel(12),
                     lambda r: r[0] == 1 and not r[1].get("multi_key_status_list"))
ab12 = db_retry(lambda: db_ability(12), lambda a: all(e == 1 for e in a))
check("T8 enable all -> channel+abilities restored", st == 1 and all(e == 1 for e in ab12)
      and not ci.get("multi_key_status_list"), (st, ab12, ci))

# T9 非多 key: 自动禁用 -> 成功上报自动启用
_, b = call("POST", "/v1/keys/report", {"channel_id": 13, "key_index": 0,
    "success": False, "status_code": 401, "error_message": "Invalid API key"})
st13, _, _ = db_retry(lambda: db_channel(13), lambda r: r[0] == 3)
_, b = call("POST", "/v1/keys/report", {"channel_id": 13, "key_index": 0, "success": True})
st13b, _, _ = db_retry(lambda: db_channel(13), lambda r: r[0] == 1)
check("T9 non-multi auto disable/enable", st13 == 3 and b["data"]["action"] == "enabled" and st13b == 1,
      (st13, b["data"], st13b))

# T10 幂等
key = "idem-" + uuid.uuid4().hex[:8]
_, b1 = call("POST", "/v1/keys/report", {"channel_id": 12, "key_index": 0, "success": True}, idem=key)
s2, b2 = call("POST", "/v1/keys/report", {"channel_id": 12, "key_index": 0, "success": True}, idem=key)
check("T10 idempotent duplicate -> 409/40003", b1["data"]["action"] == "none" and s2 == 409
      and b2.get("code") == 40003, (b1["data"], s2, b2.get("code")))

# T11 epoch 不匹配
_, b = call("POST", "/v1/keys/report", {"channel_id": 12, "key_index": 0, "epoch": "deadbeef",
    "success": False, "status_code": 401, "error_message": "Invalid API key"})
check("T11 stale epoch ignored", b["data"]["action"] == "stale_epoch_ignored", b["data"])

# T12 /keys 脱敏与状态
s, b = call("GET", "/v1/channels/12/keys")
k0 = b["data"]["keys"][0]
check("T12 keys masked + epoch", b["data"]["epoch"] == first_epoch and "****" in k0["key_mask"]
      and k0["status"] == 1, (b["data"]["epoch"], k0))

# T13 时间带轮换: 30s 带, 同时在岗 2 把
s, b = call("PUT", "/v1/channels/12/rotation", {"band_seconds": 30, "active_count": 2, "order": "index"})
check("T13 put rotation", s == 200 and b.get("code") == 0, b.get("code"))
def active_set():
    _, r = call("GET", "/v1/channels/12/keys")
    return {k["index"] for k in r["data"]["keys"] if k.get("rotation_state") == "active"}
batches = [{0,1},{2,3},{4}]  # 5 keys / active_count=2 -> 2,2,1（含取模回绕，勿假设首批）
# 以首次 select 返回的 band 为基准推导当前批次，消除运行时刻依赖
_, b = call("POST", "/v1/keys/select", {"channel_id": 12})
band1 = b["data"]["band"]["index"]
a1 = active_set()
expected1 = batches[band1 % 3]
got1 = set()
for _ in range(10):
    _, b = call("POST", "/v1/keys/select", {"channel_id": 12})
    got1.add(b["data"]["key_index"])
check("T13b active batch matches band and selects within batch", a1 == expected1 and got1 <= a1,
      (band1, a1, got1, expected1))
# 等下一个带边界
now = time.time(); wait = 30 - (now % 30) + 1.5
time.sleep(wait)
a2 = active_set()
got2 = set()
for _ in range(10):
    _, b = call("POST", "/v1/keys/select", {"channel_id": 12})
    got2.add(b["data"]["key_index"]); band2 = b["data"]["band"]["index"]
expected2 = batches[band2 % 3]
check("T13c band advanced, new batch active", band2 == (band1 + 1) % 3 and a2 == expected2
      and got2 <= a2 and a2 != a1, (band1, band2, a2, got2, expected2))
# 清理轮换配置
con = sqlite3.connect(DB); con.execute("DELETE FROM options WHERE `key`='keypool.rotation.12'"); con.commit(); con.close()
call("POST", "/v1/settings/reload", {})

# T14 usage 均衡
call("PUT", "/v1/channels/12/balance", {"mode": "usage", "metric": "tokens",
     "decay_interval": 3600, "decay_factor": 0.5})
_, b = call("POST", "/v1/keys/select", {"channel_id": 12, "est_tokens": 100})
heavy = b["data"]["key_index"]; lease = b["data"].get("lease_id")
check("T14 lease issued", bool(lease), b["data"].get("lease_id"))
_, rb = call("POST", "/v1/keys/report", {"channel_id": 12, "key_index": heavy, "lease_id": lease,
    "success": True, "usage": {"prompt_tokens": 90000, "completion_tokens": 10000}})
avoid = True
for _ in range(4):
    _, b = call("POST", "/v1/keys/select", {"channel_id": 12})
    if b["data"]["key_index"] == heavy: avoid = False
s, ub = call("GET", "/v1/channels/12/usage")
check("T14 heavy key avoided after big usage", avoid, heavy)
check("T14b usage counter ~100000-100(pre-charge)", 99000 <= ub["data"]["counters"].get(str(heavy), 0) <= 100100,
      ub["data"]["counters"])

# T15 metrics
req = urllib.request.Request(BASE + "/metrics"); req.add_header("Authorization", "Bearer " + TOK)
with urllib.request.urlopen(req, timeout=10) as r: mtext = r.read().decode()
check("T15 metrics exposed", "keypool_select_total" in mtext and "keypool_report_total" in mtext)

# T16 select include_channel：附带 new-api 渠道元数据
s, b = call("POST", "/v1/keys/select", {"channel_id": 12, "include_channel": True})
meta = b["data"].get("channel") or {}
check("T16 select include_channel meta", s == 200 and meta.get("id") == 12
      and meta.get("name") == "multi5" and meta.get("multi_key") is True
      and "gpt-4o" in (meta.get("models") or []), meta)
# T16c 投影只含 Web 可配置字段：epoch/key_count/测活/余额等运行态不返回
check("T16c meta excludes runtime fields",
      all(k not in meta for k in ("epoch", "key_count", "enabled_key_count",
          "created_time", "test_time", "response_time", "balance",
          "balance_updated_time", "used_quota", "other_info")), meta)
# 默认不附带
s, b = call("POST", "/v1/keys/select", {"channel_id": 12})
check("T16b channel omitted by default", "channel" not in b["data"], b["data"].keys())

# T17 GET /v1/channels/{id} 渠道元数据
s, b = call("GET", "/v1/channels/13")
m13 = b["data"]
check("T17 channel meta endpoint", s == 200 and m13.get("name") == "single"
      and m13.get("multi_key") is False and m13.get("priority") == 5
      and m13.get("models") == ["gpt-4o"], m13)
# T17c Web 可配置元数据全量透出：标头覆盖/模型映射/参数覆盖/渠道设置等
check("T17c channel meta web-configurable projection",
      m13.get("tag") == "paid" and m13.get("remark") == "e2e 全量元数据"
      and m13.get("openai_organization") == "org-e2e" and m13.get("test_model") == "gpt-4o"
      and m13.get("header_override", {}).get("X-Custom-Header") == "e2e"
      and m13.get("model_mapping", {}).get("gpt-4o") == "gpt-4o-2024-08-06"
      and m13.get("status_code_mapping", {}).get("503") == "500"
      and m13.get("param_override", {}).get("temperature") == 0.5
      and m13.get("setting", {}).get("proxy") == "http://127.0.0.1:7890"
      and m13.get("settings", {}).get("azure_api_version") == "2024-08-01-preview"
      and m13.get("other", {}).get("region") == "us", m13)
# T17d 运行态/统计字段不透出（DB 有值也不返回）
check("T17d meta excludes runtime fields",
      all(k not in m13 for k in ("epoch", "key_count", "enabled_key_count",
          "created_time", "test_time", "response_time", "balance",
          "balance_updated_time", "used_quota", "other_info")), m13)
s, b = call("GET", "/v1/channels/999")
check("T17b unknown channel -> 404/40002", s == 404 and b.get("code") == 40002, (s, b.get("code")))

# T18 PATCH 手动禁用/启用 key + 非法 status 校验
s, b = call("PATCH", "/v1/channels/12/keys/0", {"status": "disabled", "reason": "e2e manual"})
st, ci, _ = db_retry(lambda: db_channel(12),
                     lambda r: "0" in r[1].get("multi_key_status_list", {}))
check("T18 PATCH disable -> status_list[0]=2", s == 200 and b["data"]["action"] == "key_disabled"
      and ci["multi_key_status_list"]["0"] == 2, (b["data"], ci.get("multi_key_status_list")))
s, b = call("PATCH", "/v1/channels/12/keys/0", {"status": "enabled"})
st, ci, _ = db_retry(lambda: db_channel(12),
                     lambda r: "0" not in r[1].get("multi_key_status_list", {}))
check("T18b PATCH enable -> removed from status_list", s == 200 and b["data"]["action"] == "enabled"
      and "0" not in ci.get("multi_key_status_list", {}), (b["data"], ci.get("multi_key_status_list")))
s, b = call("PATCH", "/v1/channels/12/keys/0", {"status": "bogus"})
check("T18c bad status -> 40010", s == 400 and b.get("code") == 40010, (s, b.get("code")))

# T19 旧动作式路径不再可用（无兼容层）
s, _ = call("POST", "/v1/key:get", {"channel_id": 12})
check("T19 legacy key:get gone", s in (404, 405), s)

# T20 channel_id + key_index 精确直达
s, b = call("POST", "/v1/keys/select", {"channel_id": 12, "key_index": 3})
d20 = b.get("data") or {}
check("T20 direct hit key_index=3", s == 200 and d20.get("key_index") == 3
      and d20.get("mode") == "direct" and "band" not in d20 and "lease_id" not in d20, d20)
# 连续 5 次都返回同一把（不推游标、不受轮询影响）
same = all((call("POST", "/v1/keys/select", {"channel_id": 12, "key_index": 3})[1]["data"]["key_index"] == 3)
           for _ in range(5))
check("T20b direct is deterministic", same)
# key_index=0 合法（不被零值遮蔽）
s, b = call("POST", "/v1/keys/select", {"channel_id": 12, "key_index": 0})
check("T20c direct key_index=0 valid", s == 200 and b["data"]["key_index"] == 0, b.get("data"))
# 缺 channel_id -> 40010
s, b = call("POST", "/v1/keys/select", {"group": "default", "model": "gpt-4o", "key_index": 1})
check("T20d key_index without channel_id -> 40010", s == 400 and b.get("code") == 40010, (s, b.get("code")))
# 负数 / 越界 -> 40010
s, b = call("POST", "/v1/keys/select", {"channel_id": 12, "key_index": -1})
check("T20e negative key_index -> 40010", s == 400 and b.get("code") == 40010, (s, b.get("code")))
s, b = call("POST", "/v1/keys/select", {"channel_id": 12, "key_index": 99})
check("T20f out-of-range key_index -> 40010", s == 400 and b.get("code") == 40010, (s, b.get("code")))
# 被禁用的 key -> 503/40001
call("PATCH", "/v1/channels/12/keys/1", {"status": "disabled", "reason": "e2e direct"})
db_retry(lambda: db_channel(12), lambda r: "1" in r[1].get("multi_key_status_list", {}))
s, b = call("POST", "/v1/keys/select", {"channel_id": 12, "key_index": 1})
check("T20g disabled key direct -> 503/40001", s == 503 and b.get("code") == 40001, (s, b.get("code")))
call("PATCH", "/v1/channels/12/keys/1", {"status": "enabled"})
db_retry(lambda: db_channel(12), lambda r: "1" not in r[1].get("multi_key_status_list", {}))
# 直达命中计入 select_direct_total
req = urllib.request.Request(BASE + "/metrics"); req.add_header("Authorization", "Bearer " + TOK)
with urllib.request.urlopen(req, timeout=10) as r: mtext2 = r.read().decode()
check("T20h select_direct_total exposed", "keypool_select_direct_total" in mtext2)

fails = [n for n, ok, _ in results if not ok]
print(f"\n==== {len(results)-len(fails)}/{len(results)} PASS ====")
if fails: print("FAILED:", fails)
