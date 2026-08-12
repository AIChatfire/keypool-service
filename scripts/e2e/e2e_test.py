#!/usr/bin/env python3
"""keypool E2E test suite — real sqlite DB + real redis."""
import json, sqlite3, time, urllib.request, urllib.error, uuid

BASE = "http://127.0.0.1:18099"
TOK = "e2e-token"
DB = "/tmp/keypool-e2e.db"
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

def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))
    print(("PASS" if cond else "FAIL"), name, ("| " + str(detail)[:200] if detail else ""))

# T1 healthz 裸 JSON
s, b = call("GET", "/healthz", token=None)
check("T1 healthz bare json", s == 200 and b == {"status": "ok"}, b)

# T2 鉴权
s, b = call("POST", "/v1/key:get", {"channel_id": 12}, token=None)
check("T2 no-token -> 401", s == 401, s)

# T2b 参数校验
s, b = call("POST", "/v1/key:get", {"channel_id": 12, "mode": "bogus"})
check("T2b bad mode -> 40010", s == 400 and b.get("code") == 40010, (s, b.get("code")))
s, b = call("POST", "/v1/key:get", {})
check("T2c empty params -> 40010", s == 400 and b.get("code") == 40010, (s, b.get("code")))

# T3 轮询均匀性: 25 次, 5 把 key 应各 5 次
dist = {}
first_epoch = None
for _ in range(25):
    s, b = call("POST", "/v1/key:get", {"channel_id": 12})
    d = b["data"]; first_epoch = first_epoch or d["epoch"]
    dist[d["key_index"]] = dist.get(d["key_index"], 0) + 1
check("T3 polling uniform 25/5", sorted(dist) == [0,1,2,3,4] and len(set(dist.values())) == 1, dist)

# T4 上报 401 -> key_disabled, DB 验证
s, b = call("POST", "/v1/key:get", {"channel_id": 12})
victim = b["data"]["key_index"]
s, b = call("POST", "/v1/key:report", {"channel_id": 12, "key_index": victim,
    "success": False, "status_code": 401, "error_message": "Invalid API key provided"})
check("T4 report 401 -> key_disabled", b["data"]["action"] == "key_disabled", b["data"])
st, ci, _ = db_channel(12)
check("T4b db status_list[idx]=3 + reason/time", ci["multi_key_status_list"][str(victim)] == 3
      and str(victim) in ci.get("multi_key_disabled_reason", {}), ci)

# T5 禁用后不再被选中
got = set()
for _ in range(12):
    _, b = call("POST", "/v1/key:get", {"channel_id": 12})
    got.add(b["data"]["key_index"])
check("T5 disabled key skipped", victim not in got and len(got) == 4, got)

# T6 全部禁用 -> channel_disabled + abilities 关闭 + key:get 40001
acts = []
for _ in range(4):
    _, b = call("POST", "/v1/key:get", {"channel_id": 12})
    idx = b["data"]["key_index"]
    _, rb = call("POST", "/v1/key:report", {"channel_id": 12, "key_index": idx,
        "success": False, "status_code": 401, "error_message": "Invalid API key"})
    acts.append(rb["data"]["action"])
st, ci, oi = db_channel(12)
check("T6 last disable -> channel_disabled", acts[-1] == "channel_disabled", acts)
check("T6b channel status=3 + abilities off + reason", st == 3 and all(e == 0 for e in db_ability(12))
      and "All keys are disabled" in oi, (st, db_ability(12), oi))
s, b = call("POST", "/v1/key:get", {"channel_id": 12})
check("T6c key:get -> 503/40001", s == 503 and b.get("code") == 40001, (s, b.get("code")))

# T7 group+model 选渠道 (12 全灭 -> 13)
s, b = call("POST", "/v1/key:get", {"group": "default", "model": "gpt-4o"})
check("T7 group+model picks ch13", b["data"]["channel_id"] == 13 and b["data"]["key"] == "sk-single", b["data"])

# T8 逐个启用 -> 渠道与 abilities 恢复
for i in range(5):
    call("POST", f"/v1/channels/12/keys/{i}:enable", {"reason": "recover"})
st, ci, _ = db_channel(12)
check("T8 enable all -> channel+abilities restored", st == 1 and all(e == 1 for e in db_ability(12))
      and not ci.get("multi_key_status_list"), (st, db_ability(12), ci))

# T9 非多 key: 自动禁用 -> 成功上报自动启用
_, b = call("POST", "/v1/key:report", {"channel_id": 13, "key_index": 0,
    "success": False, "status_code": 401, "error_message": "Invalid API key"})
st13, _, _ = db_channel(13)
_, b = call("POST", "/v1/key:report", {"channel_id": 13, "key_index": 0, "success": True})
st13b, _, _ = db_channel(13)
check("T9 non-multi auto disable/enable", st13 == 3 and b["data"]["action"] == "enabled" and st13b == 1,
      (st13, b["data"], st13b))

# T10 幂等
key = "idem-" + uuid.uuid4().hex[:8]
_, b1 = call("POST", "/v1/key:report", {"channel_id": 12, "key_index": 0, "success": True}, idem=key)
s2, b2 = call("POST", "/v1/key:report", {"channel_id": 12, "key_index": 0, "success": True}, idem=key)
check("T10 idempotent duplicate -> 409/40003", b1["data"]["action"] == "none" and s2 == 409
      and b2.get("code") == 40003, (b1["data"], s2, b2.get("code")))

# T11 epoch 不匹配
_, b = call("POST", "/v1/key:report", {"channel_id": 12, "key_index": 0, "epoch": "deadbeef",
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
a1 = active_set()
got1 = set()
for _ in range(10):
    _, b = call("POST", "/v1/key:get", {"channel_id": 12})
    got1.add(b["data"]["key_index"]); band1 = b["data"]["band"]["index"]
check("T13b active==2 and selects within batch", len(a1) == 2 and got1 <= a1, (a1, got1))
# 等下一个带边界
now = time.time(); wait = 30 - (now % 30) + 1.5
time.sleep(wait)
a2 = active_set()
got2 = set()
for _ in range(10):
    _, b = call("POST", "/v1/key:get", {"channel_id": 12})
    got2.add(b["data"]["key_index"]); band2 = b["data"]["band"]["index"]
batches = [{0,1},{2,3},{4}]  # 5 keys / active_count=2 -> 2,2,1
expected2 = batches[band2 % 3]
check("T13c band advanced, new batch active", band2 == band1 + 1 and a2 == expected2
      and got2 <= a2 and a2 != a1, (band1, band2, a2, got2, expected2))
# 清理轮换配置
con = sqlite3.connect(DB); con.execute("DELETE FROM options WHERE `key`='keypool.rotation.12'"); con.commit(); con.close()
call("POST", "/v1/cache:invalidate", {})

# T14 usage 均衡
call("PUT", "/v1/channels/12/balance", {"mode": "usage", "metric": "tokens",
     "decay_interval": 3600, "decay_factor": 0.5})
_, b = call("POST", "/v1/key:get", {"channel_id": 12, "est_tokens": 100})
heavy = b["data"]["key_index"]; lease = b["data"].get("lease_id")
check("T14 lease issued", bool(lease), b["data"].get("lease_id"))
_, rb = call("POST", "/v1/key:report", {"channel_id": 12, "key_index": heavy, "lease_id": lease,
    "success": True, "usage": {"prompt_tokens": 90000, "completion_tokens": 10000}})
avoid = True
for _ in range(4):
    _, b = call("POST", "/v1/key:get", {"channel_id": 12})
    if b["data"]["key_index"] == heavy: avoid = False
s, ub = call("GET", "/v1/channels/12/usage")
check("T14 heavy key avoided after big usage", avoid, heavy)
check("T14b usage counter ~100000-100(pre-charge)", 99000 <= ub["data"]["counters"].get(str(heavy), 0) <= 100100,
      ub["data"]["counters"])

# T15 metrics
req = urllib.request.Request(BASE + "/metrics"); req.add_header("Authorization", "Bearer " + TOK)
with urllib.request.urlopen(req, timeout=10) as r: mtext = r.read().decode()
check("T15 metrics exposed", "keypool_select_total" in mtext and "keypool_report_total" in mtext)

fails = [n for n, ok, _ in results if not ok]
print(f"\n==== {len(results)-len(fails)}/{len(results)} PASS ====")
if fails: print("FAILED:", fails)
