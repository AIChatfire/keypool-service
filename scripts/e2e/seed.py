#!/usr/bin/env python3
"""E2E 种子数据：创建 new-api 兼容的 sqlite 库（channels/abilities/options）并重置 Redis。
用法: python3 scripts/e2e/seed.py [/tmp/keypool-e2e.db] [redis-host:port]
(随后启动 keypool 并调 /v1/settings/reload)"""
import sqlite3, json, sys, socket

db_path = sys.argv[1] if len(sys.argv) > 1 else '/tmp/keypool-e2e.db'
redis_addr = sys.argv[2] if len(sys.argv) > 2 else '127.0.0.1:6379'
con = sqlite3.connect(db_path)
c = con.cursor()
c.executescript('''
DROP TABLE IF EXISTS channels;
CREATE TABLE channels (id INTEGER PRIMARY KEY, type INTEGER DEFAULT 0, key TEXT NOT NULL,
 open_ai_organization TEXT, test_model TEXT,
 status INTEGER DEFAULT 1, name TEXT, weight INTEGER DEFAULT 0,
 created_time INTEGER DEFAULT 0, test_time INTEGER DEFAULT 0, response_time INTEGER DEFAULT 0,
 base_url TEXT DEFAULT '', other TEXT DEFAULT '', balance REAL DEFAULT 0, balance_updated_time INTEGER DEFAULT 0,
 models TEXT, `group` TEXT DEFAULT 'default', used_quota INTEGER DEFAULT 0,
 model_mapping TEXT, status_code_mapping TEXT,
 priority INTEGER DEFAULT 0, auto_ban INTEGER DEFAULT 1, other_info TEXT,
 tag TEXT, setting TEXT, param_override TEXT, header_override TEXT, remark TEXT,
 channel_info TEXT, settings TEXT DEFAULT '');
CREATE TABLE IF NOT EXISTS abilities (`group` TEXT, model TEXT, channel_id INTEGER, enabled BOOLEAN,
 priority INTEGER DEFAULT 0, weight INTEGER DEFAULT 0, tag TEXT, PRIMARY KEY(`group`,model,channel_id));
CREATE TABLE IF NOT EXISTS options (`key` TEXT PRIMARY KEY, value TEXT);
DELETE FROM channels; DELETE FROM abilities; DELETE FROM options;
''')
ci5 = json.dumps({"is_multi_key": True, "multi_key_size": 5, "multi_key_polling_index": 0, "multi_key_mode": "polling"})
c.execute("INSERT INTO channels (id,type,key,status,name,base_url,models,`group`,other_info,channel_info) "
          "VALUES (12,1,?,1,'multi5','https://api.upstream.test','gpt-4o,gpt-4o-mini','default','{}',?)",
          ("\n".join(["sk-alpha","sk-beta","sk-gamma","sk-delta","sk-epsilon"]), ci5))
# 渠道 13：全量元数据样例（含自定义请求标头/模型映射/参数覆盖/渠道设置等）
c.execute("INSERT INTO channels (id,type,key,open_ai_organization,test_model,status,name,priority,"
          "created_time,test_time,response_time,base_url,other,balance,balance_updated_time,"
          "models,`group`,used_quota,model_mapping,status_code_mapping,other_info,"
          "tag,setting,param_override,header_override,remark,channel_info,settings) "
          "VALUES (13,1,'sk-single','org-e2e','gpt-4o',1,'single',5,"
          "1700000000,1700000100,233,'https://api2.test','{\"region\":\"us\"}',12.5,1700000200,"
          "'gpt-4o','default',12345,'{\"gpt-4o\":\"gpt-4o-2024-08-06\"}','{\"503\":\"500\"}','{}',"
          "'paid','{\"proxy\":\"http://127.0.0.1:7890\"}','{\"temperature\":0.5}',"
          "'{\"X-Custom-Header\":\"e2e\",\"Authorization\":\"Bearer override\"}',"
          "'e2e 全量元数据','{\"is_multi_key\":false}','{\"azure_api_version\":\"2024-08-01-preview\"}')")
for m in ("gpt-4o", "gpt-4o-mini"):
    c.execute("INSERT INTO abilities VALUES ('default',?,12,1,0,0,NULL)", (m,))
c.execute("INSERT INTO abilities VALUES ('default','gpt-4o',13,1,5,0,NULL)")
c.executemany("INSERT INTO options VALUES (?,?)", {
    "AutomaticDisableChannelEnabled": "true", "AutomaticEnableChannelEnabled": "true",
    "AutomaticDisableStatusCodes": "401", "AutomaticDisableKeywords": "invalid api key\nquota exceeded"}.items())
con.commit(); con.close()
try:
    host, _, port = redis_addr.partition(':')
    s = socket.create_connection((host, int(port or 6379)), 2)
    s.sendall(b'FLUSHALL\r\n'); s.recv(50); s.close()
except OSError:
    pass
print("seed OK ->", db_path)
