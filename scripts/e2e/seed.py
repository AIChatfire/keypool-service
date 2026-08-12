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
CREATE TABLE IF NOT EXISTS channels (id INTEGER PRIMARY KEY, type INTEGER DEFAULT 0, key TEXT NOT NULL,
 status INTEGER DEFAULT 1, name TEXT, weight INTEGER DEFAULT 0, base_url TEXT DEFAULT '',
 models TEXT, `group` TEXT DEFAULT 'default', priority INTEGER DEFAULT 0,
 auto_ban INTEGER DEFAULT 1, other_info TEXT, channel_info TEXT);
CREATE TABLE IF NOT EXISTS abilities (`group` TEXT, model TEXT, channel_id INTEGER, enabled BOOLEAN,
 priority INTEGER DEFAULT 0, weight INTEGER DEFAULT 0, tag TEXT, PRIMARY KEY(`group`,model,channel_id));
CREATE TABLE IF NOT EXISTS options (`key` TEXT PRIMARY KEY, value TEXT);
DELETE FROM channels; DELETE FROM abilities; DELETE FROM options;
''')
ci5 = json.dumps({"is_multi_key": True, "multi_key_size": 5, "multi_key_polling_index": 0, "multi_key_mode": "polling"})
c.execute("INSERT INTO channels VALUES (12,1,?,1,'multi5',0,'https://api.upstream.test','gpt-4o,gpt-4o-mini','default',0,1,'{}',?)",
          ("\n".join(["sk-alpha","sk-beta","sk-gamma","sk-delta","sk-epsilon"]), ci5))
c.execute("INSERT INTO channels VALUES (13,1,'sk-single',1,'single',0,'https://api2.test','gpt-4o','default',5,1,'{}','{\"is_multi_key\":false}')")
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
