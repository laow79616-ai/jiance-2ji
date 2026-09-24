#!/usr/bin/env python3
"""TG用户名检测工具 - 后端Agent
运行在8902端口，提供水军管理、批量检测、关键词黑名单等API
修复：使用独立线程运行asyncio事件循环，解决Telethon兼容性问题
"""
import os
import json
import time
import asyncio
import threading
import random
from datetime import datetime
from functools import wraps
from flask import Flask, request, jsonify



app = Flask(__name__)

# ============ 配置 ============
AUTH_KEY = "fe570f573d2840308f6a298daa3ad4a0"
API_POOL_FILE = "/root/tg-scan-clean/api_pool.json"
PROXY_POOL_FILE = "/root/tg-scan-clean/proxy_pool.json"
COOLDOWN_FILE = "/root/tg-scan-clean/bot_cooldown.json"
DAILY_STATS_FILE = "/root/tg-scan-clean/bot_daily_stats.json"
DEFAULT_DAILY_LIMIT = 500
MAX_BATCH_SIZE = 300

LOGIN_USER = "admin"
LOGIN_PASS = "Ab123456987"
CONFIG_FILE = "/root/tg-scan-clean/config.json"
BLACKLIST_FILE = "/root/tg-scan-clean/blacklist.json"
AVAILABLE_FILE = "/root/tg-scan-clean/available_usernames.json"
PREMIUM_FILE = "/root/tg-scan-clean/premium_usernames.json"

SEEN_FILE = "/root/tg-scan-clean/checked_history.json"
SKIP_LOG_FILE = "/root/tg-scan-clean/skip_history.json"
CHECK_POOL_FILE = "/root/tg-scan-clean/check_pool.json"
TARGETS_FILE = "/root/tg-scan-clean/target_usernames.json"
SESSIONS_DIR = "/root/tg-scan-clean/sessions"

# Telethon API 配置（多组轮换）
API_CONFIGS = [
    {"api_id": 31034207, "api_hash": "c6d49c6a93371381efb3fa3033d7c73a"},
    {"api_id": 37900420, "api_hash": "417d6f1b7e58418e81dff5b2c4f33943"},
    {"api_id": 38928927, "api_hash": "e5e54a03a61c3c9083899d9dffecabaf"},
    {"api_id": 36404424, "api_hash": "4fe722fcee7095dd78dab10ce3a7d1f0"},
    {"api_id": 24701885, "api_hash": "6FE8f295d213368f7b489982c68d6b6d"},
]

# 确保目录存在
os.makedirs("/root/tg-scan-clean", exist_ok=True)
os.makedirs(SESSIONS_DIR, exist_ok=True)

# ============ 独立的asyncio事件循环（在单独线程中运行） ============
_loop = asyncio.new_event_loop()
_loop_thread = None

def start_event_loop():
    """在独立线程中运行事件循环"""
    asyncio.set_event_loop(_loop)
    _loop.run_forever()

def ensure_loop_running():
    """确保事件循环线程在运行"""
    global _loop_thread
    if _loop_thread is None or not _loop_thread.is_alive():
        _loop_thread = threading.Thread(target=start_event_loop, daemon=True)
        _loop_thread.start()

def run_async(coro):
    """在独立事件循环中运行协程并等待结果"""
    ensure_loop_running()
    future = asyncio.run_coroutine_threadsafe(coro, _loop)
    return future.result(timeout=60)

# ============ 工具函数 ============

def _norm_uname(u):
    u = str(u or "").strip()
    if u.startswith("@"):
        u = u[1:]
    return u.lower()

def load_json_list(path):
    if not os.path.exists(path):
        return []
    try:
        data = json.load(open(path, encoding="utf-8"))
    except Exception:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("usernames") or data.get("items") or []
    return []

def load_skip_set():
    skip = set()
    for path in (TARGETS_FILE if "TARGETS_FILE" in globals() else "/root/tg-scan-clean/target_usernames.json",
                 PREMIUM_FILE if "PREMIUM_FILE" in globals() else "/root/tg-scan-clean/premium_usernames.json",
                 "/root/tg-scan-clean/checked_history.json"):
        for item in load_json_list(path):
            if isinstance(item, dict):
                u = item.get("username") or item.get("user") or ""
            else:
                u = item
            n = _norm_uname(u)
            if n:
                skip.add(n)
    return skip

def save_checked_history(usernames):
    path = "/root/tg-scan-clean/checked_history.json"
    old = load_json_list(path)
    have = set(_norm_uname(x if not isinstance(x, dict) else x.get("username")) for x in old)
    added = 0
    for u in usernames:
        n = _norm_uname(u)
        if not n or n in have:
            continue
        old.append("@" + n)
        have.add(n)
        added += 1
    json.dump(old, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    return added

def filter_job_usernames(raw_list):
    """本文件去重 + 跳过 target/premium/历史检测"""
    skip = load_skip_set()
    seen = set()
    kept = []
    stats = {"input": 0, "dup_in_file": 0, "skip_history": 0, "kept": 0}
    for item in raw_list or []:
        if isinstance(item, dict):
            u = item.get("username") or item.get("user") or ""
        else:
            u = item
        n = _norm_uname(u)
        if not n:
            continue
        stats["input"] += 1
        if n in seen:
            stats["dup_in_file"] += 1
            continue
        seen.add(n)
        if n in skip:
            stats["skip_history"] += 1
            continue
        kept.append(n)
        stats["kept"] += 1
    log = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        **stats,
        "skip_pool": len(skip),
    }
    try:
        save_checked_history(list(seen))
    except Exception as e:
        print("save_checked_history", e)
    hist = load_json_list("/root/tg-scan-clean/skip_history.json")
    if not isinstance(hist, list):
        hist = []
    hist.append(log)
    json.dump(hist[-200:], open("/root/tg-scan-clean/skip_history.json", "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print("[dedup]", log)
    return kept, stats

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {"bots": [], "settings": {}}

def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

def load_blacklist():
    if os.path.exists(BLACKLIST_FILE):
        with open(BLACKLIST_FILE, 'r') as f:
            return json.load(f)
    return []

def save_blacklist(keywords):
    with open(BLACKLIST_FILE, 'w') as f:
        json.dump(keywords, f, ensure_ascii=False, indent=2)

def load_available():
    if os.path.exists(AVAILABLE_FILE):
        with open(AVAILABLE_FILE, 'r') as f:
            return json.load(f)
    return []

def save_available(usernames):
    with open(AVAILABLE_FILE, 'w') as f:
        json.dump(usernames, f, ensure_ascii=False, indent=2)


def load_targets():
    if os.path.exists(TARGETS_FILE):
        with open(TARGETS_FILE, "r") as f:
            return json.load(f)
    return []

def save_targets(usernames):
    with open(TARGETS_FILE, "w") as f:
        json.dump(usernames, f, ensure_ascii=False, indent=2)

def load_premium():
    if os.path.exists(PREMIUM_FILE):
        with open(PREMIUM_FILE, 'r') as f:
            return json.load(f)
    return []

def save_premium(usernames):
    with open(PREMIUM_FILE, 'w') as f:
        json.dump(usernames, f, ensure_ascii=False, indent=2)

# ============ 认证中间件 ============

# 广告用户识别关键词（昵称/用户名命中则标记为广告）

AD_KEYWORDS = [
    "主号", "代理", "担保", "收u", "收U", "出货", "出U", "卖", "微信", "威信", "薇信", "加v", "加V",
    "飞机", "telegram", "赌博", "博彩", "反", "押金", "苹果", "手机", "折扣", "回收", "兑换",
    "开户", "网赌", "赌", "彩票", "六合", "时时彩", "兼职", "日结", "刷单", "招聘", "招代理",
    "优惠", "促销", "免费领", "红包", "约炮", "交友", "裸聊", "币商", "换汇", "usdt", "承兑",
    "代付", "代收", "出粉", "引流", "精准粉", "电报粉", "赌场", "棋牌", "菠菜", "百乐", "视讯",
    "一手", "数据", "探长", "五折", "走私", "线下",
]

def is_ad_account(username, first_name="", last_name=""):
    text = ("%s %s %s" % (username or "", first_name or "", last_name or "")).lower()
    for kw in AD_KEYWORDS:
        if kw.lower() in text:
            return True
    return False

def is_bot_like_username(username):
    import re as _re
    u = (username or "").strip().lstrip("@")
    if not u:
        return True
    digits = sum(1 for ch in u if ch.isdigit())
    letters = sum(1 for ch in u if ch.isalpha())
    if digits == 0 and len(u) <= 12:
        return False
    if len(u) >= 10 and digits >= 4:
        return True
    if _re.match(r"^[A-Za-z]{2,}\d{4,}$", u):
        return True
    if _re.search(r"\d{6,}", u):
        return True
    if letters >= 3 and digits >= 3 and len(u) >= 8:
        return True
    return False

def classify_last_online(status_obj):
    from datetime import datetime, timezone, timedelta
    if status_obj is None:
        return "unknown"
    name = type(status_obj).__name__
    if name in ("UserStatusOnline", "UserStatusRecently", "UserStatusLastWeek"):
        return "active"
    if name == "UserStatusLastMonth":
        return "active"
    if name == "UserStatusOffline":
        was = getattr(status_obj, "was_online", None)
        if was is None:
            return "unknown"
        if getattr(was, "tzinfo", None) is None:
            was = was.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) - was > timedelta(days=30):
            return "stale"
        return "active"
    if name in ("UserStatusEmpty",):
        return "unknown"
    return "unknown"

def is_frozen_user(user):
    if user is None:
        return False
    if getattr(user, "restricted", False):
        return True
    if getattr(user, "restriction_reason", None):
        return True
    if getattr(user, "scam", False) or getattr(user, "fake", False):
        return True
    return False

def classify_found_user(username, user):
    fn = getattr(user, "first_name", "") or ""
    ln = getattr(user, "last_name", "") or ""
    premium = bool(getattr(user, "premium", False))
    is_bot = bool(getattr(user, "bot", False))
    if getattr(user, "deleted", False):
        return {"username": username, "status": "deleted", "label": "已注销", "premium": False, "collect": False}
    if is_frozen_user(user):
        return {"username": username, "status": "frozen", "label": "冻结", "premium": premium, "collect": False, "first_name": fn, "last_name": ln}
    ad = is_ad_account(username, fn, ln)
    bot_like = is_bot_like_username(username) or is_bot
    online_kind = classify_last_online(getattr(user, "status", None))
    if ad:
        st, collect, lab = "ad", False, "广告"
    elif bot_like:
        st, collect, lab = "spam", False, "水军号"
    elif online_kind == "stale":
        st, collect, lab = "inactive", False, "长期未在线"
    else:
        st, collect, lab = "clean", True, "可用"
    return {
        "username": username, "status": st, "label": lab, "premium": premium, "collect": collect,
        "is_ad": ad, "is_spam": bot_like, "online": online_kind,
        "first_name": fn, "last_name": ln, "user_id": getattr(user, "id", None),
    }

def load_api_pool():
    if os.path.exists(API_POOL_FILE):
        with open(API_POOL_FILE, "r") as f:
            return json.load(f)
    return list(API_CONFIGS)

def save_api_pool(pool):
    with open(API_POOL_FILE, "w") as f:
        json.dump(pool, f, ensure_ascii=False, indent=2)

def load_proxy_pool():
    if os.path.exists(PROXY_POOL_FILE):
        with open(PROXY_POOL_FILE, "r") as f:
            return json.load(f)
    return []

def save_proxy_pool(pool):
    with open(PROXY_POOL_FILE, "w") as f:
        json.dump(pool, f, ensure_ascii=False, indent=2)

def pick_api_evenly():
    """按当前水军已占用的 api_id 数量，选使用最少的 API"""
    pool = load_api_pool()
    if not pool:
        return random.choice(API_CONFIGS)
    config = load_config()
    usage = {}
    for b in config.get("bots", []):
        aid = str(b.get("api_id") or "")
        if aid:
            usage[aid] = usage.get(aid, 0) + 1
    best = None
    best_count = 10**9
    for item in pool:
        aid = str(item.get("api_id"))
        cnt = usage.get(aid, 0)
        if cnt < best_count:
            best_count = cnt
            best = item
    return best or pool[0]

def pick_proxy_evenly():
    """选使用最少的代理；池为空则返回 None"""
    pool = load_proxy_pool()
    if not pool:
        return None
    config = load_config()
    usage = {}
    for b in config.get("bots", []):
        px = b.get("proxy") or ""
        if px:
            usage[px] = usage.get(px, 0) + 1
    best = None
    best_count = 10**9
    for item in pool:
        px = item if isinstance(item, str) else (item.get("proxy") or item.get("url") or "")
        if not px:
            continue
        cnt = usage.get(px, 0)
        if cnt < best_count:
            best_count = cnt
            best = px
    return best

def parse_proxy(proxy_url):
    """支持:
    - socks5://user:pass@host:port
    - http://host:port
    - host:port:user:pass
    - host:port
    """
    if not proxy_url:
        return None
    try:
        import python_socks
        from urllib.parse import urlparse
        u = str(proxy_url).strip()
        # host:port:user:pass
        if "://" not in u and u.count(":") >= 3:
            parts = u.split(":")
            host, port, user, pwd = parts[0], int(parts[1]), parts[2], ":".join(parts[3:])
            return (python_socks.ProxyType.SOCKS5, host, port, True, user, pwd)
        if "://" not in u and u.count(":") == 1:
            host, port = u.split(":")
            return (python_socks.ProxyType.SOCKS5, host, int(port), True, None, None)
        if "://" not in u:
            u = "socks5://" + u
        p = urlparse(u)
        scheme = (p.scheme or "socks5").lower()
        host = p.hostname
        port = p.port or 1080
        user = p.username
        pwd = p.password
        if scheme.startswith("socks5"):
            ptype = python_socks.ProxyType.SOCKS5
        elif scheme.startswith("socks4"):
            ptype = python_socks.ProxyType.SOCKS4
        else:
            ptype = python_socks.ProxyType.HTTP
        return (ptype, host, port, True, user, pwd)
    except Exception as e:
        print("parse_proxy error", proxy_url, e)
        return None



def load_cooldown():
    import json, os, time
    if not os.path.exists(COOLDOWN_FILE):
        return {}
    try:
        data = json.load(open(COOLDOWN_FILE))
    except Exception:
        return {}
    now = time.time()
    # 清过期
    data = {k: v for k, v in data.items() if float(v.get("until", 0) or 0) > now}
    return data

def save_cooldown(data):
    import json
    json.dump(data, open(COOLDOWN_FILE, "w"), ensure_ascii=False, indent=2)


def _bot_key(bot):
    return bot.get("id") or bot.get("phone") or bot.get("session_path") or ""

def inspect_bot_alive(bot, timeout=8):
    """开工前体检：能否连接、是否授权、是否被冻。"""
    from telethon import TelegramClient
    from telethon.errors import FloodWaitError, AuthKeyError, UserDeactivatedError, UserDeactivatedBanError, SessionRevokedError
    sp = bot.get("session_path")
    if not sp:
        return {"ok": False, "reason": "no_session"}
    key = _bot_key(bot)
    try:
        api_id = int(bot.get("api_id") or 0)
        api_hash = str(bot.get("api_hash") or "")
        client = TelegramClient(sp, api_id, api_hash)
        async def _t():
            await client.connect()
            try:
                if not await client.is_user_authorized():
                    return {"ok": False, "reason": "unauthorized"}
                me = await client.get_me()
                if not me:
                    return {"ok": False, "reason": "no_me"}
                if getattr(me, "restricted", False) or getattr(me, "deleted", False):
                    return {"ok": False, "reason": "frozen"}
                return {"ok": True, "reason": "ok", "phone": getattr(me, "phone", None)}
            finally:
                try:
                    await client.disconnect()
                except Exception:
                    pass
        return run_async(_t()) or {"ok": False, "reason": "empty"}
    except FloodWaitError as e:
        set_bot_cooldown(key, int(e.seconds or 3600), reason="FloodWait-precheck")
        return {"ok": False, "reason": "flood", "seconds": int(e.seconds or 0)}
    except (UserDeactivatedError, UserDeactivatedBanError, SessionRevokedError, AuthKeyError) as e:
        set_bot_cooldown(key, 86400, reason=type(e).__name__)
        return {"ok": False, "reason": "frozen"}
    except Exception as e:
        return {"ok": False, "reason": type(e).__name__}

def preflight_workable_bots(config=None):
    """只保留：在线 + 未限流 + 未冻结。"""
    cfg = config or load_config()
    raw = list_workable_bots(cfg) if "list_workable_bots" in globals() else (cfg.get("bots") or [])
    ok, bad = [], []
    for b in raw:
        r = inspect_bot_alive(b)
        if r.get("ok"):
            ok.append(b)
        else:
            bad.append({"phone": b.get("phone"), "reason": r.get("reason")})
            print("[preflight]", b.get("phone"), r)
    return ok, bad

def is_target_frozen(user, err_text=""):
    if user is not None:
        if getattr(user, "deleted", False):
            return True
        if getattr(user, "restricted", False) and not getattr(user, "premium", False):
            # restricted 且非会员，当冻结/限制号剔除
            return True
    t = (err_text or "")
    keys = ("USER_DEACTIVATED", "USER_BANNED", "USER_RESTRICTED", "FROZEN", "deactivated", "banned")
    return any(k.lower() in t.lower() for k in keys)

def set_bot_cooldown(bot_key, seconds, reason="FloodWait"):
    import time
    data = load_cooldown()
    until = time.time() + max(int(seconds), 60)
    data[str(bot_key)] = {"until": until, "reason": reason, "seconds": int(seconds)}
    save_cooldown(data)
    return until

def is_bot_cooling(bot_key):
    import time
    data = load_cooldown()
    item = data.get(str(bot_key))
    if not item:
        return False, 0
    left = float(item.get("until", 0)) - time.time()
    return left > 0, max(int(left), 0)

def load_daily_stats():
    import json, os, datetime
    today = datetime.date.today().isoformat()
    if not os.path.exists(DAILY_STATS_FILE):
        return {"date": today, "counts": {}}
    try:
        data = json.load(open(DAILY_STATS_FILE))
    except Exception:
        return {"date": today, "counts": {}}
    if data.get("date") != today:
        return {"date": today, "counts": {}}
    return data

def save_daily_stats(data):
    import json
    json.dump(data, open(DAILY_STATS_FILE, "w"), ensure_ascii=False, indent=2)

def incr_bot_daily(bot_key):
    data = load_daily_stats()
    k = str(bot_key)
    data.setdefault("counts", {})
    data["counts"][k] = int(data["counts"].get(k, 0)) + 1
    save_daily_stats(data)
    return data["counts"][k]

def bot_daily_left(bot_key, limit=None):
    limit = int(limit or DEFAULT_DAILY_LIMIT)
    data = load_daily_stats()
    used = int(data.get("counts", {}).get(str(bot_key), 0))
    return max(limit - used, 0), used, limit


def proxy_ip_key(proxy):
    """从 proxy 串提取 IP，作为线路 key"""
    if not proxy:
        return "direct"
    s = str(proxy).strip()
    # ip:port:user:pass 或 host:port
    host = s.split(":")[0].strip()
    return host or "direct"

def list_workable_bots_by_ip(config=None):
    """可工作水军按 IP 线路分组，选号时跨 IP 轮转，避免同一线路打爆"""
    bots = list_workable_bots(config)
    # 按 IP 分组
    groups = {}
    for b in bots:
        ip = proxy_ip_key(b.get("proxy"))
        groups.setdefault(ip, []).append(b)
    # 跨组交错：ip1号1, ip2号1, ip3号1, ip1号2...
    ordered = []
    if not groups:
        return ordered
    keys = list(groups.keys())
    max_len = max(len(v) for v in groups.values())
    for i in range(max_len):
        for k in keys:
            if i < len(groups[k]):
                ordered.append(groups[k][i])
    return ordered



def is_bot_selectable(b):
    st = str(b.get("status") or "").lower()
    if st in ("need_relogin", "unauth", "unauthorized", "stopped", "stop"):
        return False
    if not b.get("session_path"):
        return False
    return True

def list_workable_bots(config=None):
    """未冷却且未超每日上限的水军"""
    if config is None:
        config = load_config()
    bots = config.get("bots") or []
    out = []
    for b in bots:
        if not is_bot_selectable(b):
            continue
        key = b.get("id") or b.get("phone") or b.get("session_path")
        cooling, left = is_bot_cooling(key)
        if cooling:
            b = dict(b)
            b["_status"] = "cooldown"
            b["_cooldown_left"] = left
            continue
        remain, used, limit = bot_daily_left(key)
        if remain <= 0:
            continue
        bb = dict(b)
        bb["_status"] = "work"
        bb["_daily_used"] = used
        bb["_daily_left"] = remain
        bb["_daily_limit"] = limit
        out.append(bb)
    return out


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get('Authorization', '')
        key = request.args.get('key') or request.args.get('token') or ''
        if auth != f'Bearer {AUTH_KEY}' and key != AUTH_KEY:
            return jsonify({"error": "未授权"}), 401
        return f(*args, **kwargs)
    return decorated

# ============ 登录接口 ============
@app.route('/api/login', methods=['POST'])
def api_login():
    data = request.json
    username = data.get('username', '')
    password = data.get('password', '')
    if username == LOGIN_USER and password == LOGIN_PASS:
        return jsonify({"success": True, "token": AUTH_KEY})
    return jsonify({"error": "用户名或密码错误"}), 401

# ============ 水军管理接口 ============
@app.route('/api/bot/list', methods=['GET'])
@require_auth
def api_bot_list():
    config = load_config()
    bots = config.get('bots', [])
    safe_bots = []
    for bot in bots:
        safe_bots.append({
            "id": bot.get("id", ""),
            "name": bot.get("name", ""),
            "username": bot.get("username", ""),
            "phone": bot.get("phone", ""),
            "first_name": bot.get("first_name", ""),
            "status": bot.get("status", "unknown"),
            "type": bot.get("type", "userbot"),
            "added_time": bot.get("added_time", "")
        })
    return jsonify({"bots": safe_bots})

@app.route('/api/bot/remove', methods=['POST'])
@require_auth
def api_bot_remove():
    data = request.json or {}
    bot_id = data.get('id', '') or data.get('bot_id', '')
    config = load_config()
    config['bots'] = [b for b in config.get('bots', []) if b.get('id') != bot_id]
    save_config(config)
    return jsonify({"success": True, "message": "水军已删除"})

@app.route('/api/bot/start', methods=['POST'])
@require_auth
def api_bot_start():
    """启动水军：只恢复工作状态，不在这里连 Telegram（避免卡住）。"""
    data = request.json or {}
    bot_id = str(data.get("bot_id") or data.get("id") or data.get("name") or "").strip()
    if not bot_id:
        return jsonify({"error": "缺少水军 id"}), 400
    config = load_config()
    found = None
    for b in config.get("bots") or []:
        if str(b.get("id")) == bot_id or str(b.get("name")) == bot_id or str(b.get("phone")) == bot_id:
            found = b
            break
    if not found:
        return jsonify({"error": "水军不存在"}), 404
    found["status"] = "running"
    found.pop("cooldown_until", None)
    save_config(config)
    return jsonify({"success": True, "message": "已启动", "id": found.get("id"), "status": "running"})


@app.route('/api/bot/stop', methods=['POST'])
@require_auth
def api_bot_stop():
    data = request.json or {}
    bot_id = data.get('bot_id', '') or data.get('id', '')
    config = load_config()
    found = None
    for b in config.get('bots', []):
        if b.get('id') == bot_id:
            found = b
            break
    if not found:
        return jsonify({"error": "水军不存在"}), 404
    found['status'] = 'ready'
    save_config(config)
    return jsonify({"success": True, "message": f"水军 {found.get('name', bot_id)} 已停止", "status": "ready"})

@app.route('/api/bot/delete', methods=['POST'])
@require_auth
def api_bot_delete():
    """兼容前端 delete 调用"""
    data = request.json or {}
    bot_id = data.get('bot_id', '') or data.get('id', '')
    config = load_config()
    config['bots'] = [b for b in config.get('bots', []) if b.get('id') != bot_id]
    save_config(config)
    return jsonify({"success": True, "message": "水军已删除"})

# ============ Telethon 手机号登录（异步） ============
pending_clients = {}
FLOOD_COOLDOWN = {}  # phone -> unix timestamp until
CHECK_LOCK = None

def get_api_config(api_id=None, api_hash=None):
    """添加水军默认从 API 池均匀分配；仅明确传入时才用自定义。"""
    if api_id and api_hash:
        try:
            return {"api_id": int(api_id), "api_hash": str(api_hash).strip()}
        except (ValueError, TypeError):
            pass
    try:
        picked = pick_api_evenly()
        return {"api_id": int(picked["api_id"]), "api_hash": str(picked["api_hash"])}
    except Exception:
        return random.choice(API_CONFIGS)

async def async_send_code(phone, api_id=None, api_hash=None):
    """异步发送验证码。支持自定义 api_id / api_hash。"""
    from telethon import TelegramClient
    
    api_config = get_api_config(api_id, api_hash)
    api_id = api_config["api_id"]
    api_hash = api_config["api_hash"]
    
    session_file = os.path.join(SESSIONS_DIR, f"session_{phone.replace('+', '').replace(' ', '')}")
    client = TelegramClient(session_file, api_id, api_hash, loop=_loop)
    await client.connect()
    
    result = await client.send_code_request(phone)
    
    pending_clients[phone] = {
        "client": client,
        "phone_code_hash": result.phone_code_hash,
        "api_id": api_id,
        "api_hash": api_hash,
        "session_file": session_file
    }
    
    return {"success": True, "message": f"验证码已发送到 {phone}", "phone_code_hash": result.phone_code_hash}

async def async_verify_code(phone, code, password=None):
    """异步验证码验证"""
    if phone not in pending_clients:
        return {"success": False, "error": "请先发送验证码"}
    
    pending = pending_clients[phone]
    client = pending["client"]
    phone_code_hash = pending["phone_code_hash"]
    
    try:
        await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
    except Exception as e:
        err_str = str(e)
        if "Two-steps verification" in err_str or "password" in err_str.lower() or "SessionPasswordNeeded" in err_str:
            if not password:
                return {"success": False, "error": "该账号需要两步验证密码"}
            await client.sign_in(password=password)
        else:
            raise e
    
    me = await client.get_me()
    account = {
        "username": me.username or "",
        "phone": phone,
        "first_name": me.first_name or "",
        "last_name": me.last_name or "",
        "user_id": me.id
    }
    
    await client.disconnect()
    del pending_clients[phone]
    
    return {
        "success": True,
        "account": account,
        "session_file": pending["session_file"],
        "api_id": pending["api_id"],
        "api_hash": pending["api_hash"]
    }

@app.route('/api/bot/send_code', methods=['POST'])
@require_auth
def api_send_code():
    """发送验证码到手机号。支持自定义 api_id / api_hash。"""
    data = request.json or {}
    phone = data.get('phone', '').strip()
    if not phone:
        return jsonify({"error": "请提供手机号"}), 400
    
    # 添加水军不再使用前端 API，统一从 API 池均匀分配
    api_id = None
    api_hash = None
    
    try:
        result = run_async(async_send_code(phone, api_id=api_id, api_hash=api_hash))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": f"发送验证码失败: {str(e)}"}), 400

@app.route('/api/bot/verify', methods=['POST'])
@require_auth
def api_verify_code():
    """验证码验证并添加水军"""
    data = request.json
    phone = data.get('phone', '').strip()
    code = data.get('code', '').strip()
    password = data.get('password', '').strip()
    name = data.get('name', '').strip()
    
    if not phone or not code:
        return jsonify({"error": "请提供手机号和验证码"}), 400
    
    try:
        result = run_async(async_verify_code(phone, code, password if password else None))
        
        if not result["success"]:
            return jsonify({"error": result["error"]}), 400
        
        # 登录成功，添加到水军列表
        account = result["account"]
        config = load_config()
        bot_id = f"soldier_{name}_{int(time.time())}"
        new_bot = {
            "id": bot_id,
            "name": name if name else account.get("first_name", phone),
            "username": account.get("username", ""),
            "phone": phone,
            "first_name": account.get("first_name", ""),
            "session_path": result.get("session_file", ""),
            "api_id": str(result.get("api_id", "")),
            "api_hash": result.get("api_hash", ""),
            "proxy": pick_proxy_evenly() or "",
            "status": "ready",
            "type": "userbot",
            "added_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        phone_norm = str(new_bot.get("phone") or "").strip()
        for _b in config.get("bots") or []:
            if str(_b.get("phone") or "").strip() == phone_norm and phone_norm:
                return jsonify({"error": "该手机号已存在，请勿重复添加", "phone": phone_norm}), 400
        config.setdefault('bots', []).append(new_bot)
        save_config(config)
        
        return jsonify({
            "success": True,
            "message": "水军添加成功",
            "account": account
        })
    except Exception as e:
        return jsonify({"error": f"验证失败: {str(e)}"}), 400


# ============ API 池 / 代理池 ============

@app.route('/api/pool/api/item', methods=['POST'])
@require_auth
def api_pool_api_delete_item():
    data = request.json or {}
    api_id = str(data.get("api_id") or data.get("id") or "").strip()
    if not api_id:
        return jsonify({"error": "缺少 api_id"}), 400
    pool = load_api_pool()
    new_pool = []
    for x in pool:
        aid = str(x.get("api_id") if isinstance(x, dict) else x)
        if aid != api_id:
            new_pool.append(x)
    save_api_pool(new_pool)
    return jsonify({"success": True, "deleted": api_id, "total": len(new_pool)})

@app.route('/api/pool/api', methods=['GET'])
@require_auth
def api_pool_list():
    return jsonify({"pool": load_api_pool(), "total": len(load_api_pool())})

@app.route('/api/pool/api', methods=['POST'])
@require_auth
def api_pool_add():
    data = request.json or {}
    api_id = data.get("api_id")
    api_hash = data.get("api_hash")
    label = data.get("label") or f"API-{api_id}"
    if not api_id or not api_hash:
        return jsonify({"error": "需要 api_id 和 api_hash"}), 400
    pool = load_api_pool()
    # 去重
    pool = [x for x in pool if str(x.get("api_id")) != str(api_id)]
    pool.append({"api_id": int(api_id), "api_hash": str(api_hash).strip(), "label": label})
    save_api_pool(pool)
    return jsonify({"success": True, "total": len(pool), "pool": pool})


@app.route('/api/pool/api/batch', methods=['POST'])
@require_auth
def api_pool_add_batch():
    """批量添加 API。支持每行: api_id-api_hash 或 api_id,api_hash 或 api_id api_hash"""
    data = request.json or {}
    text = data.get("lines") or data.get("text") or ""
    if isinstance(text, list):
        lines = [str(x).strip() for x in text if str(x).strip()]
    else:
        lines = [x.strip() for x in str(text).replace("\r", "\n").split("\n") if x.strip()]
    pool = load_api_pool()
    existing = {str(x.get("api_id")) for x in pool}
    added = 0
    for line in lines:
        line = line.strip()
        api_id, api_hash = None, None
        if "-" in line and line.split("-", 1)[0].strip().isdigit():
            a, b = line.split("-", 1)
            api_id, api_hash = a.strip(), b.strip()
        elif "," in line:
            a, b = line.split(",", 1)
            api_id, api_hash = a.strip(), b.strip()
        elif "\t" in line:
            a, b = line.split("\t", 1)
            api_id, api_hash = a.strip(), b.strip()
        else:
            parts = line.split()
            if len(parts) >= 2 and parts[0].isdigit():
                api_id, api_hash = parts[0], parts[1]
        if not api_id or not api_hash:
            continue
        if str(api_id) in existing:
            continue
        try:
            pool.append({"api_id": int(api_id), "api_hash": api_hash, "label": f"API-{api_id}"})
            existing.add(str(api_id))
            added += 1
        except Exception:
            continue
    save_api_pool(pool)
    return jsonify({"success": True, "added": added, "total": len(pool)})

@app.route('/api/pool/api/delete', methods=['POST'])
@require_auth
def api_pool_del():
    data = request.json or {}
    api_id = str(data.get("api_id", ""))
    pool = [x for x in load_api_pool() if str(x.get("api_id")) != api_id]
    save_api_pool(pool)
    return jsonify({"success": True, "total": len(pool)})

@app.route('/api/pool/api/redistribute', methods=['POST'])
@require_auth
def api_pool_redistribute():
    """一键均匀分配 API 到所有水军"""
    pool = load_api_pool()
    if not pool:
        return jsonify({"error": "API 池为空"}), 400
    config = load_config()
    bots = config.get("bots", [])
    for i, b in enumerate(bots):
        item = pool[i % len(pool)]
        b["api_id"] = int(item["api_id"])
        b["api_hash"] = item["api_hash"]
    save_config(config)
    return jsonify({"success": True, "message": f"已均匀分配 {len(bots)} 个水军", "bots": len(bots), "apis": len(pool)})


@app.route('/api/pool/proxy/bind', methods=['POST'])
@require_auth
def api_proxy_bind():
    """把选中的水军绑定到指定代理。不改未选中的号。"""
    data = request.json or {}
    proxy = (data.get("proxy") or "").strip()
    phones = data.get("phones") or data.get("bot_ids") or []
    if not proxy:
        return jsonify({"error": "缺少 proxy"}), 400
    if isinstance(phones, str):
        phones = [x.strip() for x in re.split(r"[\s,]+", phones) if x.strip()]
    phones = [str(x).strip() for x in phones if str(x).strip()]
    config = load_config()
    changed = 0
    for b in config.get("bots") or []:
        ph = str(b.get("phone") or "")
        bid = str(b.get("id") or "")
        name = str(b.get("name") or "")
        if ph in phones or bid in phones or name in phones:
            b["proxy"] = proxy
            changed += 1
    save_config(config)
    return jsonify({"success": True, "changed": changed, "proxy": proxy, "phones": phones})

@app.route('/api/pool/proxy/item', methods=['POST'])
@require_auth
def api_proxy_delete_item():
    """删除一条代理，不改水军绑定（除非明确 unbind=true）"""
    data = request.json or {}
    proxy = (data.get("proxy") or "").strip()
    unbind = bool(data.get("unbind"))
    if not proxy:
        return jsonify({"error": "缺少 proxy"}), 400
    pool = load_proxy_pool()
    new_pool = []
    for x in pool:
        s = x if isinstance(x, str) else (x.get("proxy") or x.get("url") or "")
        if s != proxy:
            new_pool.append(x)
    save_proxy_pool(new_pool)
    unbound = 0
    if unbind:
        config = load_config()
        for b in config.get("bots") or []:
            if str(b.get("proxy") or "") == proxy:
                b["proxy"] = ""
                unbound += 1
        save_config(config)
    return jsonify({"success": True, "total": len(new_pool), "unbound": unbound})

@app.route('/api/pool/proxy/bindings', methods=['GET'])
@require_auth
def api_proxy_bindings():
    """每条代理 + 全部水军（标注是否已绑定该代理）"""
    pool = load_proxy_pool()
    config = load_config()
    bots = []
    for i, b in enumerate(config.get("bots") or [], 1):
        bots.append({
            "index": i,
            "id": b.get("id"),
            "name": b.get("name") or str(i),
            "phone": b.get("phone") or "",
            "proxy": b.get("proxy") or "",
            "status": b.get("status") or "",
        })
    items = []
    for x in pool:
        if isinstance(x, str):
            px, label = x, x
        else:
            px = x.get("proxy") or x.get("url") or ""
            label = x.get("label") or px
        bound = [b for b in bots if (b.get("proxy") or "") == px]
        items.append({"proxy": px, "label": label, "bound": bound, "bound_count": len(bound)})
    return jsonify({"success": True, "proxies": items, "bots": bots, "total_proxies": len(items), "total_bots": len(bots)})


@app.route('/api/pool/proxy', methods=['GET'])
@require_auth
def proxy_pool_list():
    return jsonify({"pool": load_proxy_pool(), "total": len(load_proxy_pool())})

@app.route('/api/pool/proxy', methods=['POST'])
@require_auth
def proxy_pool_add():
    data = request.json or {}
    # 支持单条或批量 lines
    lines = data.get("proxies") or data.get("lines") or []
    if isinstance(lines, str):
        lines = [x.strip() for x in lines.split("\n") if x.strip()]
    single = data.get("proxy") or data.get("url")
    if single:
        lines.append(single)
    pool = load_proxy_pool()
    existing = set()
    for x in pool:
        existing.add(x if isinstance(x, str) else (x.get("proxy") or x.get("url") or ""))
    added = 0
    for line in lines:
        line = line.strip()
        if not line or line in existing:
            continue
        pool.append({"proxy": line, "label": line[:40]})
        existing.add(line)
        added += 1
    save_proxy_pool(pool)
    return jsonify({"success": True, "added": added, "total": len(pool)})

@app.route('/api/pool/proxy/delete', methods=['POST'])
@require_auth
def proxy_pool_del():
    data = request.json or {}
    px = data.get("proxy") or ""
    pool = []
    for x in load_proxy_pool():
        val = x if isinstance(x, str) else (x.get("proxy") or x.get("url") or "")
        if val != px:
            pool.append(x)
    save_proxy_pool(pool)
    return jsonify({"success": True, "total": len(pool)})

@app.route('/api/pool/proxy/redistribute', methods=['POST'])
@require_auth
def proxy_pool_redistribute():
    """一键均匀分配代理到所有水军"""
    pool = load_proxy_pool()
    if not pool:
        return jsonify({"error": "代理池为空"}), 400
    config = load_config()
    bots = config.get("bots", [])
    for i, b in enumerate(bots):
        item = pool[i % len(pool)]
        px = item if isinstance(item, str) else (item.get("proxy") or item.get("url") or "")
        b["proxy"] = px
    save_config(config)
    return jsonify({"success": True, "message": f"已均匀分配代理到 {len(bots)} 个水军", "bots": len(bots), "proxies": len(pool)})


# ============ 批量检测接口（真实 Telegram 查询） ============

async def async_check_one_username(client, username):
    """真实检测单个用户名：available / taken / deleted / invalid / error"""
    from telethon.tl.functions.contacts import ResolveUsernameRequest
    from telethon.errors import UsernameNotOccupiedError, UsernameInvalidError, FloodWaitError
    try:
        result = await client(ResolveUsernameRequest(username))
        users = getattr(result, 'users', None) or []
        if users:
            user = users[0]
            if getattr(user, 'deleted', False):
                return {"username": username, "status": "deleted", "premium": False}
            is_premium = bool(getattr(user, 'premium', False))
            fn = getattr(user, 'first_name', '') or ''
            ln = getattr(user, 'last_name', '') or ''
            ad = is_ad_account(username, fn, ln)
            return {
                "username": username,
                "status": "ad" if ad and not is_premium else "taken",
                "premium": is_premium,
                "is_ad": ad,
                "user_id": getattr(user, 'id', None),
                "first_name": fn,
                "last_name": ln,
            }
        chats = getattr(result, 'chats', None) or []
        if chats:
            return {"username": username, "status": "taken", "premium": False}
        return {"username": username, "status": "taken", "premium": False}
    except UsernameNotOccupiedError:
        return {"username": username, "status": "available", "premium": False}
    except UsernameInvalidError:
        return {"username": username, "status": "invalid", "premium": False}
    except FloodWaitError as e:
        return {"username": username, "status": "flood", "error": f"FloodWait {e.seconds}s", "_flood_seconds": int(e.seconds), "premium": False}
    except Exception as e:
        err = str(e)
        if "No user has" in err or "USERNAME_NOT_OCCUPIED" in err:
            return {"username": username, "status": "available", "premium": False}
        if "USERNAME_INVALID" in err:
            return {"username": username, "status": "invalid", "premium": False}
        return {"username": username, "status": "error", "error": err[:120], "premium": False}

async def async_check_usernames_batch(usernames, bot_sessions):
    """使用已登录水军轮流检测用户名"""
    from telethon import TelegramClient
    results = []
    if not bot_sessions:
        for u in usernames:
            results.append({"username": u, "status": "error", "error": "无可用水军账号"})
        return results

    clients = []
    try:
        for sess in bot_sessions:
            try:
                sp = sess["session_path"]
                # Telethon 接受不带 .session 后缀的路径
                client = TelegramClient(
                    sp,
                    int(sess["api_id"]),
                    str(sess["api_hash"]),
                )
                await client.connect()
                ok = await client.is_user_authorized()
                if ok:
                    clients.append(client)
                else:
                    await client.disconnect()
            except Exception as e:
                print(f"[check] session fail {sess.get('session_path')}: {e}")
                continue

        if not clients:
            for u in usernames:
                results.append({"username": u, "status": "error", "error": "水军未授权或session失效"})
            return results

        for i, username in enumerate(usernames):
            client = clients[i % len(clients)]
            r = await async_check_one_username(client, username)
            results.append(r)
            await asyncio.sleep(0.35 + random.random() * 0.4)

        return results
    finally:
        for c in clients:
            try:
                await c.disconnect()
            except Exception:
                pass

@app.route('/api/check', methods=['POST'])
@require_auth
def api_check_usernames():
    """真实批量检测用户名（available / taken / deleted / invalid）"""
    data = request.json or {}
    usernames = data.get('usernames', [])
    if not usernames:
        return jsonify({"error": "请提供用户名列表"}), 400

    cleaned = []
    seen = set()
    blacklist = load_blacklist()
    for u in usernames:
        u = u.strip().lstrip('@')
        if not u or u.lower() in seen:
            continue
        if any(kw.lower() in u.lower() for kw in blacklist):
            continue
        seen.add(u.lower())
        cleaned.append(u)

    if not cleaned:
        return jsonify({"results": [], "total": 0})

    config = load_config()
    bot_sessions = []
    for bot in config.get('bots', []):
        if bot.get('session_path'):
            bot_sessions.append({
                "session_path": bot["session_path"],
                "api_id": bot.get("api_id") or API_CONFIGS[0]["api_id"],
                "api_hash": bot.get("api_hash") or API_CONFIGS[0]["api_hash"],
            })

    try:
        results = run_async(async_check_usernames_batch(cleaned, bot_sessions))
        return jsonify({"results": results, "total": len(results)})
    except Exception as e:
        return jsonify({"error": f"检测失败: {str(e)}"}), 500



# ============ 后台检测任务（刷新/断网不中断） ============
import copy
JOB_LOCK = threading.Lock()
CHECK_JOB = {
    "running": False,
    "should_stop": False,
    "total": 0,
    "done": 0,
    "queue": [],
    "results": [],
    "started_at": None,
    "updated_at": None,
    "message": "",
}

def _job_snapshot():
    with JOB_LOCK:
        total = CHECK_JOB.get("total") or 0
        done = CHECK_JOB.get("done") or 0
        running = bool(CHECK_JOB.get("running"))
        batch_size = int(CHECK_JOB.get("batch_size") or 300)
        qlen = len(CHECK_JOB.get("queue") or [])
        # 本批进行中：队列长度保持不变；本批人数=总数-已完成-队列
        in_batch = max(0, total - done - qlen)
        if running:
            batch_work = in_batch if in_batch else min(batch_size, max(0, total - done))
            queue_left = qlen
        else:
            batch_work = 0
            queue_left = qlen
        results = CHECK_JOB.get("results") or []
        return {
            "running": running,
            "total": total,
            "done": done,
            "left": queue_left,
            "batch_size": batch_size,
            "batch_work": batch_work,
            "queue_left": queue_left,
            "in_batch": in_batch,
            "message": CHECK_JOB.get("message") or "",
            "started_at": CHECK_JOB.get("started_at"),
            "updated_at": CHECK_JOB.get("updated_at"),
            "results": results[-200:],
            "available": sum(1 for x in results if x.get("status") in ("clean", "available") or x.get("collect")),
            "premium": sum(1 for x in results if x.get("premium")),
            "deleted": sum(1 for x in results if x.get("status") in ("deleted", "unavailable")),
            "error": sum(1 for x in results if x.get("status") == "error"),
        }

def _check_job_worker():
    from time import sleep as _sleep
    batch_size = 300
    while True:
        with JOB_LOCK:
            if CHECK_JOB.get("should_stop"):
                CHECK_JOB["running"] = False
                CHECK_JOB["message"] = "已手动停止"
                CHECK_JOB["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                break
            q = CHECK_JOB.get("queue") or []
            if not q:
                CHECK_JOB["running"] = False
                CHECK_JOB["message"] = "检测完成"
                CHECK_JOB["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                break
            # 只取出本批最多300个，其余留在队列
            batch = q[:batch_size]
            CHECK_JOB["queue"] = q[batch_size:]
            CHECK_JOB["batch_size"] = batch_size
            CHECK_JOB["message"] = "本批检测 %s，队列剩余 %s" % (len(batch), len(CHECK_JOB["queue"]))
            CHECK_JOB["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for username in batch:
            with JOB_LOCK:
                if CHECK_JOB.get("should_stop"):
                    # 本批未跑完的退回队列头
                    rest = batch[batch.index(username):]
                    CHECK_JOB["queue"] = rest + (CHECK_JOB.get("queue") or [])
                    CHECK_JOB["running"] = False
                    CHECK_JOB["message"] = "已手动停止"
                    CHECK_JOB["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    return
            try:
                with app.test_request_context(
                    "/api/check/one",
                    method="POST",
                    json={"username": username},
                    headers={"Authorization": "Bearer " + AUTH_KEY},
                ):
                    resp = api_check_one()
                if hasattr(resp, "get_json"):
                    data = resp.get_json() or {}
                elif isinstance(resp, tuple):
                    data = resp[0].get_json() if hasattr(resp[0], "get_json") else {}
                else:
                    data = {}
            except Exception as e:
                data = {"username": username, "status": "error", "error": str(e)[:160], "premium": False}
            data = data or {}
            data.setdefault("username", username)
            st0 = str(data.get("status") or "")
            err0 = str(data.get("error") or "")
            if st0 in ("retry_session", "flood") or "FloodWait" in err0 or "未授权" in err0 or "session" in err0.lower():
                with JOB_LOCK:
                    q = CHECK_JOB.get("queue") or []
                    if username not in q:
                        CHECK_JOB["queue"] = [username] + list(q)
                    CHECK_JOB["message"] = "水军故障换号，用户退回队列"
                    CHECK_JOB["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                continue

            with JOB_LOCK:
                uname = (data.get("username") or username or "").strip().lstrip("@").lower()
                rows = CHECK_JOB.get("results") or []
                exist = False
                for i, r in enumerate(rows):
                    ru = str((r or {}).get("username") or "").strip().lstrip("@").lower()
                    if ru and ru == uname:
                        rows[i] = data
                        exist = True
                        break
                if not exist:
                    rows.append(data)
                CHECK_JOB["results"] = rows
                CHECK_JOB["done"] = len(rows)
                CHECK_JOB["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                left_q = len(CHECK_JOB.get("queue") or [])
                CHECK_JOB["message"] = "本批工作中 · 完成 %s/%s · 队列剩余 %s" % (
                    CHECK_JOB["done"], CHECK_JOB["total"], left_q
                )
            err = str(data.get("error") or "")
            st = str(data.get("status") or "")
            if st in ("flood",) or "FloodWait" in err:
                _sleep(60)
            else:
                _sleep(1.2)
        # 本批结束，下一轮 while 再从剩余取300



@app.route("/api/check/job/export", methods=["GET"])
@require_auth
def api_check_job_export():
    kind = (request.args.get("type") or "clean").strip().lower()
    with JOB_LOCK:
        rows = list(CHECK_JOB.get("results") or [])
    out = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        u = (r.get("username") or "").strip().lstrip("@")
        if not u:
            continue
        st = r.get("status")
        prem = bool(r.get("premium"))
        collect = bool(r.get("collect"))
        if kind in ("clean", "available", "target"):
            if collect or st in ("clean", "available"):
                out.append("@" + u)
        elif kind in ("premium", "vip"):
            if prem:
                out.append("@" + u)
        elif kind in ("deleted",):
            if st in ("deleted", "unavailable"):
                out.append("@" + u)
        else:
            out.append("@" + u)
    # 去重保序
    seen = set()
    uniq = []
    for x in out:
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(x)
    text = "\n".join(uniq) + ("\n" if uniq else "")
    from flask import Response
    fname = "job_%s_%s.txt" % (kind, len(uniq))
    return Response(
        text,
        mimetype="text/plain; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=%s" % fname},
    )

@app.route("/api/check/job/export_json", methods=["GET"])
@require_auth
def api_check_job_export_json():
    kind = (request.args.get("type") or "clean").strip().lower()
    with JOB_LOCK:
        rows = list(CHECK_JOB.get("results") or [])
    items = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        u = (r.get("username") or "").strip().lstrip("@")
        if not u:
            continue
        st = r.get("status")
        prem = bool(r.get("premium"))
        collect = bool(r.get("collect"))
        ok = False
        if kind in ("clean", "available", "target"):
            ok = collect or st in ("clean", "available")
        elif kind in ("premium", "vip"):
            ok = prem
        elif kind in ("deleted",):
            ok = st in ("deleted", "unavailable")
        else:
            ok = True
        if ok:
            items.append("@" + u)
    seen = set(); uniq = []
    for x in items:
        k = x.lower()
        if k in seen:
            continue
        seen.add(k); uniq.append(x)
    return jsonify({"success": True, "type": kind, "total": len(uniq), "usernames": uniq})




@app.route("/api/check/pool", methods=["GET"])
@require_auth
def api_check_pool_get():
    import os, json
    path = globals().get("CHECK_POOL_FILE") or "/root/tg-scan-clean/check_pool.json"
    if os.path.exists(path):
        try:
            data = json.load(open(path, encoding="utf-8"))
        except Exception:
            data = []
    else:
        data = []
    if isinstance(data, dict):
        data = data.get("usernames") or []
    return jsonify({"success": True, "total": len(data), "usernames": data})

@app.route("/api/check/pool", methods=["POST"])
@require_auth
def api_check_pool_save():
    import json
    path = globals().get("CHECK_POOL_FILE") or "/root/tg-scan-clean/check_pool.json"
    body = request.get_json(silent=True) or {}
    raw = body.get("usernames") or body.get("text") or []
    if isinstance(raw, str):
        arr = [x.strip().lstrip("@") for x in raw.replace("\r","\n").split("\n") if x.strip()]
    else:
        arr = [str(x).strip().lstrip("@") for x in raw if str(x).strip()]
    seen=set(); uniq=[]
    for u in arr:
        k=u.lower()
        if k in seen: continue
        seen.add(k); uniq.append(u)
    json.dump(uniq, open(path,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
    return jsonify({"success": True, "total": len(uniq)})

@app.route("/api/check/pool", methods=["DELETE"])
@require_auth
def api_check_pool_clear():
    import json
    path = globals().get("CHECK_POOL_FILE") or "/root/tg-scan-clean/check_pool.json"
    json.dump([], open(path,"w",encoding="utf-8"))
    return jsonify({"success": True, "total": 0})

@app.route("/api/check/job/start", methods=["POST"])
@require_auth
def api_check_job_start():
    data = request.get_json(silent=True) or request.json or {}
    raw = data.get("usernames") or data.get("list") or data.get("text") or ""
    if isinstance(raw, str):
        usernames = [x.strip().lstrip("@") for x in raw.replace("\r", "\n").split("\n") if x.strip()]
    else:
        usernames = []
        for x in (raw or []):
            u = x.get("username") if isinstance(x, dict) else str(x)
            u = (u or "").strip().lstrip("@")
            if u:
                usernames.append(u)
    seen = set(); uniq = []
    for u in usernames:
        k = u.lower()
        if k in seen: continue
        seen.add(k); uniq.append(u)
    if not uniq:
        return jsonify({"success": False, "error": "没有用户名"}), 400
    with JOB_LOCK:
        # 旧任务卡死时允许强制接管
        CHECK_JOB["running"] = True
        CHECK_JOB["should_stop"] = False
        CHECK_JOB["queue"] = uniq[:]
        CHECK_JOB["results"] = []
        CHECK_JOB["total"] = len(uniq)
        CHECK_JOB["done"] = 0
        CHECK_JOB["batch_size"] = 300
        CHECK_JOB["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        CHECK_JOB["updated_at"] = CHECK_JOB["started_at"]
        CHECK_JOB["message"] = "后台任务已启动 %s" % len(uniq)
    t = threading.Thread(target=_check_job_worker, daemon=True)
    t.start()
    snap = _job_snapshot() if " _job_snapshot" in dir() or "_job_snapshot" in globals() else {"running": True, "total": len(uniq)}
    return jsonify({"success": True, "running": True, "total": len(uniq), "job": snap})


@app.route("/api/check/job/status", methods=["GET"])
@require_auth
def api_check_job_status():
    return jsonify(_job_snapshot())

@app.route("/api/check/job/stop", methods=["POST"])
@require_auth
def api_check_job_stop():
    with JOB_LOCK:
        CHECK_JOB["should_stop"] = True
        CHECK_JOB["message"] = "正在停止"
    return jsonify({"success": True})


@app.route('/api/check/one', methods=['POST'])
@require_auth

def _check_one_with_failover(username, bots):
    last = None
    flood_hits = 0
    for bot in bots:
        if flood_hits >= 3:
            break
        key = _bot_key(bot)
        try:
            out = None
            if "async_check_one_username" in globals():
                from telethon import TelegramClient
                from telethon.errors import FloodWaitError
                async def _run():
                    client = TelegramClient(bot.get("session_path"), int(bot.get("api_id") or 0), str(bot.get("api_hash") or ""))
                    await client.connect()
                    try:
                        if not await client.is_user_authorized():
                            return {"username": username, "status": "error", "error": "session未授权", "_skip_write": True, "status": "retry_session", "collect": False}
                        return await async_check_one_username(client, username)
                    finally:
                        try:
                            await client.disconnect()
                        except Exception:
                            pass
                out = run_async(_run())
            last = out or last
            if not out:
                continue
            err = (out.get("error") or "") + str(out.get("status") or "")
            if out.get("status") == "flood" or "FloodWait" in err:
                flood_hits += 1
                sec = int(out.get("_flood_seconds") or 0) or 3600
                m = re.search(r"(\d+)", err)
                if m and sec == 3600:
                    try:
                        sec = int(m.group(1))
                    except Exception:
                        pass
                set_bot_cooldown(key, min(sec, 86400), "FloodWait")
                print("[failover] flood", bot.get("phone"), "user", username, "sec", sec, "hit", flood_hits)
                continue
            if is_target_frozen(None, err) or out.get("status") in ("frozen", "deleted"):
                out["collect"] = False
                if out.get("status") not in ("deleted",):
                    out["status"] = "frozen"
                return out
            return out
        except Exception as e:
            last = {"username": username, "status": "error", "error": str(e)[:160], "collect": False}
            if "FloodWait" in str(e):
                flood_hits += 1
                set_bot_cooldown(key, 3600, "FloodWait")
                continue
    if flood_hits >= 3:
        return {"username": username, "status": "skip_flood", "error": "连续3次限流，本条跳过", "collect": False, "premium": False}
    return last or {"username": username, "status": "error", "error": "无可用水军", "collect": False}

def api_check_one():
    """检测单个用户名：只用水军工作池；FloodWait 写入冷却仓；尊重每日上限"""
    import time, asyncio
    data = request.json or {}
    username = (data.get('username') or '').strip().lstrip('@')
    if not username:
        return jsonify({"error": "请提供用户名", "status": "error", "username": ""}), 400

    config = load_config()
    workable = list_workable_bots_by_ip(config)
    if not workable:
        # 区分全冷却 / 全日额满
        all_bots = config.get('bots') or []
        any_cool = False
        for b in all_bots:
            key = b.get('id') or b.get('phone') or ''
            cool, left = is_bot_cooling(key)
            if cool:
                any_cool = True
                break
        msg = "全部水军冷却中或已达每日上限" if any_cool or all_bots else "无可用水军账号"
        return jsonify({
            "username": username,
            "status": "flood" if any_cool else "error",
            "error": msg,
            "premium": False,
            "collect": False,
        })

    # 最多试 3 个工作号
    last_err = None
    max_try = min(3, len(workable))
    # 简单轮询：按 daily_used 少的优先
    workable = sorted(workable, key=lambda b: int(b.get('_daily_used') or 0))

    for i in range(max_try):
        bot = workable[i]
        bot_key = bot.get('id') or bot.get('phone') or bot.get('session_path')
        sp = bot.get('session_path')
        if not sp:
            continue
        try:
            api_id = int(bot.get('api_id') or 31034207)
            api_hash = str(bot.get('api_hash') or '')
            if not api_hash:
                continue

            async def _run():
                from telethon import TelegramClient
                from telethon.tl.functions.contacts import ResolveUsernameRequest
                from telethon.errors import UsernameNotOccupiedError, UsernameInvalidError, FloodWaitError
                client = TelegramClient(sp, api_id, api_hash, loop=_loop)
                try:
                    await asyncio.wait_for(client.connect(), timeout=8)
                    if not await client.is_user_authorized():
                        set_bot_cooldown(bot_key, 24*3600, reason="session未授权")
                        try:
                            cfg2 = load_config()
                            for _b in cfg2.get("bots") or []:
                                if (_b.get("id") or _b.get("phone") or _b.get("session_path")) == bot_key or _b.get("phone") and str(_b.get("phone")) in str(bot_key):
                                    _b["status"] = "need_relogin"
                            save_config(cfg2)
                        except Exception:
                            pass
                        return {"username": username, "status": "retry_session", "error": "session未授权", "_skip_write": True, "status": "retry_session", "premium": False, "collect": False, "_bot": bot_key, "_skip_write": True}
                    try:
                        result = await asyncio.wait_for(client(ResolveUsernameRequest(username)), timeout=8)
                    except FloodWaitError as e:
                        return {"username": username, "status": "flood", "error": f"FloodWait {e.seconds}s", "premium": False, "collect": False, "_flood_seconds": int(e.seconds), "_bot": bot_key}
                    except UsernameNotOccupiedError:
                        try:
                            ent = await asyncio.wait_for(client.get_entity(username), timeout=8)
                            et = type(ent).__name__
                            if et == "User" or getattr(ent, "first_name", None) is not None:
                                user = ent
                                if getattr(user, "deleted", False):
                                    return {"username": username, "status": "deleted", "premium": False, "collect": False, "_bot": bot_key}
                                fn = getattr(user, "first_name", "") or ""
                                ln = getattr(user, "last_name", "") or ""
                                premium = bool(getattr(user, "premium", False))
                                is_bot = bool(getattr(user, "bot", False))
                                ad = is_ad_account(username, fn, ln) if "is_ad_account" in globals() else False
                                bot_like = (is_bot_like_username(username) if "is_bot_like_username" in globals() else False) or is_bot
                                if ad:
                                    st, collect = "ad", False
                                elif bot_like:
                                    st, collect = "spam", False
                                else:
                                    st, collect = "clean", True
                                return {"username": username, "status": st, "premium": premium, "collect": collect, "is_ad": ad, "is_spam": bot_like, "first_name": fn, "last_name": ln, "_bot": bot_key}
                            return {"username": username, "status": "unavailable", "premium": False, "collect": False, "entity_type": et, "_bot": bot_key}
                        except UsernameNotOccupiedError:
                            return {"username": username, "status": "available", "premium": False, "collect": False, "_bot": bot_key}
                        except Exception as _e:
                            return {"username": username, "status": "error", "error": str(_e)[:120], "premium": False, "collect": False, "_bot": bot_key}
                    except UsernameInvalidError:
                        return {"username": username, "status": "invalid", "premium": False, "collect": False, "_bot": bot_key}

                    users = list(getattr(result, 'users', None) or [])
                    chats = list(getattr(result, 'chats', None) or [])
                    if users:
                        user = users[0]
                        if getattr(user, 'deleted', False):
                            return {"username": username, "status": "deleted", "premium": False, "collect": False, "_bot": bot_key}
                        is_premium = bool(getattr(user, 'premium', False))
                        is_bot = bool(getattr(user, 'bot', False))
                        fn = getattr(user, 'first_name', '') or ''
                        ln = getattr(user, 'last_name', '') or ''
                        ad = is_ad_account(username, fn, ln) if 'is_ad_account' in globals() else False
                        bot_like = (is_bot_like_username(username) if 'is_bot_like_username' in globals() else False) or is_bot
                        # 在线粗分
                        st_obj = getattr(user, 'status', None)
                        st_name = type(st_obj).__name__ if st_obj is not None else ''
                        inactive = st_name in ('UserStatusEmpty', 'UserStatusOffline') and False
                        # 若有 classify_last_online 则用
                        if 'classify_last_online' in dir() or 'classify_last_online' in globals():
                            try:
                                online_flag, inactive = classify_last_online(user)
                            except Exception:
                                inactive = st_name in ('UserStatusLastMonth', 'UserStatusEmpty')
                        if ad:
                            st = 'ad'
                            collect = False
                        elif bot_like:
                            st = 'spam'
                            collect = False
                        elif inactive:
                            st = 'inactive'
                            collect = False
                        else:
                            st = 'clean'
                            collect = True
                        return {
                            "username": username,
                            "status": st,
                            "premium": is_premium,
                            "collect": collect,
                            "is_ad": ad,
                            "is_spam": bot_like,
                            "first_name": fn,
                            "last_name": ln,
                            "user_id": getattr(user, 'id', None),
                            "online": st_name,
                            "_bot": bot_key,
                        }
                    if chats:
                        return {"username": username, "status": "unavailable", "premium": False, "collect": False, "entity_type": type(chats[0]).__name__, "_bot": bot_key}
                    return {"username": username, "status": "unavailable", "premium": False, "collect": False, "_bot": bot_key}
                finally:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

            out = run_async(_run())
            if not out:
                last_err = "empty result"
                continue

            # FloodWait → 冷却仓，换号或返回
            if out.get('status') in ('retry_session',) or out.get('_skip_write'):
                last_err = out.get('error') or 'session未授权'
                continue
            if out.get("status") in ("retry_session",) or out.get("_skip_write"):
                last_err = out.get("error") or "session未授权"
                continue
            if out.get('status') == 'flood' or (out.get('error') or '').startswith('FloodWait'):
                sec = int(out.get('_flood_seconds') or 0)
                if not sec:
                    # 从 error 文本解析
                    import re as _re
                    m = _re.search(r'(\d+)\s*s', str(out.get('error') or ''))
                    sec = int(m.group(1)) if m else 3600
                set_bot_cooldown(bot_key, sec, reason=out.get('error') or 'FloodWait')
                last_err = out.get('error')
                # 尝试下一个工作号
                continue

            # 成功占用一次每日额度（含 clean/ad/spam/deleted 等有效响应）
            if out.get('status') not in ('error',):
                try:
                    incr_bot_daily(bot_key)
                except Exception:
                    pass

            # 去掉内部字段
            out.pop('_bot', None)
            out.pop('_flood_seconds', None)
            return jsonify(out)

        except Exception as e:
            last_err = str(e)[:160]
            # session 类错误不整池冷却，仅换号
            continue

    # 全部尝试失败
    return jsonify({
        "username": username,
        "status": "error",
        "error": last_err or "水军未授权或session失效", "label": "错误",
        "premium": False,
        "collect": False,
    })



def api_check_one():
    """检测单个用户名：最多试3个水军，单号8秒超时，FloodWait自动冷却"""
    import time
    data = request.json or {}
    username = (data.get('username') or '').strip().lstrip('@')
    if not username:
        return jsonify({"error": "请提供用户名"}), 400

    config = load_config()
    bots = [b for b in config.get('bots', []) if b.get('session_path')]
    if not bots:
        return jsonify({"username": username, "status": "error", "error": "无可用水军账号"})

    now = time.time()
    # 过滤还在冷却的号
    ready = []
    for b in bots:
        phone = b.get("phone") or b.get("id") or ""
        until = FLOOD_COOLDOWN.get(phone, 0)
        if until > now:
            continue
        ready.append(b)
    if not ready:
        ready = bots  # 全在冷却则仍尝试，避免完全不可用

    start_idx = sum(ord(ch) for ch in username) % len(ready)
    ordered = ready[start_idx:] + ready[:start_idx]
    ordered = ordered[:3]  # 最多试3个号

    async def _one():
        from telethon import TelegramClient
        from telethon.tl.functions.contacts import ResolveUsernameRequest
        from telethon.errors import UsernameNotOccupiedError, UsernameInvalidError, FloodWaitError
        last_err = "全部水军失败"
        for bot in ordered:
            phone = bot.get("phone") or bot.get("id") or ""
            sp = bot.get("session_path")
            client = None
            try:
                api_id = int(bot.get("api_id") or API_CONFIGS[0]["api_id"])
                api_hash = str(bot.get("api_hash") or API_CONFIGS[0]["api_hash"])
                # 暂时不用代理，避免断连
                client = TelegramClient(sp, api_id, api_hash)
                await asyncio.wait_for(client.connect(), timeout=8)
                if not await asyncio.wait_for(client.is_user_authorized(), timeout=5):
                    last_err = f"{phone} 未授权"
                    await client.disconnect()
                    continue
                try:
                    entity = await asyncio.wait_for(client.get_entity(username), timeout=10)
                    et = type(entity).__name__
                    if et == "User" or (hasattr(entity, "first_name") and hasattr(entity, "bot") and not getattr(entity, "broadcast", False) and not getattr(entity, "megagroup", False)):
                        user = entity
                        if getattr(user, 'deleted', False):
                            out = {"username": username, "status": "deleted", "premium": False, "collect": False}
                        else:
                            fn = getattr(user, 'first_name', '') or ''
                            ln = getattr(user, 'last_name', '') or ''
                            premium = bool(getattr(user, 'premium', False))
                            is_bot = bool(getattr(user, 'bot', False))
                            ad = is_ad_account(username, fn, ln)
                            bot_like = is_bot_like_username(username) or is_bot
                            online_kind = classify_last_online(getattr(user, 'status', None))
                            if ad:
                                st = "ad"
                                collect = False
                            elif bot_like:
                                st = "spam"
                                collect = False
                            elif online_kind == "stale":
                                st = "inactive"
                                collect = False
                            else:
                                st = "clean"
                                collect = True
                            out = {
                                "username": username,
                                "status": st,
                                "premium": premium,
                                "is_ad": ad,
                                "is_spam": bot_like,
                                "online": online_kind,
                                "collect": collect,
                                "user_id": getattr(user, 'id', None),
                                "first_name": fn,
                                "last_name": ln,
                            }
                    else:
                        # 频道/群：不是个人用户采集对象
                        out = {"username": username, "status": "unavailable", "premium": False, "collect": False, "is_spam": False, "entity_type": type(entity).__name__}
                    await client.disconnect()
                    return out
                except UsernameNotOccupiedError:
                    await client.disconnect()
                    return {"username": username, "status": "available", "premium": False, "collect": False}
                except UsernameInvalidError:
                    await client.disconnect()
                    return {"username": username, "status": "invalid", "premium": False, "collect": False}
                except ValueError as e:
                    # get_entity 找不到时常见 ValueError
                    msg = str(e).lower()
                    await client.disconnect()
                    if "no user" in msg or "not found" in msg or "nobody" in msg:
                        return {"username": username, "status": "available", "premium": False, "collect": False}
                    return {"username": username, "status": "error", "error": str(e)[:100], "premium": False}
                except FloodWaitError as e:
                    # 冷却：最多记 6 小时，避免一次记 20 小时导致全废
                    sec = min(int(e.seconds), 6 * 3600)
                    FLOOD_COOLDOWN[phone] = time.time() + sec
                    last_err = f"FloodWait {e.seconds}s"
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    continue
                except asyncio.TimeoutError:
                    last_err = "查询超时"
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    continue
                except Exception as e:
                    last_err = str(e)[:100]
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    continue
            except asyncio.TimeoutError:
                last_err = "连接超时"
                if client:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                continue
            except Exception as e:
                last_err = str(e)[:100]
                if client:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                continue
        return {"username": username, "status": "error", "error": last_err, "premium": False}

    try:
        result = run_async(_one())
        return jsonify(result)
    except Exception as e:
        return jsonify({"username": username, "status": "error", "error": str(e)[:120]}), 500


@app.route('/api/check/result', methods=['POST'])
@require_auth
def api_check_result():
    """只允许 available 写入有用文档，deleted 一律拒绝"""
    data = request.json or {}
    items = data.get('available', data.get('results', []))
    
    available = load_available()
    blacklist = load_blacklist()
    added = 0
    skipped_deleted = 0

    for item in items:
        if isinstance(item, dict):
            username = (item.get('username') or '').strip().lstrip('@')
            status = item.get('status', 'available')
            if status == 'deleted':
                skipped_deleted += 1
                continue
            if status != 'available':
                continue
        else:
            username = str(item).strip().lstrip('@')

        if not username:
            continue
        formatted = f"@{username}"
        if any(kw.lower() in username.lower() for kw in blacklist):
            continue
        if formatted not in available:
            available.append(formatted)
            added += 1

    save_available(available)
    return jsonify({
        "success": True,
        "added": added,
        "skipped_deleted": skipped_deleted,
        "total": len(available)
    })

@app.route('/api/available', methods=['GET'])
@require_auth
def api_get_available():
    available = load_available()
    return jsonify({"usernames": available, "total": len(available)})

@app.route('/api/available/clear', methods=['POST'])
@require_auth
def api_clear_available():
    save_available([])
    return jsonify({"success": True, "message": "已清空"})

@app.route('/api/available/export', methods=['GET'])
@require_auth
def api_export_available():
    available = load_available()
    text = "\n".join(available)
    return text, 200, {'Content-Type': 'text/plain; charset=utf-8'}


# ============ Telegram 会员（Premium）采集接口 ============

@app.route('/api/targets', methods=['GET'])
@require_auth
def api_get_targets():
    t = load_targets()
    return jsonify({"usernames": t, "total": len(t)})

@app.route('/api/targets/clear', methods=['POST'])
@require_auth
def api_clear_targets():
    save_targets([])
    return jsonify({"success": True})

@app.route('/api/targets/export', methods=['GET'])
@require_auth
def api_export_targets():
    t = load_targets()
    return "\n".join(t), 200, {"Content-Type": "text/plain; charset=utf-8"}


@app.route("/api/targets/result", methods=["POST"])
@require_auth
def api_targets_result():
    data = request.json or {}
    items = data.get("targets") or data.get("usernames") or data.get("list") or []
    if isinstance(items, str):
        items = [x.strip() for x in items.replace("\r", "\n").split("\n") if x.strip()]
    targets = load_targets() if "load_targets" in globals() else []
    if not isinstance(targets, list):
        targets = []
    added = 0
    skip_status = ("frozen", "deleted", "spam", "ad", "skip_flood", "flood", "error", "invalid", "unavailable")
    for item in items:
        st = ""
        username = ""
        collect = None
        if isinstance(item, dict):
            st = str(item.get("status") or "")
            collect = item.get("collect")
            username = (item.get("username") or item.get("user") or "").strip().lstrip("@")
            if item.get("status") in skip_status:
                continue
            if collect is False:
                continue
            if st and st not in ("clean", "taken"):
                continue
        else:
            username = str(item).strip().lstrip("@")
        if not username:
            continue
        formatted = "@" + username
        if formatted not in targets:
            targets.append(formatted)
            added += 1
    save_targets(targets)
    return jsonify({"success": True, "added": added, "total": len(targets)})


@app.route('/api/premium', methods=['GET'])
@require_auth
def api_get_premium():
    premium = load_premium()
    return jsonify({"usernames": premium, "total": len(premium)})

@app.route('/api/premium/clear', methods=['POST'])
@require_auth
def api_clear_premium():
    save_premium([])
    return jsonify({"success": True, "message": "已清空会员列表"})

@app.route('/api/premium/export', methods=['GET'])
@require_auth
def api_export_premium():
    premium = load_premium()
    text = "\n".join(premium)
    return text, 200, {'Content-Type': 'text/plain; charset=utf-8'}

@app.route('/api/premium/result', methods=['POST'])
@require_auth
def api_premium_result():
    """将 Premium 会员写入会员文档"""
    data = request.json or {}
    items = data.get('premium', data.get('results', data.get('usernames', [])))
    premium = load_premium()
    added = 0
    for item in items:
        if isinstance(item, dict):
            if not item.get('premium'):
                continue
            username = (item.get('username') or '').strip().lstrip('@')
        else:
            username = str(item).strip().lstrip('@')
        if not username:
            continue
        formatted = f"@{username}"
        if formatted not in premium:
            premium.append(formatted)
            added += 1
    save_premium(premium)
    return jsonify({"success": True, "added": added, "total": len(premium)})

# ============ 恢复数据接口 ============
@app.route('/api/restore', methods=['POST'])
@require_auth
def api_restore_data():
    backup_file = "/var/www/tg-scan-clean/tg_check_available.txt"
    if not os.path.exists(backup_file):
        backup_file = "/root/HTTP-TG77777.pw/tg_check_available.txt"
    
    if not os.path.exists(backup_file):
        return jsonify({"error": "备份文件不存在"}), 404
    
    with open(backup_file, 'r') as f:
        lines = f.read().strip().split('\n')
    
    available = load_available()
    blacklist = load_blacklist()
    added = 0
    
    for line in lines:
        username = line.strip()
        if not username:
            continue
        if not username.startswith('@'):
            username = f"@{username}"
        is_blacklisted = any(kw.lower() in username.lstrip('@').lower() for kw in blacklist)
        if is_blacklisted:
            continue
        if username not in available:
            available.append(username)
            added += 1
    
    save_available(available)
    return jsonify({"success": True, "added": added, "total": len(available)})

# ============ 关键词黑名单接口 ============
@app.route('/api/blacklist', methods=['GET'])
@require_auth
def api_get_blacklist():
    keywords = load_blacklist()
    return jsonify({"keywords": keywords})

@app.route('/api/blacklist/add', methods=['POST'])
@require_auth
def api_add_blacklist():
    data = request.json
    keywords = data.get('keywords', [])
    if isinstance(keywords, str):
        keywords = [k.strip() for k in keywords.split('\n') if k.strip()]
    
    current = load_blacklist()
    added = 0
    for kw in keywords:
        kw = kw.strip()
        if kw and kw not in current:
            current.append(kw)
            added += 1
    
    save_blacklist(current)
    
    # 自动从可用列表中删除匹配的用户名
    available = load_available()
    removed = 0
    new_available = []
    for username in available:
        name = username.lstrip('@').lower()
        if any(kw.lower() in name for kw in current):
            removed += 1
        else:
            new_available.append(username)
    save_available(new_available)
    
    return jsonify({"success": True, "added": added, "removed": removed, "total_keywords": len(current)})

@app.route('/api/blacklist/remove', methods=['POST'])
@require_auth
def api_remove_blacklist():
    data = request.json
    keyword = data.get('keyword', '').strip()
    current = load_blacklist()
    current = [k for k in current if k != keyword]
    save_blacklist(current)
    return jsonify({"success": True, "total": len(current)})

@app.route('/api/blacklist/clear', methods=['POST'])
@require_auth
def api_clear_blacklist():
    save_blacklist([])
    return jsonify({"success": True})

# ============ 统计接口 ============

# ============ 状态接口（兼容前端） ============

@app.route('/api/bots/work_status', methods=['GET'])
@require_auth
def api_bots_work_status():
    import time
    config = load_config()
    bots = config.get("bots") or []
    cd = load_cooldown()
    daily = load_daily_stats()
    rows = []
    work = 0
    cool = 0
    capped = 0
    for b in bots:
        key = str(b.get("id") or b.get("phone") or "")
        cooling, left = is_bot_cooling(key)
        remain, used, limit = bot_daily_left(key)
        if cooling:
            st = "cooldown"
            cool += 1
        elif remain <= 0:
            st = "daily_capped"
            capped += 1
        else:
            st = "work"
            work += 1
        rows.append({
            "id": key,
            "phone": b.get("phone"),
            "name": b.get("name"),
            "status": st,
            "cooldown_left": left if cooling else 0,
            "daily_used": used,
            "daily_left": remain,
            "daily_limit": limit,
        })
    return jsonify({
        "bots": rows,
        "summary": {"work": work, "cooldown": cool, "daily_capped": capped, "total": len(rows)},
        "max_batch": MAX_BATCH_SIZE,
        "default_daily_limit": DEFAULT_DAILY_LIMIT,
    })



@app.route("/api/export/round", methods=["GET"])
@require_auth
def api_export_round():
    kind = (request.args.get("type") or "clean").strip().lower()
    files = {
        "clean": "/root/tg-scan-clean/job_round_clean.txt",
        "available": "/root/tg-scan-clean/job_round_clean.txt",
        "premium": "/root/tg-scan-clean/job_round_premium.txt",
        "vip": "/root/tg-scan-clean/job_round_premium.txt",
        "all": "/root/tg-scan-clean/job_round_all.txt",
    }
    path = files.get(kind) or files["clean"]
    if not os.path.exists(path):
        return jsonify({"success": False, "error": "本轮文件不存在", "usernames": [], "total": 0})
    lines = []
    with open(path, encoding="utf-8") as f:
        for raw in f:
            u = raw.strip()
            if not u:
                continue
            if not u.startswith("@"):
                u = "@" + u
            lines.append(u)
    return jsonify({"success": True, "type": kind, "total": len(lines), "usernames": lines})


@app.route('/api/status', methods=['GET'])
@require_auth
def api_status():
    """返回水军列表和状态（前端仪表盘使用）"""
    config = load_config()
    bots = config.get('bots', [])
    safe_bots = []
    for bot in bots:
        safe_bots.append({
            "id": bot.get("id", ""),
            "name": bot.get("name", ""),
            "username": bot.get("username", ""),
            "phone": bot.get("phone", ""),
            "first_name": bot.get("first_name", ""),
            "status": bot.get("status", "ready"),
            "type": bot.get("type", "userbot"),
            "added_time": bot.get("added_time", "")
        })
    return jsonify({"bots": safe_bots, "total": len(safe_bots)})

# ============ 统计接口（兼容前端） ============
@app.route('/api/stats', methods=['GET'])
@require_auth
def api_stats():
    config = load_config()
    available = load_available()
    premium = load_premium()
    blacklist = load_blacklist()
    bots = config.get('bots', [])
    stats = config.get('stats', {})
    return jsonify({
        "bots": len(bots),
        "available": len(available),
        "premium": len(premium),
        "blacklist": len(blacklist),
        "stats": {
            "today_sent": stats.get("today_sent", 0),
            "total_sent": stats.get("total_sent", 0),
            "total_success": stats.get("total_success", 0),
            "total_failed": stats.get("total_failed", 0),
            "today_success": stats.get("today_success", 0)
        }
    })

# ============ 健康检查 ============

@app.route('/api/pool/api/batch', methods=['POST'])
@require_auth
def api_pool_api_batch():
    """批量添加 API，不限制条数。支持 text 或 lines，格式 api_id-api_hash / api_id:api_hash"""
    data = request.json or {}
    text = data.get('text') or ''
    lines = data.get('lines') or []
    if text and not lines:
        lines = str(text).splitlines()
    pool = load_api_pool()
    existing = {str(x.get('api_id')) for x in pool}
    added = 0
    for line in lines:
        line = str(line).strip()
        if not line:
            continue
        api_id, api_hash = None, None
        if '-' in line:
            a, b = line.split('-', 1)
            api_id, api_hash = a.strip(), b.strip()
        elif ':' in line:
            a, b = line.split(':', 1)
            api_id, api_hash = a.strip(), b.strip()
        elif ' ' in line:
            parts = line.split()
            if len(parts) >= 2:
                api_id, api_hash = parts[0].strip(), parts[1].strip()
        if not api_id or not api_hash:
            continue
        if str(api_id) in existing:
            continue
        try:
            api_id_i = int(api_id)
        except Exception:
            continue
        pool.append({"api_id": api_id_i, "api_hash": api_hash, "label": f"API-{api_id_i}"})
        existing.add(str(api_id_i))
        added += 1
    save_api_pool(pool)
    return jsonify({"success": True, "added": added, "total": len(pool)})

@app.route('/api/pool/proxy/batch', methods=['POST'])
@require_auth
def api_pool_proxy_batch():
    """批量添加代理，不限制条数"""
    import os, json
    data = request.json or {}
    text = data.get('text') or ''
    lines = data.get('lines') or data.get('proxies') or []
    if text and not lines:
        lines = str(text).splitlines()
    path = PROXY_POOL_FILE if 'PROXY_POOL_FILE' in globals() or 'PROXY_POOL_FILE' in dir() else '/root/tg-scan-clean/proxy_pool.json'
    # dir() in function is wrong for global - use globals
    path = globals().get('PROXY_POOL_FILE', '/root/tg-scan-clean/proxy_pool.json')
    raw = []
    if os.path.exists(path):
        try:
            raw = json.load(open(path))
        except Exception:
            raw = []
    # normalize to list of dicts
    norm = []
    seen = set()
    for x in raw:
        if isinstance(x, str):
            s = x.strip()
            if s and s not in seen:
                norm.append({"proxy": s, "label": s[:40]})
                seen.add(s)
        elif isinstance(x, dict):
            s = (x.get('proxy') or x.get('url') or '').strip()
            if s and s not in seen:
                norm.append({"proxy": s, "label": x.get('label') or s[:40]})
                seen.add(s)
    added = 0
    for line in lines:
        s = str(line).strip()
        if not s or s in seen:
            continue
        norm.append({"proxy": s, "label": s[:40]})
        seen.add(s)
        added += 1
    json.dump(norm, open(path, 'w'), ensure_ascii=False, indent=2)
    return jsonify({"success": True, "added": added, "total": len(norm)})


@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "time": datetime.now().isoformat()})

@app.route('/api/health', methods=['GET'])
def api_health():
    return jsonify({"status": "ok", "time": datetime.now().isoformat()})

# ============ 启动 ============
if __name__ == '__main__':
    ensure_loop_running()
    print(f"[{datetime.now()}] TG用户名检测工具 Agent 启动在端口 8902")
    print(f"[{datetime.now()}] Asyncio事件循环已在独立线程中运行")
    app.run(host='0.0.0.0', port=8902, debug=False)
