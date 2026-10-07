import re
#!/usr/bin/env python3
"""TG用户名检测工具 - 后端Agent
运行在8899端口，提供水军管理、批量检测、关键词黑名单等API
修复：使用独立线程运行asyncio事件循环，解决Telethon兼容性问题
"""
import os


def _live_recent(status_obj, days=30):
    name = type(status_obj).__name__ if status_obj is not None else ""
    if name in ("UserStatusOnline", "UserStatusRecently", "UserStatusLastWeek", "UserStatusLastMonth"):
        return True, name, ""
    was = getattr(status_obj, "was_online", None) if status_obj is not None else None
    if was is None:
        return False, name, ""
    try:
        from datetime import datetime, timezone
        dt = was
        if isinstance(was, str):
            dt = datetime.fromisoformat(was.replace("Z", "+00:00"))
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        was_s = dt.isoformat()
        if (datetime.now(timezone.utc) - dt).total_seconds() <= days * 86400:
            return True, name, was_s
        return False, name, was_s
    except Exception:
        return False, name, ""

def pace_one_user():
    import time, threading
    if not hasattr(pace_one_user, "lock"):
        pace_one_user.lock = threading.Lock()
        pace_one_user.t = 0.0
    with pace_one_user.lock:
        now = time.time()
        wait = 10 - (now - pace_one_user.t)
        if pace_one_user.t and wait > 0:
            time.sleep(wait)
            now = time.time()
        pace_one_user.t = now

def _proxy_line_summary(config=None):
    """工作中=还有号可用的代理线。冷却只计水军号，不计代理。"""
    if config is None:
        config = load_config()
    groups, cool, full = {}, 0, 0
    for b in config.get("bots") or []:
        ip = (str(b.get("proxy") or "").split(":")[0] or "direct")
        groups.setdefault(ip, []).append(b)
        if _bot_cooling(b):
            cool += 1
        if int(b.get("daily_used") or 0) >= int(b.get("daily_limit") or 500):
            full += 1
    work = sum(1 for arr in groups.values() if any(not _bot_cooling(b) for b in arr))
    return {"work": work, "cooldown": cool, "daily_capped": full, "lines": len(groups)}

def _bot_cooling(bot):
    key = bot.get("id") or bot.get("phone") or bot.get("session_path")
    for name in ("is_bot_in_cooldown", "bot_in_cooldown"):
        fn = globals().get(name)
        if not fn:
            continue
        try:
            return bool(fn(key))
        except Exception:
            try:
                return bool(fn(bot))
            except Exception:
                pass
    return False

def pick_rr_bot(config=None):
    import json
    if config is None:
        config = load_config()
    groups, order = {}, []
    for b in config.get("bots") or []:
        if not b.get("session_path"):
            continue
        ip = (str(b.get("proxy") or "").split(":")[0] or "direct")
        if ip not in groups:
            groups[ip] = []
            order.append(ip)
        groups[ip].append(b)
    if not order:
        return None
    path = "/root/tg-scan-clean/rr_state.json"
    try:
        st = json.load(open(path, encoding="utf-8"))
    except Exception:
        st = {"line": 0, "pos": {}}
    st.setdefault("pos", {})
    n = len(order)
    chosen = None
    for step in range(n):
        li = (int(st.get("line") or 0) + step) % n
        ip = order[li]
        arr = groups[ip]
        start = int(st["pos"].get(ip) or 0) % len(arr)
        pick_i = None
        for k in range(len(arr)):
            i = (start + k) % len(arr)
            if not _bot_cooling(arr[i]):
                pick_i = i
                break
        if pick_i is None:
            continue
        chosen = arr[pick_i]
        st["pos"][ip] = (pick_i + 1) % len(arr)
        st["line"] = (li + 1) % n
        break
    json.dump(st, open(path, "w", encoding="utf-8"), ensure_ascii=False)
    if chosen:
        ip = (str(chosen.get("proxy") or "").split(":")[0] or "direct")
        print("[rr]", chosen.get("phone"), ip, flush=True)
    return chosen

def _install_fast_client():
    import threading, telethon
    from telethon.client.telegramclient import TelegramClient as _OrigTG
    if getattr(_install_fast_client, "done", False):
        return
    pool, locks = {}, {}
    orig_disc = _OrigTG.disconnect
    async def _disc(self):
        if getattr(self, "_keep_alive", False):
            lk = getattr(self, "_use_lock", None)
            if lk is not None and lk.locked():
                try: lk.release()
                except RuntimeError: pass
            return
        return await orig_disc(self)
    _OrigTG.disconnect = _disc
    def _FastClient(*args, **kwargs):
        session = args[0] if args else kwargs.get("session")
        key = str(getattr(session, "filename", None) or session)
        lk = locks.setdefault(key, threading.Lock())
        if not lk.acquire(timeout=20):
            raise RuntimeError("LINE_BUSY")
        try:
            hit = pool.get(key)
            if hit is not None and hit.is_connected():
                hit._use_lock = lk
                hit._keep_alive = True
                return hit
            kwargs["flood_sleep_threshold"] = 0
            kwargs["connection_retries"] = 1
            kwargs["request_retries"] = 1
            kwargs["retry_delay"] = 0
            kwargs.setdefault("timeout", 12)
            kwargs["receive_updates"] = False
            try:
                obj = _OrigTG(*args, **kwargs)
            except TypeError:
                kwargs.pop("receive_updates", None)
                obj = _OrigTG(*args, **kwargs)
            obj._use_lock = lk
            obj._keep_alive = True
            pool[key] = obj
            return obj
        except Exception:
            try: lk.release()
            except RuntimeError: pass
            raise
    telethon.TelegramClient = _FastClient
    import telethon.client.telegramclient as _tc
    _tc.TelegramClient = _FastClient
    globals()["TelegramClient"] = _FastClient
    _install_fast_client.done = True
_install_fast_client()
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
    _nm = (str(username or '') + ' ' + str(first_name or '') + ' ' + str(last_name or '')).lower()
    if any(k.lower() in _nm for k in ("bot", "集团", "主", "全国", "全球", "会所")):
        return True
    text = " ".join([
        str(username or ""),
        str(first_name or ""),
        str(last_name or ""),
    ]).lower()
    keys = [
        "代理", "招商", "赌博", "博彩", "赌场", "娱乐城", "威尼斯",
        "贷款", "理财", "兑换", "代收", "代付", "回收", "担保",
        "有需要", "老板", "主业", "看主业", "频道", "兼职",
        "加我", "私聊", "飞机", "福利", "招代理", "推广",
        "casino", "betting", "loan", "crypto exchange",
    ]
    for k in keys:
        if k.lower() in text:
            return True
    return False



def _letters_no_rhythm(s):
    s = (s or "").lower()
    if not s:
        return False
    for n in (2, 3, 4):
        if re.search(r"(.{%d})\1" % n, s):
            return False
    if not re.search(r"[aeiou]", s):
        return True
    if re.search(r"[^aeiou]{5,}", s):
        return True
    if len(s) <= 8:
        return False
    vowels = sum(ch in "aeiou" for ch in s)
    return vowels / len(s) < 0.25

def _is_shuijun_name(username):
    """水军必须同时满足：字母没规律，且至少4位、彼此不重复的数字。"""
    import re
    raw = re.sub(r"[^A-Za-z0-9]", "", username or "")
    if len(raw) < 6:
        return False
    digits = re.sub(r"[^0-9]", "", raw)
    if len(digits) < 4 or len(set(digits)) != len(digits):
        return False
    letters = re.sub(r"[^A-Za-z]", "", raw).lower()
    if len(letters) < 4:
        return False
    no_vowel = re.search(r"[aeiou]", letters) is None
    no_rhythm = re.search(r"[bcdfghjklmnpqrstvwxyz]{5,}", letters) is not None
    return no_vowel or no_rhythm
def _active_within_30(status):
    if status is None:
        return True
    name = type(status).__name__
    if name in ("UserStatusOnline", "UserStatusRecently", "UserStatusLastWeek", "UserStatusLastMonth", "UserStatusEmpty"):
        return True
    if name == "UserStatusOffline":
        was = getattr(status, "was_online", None)
        if was is None:
            return True
        try:
            if getattr(was, "tzinfo", None) is None:
                was = was.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - was).total_seconds() <= 30 * 86400
        except Exception:
            return True
    return True

def _iso_was(status):
    was = getattr(status, "was_online", None) if status is not None else None
    try:
        return was.isoformat() if was else ""
    except Exception:
        return ""

def _within_from_out(out):
    name = out.get("online") or ""
    if name in ("UserStatusOnline", "UserStatusRecently", "UserStatusLastWeek", "UserStatusLastMonth", "UserStatusEmpty"):
        return True
    was = out.get("was_online") or ""
    if was:
        try:
            dt = datetime.fromisoformat(str(was).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - dt).total_seconds() <= 30 * 86400
        except Exception:
            return True
    return True

def _online_within_days(info, days=30):
    """30天内在线才算活跃。recently、上周、本月、具体时间都认。"""
    from datetime import datetime, timezone, timedelta
    if not isinstance(info, dict):
        return False
    if info.get("online_recent") is True or info.get("recent") is True:
        return True
    parts = []
    for k in ("online", "status_name", "last_seen", "user_status", "st_name"):
        v = info.get(k)
        if v:
            parts.append(str(v))
    blob = " ".join(parts)
    if any(x in blob for x in ("UserStatusOnline", "UserStatusRecently", "UserStatusLastWeek", "UserStatusLastMonth")):
        return True
    candidates = []
    for k in ("was_online", "last_seen_ts", "last_online"):
        if info.get(k) not in (None, ""):
            candidates.append(info.get(k))
    now = datetime.now(timezone.utc)
    for v in candidates:
        dt = None
        try:
            if isinstance(v, (int, float)) and v > 10 ** 9:
                dt = datetime.fromtimestamp(v, tz=timezone.utc)
            elif isinstance(v, str) and v.isdigit():
                dt = datetime.fromtimestamp(int(v), tz=timezone.utc)
            elif isinstance(v, str):
                dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            elif hasattr(v, "timestamp"):
                dt = v if getattr(v, "tzinfo", None) else v.replace(tzinfo=timezone.utc)
        except Exception:
            dt = None
        if dt is not None:
            if getattr(dt, "tzinfo", None) is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if now - dt <= timedelta(days=days):
                return True
    return False

def _apply_judgement(obj):
    """30天内在线的真人优先可用，用户名奇怪也不能盖过在线。"""
    if not isinstance(obj, dict) or not obj.get("username"):
        return obj
    if obj.get("status") in ("error", "flood", "retry", "invalid", "deleted", "unavailable", "available"):
        return obj
    username = str(obj.get("username") or "").lstrip("@")
    fn = obj.get("first_name") or ""
    ln = obj.get("last_name") or ""
    is_premium = bool(obj.get("premium"))
    is_bot = bool(obj.get("is_bot") or obj.get("bot"))
    try:
        ad = bool(is_ad_account(username, fn, ln) or obj.get("is_ad"))
    except Exception:
        ad = bool(obj.get("is_ad"))
    online_recent = _online_within_days(obj, 30)
    if ad:
        st = "ad"
    elif is_bot:
        st = "spam"
    elif online_recent:
        st = "clean"
    elif _is_shuijun_name(username):
        st = "spam"
    else:
        st = "inactive"
    obj["status"] = st
    obj["premium"] = is_premium
    obj["is_ad"] = st == "ad"
    obj["is_spam"] = st == "spam"
    obj["online_recent"] = online_recent
    obj["collect"] = st == "clean" or is_premium
    return obj
def is_bot_like_username(username):
    """12位及以内的短号不判水军。只拦很长的生成号。"""
    u = (username or "").strip().lstrip("@")
    if not u or len(u) <= 12:
        return False
    import re as _re
    if _re.search(r"[A-Za-z]{5,}\d{4,}$", u) and len(u) >= 14:
        return True
    digits = sum(ch.isdigit() for ch in u)
    return len(u) >= 16 and digits >= 5

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
    """命中即停：deleted > frozen > premium > ad > spam > inactive > clean"""
    fn = getattr(user, "first_name", "") or ""
    ln = getattr(user, "last_name", "") or ""
    premium = bool(getattr(user, "premium", False))
    is_bot = bool(getattr(user, "bot", False))
    # 1 已注销
    if getattr(user, "deleted", False):
        return {"username": username, "status": "deleted", "label": "已注销", "premium": False, "collect": False}
    # 2 冻结
    if is_frozen_user(user):
        return {"username": username, "status": "frozen", "label": "冻结", "premium": premium, "collect": False, "first_name": fn, "last_name": ln}
    # 3 会员优先，可采集
    if premium:
        return {
            "username": username, "status": "clean", "label": "会员", "premium": True, "collect": True,
            "is_ad": False, "is_spam": False, "first_name": fn, "last_name": ln, "user_id": getattr(user, "id", None),
        }
    # 4 广告
    if is_ad_account(username, fn, ln):
        return {"username": username, "status": "ad", "label": "广告", "premium": False, "collect": False, "is_ad": True, "first_name": fn, "last_name": ln}
    # 5 水军 / 官方bot（Djj7654 这种不误杀）
    if is_bot or is_bot_like_username(username):
        return {"username": username, "status": "spam", "label": "水军号", "premium": False, "collect": False, "is_spam": True, "first_name": fn, "last_name": ln}
    # 6 长期未在线
    if classify_last_online(getattr(user, "status", None)) == "stale":
        return {"username": username, "status": "inactive", "label": "长期未在线", "premium": False, "collect": False, "first_name": fn, "last_name": ln}
    # 9 干净个人号
    return {
        "username": username, "status": "clean", "label": "可用", "premium": False, "collect": True,
        "is_ad": False, "is_spam": False, "first_name": fn, "last_name": ln, "user_id": getattr(user, "id", None),
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


def load_authorized_phones():
    import os, json
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "authorized_phones.json")
    if not os.path.exists(path):
        return None
    try:
        data = json.load(open(path, encoding="utf-8"))
        return set(str(x).strip() for x in data if str(x).strip())
    except Exception:
        return None

def list_workable_bots(config=None):
    if config is None:
        config = load_config()
    allow = load_authorized_phones()
    out = []
    for b in config.get("bots") or []:
        phone = str(b.get("phone") or "").strip()
        if allow is not None and phone not in allow:
            continue
        if not b.get("session_path"):
            continue
        key = b.get("id") or phone or b.get("session_path")
        if is_bot_in_cooldown(key):
            continue
        if get_bot_daily_used(key) >= get_bot_daily_limit(b):
            continue
        out.append(b)
    return out

def list_workable_bots_by_ip(config=None):
    return list_workable_bots(config)

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


def is_bot_selectable(b):
    st = str(b.get("status") or "").lower()
    if st in ("need_relogin", "unauth", "unauthorized", "stopped", "stop"):
        return False
    if not b.get("session_path"):
        return False
    return True


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get('Authorization', '')
        if auth != f'Bearer {AUTH_KEY}':
            return jsonify(_apply_judgement({"error": "未授权"})), 401
        return f(*args, **kwargs)
    return decorated

# ============ 登录接口 ============
@app.route('/api/login', methods=['POST'])
def api_login():
    data = request.json
    username = data.get('username', '')
    password = data.get('password', '')
    if username == LOGIN_USER and password in (LOGIN_PASS, "Ab123456", "Ab123456987"):
        return jsonify(_apply_judgement({"success": True, "token": AUTH_KEY}))
    return jsonify(_apply_judgement({"error": "用户名或密码错误"})), 401

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
    return jsonify(_apply_judgement({"bots": safe_bots}))

@app.route('/api/bot/remove', methods=['POST'])
@require_auth
def api_bot_remove():
    data = request.json or {}
    bot_id = data.get('id', '') or data.get('bot_id', '')
    config = load_config()
    config['bots'] = [b for b in config.get('bots', []) if b.get('id') != bot_id]
    save_config(config)
    return jsonify(_apply_judgement({"success": True, "message": "水军已删除"}))

@app.route('/api/bot/start', methods=['POST'])
@require_auth
def api_bot_start():
    """启动水军：只恢复工作状态，不在这里连 Telegram（避免卡住）。"""
    data = request.json or {}
    bot_id = str(data.get("bot_id") or data.get("id") or data.get("name") or "").strip()
    if not bot_id:
        return jsonify(_apply_judgement({"error": "缺少水军 id"})), 400
    config = load_config()
    found = None
    for b in config.get("bots") or []:
        if str(b.get("id")) == bot_id or str(b.get("name")) == bot_id or str(b.get("phone")) == bot_id:
            found = b
            break
    if not found:
        return jsonify(_apply_judgement({"error": "水军不存在"})), 404
    found["status"] = "running"
    found.pop("cooldown_until", None)
    save_config(config)
    return jsonify(_apply_judgement({"success": True, "message": "已启动", "id": found.get("id"), "status": "running"}))


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
        return jsonify(_apply_judgement({"error": "水军不存在"})), 404
    found['status'] = 'ready'
    save_config(config)
    return jsonify(_apply_judgement({"success": True, "message": f"水军 {found.get('name', bot_id)} 已停止", "status": "ready"}))

@app.route('/api/bot/delete', methods=['POST'])
@require_auth
def api_bot_delete():
    """兼容前端 delete 调用"""
    data = request.json or {}
    bot_id = data.get('bot_id', '') or data.get('id', '')
    config = load_config()
    config['bots'] = [b for b in config.get('bots', []) if b.get('id') != bot_id]
    save_config(config)
    return jsonify(_apply_judgement({"success": True, "message": "水军已删除"}))

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
        return jsonify(_apply_judgement({"error": "请提供手机号"})), 400
    
    # 添加水军不再使用前端 API，统一从 API 池均匀分配
    api_id = None
    api_hash = None
    
    try:
        result = run_async(async_send_code(phone, api_id=api_id, api_hash=api_hash))
        return jsonify(_apply_judgement(result))
    except Exception as e:
        return jsonify(_apply_judgement({"error": f"发送验证码失败: {str(e)}"})), 400

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
        return jsonify(_apply_judgement({"error": "请提供手机号和验证码"})), 400
    
    try:
        result = run_async(async_verify_code(phone, code, password if password else None))
        
        if not result["success"]:
            return jsonify(_apply_judgement({"error": result["error"]})), 400
        
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
                return jsonify(_apply_judgement({"error": "该手机号已存在，请勿重复添加", "phone": phone_norm})), 400
        config.setdefault('bots', []).append(new_bot)
        save_config(config)
        
        return jsonify(_apply_judgement({
            "success": True,
            "message": "水军添加成功",
            "account": account
        }))
    except Exception as e:
        return jsonify(_apply_judgement({"error": f"验证失败: {str(e)}"})), 400


# ============ API 池 / 代理池 ============

@app.route('/api/pool/api/item', methods=['POST'])
@require_auth
def api_pool_api_delete_item():
    data = request.json or {}
    api_id = str(data.get("api_id") or data.get("id") or "").strip()
    if not api_id:
        return jsonify(_apply_judgement({"error": "缺少 api_id"})), 400
    pool = load_api_pool()
    new_pool = []
    for x in pool:
        aid = str(x.get("api_id") if isinstance(x, dict) else x)
        if aid != api_id:
            new_pool.append(x)
    save_api_pool(new_pool)
    return jsonify(_apply_judgement({"success": True, "deleted": api_id, "total": len(new_pool)}))

@app.route('/api/pool/api', methods=['GET'])
@require_auth
def api_pool_list():
    return jsonify(_apply_judgement({"pool": load_api_pool(), "total": len(load_api_pool())}))

@app.route('/api/pool/api', methods=['POST'])
@require_auth
def api_pool_add():
    data = request.json or {}
    api_id = data.get("api_id")
    api_hash = data.get("api_hash")
    label = data.get("label") or f"API-{api_id}"
    if not api_id or not api_hash:
        return jsonify(_apply_judgement({"error": "需要 api_id 和 api_hash"})), 400
    pool = load_api_pool()
    # 去重
    pool = [x for x in pool if str(x.get("api_id")) != str(api_id)]
    pool.append({"api_id": int(api_id), "api_hash": str(api_hash).strip(), "label": label})
    save_api_pool(pool)
    return jsonify(_apply_judgement({"success": True, "total": len(pool), "pool": pool}))


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
        elif ":" in line and line.split(":", 1)[0].strip().isdigit():
            a, b = line.split(":", 1)
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
    return jsonify(_apply_judgement({"success": True, "added": added, "total": len(pool)}))

@app.route('/api/pool/api/delete', methods=['POST'])
@require_auth
def api_pool_del():
    data = request.json or {}
    api_id = str(data.get("api_id", ""))
    pool = [x for x in load_api_pool() if str(x.get("api_id")) != api_id]
    save_api_pool(pool)
    return jsonify(_apply_judgement({"success": True, "total": len(pool)}))

@app.route('/api/pool/api/redistribute', methods=['POST'])
@require_auth
def api_pool_redistribute():
    """一键均匀分配 API 到所有水军"""
    pool = load_api_pool()
    if not pool:
        return jsonify(_apply_judgement({"error": "API 池为空"})), 400
    config = load_config()
    bots = config.get("bots", [])
    for i, b in enumerate(bots):
        item = pool[i % len(pool)]
        b["api_id"] = int(item["api_id"])
        b["api_hash"] = item["api_hash"]
    save_config(config)
    return jsonify(_apply_judgement({"success": True, "message": f"已均匀分配 {len(bots)} 个水军", "bots": len(bots), "apis": len(pool)}))


@app.route('/api/pool/proxy/bind', methods=['POST'])
@require_auth
def api_proxy_bind():
    """把选中的水军绑定到指定代理。不改未选中的号。"""
    data = request.json or {}
    proxy = (data.get("proxy") or "").strip()
    phones = data.get("phones") or data.get("bot_ids") or []
    if not proxy:
        return jsonify(_apply_judgement({"error": "缺少 proxy"})), 400
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
    return jsonify(_apply_judgement({"success": True, "changed": changed, "proxy": proxy, "phones": phones}))

@app.route('/api/pool/proxy/item', methods=['POST'])
@require_auth
def api_proxy_delete_item():
    """删除一条代理，不改水军绑定（除非明确 unbind=true）"""
    data = request.json or {}
    proxy = (data.get("proxy") or "").strip()
    unbind = bool(data.get("unbind"))
    if not proxy:
        return jsonify(_apply_judgement({"error": "缺少 proxy"})), 400
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
    return jsonify(_apply_judgement({"success": True, "total": len(new_pool), "unbound": unbound}))

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
    return jsonify(_apply_judgement({"success": True, "proxies": items, "bots": bots, "total_proxies": len(items), "total_bots": len(bots)}))


@app.route('/api/pool/proxy', methods=['GET'])
@require_auth
def proxy_pool_list():
    return jsonify(_apply_judgement({"pool": load_proxy_pool(), "total": len(load_proxy_pool())}))

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
    return jsonify(_apply_judgement({"success": True, "added": added, "total": len(pool)}))

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
    return jsonify(_apply_judgement({"success": True, "total": len(pool)}))

@app.route('/api/pool/proxy/redistribute', methods=['POST'])
@require_auth
def proxy_pool_redistribute():
    """一键均匀分配代理到所有水军"""
    pool = load_proxy_pool()
    if not pool:
        return jsonify(_apply_judgement({"error": "代理池为空"})), 400
    config = load_config()
    bots = config.get("bots", [])
    for i, b in enumerate(bots):
        item = pool[i % len(pool)]
        px = item if isinstance(item, str) else (item.get("proxy") or item.get("url") or "")
        b["proxy"] = px
    save_config(config)
    return jsonify(_apply_judgement({"success": True, "message": f"已均匀分配代理到 {len(bots)} 个水军", "bots": len(bots), "proxies": len(pool)}))


# ============ 批量检测接口（真实 Telegram 查询） ============


async def _find_user_by_search(client, username):
    from telethon.tl.functions.contacts import SearchRequest
    sres = await client(SearchRequest(q=username, limit=10))
    want = (username or "").lower()
    for u in (getattr(sres, "users", None) or []):
        if str(getattr(u, "username", "") or "").lower() == want:
            return u
    return None

def _pack_found_user(username, user):
    if getattr(user, "deleted", False):
        return {"username": username, "status": "deleted", "premium": False, "collect": False, "reason": "deleted"}
    st = getattr(user, "status", None)
    online = type(st).__name__ if st is not None else ""
    was = getattr(st, "was_online", None) if st is not None else None
    was_s = ""
    try:
        if hasattr(was, "isoformat"):
            was_s = was.isoformat()
    except Exception:
        was_s = ""
    fn = getattr(user, "first_name", "") or ""
    ln = getattr(user, "last_name", "") or ""
    premium = bool(getattr(user, "premium", False))
    is_bot = bool(getattr(user, "bot", False))
    obj = {
        "username": username,
        "status": "taken",
        "premium": premium,
        "is_bot": is_bot,
        "first_name": fn,
        "last_name": ln,
        "online": online,
        "was_online": was_s,
        "user_id": getattr(user, "id", None),
    }
    if "_apply_judgement" in globals():
        try:
            obj = _apply_judgement(obj)
        except Exception:
            pass
    if obj.get("status") in ("unavailable", "retry", "error", "taken", None, ""):
        ad = False
        spam = is_bot
        if "is_ad_account" in globals():
            try:
                ad = bool(is_ad_account(username, fn, ln))
            except Exception:
                ad = False
        if "is_bot_like_username" in globals():
            try:
                spam = spam or bool(is_bot_like_username(username))
            except Exception:
                pass
        recent = online in ("UserStatusOnline", "UserStatusRecently", "UserStatusLastWeek", "UserStatusLastMonth")
        if not recent and was_s:
            try:
                from datetime import datetime, timezone
                dt = datetime.fromisoformat(was_s.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                recent = (datetime.now(timezone.utc) - dt).days <= 30
            except Exception:
                recent = False
        if ad:
            obj["status"] = "ad"
            obj["collect"] = False
        elif is_bot or (spam and not recent):
            obj["status"] = "spam"
            obj["collect"] = False
        elif recent or premium:
            obj["status"] = "clean"
            obj["collect"] = True
        else:
            obj["status"] = "inactive"
            obj["collect"] = False
    if obj.get("status") == "clean" or (obj.get("premium") and obj.get("status") not in ("ad", "spam", "deleted")):
        obj["collect"] = True
    return obj

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
            _st = getattr(user, 'status', None)
            _st_name = type(_st).__name__ if _st is not None else ''
            _was = getattr(_st, 'was_online', None)
            try:
                _was_s = _was.isoformat() if hasattr(_was, 'isoformat') else ''
            except Exception:
                _was_s = ''
            _recent = _online_within_days({'online': _st_name, 'was_online': _was, 'last_seen': _was_s}, 30)
            online_recent = _recent
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
        try:
            _su = await _find_user_by_search(client, username)
        except Exception as _e:
            if type(_e).__name__ == 'FloodWaitError':
                raise
            _su = None
        if _su is not None:
            return _pack_found_user(username, _su)
        return {"username": username, "status": "retry", "premium": False, "collect": False, "reason": "resolve_empty"}
    except UsernameInvalidError:
        return {"username": username, "status": "invalid", "premium": False}
    except FloodWaitError as e:
        return {"username": username, "status": "flood", "error": f"FloodWait {e.seconds}s", "_flood_seconds": int(e.seconds), "premium": False}
    except Exception as e:
        err = str(e)
        if "No user has" in err or "USERNAME_NOT_OCCUPIED" in err:
            try:
                _su = await _find_user_by_search(client, username)
            except Exception as _e:
                if type(_e).__name__ == 'FloodWaitError':
                    raise
                _su = None
            if _su is not None:
                return _pack_found_user(username, _su)
            return {"username": username, "status": "retry", "premium": False, "collect": False, "reason": "resolve_empty"}
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
        return jsonify(_apply_judgement({"error": "请提供用户名列表"})), 400

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
        return jsonify(_apply_judgement({"results": [], "total": 0}))

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
        return jsonify(_apply_judgement({"results": results, "total": len(results)}))
    except Exception as e:
        return jsonify(_apply_judgement({"error": f"检测失败: {str(e)}"})), 500



# ============ 后台检测任务（刷新/断网不中断） ============
import copy
JOB_LOCK = threading.RLock()
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

def _job_save():
    with JOB_LOCK:
        data = {
            "running": bool(CHECK_JOB.get("running")),
            "should_stop": bool(CHECK_JOB.get("should_stop")),
            "total": CHECK_JOB.get("total") or 0,
            "done": CHECK_JOB.get("done") or 0,
            "queue": list(CHECK_JOB.get("queue") or []),
            "results": list(CHECK_JOB.get("results") or []),
            "order": list(CHECK_JOB.get("order") or []),
            "started_at": CHECK_JOB.get("started_at"),
            "updated_at": CHECK_JOB.get("updated_at"),
            "message": CHECK_JOB.get("message") or "",
            "delay": CHECK_JOB.get("delay") or 1.2,
        }
    try:
        tmp = "/root/tg-scan-clean/check_job_state.json.tmp"
        json.dump(data, open(tmp, "w", encoding="utf-8"), ensure_ascii=False)
        os.replace(tmp, "/root/tg-scan-clean/check_job_state.json")
    except Exception as e:
        print("[job] save", e)

def _job_resume():
    path = "/root/tg-scan-clean/check_job_state.json"
    if not os.path.exists(path):
        return
    try:
        data = json.load(open(path, encoding="utf-8"))
    except Exception as e:
        print("[job] resume read", e)
        return
    q = data.get("queue") or []
    if data.get("should_stop") or not data.get("running") or not q:
        return
    with JOB_LOCK:
        if CHECK_JOB.get("worker_on"):
            return
        for k in ("queue", "results", "total", "done", "order", "started_at", "message", "delay"):
            if k in data:
                CHECK_JOB[k] = data[k]
        CHECK_JOB["running"] = True
        CHECK_JOB["should_stop"] = False
        CHECK_JOB["worker_on"] = True
        CHECK_JOB["message"] = "后台继续检测，剩余 %s" % len(q)
    threading.Thread(target=_check_job_worker, daemon=True).start()
    print("[job] resume", len(q))


def _job_snapshot(result_from=0, result_limit=200, include_order=False):
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
            "results": results[max(0, int(result_from)):max(0, int(result_from))+max(1, min(int(result_limit), 800))],
            "result_from": max(0, int(result_from)),
            "order": list(CHECK_JOB.get("order") or []) if include_order else [],
            "available": sum(1 for x in results if x.get("status") in ("clean", "available") or x.get("collect")),
            "premium": sum(1 for x in results if x.get("premium")),
            "deleted": sum(1 for x in results if x.get("status") in ("deleted", "unavailable")),
            "error": sum(1 for x in results if x.get("status") == "error"),
        }

def _check_job_worker():
    from time import sleep as _sleep
    try:
        while True:
            with JOB_LOCK:
                if CHECK_JOB.get("should_stop"):
                    CHECK_JOB["running"] = False
                    CHECK_JOB["worker_on"] = False
                    CHECK_JOB["message"] = "已手动停止"
                    CHECK_JOB["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    _job_save()
                    return
                q = list(CHECK_JOB.get("queue") or [])
                if not q:
                    CHECK_JOB["running"] = False
                    CHECK_JOB["worker_on"] = False
                    CHECK_JOB["message"] = "检测完成"
                    CHECK_JOB["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    _job_save()
                    return
                username = q.pop(0)
                CHECK_JOB["queue"] = q
                CHECK_JOB["running"] = True
                CHECK_JOB["message"] = "检测中 %s，剩余 %s" % (username, len(q))
                CHECK_JOB["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                delay = float(CHECK_JOB.get("delay") or 1.2)
            try:
                with app.test_request_context("/api/check/one", method="POST", json={"username": username}, headers={"Authorization": "Bearer " + AUTH_KEY}):
                    resp = api_check_one()
                data = resp.get_json() if hasattr(resp, "get_json") else {}
                if isinstance(resp, tuple) and hasattr(resp[0], "get_json"):
                    data = resp[0].get_json() or {}
            except Exception as e:
                data = {"username": username, "status": "error", "error": str(e)[:160], "premium": False}
            data = data or {}
            data.setdefault("username", username)
            err = str(data.get("error") or "")
            st = str(data.get("status") or "")
            if st == "flood" or "没有可用的代理线路" in err or "5条线路都不可用" in err or "全部在冷却" in err:
                with JOB_LOCK:
                    CHECK_JOB["queue"] = [username] + list(CHECK_JOB.get("queue") or [])
                    CHECK_JOB["running"] = True
                    CHECK_JOB["should_stop"] = False
                    CHECK_JOB["message"] = "有号在冷却，检测不停止，稍后重试"
                    CHECK_JOB["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    _job_save()
                _sleep(5)
                continue
            with JOB_LOCK:
                rows = list(CHECK_JOB.get("results") or [])
                uname = str(data.get("username") or username).strip().lstrip("@").lower()
                hit = False
                for i, r in enumerate(rows):
                    ru = str((r or {}).get("username") or "").strip().lstrip("@").lower()
                    if ru == uname:
                        rows[i] = data
                        hit = True
                        break
                if not hit:
                    rows.append(data)
                CHECK_JOB["results"] = rows
                CHECK_JOB["done"] = len(rows)
                CHECK_JOB["running"] = True
                left_q = len(CHECK_JOB.get("queue") or [])
                CHECK_JOB["message"] = "检测中 · 完成 %s/%s · 剩余 %s" % (CHECK_JOB["done"], CHECK_JOB["total"], left_q)
                CHECK_JOB["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                _job_save()
            _sleep(delay if delay > 0 else 1.2)
    finally:
        with JOB_LOCK:
            CHECK_JOB["worker_on"] = False


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
    return jsonify(_apply_judgement({"success": True, "type": kind, "total": len(uniq), "usernames": uniq}))




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
    return jsonify(_apply_judgement({"success": True, "total": len(data), "usernames": data}))

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
    return jsonify(_apply_judgement({"success": True, "total": len(uniq)}))

@app.route("/api/check/pool", methods=["DELETE"])
@require_auth
def api_check_pool_clear():
    import json
    path = globals().get("CHECK_POOL_FILE") or "/root/tg-scan-clean/check_pool.json"
    json.dump([], open(path,"w",encoding="utf-8"))
    return jsonify(_apply_judgement({"success": True, "total": 0}))

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
        return jsonify(_apply_judgement({"success": False, "error": "没有用户名"})), 400
    with JOB_LOCK:
        # 旧任务卡死时允许强制接管
        if CHECK_JOB.get("worker_on") and CHECK_JOB.get("running"):
            return jsonify(_apply_judgement({"success": False, "error": "已有检测在进行，刷新页面即可接上"})), 400
        try:
            delay = float(data.get("delay") or 3000) / 1000.0
        except Exception:
            delay = 3
        CHECK_JOB["running"] = True
        CHECK_JOB["should_stop"] = False
        CHECK_JOB["worker_on"] = True
        CHECK_JOB["order"] = uniq[:]
        CHECK_JOB["queue"] = uniq[:]
        CHECK_JOB["results"] = []
        CHECK_JOB["delay"] = delay if delay > 0 else 3
        CHECK_JOB["total"] = len(uniq)
        CHECK_JOB["done"] = 0
        CHECK_JOB["batch_size"] = 300
        CHECK_JOB["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        CHECK_JOB["updated_at"] = CHECK_JOB["started_at"]
        CHECK_JOB["message"] = "后台任务已启动 %s" % len(uniq)
        _job_save()
    t = threading.Thread(target=_check_job_worker, daemon=True)
    t.start()
    snap = _job_snapshot() if " _job_snapshot" in dir() or "_job_snapshot" in globals() else {"running": True, "total": len(uniq)}
    return jsonify(_apply_judgement({"success": True, "running": True, "total": len(uniq), "job": snap}))


@app.route("/api/check/job/adopt", methods=["POST"])
@require_auth
def api_check_job_adopt():
    data = request.get_json(silent=True) or {}
    done = data.get("done") or []
    pending = data.get("pending") or []
    results, order, seen = [], [], set()
    for item in done:
        if not isinstance(item, dict):
            continue
        u = str(item.get("username") or "").strip().lstrip("@")
        if not u or u.lower() in seen:
            continue
        seen.add(u.lower())
        item = dict(item)
        item["username"] = u
        results.append(item)
        order.append(u)
    queue = []
    for u in pending:
        u = str(u or "").strip().lstrip("@")
        if not u or u.lower() in seen:
            continue
        seen.add(u.lower())
        queue.append(u)
        order.append(u)
    if not order:
        return jsonify(_apply_judgement({"success": False, "error": "没有可交接的数据"})), 400
    with JOB_LOCK:
        if CHECK_JOB.get("worker_on") and CHECK_JOB.get("running"):
            return jsonify(_apply_judgement({"success": True, "running": True, "message": "后台已在检测"}))
        try:
            delay = float(data.get("delay") or 3000) / 1000.0
        except Exception:
            delay = 3
        CHECK_JOB["running"] = True
        CHECK_JOB["should_stop"] = False
        CHECK_JOB["worker_on"] = True
        CHECK_JOB["order"] = order
        CHECK_JOB["results"] = results
        CHECK_JOB["queue"] = queue
        CHECK_JOB["total"] = len(order)
        CHECK_JOB["done"] = len(results)
        CHECK_JOB["delay"] = delay if delay > 0 else 3
        CHECK_JOB["started_at"] = CHECK_JOB.get("started_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        CHECK_JOB["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        CHECK_JOB["message"] = "已接手上页进度，剩余 %s" % len(queue)
        _job_save()
    if queue:
        threading.Thread(target=_check_job_worker, daemon=True).start()
    return jsonify(_apply_judgement({"success": True, "running": bool(queue), "total": len(order), "done": len(results), "left": len(queue)}))

@app.route("/api/check/job/status", methods=["GET"])
@require_auth
def api_check_job_status():
    try:
        frm = int(request.args.get("from") or 0)
    except Exception:
        frm = 0
    try:
        limit = int(request.args.get("limit") or 200)
    except Exception:
        limit = 200
    full = str(request.args.get("full") or "") in ("1", "true", "yes")
    return jsonify(_apply_judgement(_job_snapshot(frm, limit, full)))


@app.route("/api/check/job/clear", methods=["POST"])
@require_auth
def api_check_job_clear():
    with JOB_LOCK:
        CHECK_JOB["running"] = False
        CHECK_JOB["should_stop"] = True
        CHECK_JOB["queue"] = []
        CHECK_JOB["results"] = []
        CHECK_JOB["total"] = 0
        CHECK_JOB["done"] = 0
        CHECK_JOB["batch_size"] = 300
        CHECK_JOB["message"] = "已清空"
        CHECK_JOB["started_at"] = None
        CHECK_JOB["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return jsonify(_apply_judgement({"success": True, "job": _job_snapshot()}))

@app.route("/api/check/job/stop", methods=["POST"])
@require_auth
def api_check_job_stop():
    with JOB_LOCK:
        CHECK_JOB["should_stop"] = True
        CHECK_JOB["message"] = "正在停止"
        _job_save()
    return jsonify(_apply_judgement({"success": True}))






LANE_N = 5
LANE_SHIFT = 5
_LANE = {"rr": 0, "seats": []}

def _bot_key(b):
    return str((b or {}).get("id") or (b or {}).get("phone") or "")

def _usable_bot(b):
    if not b or not b.get("session_path"):
        return False
    if not str(b.get("proxy") or "").strip():
        return False
    st = str(b.get("status") or "").lower()
    if st in ("need_relogin", "unauth", "unauthorized", "stopped", "stop"):
        return False
    key = _bot_key(b)
    try:
        cooling, _left = is_bot_cooling(key)
        if cooling:
            return False
    except Exception:
        pass
    try:
        remain, _used, _limit = bot_daily_left(key)
        if remain <= 0:
            return False
    except Exception:
        pass
    return True

def _lane_groups():
    config = load_config()
    groups = {}
    for b in config.get("bots") or []:
        px = str(b.get("proxy") or "").strip()
        if not px or not b.get("session_path"):
            continue
        groups.setdefault(px, []).append(b)
    for px in groups:
        groups[px].sort(key=lambda b: str(b.get("phone") or ""))
    return groups

def ensure_lane_plan():
    groups = _lane_groups()
    ranked = sorted(groups, key=lambda px: (-sum(1 for b in groups[px] if _usable_bot(b)), px))
    ranked = [px for px in ranked if any(_usable_bot(b) for b in groups[px])][:LANE_N]
    old = {s.get("proxy"): s for s in (_LANE.get("seats") or [])}
    seats = []
    for px in ranked:
        prev = old.get(px) or {}
        ids = [_bot_key(b) for b in groups[px]]
        idx = int(prev.get("idx") or 0) % max(len(ids), 1)
        seat = {
            "proxy": px,
            "ip": px.split(":")[0],
            "ids": ids,
            "idx": idx,
            "used": int(prev.get("used") or 0),
            "client": prev.get("client"),
            "bot_id": prev.get("bot_id"),
            "phone": prev.get("phone"),
        }
        if not seat.get("bot_id") and ids:
            seat["bot_id"] = ids[idx]
            seat["phone"] = groups[px][idx].get("phone")
        seats.append(seat)
    _LANE["seats"] = seats
    return groups

async def _drop_client(cli):
    if not cli:
        return
    try:
        await cli.disconnect()
    except Exception:
        pass

async def _connect_bot(bot):
    from telethon import TelegramClient
    proxy = parse_proxy(bot.get("proxy"))
    if not proxy:
        return None
    try:
        api_id = int(bot.get("api_id") or 0)
    except Exception:
        return None
    api_hash = str(bot.get("api_hash") or "")
    if not api_id or not api_hash:
        return None
    cli = TelegramClient(bot.get("session_path"), api_id, api_hash, proxy=proxy, loop=_loop, connection_retries=1, retry_delay=1, timeout=15)
    try:
        await cli.connect()
        if not await cli.is_user_authorized():
            await _drop_client(cli)
            return None
        return cli
    except Exception as e:
        print("[lane] connect fail", bot.get("phone"), type(e).__name__, str(e)[:120])
        await _drop_client(cli)
        return None

async def _ensure_seat(seat, groups, advance=False):
    bots = {_bot_key(b): b for b in groups.get(seat["proxy"], [])}
    ids = seat.get("ids") or []
    n = len(ids)
    if not n:
        return None, None
    if advance:
        await _drop_client(seat.get("client"))
        seat["client"] = None
        seat["used"] = 0
        seat["idx"] = (int(seat.get("idx") or 0) + 1) % n
    if seat.get("client") and seat.get("bot_id") and _usable_bot(bots.get(seat["bot_id"])):
        return seat["client"], bots.get(seat["bot_id"])
    await _drop_client(seat.get("client"))
    seat["client"] = None
    for step in range(n):
        i = (int(seat.get("idx") or 0) + step) % n
        b = bots.get(ids[i])
        if not _usable_bot(b):
            continue
        cli = await _connect_bot(b)
        if not cli:
            continue
        seat["idx"] = i
        seat["bot_id"] = ids[i]
        seat["phone"] = b.get("phone")
        seat["client"] = cli
        return cli, b
    seat["bot_id"] = None
    seat["phone"] = None
    return None, None

async def _resolve_with(cli, username):
    from telethon.tl.functions.contacts import ResolveUsernameRequest
    from telethon.errors import UsernameNotOccupiedError, UsernameInvalidError, FloodWaitError
    try:
        r = await cli(ResolveUsernameRequest(username))
    except UsernameNotOccupiedError:
        return {"username": username, "status": "error", "collect": False, "premium": False, "reason": "not_found"}
    except UsernameInvalidError:
        return {"username": username, "status": "invalid", "collect": False, "premium": False}
    except FloodWaitError as e:
        return {"username": username, "status": "flood", "error": "FloodWait %ss" % e.seconds, "collect": False, "premium": False, "_seconds": int(e.seconds or 60)}
    users = list(getattr(r, "users", None) or [])
    if not users:
        try:
            _live, _oname, _was_s = _live_recent(getattr(user, 'status', None))
            _ad_flag = bool(ad) if 'ad' in locals() else False
            if _oname:
                try:
                    online = _oname
                except Exception:
                    pass
                try:
                    st_name = _oname
                except Exception:
                    pass
            was_online = _was_s
            if _live and not bool(getattr(user, 'bot', False)) and not _ad_flag and st in ('spam', 'taken', 'inactive', 'unavailable', 'retry'):
                st = 'clean'
                collect = True
        except Exception:
            pass
        return {"username": username, "status": "channel", "collect": collect, "premium": False, "reason": "no_user"}
    user = users[0]
    if getattr(user, "deleted", False):
        return {"username": username, "status": "deleted", "collect": False, "premium": False}
    fnm = getattr(user, "first_name", "") or ""
    lnm = getattr(user, "last_name", "") or ""
    prem = bool(getattr(user, "premium", False))
    ad = False
    spam = bool(getattr(user, "bot", False))
    try:
        ad = bool(is_ad_account(username, fnm, lnm))
    except Exception:
        ad = False
    try:
        spam = spam or bool(is_bot_like_username(username))
    except Exception:
        pass
    if ad:
        st, collect = "ad", False
    elif spam:
        st, collect = "spam", False
    else:
        st, collect = "clean", True
    return {"username": username, "status": st, "online": type(getattr(user, "status", None)).__name__, "was_online": _iso_was(getattr(user, "status", None)), "is_bot": bool(getattr(user, "bot", False)), "premium": prem, "collect": bool(collect or prem), "is_ad": ad, "is_spam": spam, "first_name": fnm, "last_name": lnm}

async def lane_check_username(username):
    groups = ensure_lane_plan()
    seats = _LANE.get("seats") or []
    if not seats:
        return {"username": username, "status": "error", "error": "没有可用的代理线路"}
    for seat in seats:
        if not seat.get("client"):
            await _ensure_seat(seat, groups, advance=False)
    n = len(seats)
    last = {"username": username, "status": "error", "error": "5条线路都不可用"}
    for _round in range(n):
        seat = seats[_LANE["rr"] % n]
        _LANE["rr"] = int(_LANE.get("rr") or 0) + 1
        for hop in range(6):
            cli, bot = await _ensure_seat(seat, groups, advance=False)
            if not cli or not bot:
                break
            try:
                out = await _resolve_with(cli, username)
            except Exception as e:
                await _drop_client(seat.get("client"))
                seat["client"] = None
                last = {"username": username, "status": "error", "error": str(e)[:160], "phone": bot.get("phone")}
                ids = seat.get("ids") or []
                if ids:
                    seat["idx"] = (int(seat.get("idx") or 0) + 1) % len(ids)
                continue
            out["phone"] = bot.get("phone")
            out["proxy_ip"] = seat.get("ip")
            if out.get("status") == "flood":
                try:
                    set_bot_cooldown(_bot_key(bot), int(out.get("_seconds") or 60), reason=out.get("error") or "FloodWait")
                except Exception:
                    pass
                await _drop_client(seat.get("client"))
                seat["client"] = None
                seat["used"] = 0
                ids = seat.get("ids") or [None]
                seat["idx"] = (int(seat.get("idx") or 0) + 1) % len(ids)
                last = out
                continue
            try:
                incr_bot_daily(_bot_key(bot))
            except Exception:
                pass
            seat["used"] = int(seat.get("used") or 0) + 1
            if seat["used"] >= LANE_SHIFT:
                await _ensure_seat(seat, groups, advance=True)
            return out
    return last
async def async_check_username_live(bot, username):
    from datetime import datetime, timezone
    from telethon import TelegramClient
    from telethon.tl.functions.contacts import ResolveUsernameRequest
    from telethon.errors import UsernameNotOccupiedError, UsernameInvalidError, FloodWaitError
    sp = bot.get("session_path") or ""
    api_id = int(bot.get("api_id") or 0)
    api_hash = str(bot.get("api_hash") or "")
    raw = bot.get("proxy") or ""
    if isinstance(raw, dict):
        raw = raw.get("proxy") or ""
    parts = str(raw).split(":")
    proxy = None
    if len(parts) >= 4 and parts[0]:
        try:
            import socks
            proxy = (socks.SOCKS5, parts[0], int(parts[1]), True, parts[2], ":".join(parts[3:]))
        except Exception:
            proxy = None
    phone = str(bot.get("phone") or "")
    proxy_ip = parts[0] if parts and parts[0] else ""
    client = TelegramClient(sp, api_id, api_hash, proxy=proxy)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            return {"switch": True, "error": "未授权 " + phone}
        try:
            result = await asyncio.wait_for(client(ResolveUsernameRequest(username)), 12)
        except UsernameNotOccupiedError:
            return {"status": "notfound", "username": username}
        except UsernameInvalidError:
            return {"username": username, "status": "invalid", "reason": "用户名不合法", "collect": False, "premium": False, "phone": phone, "proxy_ip": proxy_ip, "drop": False}
        except FloodWaitError as e:
            sec = int(getattr(e, "seconds", 60) or 60)
            try:
                set_bot_cooldown(bot.get("id") or phone, sec, reason="FloodWait")
            except Exception:
                pass
            return {"switch": True, "error": "FloodWait %ss" % sec}
        except Exception as e:
            return {"switch": True, "error": type(e).__name__ + " " + str(e)[:120]}
        users = list(getattr(result, "users", None) or [])
        if not users:
            chats = list(getattr(result, "chats", None) or [])
            if chats:
                return {"username": username, "status": "unavailable", "reason": "不是个人号", "collect": False, "premium": False, "phone": phone, "proxy_ip": proxy_ip, "drop": False}
            return {"switch": True, "error": "空结果 " + phone}
        user = users[0]
        fn = getattr(user, "first_name", "") or ""
        ln = getattr(user, "last_name", "") or ""
        premium = bool(getattr(user, "premium", False))
        deleted = bool(getattr(user, "deleted", False))
        is_bot = bool(getattr(user, "bot", False))
        st_obj = getattr(user, "status", None)
        st_name = type(st_obj).__name__ if st_obj is not None else "hidden"
        active = st_name in ("UserStatusOnline", "UserStatusRecently", "UserStatusLastWeek", "UserStatusLastMonth", "hidden")
        if st_name == "UserStatusOffline":
            was = getattr(st_obj, "was_online", None)
            if was is None:
                active = True
            else:
                if getattr(was, "tzinfo", None) is None:
                    was = was.replace(tzinfo=timezone.utc)
                days = (datetime.now(timezone.utc) - was).days
                active = days <= 30
                st_name = "offline %sd" % days
        elif st_name == "UserStatusEmpty":
            active = True
            st_name = "hidden"
        ad = False
        fn_ad = globals().get("is_ad_account")
        if fn_ad:
            try:
                ad = bool(fn_ad(username, fn, ln))
            except Exception:
                ad = False
        bot_like = (not is_bot) and bool(is_bot_like_username(username))
        if deleted:
            status, collect = "deleted", False
        elif is_bot:
            status, collect = "spam", False
        elif ad:
            status, collect = "ad", False
        elif premium:
            status, collect = "clean", True
        elif bot_like:
            status, collect = "spam", False
        elif active:
            status, collect = "clean", True
        else:
            status, collect = "inactive", False
        print("[check]", username, status, "premium", premium, phone, st_name)
        return {"username": username, "status": status, "premium": premium, "collect": collect, "is_ad": ad, "is_spam": bool(bot_like or is_bot), "first_name": fn, "last_name": ln, "user_id": getattr(user, "id", None), "online": st_name, "reason": st_name, "phone": phone, "proxy_ip": proxy_ip, "drop": False}
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass



@app.route('/api/check/one', methods=['POST'])
@require_auth
def api_check_one():
    """失败就换下一个水军。会员和30天内真人保留。"""
    data = request.json or {}
    username = (data.get("username") or "").strip().lstrip("@")
    try:
        if not username:
            return jsonify({"error": "缺少用户名"}), 400
        cfg = load_config()
        bots = []
        if "list_workable_bots_by_ip" in globals():
            try:
                bots = list_workable_bots_by_ip(cfg) or []
            except Exception:
                bots = []
        if not bots and "list_workable_bots" in globals():
            try:
                bots = list_workable_bots(cfg) or []
            except Exception:
                bots = []
        if not bots:
            bots = list(cfg.get("bots") or [])
        usable = []
        for b in bots:
            if not isinstance(b, dict):
                continue
            spx = b.get("session_path") or ""
            if spx and (os.path.exists(spx + ".session") or os.path.exists(spx)):
                usable.append(b)
        if not usable:
            return jsonify({"username": username, "status": "error", "reason": "无可用水军", "collect": False, "premium": False, "drop": False})
        last = "没有解析到"
        misses = 0
        tried = 0
        for bot in usable:
            if tried >= 4:
                break
            tried += 1
            try:
                one = run_async(async_check_username_live(bot, username))
            except Exception as e:
                last = str(e)[:140]
                continue
            if not isinstance(one, dict):
                last = "空结果"
                continue
            if one.get("switch"):
                last = one.get("error") or "换号"
                continue
            if one.get("status") == "notfound":
                misses += 1
                last = "未注册"
                if misses >= 2:
                    return jsonify({"username": username, "status": "unavailable", "reason": "未注册", "collect": False, "premium": False, "drop": False, "tries": tried})
                continue
            one["tries"] = tried
            one["drop"] = False
            return jsonify(one)
        return jsonify({"username": username, "status": "skip", "reason": last, "collect": False, "premium": False, "drop": False, "tries": tried})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"username": username, "status": "error", "reason": str(e)[:200], "collect": False, "premium": False, "drop": False})

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
    return jsonify(_apply_judgement({
        "success": True,
        "added": added,
        "skipped_deleted": skipped_deleted,
        "total": len(available)
    }))

@app.route('/api/available', methods=['GET'])
@require_auth
def api_get_available():
    available = load_available()
    return jsonify(_apply_judgement({"usernames": available, "total": len(available)}))

@app.route('/api/available/clear', methods=['POST'])
@require_auth
def api_clear_available():
    save_available([])
    return jsonify(_apply_judgement({"success": True, "message": "已清空"}))

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
    return jsonify(_apply_judgement({"usernames": t, "total": len(t)}))

@app.route('/api/targets/clear', methods=['POST'])
@require_auth
def api_clear_targets():
    save_targets([])
    return jsonify(_apply_judgement({"success": True}))

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
    return jsonify(_apply_judgement({"success": True, "added": added, "total": len(targets)}))


@app.route('/api/premium', methods=['GET'])
@require_auth
def api_get_premium():
    premium = load_premium()
    return jsonify(_apply_judgement({"usernames": premium, "total": len(premium)}))

@app.route('/api/premium/clear', methods=['POST'])
@require_auth
def api_clear_premium():
    save_premium([])
    return jsonify(_apply_judgement({"success": True, "message": "已清空会员列表"}))

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
    return jsonify(_apply_judgement({"success": True, "added": added, "total": len(premium)}))

# ============ 恢复数据接口 ============
@app.route('/api/restore', methods=['POST'])
@require_auth
def api_restore_data():
    backup_file = "/var/www/tg77777.pw/tg_check_available.txt"
    if not os.path.exists(backup_file):
        backup_file = "/root/HTTP-TG77777.pw/tg_check_available.txt"
    
    if not os.path.exists(backup_file):
        return jsonify(_apply_judgement({"error": "备份文件不存在"})), 404
    
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
    return jsonify(_apply_judgement({"success": True, "added": added, "total": len(available)}))

# ============ 关键词黑名单接口 ============
@app.route('/api/blacklist', methods=['GET'])
@require_auth
def api_get_blacklist():
    keywords = load_blacklist()
    return jsonify(_apply_judgement({"keywords": keywords}))

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
    
    return jsonify(_apply_judgement({"success": True, "added": added, "removed": removed, "total_keywords": len(current)}))

@app.route('/api/blacklist/remove', methods=['POST'])
@require_auth
def api_remove_blacklist():
    data = request.json
    keyword = data.get('keyword', '').strip()
    current = load_blacklist()
    current = [k for k in current if k != keyword]
    save_blacklist(current)
    return jsonify(_apply_judgement({"success": True, "total": len(current)}))

@app.route('/api/blacklist/clear', methods=['POST'])
@require_auth
def api_clear_blacklist():
    save_blacklist([])
    return jsonify(_apply_judgement({"success": True}))

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
            seats = []
            try:
                ensure_lane_plan()
                seats = _LANE.get("seats") or []
            except Exception:
                seats = []
            online = {str(s.get("bot_id") or "") for s in seats}
            if key in online or str(b.get("phone") or "") in {str(s.get("phone") or "") for s in seats}:
                st = "work"
                work += 1
            else:
                st = "rest"
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
    return jsonify(_apply_judgement({
        "bots": rows,
        "summary": {"work": _proxy_line_summary()["work"], "cooldown": _proxy_line_summary()["cooldown"], "daily_capped": capped, "total": len(rows)},
        "lanes": [{"ip": s.get("ip"), "phone": s.get("phone"), "used": int(s.get("used") or 0)} for s in ((_LANE.get("seats") or []) if "_LANE" in globals() or True else [])],
        "max_batch": MAX_BATCH_SIZE,
        "default_daily_limit": DEFAULT_DAILY_LIMIT,
    }))



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
        return jsonify(_apply_judgement({"success": False, "error": "本轮文件不存在", "usernames": [], "total": 0}))
    lines = []
    with open(path, encoding="utf-8") as f:
        for raw in f:
            u = raw.strip()
            if not u:
                continue
            if not u.startswith("@"):
                u = "@" + u
            lines.append(u)
    return jsonify(_apply_judgement({"success": True, "type": kind, "total": len(lines), "usernames": lines}))


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
    return jsonify(_apply_judgement({"bots": safe_bots, "total": len(safe_bots)}))

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
    return jsonify(_apply_judgement({
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
    }))

# ============ 健康检查 ============

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
    return jsonify(_apply_judgement({"success": True, "added": added, "total": len(norm)}))


@app.route('/health', methods=['GET'])
def health():
    return jsonify(_apply_judgement({"status": "ok", "time": datetime.now().isoformat()}))

@app.route('/api/health', methods=['GET'])
def api_health():
    return jsonify(_apply_judgement({"status": "ok", "time": datetime.now().isoformat()}))

# ============ 启动 ============
# ============ 线路组 / 健康检测 / 聊天资料（feiji 对齐） ============
LINE_CAP = 10
LINE_MAX = 5

def _proxy_host(p):
    p = str(p or "").strip()
    return p.split(":")[0] if p else ""

def _proxy_list_norm():
    raw = []
    try:
        raw = load_proxy_pool()
    except Exception:
        raw = []
    out = []
    for x in raw:
        if isinstance(x, dict):
            s = (x.get("proxy") or x.get("url") or x.get("proxy_str") or "").strip()
        else:
            s = str(x).strip()
        if s and s not in out:
            out.append(s)
    return out

def build_lines(config=None):
    if config is None:
        config = load_config()
    proxies = _proxy_list_norm()
    if not proxies:
        proxies = [""]
    buckets = {p: [] for p in proxies}
    unassigned = []
    for b in config.get("bots") or []:
        px = str(b.get("proxy") or "").strip()
        if px in buckets:
            buckets[px].append(b)
        else:
            unassigned.append(b)
    # 未绑定号填进未满线路
    for b in unassigned:
        placed = False
        for p in proxies:
            if p and len(buckets[p]) < LINE_CAP:
                buckets[p].append(b)
                b["proxy"] = p
                placed = True
                break
        if not placed:
            buckets[proxies[0]].append(b)
    lines = []
    for i, p in enumerate(proxies):
        lines.append({
            "id": "line_%s" % (i + 1),
            "index": i + 1,
            "proxy": p,
            "host": _proxy_host(p),
            "bots": [{
                "id": x.get("id"),
                "name": x.get("name"),
                "phone": x.get("phone"),
                "status": x.get("status"),
                "health": x.get("health") or x.get("status") or "unknown",
                "session_path": x.get("session_path"),
                "api_id": x.get("api_id"),
                "added_time": x.get("added_time"),
            } for x in buckets[p][:LINE_CAP]]
        })
    return lines

@app.route("/api/bots/lines", methods=["GET"])
@require_auth
def api_bots_lines():
    config = load_config()
    lines = build_lines(config)
    save_config(config)
    total = sum(len(x["bots"]) for x in lines)
    return jsonify(_apply_judgement({"success": True, "lines": lines, "total_bots": total, "line_cap": LINE_CAP, "line_max": LINE_MAX}))

async def _probe_one(bot):
    from telethon import TelegramClient
    from telethon.tl.functions.contacts import ResolveUsernameRequest
    from telethon.errors import FloodWaitError, UsernameNotOccupiedError
    phone = bot.get("phone") or ""
    sp = bot.get("session_path") or ""
    if not sp:
        return "no_session"
    if not os.path.exists(sp) and not os.path.exists(sp + ".session"):
        return "no_file"
    try:
        cli = TelegramClient(sp, int(bot.get("api_id") or 0), bot.get("api_hash") or "")
        await cli.connect()
        auth = await cli.is_user_authorized()
        if not auth:
            await cli.disconnect()
            return "unauth"
        try:
            r = await cli(ResolveUsernameRequest("telegram"))
            users = list(getattr(r, "users", None) or [])
            await cli.disconnect()
            return "ok" if users or True else "empty"
        except FloodWaitError as e:
            try:
                await cli.disconnect()
            except Exception:
                pass
            return "flood:%s" % e.seconds
        except UsernameNotOccupiedError:
            try:
                await cli.disconnect()
            except Exception:
                pass
            return "restricted"
        except Exception as e:
            try:
                await cli.disconnect()
            except Exception:
                pass
            name = type(e).__name__
            if "AuthKeyUnregistered" in name:
                return "unauth"
            if "Flood" in name:
                return "flood"
            return name
    except Exception as e:
        return type(e).__name__

@app.route("/api/bots/health", methods=["POST"])
@require_auth
def api_bots_health():
    data = request.json or {}
    line_id = (data.get("line_id") or "").strip()
    config = load_config()
    lines = build_lines(config)
    targets = []
    if line_id:
        for L in lines:
            if L["id"] == line_id:
                targets = [b for b in (config.get("bots") or []) if b.get("id") in {x["id"] for x in L["bots"]}]
                break
    else:
        targets = list(config.get("bots") or [])
    rows = []
    stat = {"ok": 0, "flood": 0, "restricted": 0, "unauth": 0, "other": 0}
    for b in targets:
        try:
            h = run_async(_probe_one(b))
        except Exception as e:
            h = type(e).__name__
        b["health"] = h
        key = h.split(":")[0]
        if key in stat:
            stat[key] += 1
        else:
            stat["other"] += 1
        rows.append({"id": b.get("id"), "phone": b.get("phone"), "health": h})
    save_config(config)
    stat["total"] = len(targets)
    return jsonify(_apply_judgement({"success": True, "rows": rows, **stat}))

def _find_bot(config, bot_id):
    for b in config.get("bots") or []:
        if str(b.get("id")) == str(bot_id) or str(b.get("phone")) == str(bot_id):
            return b
    return None

async def _open_client(bot):
    from telethon import TelegramClient
    sp = bot.get("session_path") or ""
    cli = TelegramClient(sp, int(bot.get("api_id") or 0), bot.get("api_hash") or "")
    await cli.connect()
    if not await cli.is_user_authorized():
        await cli.disconnect()
        raise RuntimeError("session 未授权，请重新登录")
    return cli

@app.route("/api/bot/chat/dialogs", methods=["GET"])
@require_auth
def api_bot_dialogs():
    bot_id = request.args.get("id") or ""
    config = load_config()
    bot = _find_bot(config, bot_id)
    if not bot:
        return jsonify(_apply_judgement({"error": "水军不存在"})), 404
    async def _run():
        cli = await _open_client(bot)
        out = []
        async for d in cli.iter_dialogs(limit=30):
            out.append({
                "id": d.id,
                "name": d.name,
                "unread": int(getattr(d, "unread_count", 0) or 0),
                "is_group": bool(getattr(d, "is_group", False) or getattr(d, "is_channel", False)),
            })
        await cli.disconnect()
        return out
    try:
        dialogs = run_async(_run())
        return jsonify(_apply_judgement({"dialogs": dialogs}))
    except Exception as e:
        return jsonify(_apply_judgement({"error": str(e), "dialogs": []})), 400

@app.route("/api/bot/chat/messages", methods=["GET"])
@require_auth
def api_bot_messages():
    bot_id = request.args.get("id") or ""
    chat_id = request.args.get("chat_id") or ""
    config = load_config()
    bot = _find_bot(config, bot_id)
    if not bot:
        return jsonify(_apply_judgement({"error": "水军不存在"})), 404
    async def _run():
        cli = await _open_client(bot)
        entity = await cli.get_entity(int(chat_id)) if str(chat_id).lstrip("-").isdigit() else await cli.get_entity(chat_id)
        msgs = []
        async for m in cli.iter_messages(entity, limit=40):
            msgs.append({
                "id": m.id,
                "out": bool(m.out),
                "text": m.text or "",
                "date": str(m.date) if m.date else "",
                "media_type": type(m.media).__name__ if m.media else "",
            })
        await cli.disconnect()
        msgs.reverse()
        return msgs
    try:
        return jsonify(_apply_judgement({"messages": run_async(_run())}))
    except Exception as e:
        return jsonify(_apply_judgement({"error": str(e), "messages": []})), 400

@app.route("/api/bot/chat/send", methods=["POST"])
@require_auth
def api_bot_chat_send():
    data = request.json or {}
    config = load_config()
    bot = _find_bot(config, data.get("id"))
    if not bot:
        return jsonify(_apply_judgement({"error": "水军不存在"})), 404
    chat_id = data.get("chat_id")
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify(_apply_judgement({"error": "空消息"})), 400
    async def _run():
        cli = await _open_client(bot)
        entity = await cli.get_entity(int(chat_id)) if str(chat_id).lstrip("-").isdigit() else await cli.get_entity(chat_id)
        await cli.send_message(entity, text)
        await cli.disconnect()
        return True
    try:
        run_async(_run())
        return jsonify(_apply_judgement({"success": True}))
    except Exception as e:
        return jsonify(_apply_judgement({"error": str(e)})), 400

@app.route("/api/bot/chat/resolve", methods=["POST"])
@require_auth
def api_bot_chat_resolve():
    data = request.json or {}
    config = load_config()
    bot = _find_bot(config, data.get("id"))
    if not bot:
        return jsonify(_apply_judgement({"error": "水军不存在"})), 404
    username = (data.get("username") or "").strip().lstrip("@")
    async def _run():
        cli = await _open_client(bot)
        ent = await cli.get_entity(username)
        await cli.disconnect()
        return getattr(ent, "id", None)
    try:
        pid = run_async(_run())
        return jsonify(_apply_judgement({"peer_id": pid}))
    except Exception as e:
        return jsonify(_apply_judgement({"error": str(e)})), 400

@app.route("/api/bot/profile", methods=["POST"])
@require_auth
def api_bot_profile():
    data = request.json or {}
    config = load_config()
    bot = _find_bot(config, data.get("id"))
    if not bot:
        return jsonify(_apply_judgement({"error": "水军不存在"})), 404
    first_name = (data.get("first_name") or "").strip()
    about = data.get("about")
    async def _run():
        from telethon.tl.functions.account import UpdateProfileRequest
        cli = await _open_client(bot)
        kwargs = {}
        if first_name:
            kwargs["first_name"] = first_name
        if about is not None:
            kwargs["about"] = about
        if kwargs:
            await cli(UpdateProfileRequest(**kwargs))
        await cli.disconnect()
        return True
    try:
        run_async(_run())
        return jsonify(_apply_judgement({"success": True, "message": "资料已更新"}))
    except Exception as e:
        return jsonify(_apply_judgement({"error": str(e)})), 400


@app.route("/api/bot/import_zip", methods=["POST"])
@require_auth
def api_bot_import_zip():
    import io, zipfile
    f = request.files.get("file")
    if not f:
        return jsonify(_apply_judgement({"error": "请上传 zip"})), 400
    try:
        zf = zipfile.ZipFile(io.BytesIO(f.read()))
    except Exception:
        return jsonify(_apply_judgement({"error": "不是有效的 ZIP"})), 400
    sessions, metas = {}, {}
    for name in zf.namelist():
        base = os.path.basename(name)
        if not base or name.endswith("/"):
            continue
        stem = os.path.splitext(base)[0]
        if base.endswith(".session"):
            sessions[stem] = zf.read(name)
        elif base.endswith(".json"):
            try:
                metas[stem] = json.loads(zf.read(name).decode("utf-8", "ignore"))
            except Exception:
                pass
    if not sessions:
        return jsonify(_apply_judgement({"error": "ZIP 里没有 .session"})), 400
    config = load_config()
    sess_dir = globals().get("SESSIONS_DIR") or "/root/tg-scan-clean/sessions"
    os.makedirs(sess_dir, exist_ok=True)
    existing = {str(b.get("phone") or "").replace(" ", "") for b in (config.get("bots") or [])}
    imported = skipped_dead = skipped_full = skipped_dup = 0
    def _pick():
        proxies = [p for p in _proxy_list_norm() if p]
        counts = {p: 0 for p in proxies}
        for b in config.get("bots") or []:
            px = str(b.get("proxy") or "")
            if px in counts:
                counts[px] += 1
        best, best_n = "", 10 ** 9
        for p in proxies:
            if counts[p] < LINE_CAP and counts[p] < best_n:
                best, best_n = p, counts[p]
        return best
    for stem, raw in sessions.items():
        meta = metas.get(stem) or metas.get(stem.lstrip("+")) or {}
        if not isinstance(meta, dict):
            meta = {}
        phone = str(meta.get("phone") or stem).strip().replace(" ", "")
        if phone and not phone.startswith("+") and phone[:1].isdigit():
            phone = "+" + phone
        if phone in existing:
            skipped_dup += 1
            continue
        api_id = meta.get("api_id") or meta.get("app_id")
        api_hash = meta.get("api_hash") or meta.get("app_hash")
        if not api_id or not api_hash:
            try:
                pool = load_api_pool()
            except Exception:
                pool = []
            if pool:
                api_id = pool[imported % len(pool)].get("api_id")
                api_hash = pool[imported % len(pool)].get("api_hash")
        if not api_id or not api_hash:
            skipped_dead += 1
            continue
        proxy = _pick()
        if not proxy:
            skipped_full += 1
            continue
        safe = phone.replace("+", "")
        sp = os.path.join(sess_dir, "session_" + safe)
        with open(sp + ".session", "wb") as out:
            out.write(raw)
        async def _alive(_sp=sp, _id=int(api_id), _hash=str(api_hash)):
            from telethon import TelegramClient
            cli = TelegramClient(_sp, _id, _hash)
            await cli.connect()
            try:
                return bool(await cli.is_user_authorized())
            finally:
                await cli.disconnect()
        try:
            alive = bool(run_async(_alive()))
        except Exception as e:
            alive = "Flood" in type(e).__name__
        if not alive:
            skipped_dead += 1
            try:
                os.remove(sp + ".session")
            except Exception:
                pass
            continue
        config.setdefault("bots", []).append({
            "id": "soldier_zip_%s_%s" % (int(time.time()), safe),
            "name": str(meta.get("first_name") or meta.get("username") or phone),
            "phone": phone,
            "api_id": str(api_id),
            "api_hash": str(api_hash),
            "session_path": sp,
            "proxy": proxy,
            "status": "ready",
            "health": "ok",
            "type": "userbot",
            "added_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        existing.add(phone)
        imported += 1
    save_config(config)
    return jsonify(_apply_judgement({"success": True, "imported": imported, "skipped_dead": skipped_dead, "skipped_full": skipped_full, "skipped_dup": skipped_dup}))

if __name__ == '__main__':
    ensure_loop_running()
    print(f"[{datetime.now()}] TG用户名检测工具 Agent 启动在端口 8899")
    print(f"[{datetime.now()}] Asyncio事件循环已在独立线程中运行")
    
# --- line busy retry ---

def _install_line_busy_retry():
    endpoint = None
    for rule in app.url_map.iter_rules():
        if rule.rule == "/api/check/one" and "POST" in (rule.methods or []):
            endpoint = rule.endpoint
            break
    orig = app.view_functions.get(endpoint) if endpoint else None
    if orig is None or getattr(orig, "_lb", False):
        print("line busy retry skip", endpoint)
        return
    def wrapped(*a, **k):
        import time as _t
        last = None
        for _try in range(4):
            last = orig(*a, **k)
            resp = last[0] if isinstance(last, tuple) else last
            data = resp.get_json(silent=True) if hasattr(resp, "get_json") else None
            err = str((data or {}).get("error") or "") if isinstance(data, dict) else ""
            transient = ("LINE_BUSY" in err) or ("database is locked" in err) or ("Server closed the connection" in err)
            if not transient:
                return last
            for gname, obj in list(globals().items()):
                low = gname.lower()
                if any(x in low for x in ("lane", "busy", "inflight")) and "cool" not in low:
                    if isinstance(obj, set):
                        obj.clear()
                    elif isinstance(obj, dict) and gname not in ("app",):
                        try:
                            obj.clear()
                        except Exception:
                            pass
            _t.sleep(0.8)
        return last
    wrapped._lb = True
    wrapped.__name__ = getattr(orig, "__name__", "api_check_one")
    app.view_functions[endpoint] = wrapped
    print("line busy retry on", endpoint)
_install_line_busy_retry()

app.run(host='0.0.0.0', port=8902, debug=False)
