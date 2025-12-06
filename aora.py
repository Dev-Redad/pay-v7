# bot.py — Mongo-backed (PTB 13.x) with multi-storage (database channel) support and failover
# Channel-only upgrades preserved:
# - Robust link delivery (join-request link with fallback)
# - Permanent buyer receipt (ONLY Order ID; link sent separately)
# - Link sent separately and auto-deleted later (receipt never deleted)
# - Admin can send Order ID to get "admin check receipt" (auto-deletes in 10 min)
# - "Contact support" on delivery failure; configurable in /settings
#
# Mongo fix: use a SPARSE UNIQUE index on order_id (older Mongo versions reject partial index).
# Channel title is captured at link creation for admin lookup even if renamed/deleted later.
#
# NEW (multi-storage database channels):
# - Config key "storage_channels": [main_storage_id, backup1_id, backup2_id, ...]
# - All product files are mirrored to every storage channel on add.
# - Existing products resync automatically on startup, and now also
#   **automatically after adding a backup**, plus manually from /settings if you wish.
# - Delivery falls back to backups if main storage message is unavailable.
# - After repeated main failures, bot promotes a backup to main automatically.
# - /settings adds a "📦 Storage Channels" pane to manage add/remove/promote/resync.
# - BUGFIX: storage add no longer conflicts with "add product channel" flow.
# - IMPROVEMENT: resync after adding a backup runs in a JobQueue (non-blocking).
# - IMPROVEMENT: backup reference parsing is lenient (works even with extra words).
# - IMPROVEMENT: auto-resync runs after ⭐ Make Main as a safety net.

import os, logging, time, random, re, unicodedata, html
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from telegram import Update, ParseMode, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Updater, CommandHandler, MessageHandler, Filters, CallbackContext,
    ConversationHandler, CallbackQueryHandler, ChatJoinRequestHandler
)
from telegram.error import BadRequest, Unauthorized

from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

logging.basicConfig(format="%(asctime)s %(levelname)s:%(name)s: %(message)s", level=logging.INFO)
log = logging.getLogger("upi-mongo-bot")

# === Bot token (updated as requested) ===
TOKEN = "8352423948:AAEP_WHdxNGziUabzMwO9_YiEp24_d0XYVk"

# === Owner and Admins (updated as requested) ===
OWNER_ID = 7381642564  # Owner has full access including /settings
ADMIN_IDS = [7223414109, 6053105336, 7748361879]  # Admins have all features except /settings
ALL_ADMINS = [OWNER_ID] + ADMIN_IDS  # Combined list for admin checks

# --- Storage (database) channels ---
# The bot uses cfg("storage_channels") → list; first is MAIN and the rest are BACKUPS.
# Default (legacy compat) main is set here:
STORAGE_CHANNEL_ID = -1003177558757

PAYMENT_NOTIF_CHANNEL_ID = -1002865174188

# Legacy single-UPI defaults (pool feature stays intact)
UPI_ID = "dexar@slc"
UPI_PAYEE_NAME = "Seller"

# Payments & housekeeping
PAY_WINDOW_MINUTES = 5
GRACE_SECONDS = 10
DELETE_AFTER_MINUTES = 30   # link + short "payment received" auto-deleted after this

# Options
PROTECT_CONTENT_ENABLED = False
FORCE_SUBSCRIBE_ENABLED = True
FORCE_SUBSCRIBE_CHANNEL_IDS = []

# Mongo
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://Hui:Hui@cluster0.3lpdrgm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
mdb = MongoClient(MONGO_URI)["upi_bot"]

c_users     = mdb["users"]
c_products  = mdb["products"]
c_config    = mdb["config"]
c_sessions  = mdb["sessions"]
c_locks     = mdb["locks"]
c_paylog    = mdb["payments"]
c_orders    = mdb["orders"]
c_upi_state = mdb["upi_state"]
c_earnings  = mdb["earnings"]  # New collection for admin earnings

# ========== Indexes ==========
c_users.create_index([("user_id", ASCENDING)], unique=True)
c_products.create_index([("item_id", ASCENDING)], unique=True)
c_config.create_index([("key", ASCENDING)], unique=True)
c_locks.create_index([("amount_key", ASCENDING)], unique=True)
c_locks.create_index([("hard_expire_at", ASCENDING)], expireAfterSeconds=0)
c_sessions.create_index([("key", ASCENDING)], unique=True)
c_sessions.create_index([("amount_key", ASCENDING)])
c_sessions.create_index([("hard_expire_at", ASCENDING)], expireAfterSeconds=0)
c_paylog.create_index([("ts", ASCENDING)])
# One order per user/channel (your original logic)
c_orders.create_index([("user_id", ASCENDING), ("channel_id", ASCENDING)], unique=True)

# Earnings indexes - FIXED: Handle existing index conflicts
try:
    # Check if existing index with same name exists
    existing_indexes = c_earnings.index_information()
    if "admin_id_1_date_1" in existing_indexes:
        # Drop the existing index
        c_earnings.drop_index("admin_id_1_date_1")
        log.info("Dropped existing admin_id_1_date_1 index to recreate it")
    
    # Create non-unique compound index for admin_id and date
    c_earnings.create_index(
        [("admin_id", ASCENDING), ("date", ASCENDING)],
        name="admin_id_date_index",
        unique=False
    )
except Exception as e:
    log.warning(f"Creating admin_id_date index failed: {e}")

try:
    if "admin_id_1_timestamp_1" in c_earnings.index_information():
        c_earnings.drop_index("admin_id_1_timestamp_1")
        log.info("Dropped existing admin_id_1_timestamp_1 index to recreate it")
    
    # Create non-unique compound index for admin_id and timestamp
    c_earnings.create_index(
        [("admin_id", ASCENDING), ("timestamp", ASCENDING)],
        name="admin_id_timestamp_index",
        unique=False
    )
except Exception as e:
    log.warning(f"Creating admin_id_timestamp index failed: {e}")

# Safe UNIQUE index on order_id (SPARSE -> ignores docs without order_id)
try:
    c_orders.update_many({"order_id": None}, {"$unset": {"order_id": ""}})
    c_orders.update_many({"order_id": ""},   {"$unset": {"order_id": ""}})
    c_orders.create_index(
        [("order_id", ASCENDING)],
        unique=True,
        sparse=True,
        name="order_id_unique_sparse"
    )
except Exception as e:
    log.warning(f"Creating sparse unique index on order_id failed (will continue): {e}")

c_upi_state.create_index([("upi", ASCENDING)], unique=True)

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))

def cfg(key, default=None):
    doc = c_config.find_one({"key": key})
    return doc["value"] if doc and "value" in doc else default

def set_cfg(key, value):
    c_config.update_one({"key": key}, {"$set": {"value": value}}, upsert=True)

def amount_key(x: float) -> str:
    return f"{x:.2f}" if abs(x - int(x)) > 1e-9 else str(int(x))

# === IST helpers ===
def now_ist(): return datetime.now(IST)
def today_ist_str(): return datetime.now(IST).strftime("%Y-%m-%d")
def fmt_inr(x: float) -> str: return f"{int(x)}" if abs(x-int(x))<1e-9 else f"{x:.2f}"

# === Week helpers for earnings ===
def get_week_start_date():
    """Get the start date (Monday) of current week in IST"""
    today = now_ist()
    # Monday is 0, Sunday is 6
    days_since_monday = today.weekday()
    week_start = today - timedelta(days=days_since_monday)
    return week_start.date()

# === Order-ID helpers ===
ORDER_ID_PATTERN = re.compile(r"\bORD-\d{8}-\d{6}-[A-Z2-9]{4}\b")
def gen_order_id() -> str:
    ts = now_ist()
    rand = "".join(random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ23456789") for _ in range(4))
    return f"ORD-{ts.strftime('%Y%m%d')}-{ts.strftime('%H%M%S')}-{rand}"

# ========== Earnings tracking functions ==========
def record_earning(admin_id: int, amount: float, order_id: str, item_id: str, user_id: int):
    """Record an earning for an admin"""
    try:
        timestamp = datetime.now(UTC)
        date_str = today_ist_str()
        week_start = get_week_start_date()
        
        c_earnings.insert_one({
            "admin_id": admin_id,
            "amount": float(amount),
            "order_id": order_id,
            "item_id": item_id,
            "user_id": user_id,
            "timestamp": timestamp,
            "date": date_str,
            "week_start": week_start.isoformat()
        })
        log.info(f"Recorded earning: Admin {admin_id} earned {amount} from order {order_id}")
    except Exception as e:
        log.error(f"Failed to record earning: {e}")

def get_admin_earnings(admin_id: int):
    """Get earnings statistics for an admin"""
    try:
        # Total earnings
        total_result = c_earnings.aggregate([
            {"$match": {"admin_id": admin_id}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ])
        total_data = list(total_result)
        total = total_data[0]["total"] if total_data else 0.0
        
        # Today's earnings
        today_str = today_ist_str()
        today_result = c_earnings.aggregate([
            {"$match": {"admin_id": admin_id, "date": today_str}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ])
        today_data = list(today_result)
        today = today_data[0]["total"] if today_data else 0.0
        
        # This week's earnings
        week_start = get_week_start_date().isoformat()
        week_result = c_earnings.aggregate([
            {"$match": {"admin_id": admin_id, "week_start": week_start}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ])
        week_data = list(week_result)
        week = week_data[0]["total"] if week_data else 0.0
        
        return {
            "total": total,
            "today": today,
            "week": week
        }
    except Exception as e:
        log.error(f"Failed to get earnings for admin {admin_id}: {e}")
        return {"total": 0.0, "today": 0.0, "week": 0.0}

# ========== Multi-storage (database channel) helpers ==========
def get_storage_channels():
    """Returns the configured storage channel list: [main, backup1, backup2, ...]."""
    lst = cfg("storage_channels")
    if not isinstance(lst, list) or not lst:
        lst = [int(STORAGE_CHANNEL_ID)]
        set_storage_channels(lst)
    # de-dup while preserving order & normalize ints
    seen = set(); out = []
    for x in lst:
        try:
            xi = int(x)
            if xi not in seen:
                out.append(xi); seen.add(xi)
        except Exception:
            pass
    if not out:
        out = [int(STORAGE_CHANNEL_ID)]
    if out != lst: set_storage_channels(out)
    return out

def set_storage_channels(lst):
    clean = []
    seen = set()
    for x in lst:
        try:
            xi = int(x)
            if xi not in seen:
                clean.append(xi); seen.add(xi)
        except Exception:
            continue
    if not clean:
        clean = [int(STORAGE_CHANNEL_ID)]
    set_cfg("storage_channels", clean)

def get_main_storage_channel(): return get_storage_channels()[0]
def get_backup_storage_channels():
    chs = get_storage_channels()
    return chs[1:] if len(chs) > 1 else []

def _storage_titles(context: CallbackContext, ids):
    out = []
    for cid in ids:
        t = None
        try:
            chat = context.bot.get_chat(cid)
            if getattr(chat, "title", None):
                t = chat.title
            elif getattr(chat, "username", None):
                t = f"@{chat.username}"
        except Exception:
            t = None
        out.append((cid, t))
    return out

def _replicate_file_to_channel(context: CallbackContext, src_chat_id: int, src_msg_id: int, target_chat_id: int):
    """Copy a stored message to another storage channel. Returns new message_id or None."""
    try:
        m = context.bot.copy_message(chat_id=target_chat_id, from_chat_id=src_chat_id, message_id=src_msg_id)
        return m.message_id
    except Exception as e:
        log.error(f"Replicate to {target_chat_id} failed (from {src_chat_id}/{src_msg_id}): {e}")
        return None

def _ensure_backups_for_product(context: CallbackContext, prod: dict):
    """Ensure every file of a product exists in every configured backup channel."""
    files = prod.get("files", [])
    if not files:
        return
    storage = get_storage_channels()
    main_id = storage[0]
    targets = storage[1:]  # backups to ensure
    changed = False

    for f in files:
        # Guarantee 'backups' array exists
        if not isinstance(f.get("backups"), list):
            f["backups"] = []
            changed = True

        have = set()
        try:
            have.add(int(f.get("channel_id")))
        except Exception:
            pass
        for b in f["backups"]:
            try:
                have.add(int(b.get("channel_id")))
            except Exception:
                pass

        missing = [t for t in targets if t not in have]
        # choose a reliable source
        src_ch = int(f.get("channel_id"))
        src_mid = int(f.get("message_id"))
        if main_id in have and main_id != src_ch:
            for b in f["backups"]:
                if int(b.get("channel_id", 0)) == main_id:
                    src_ch = main_id
                    src_mid = int(b.get("message_id"))
                    break

        for tgt in missing:
            new_mid = _replicate_file_to_channel(context, src_ch, src_mid, tgt)
            if new_mid:
                f["backups"].append({"channel_id": int(tgt), "message_id": int(new_mid)})
                changed = True
                time.sleep(0.05)

    if changed:
        try:
            c_products.update_one({"_id": prod["_id"]}, {"$set": {"files": files}})
        except Exception as e:
            log.error(f"Persist product backups failed for {prod.get('item_id')}: {e}")

def _resync_all_storage(context: CallbackContext):
    """Scan all products and ensure files are mirrored to all configured backups."""
    cur = c_products.find({"files": {"$exists": True, "$ne": []}})
    n = 0
    for prod in cur:
        _ensure_backups_for_product(context, prod)
        n += 1
        if n % 50 == 0:
            time.sleep(0.2)
    log.info(f"Storage resync checked {n} products.")

def _resync_all_job(context: CallbackContext):
    try:
        _resync_all_storage(context)
    except Exception as e:
        log.error(f"Resync job failed: {e}")

def _resync_job_to_chat(context: CallbackContext):
    """Run resync and notify the admin chat when done."""
    chat_id = None
    try:
        chat_id = (context.job.context or {}).get("chat_id")
    except Exception:
        pass
    try:
        _resync_all_storage(context)
        if chat_id:
            context.bot.send_message(chat_id, "✅ Storage resync complete.")
    except Exception as e:
        log.error(f"Resync-to-chat failed: {e}")
        if chat_id:
            context.bot.send_message(chat_id, f"⚠️ Resync ended with errors: {e}")

def _record_storage_failure(context: CallbackContext, failed_channel_id: int):
    """Track failures on the current main and auto-promote a backup if needed."""
    try:
        main_id = get_main_storage_channel()
        if int(failed_channel_id) != int(main_id):
            return
        cnt = int(cfg("storage_main_fail_count", 0)) + 1
        set_cfg("storage_main_fail_count", cnt)
        log.warning(f"Main storage failure count = {cnt}")
        chs = get_storage_channels()
        if cnt >= 3 and len(chs) > 1:
            # promote first backup to main
            new_order = [chs[1], chs[0]] + chs[2:]
            set_storage_channels(new_order)
            set_cfg("storage_main_fail_count", 0)
            log.warning(f"Auto-promoted backup {chs[1]} to MAIN storage.")
    except Exception as e:
        log.error(f"Record storage failure failed: {e}")

def _record_storage_success_on_main():
    try:
        if cfg("storage_main_fail_count", 0) != 0:
            set_cfg("storage_main_fail_count", 0)
    except Exception:
        pass

# ========== Multi-UPI helpers (unchanged) ==========
def get_upi_pool(): return cfg("upi_pool", [])
def set_upi_pool(pool):
    main_seen = False
    for u in pool:
        if u.get("main", False):
            if not main_seen: main_seen = True
            else: u["main"] = False
    if not main_seen and pool: pool[0]["main"] = True
    set_cfg("upi_pool", pool)

def _refresh_state_for_today(upi_entry):
    upi = upi_entry["upi"]; today = today_ist_str()
    st = c_upi_state.find_one({"upi": upi})
    need_reset = (not st) or (st.get("date") != today)
    prev_amt_today = (st or {}).get("amt_today", 0.0)
    prev_amt_all   = (st or {}) .get("amt_all", 0.0)
    if need_reset:
        rmin = upi_entry.get("rand_min"); rmax = upi_entry.get("rand_max"); mx = upi_entry.get("max_txn")
        if rmin is not None and rmax is not None:
            try:
                rmin_i, rmax_i = int(rmin), int(rmax)
                if rmax_i < rmin_i: rmin_i, rmax_i = rmax_i, rmin_i
                todays_max = random.randint(rmin_i, rmax_i)
            except: todays_max = int(mx) if mx is not None else None
        else:
            todays_max = int(mx) if mx is not None else None
        c_upi_state.update_one(
            {"upi": upi},
            {"$set": {
                "date": today, "count": 0, "daily_max": todays_max,
                "amt_yday": prev_amt_today if st else 0.0,
                "amt_today": 0.0, "amt_all": prev_amt_all if st else 0.0
            }},
            upsert=True
        )
        st = c_upi_state.find_one({"upi": upi})
    return st

def _get_main_upi(pool):
    for u in pool:
        if u.get("main"): return u
    return pool[0] if pool else None

def _within_amount(upi_entry, amount):
    amin = upi_entry.get("min_amt"); amax = upi_entry.get("max_amt")
    if amin is not None and amount < amin: return False
    if amax is not None and amount > amax: return False
    return True

def _forced_choice(amount):
    f = cfg("force_upi")
    if not f or not isinstance(f, dict) or not f.get("upi"): return None
    pool = get_upi_pool()
    entry = next((x for x in pool if x.get("upi") == f["upi"]), None)
    if not entry: return None
    if f.get("respect_amount") and not _within_amount(entry, amount): return None
    if f.get("respect_txn"):
        st = _refresh_state_for_today(entry)
        dmax = st.get("daily_max"); used = int(st.get("count", 0))
        if (dmax is not None) and (used >= dmax): return None
    return entry["upi"]

def select_upi_for_amount(amount):
    forced = _forced_choice(amount)
    if forced: return forced
    pool = get_upi_pool()
    if not pool: return None
    main_entry = _get_main_upi(pool)
    eligible_by_range, eligible_final = [], []
    for u in pool:
        st = _refresh_state_for_today(u)
        if _within_amount(u, amount):
            eligible_by_range.append((u, st))
            dmax = st.get("daily_max"); used = int(st.get("count", 0))
            if (dmax is None) or (used < dmax): eligible_final.append((u, used))
    if eligible_final:
        min_used = min(u for (_, u) in eligible_final)
        candidates = [u for (u, used) in eligible_final if used == min_used]
        return random.choice(candidates)["upi"]
    if eligible_by_range: return (main_entry or eligible_by_range[0][0])["upi"]
    if main_entry and _within_amount(main_entry, amount): return main_entry["upi"]
    return (main_entry or pool[0])["upi"]

def _bump_usage(upi):
    pool = get_upi_pool(); entry = next((x for x in pool if x["upi"] == upi), None)
    if not entry: return
    _refresh_state_for_today(entry)
    c_upi_state.update_one({"upi": upi}, {"$inc": {"count": 1}})

def _bump_amount(upi, amt: float):
    pool = get_upi_pool(); entry = next((x for x in pool if x["upi"] == upi), None)
    if not entry: return
    _refresh_state_for_today(entry)
    c_upi_state.update_one({"upi": upi}, {"$inc": {"amt_today": float(amt), "amt_all": float(amt)}})

def build_upi_uri(amount: float, note: str, upi_id: str):
    amt = fmt_inr(amount)
    pa = quote(upi_id, safe=''); pn = quote(UPI_PAYEE_NAME, safe=''); tn = quote(note, safe='')
    return f"upi://pay?pa={pa}&pn={pn}&am={amt}&cu=INR&tn={tn}"

def qr_url(data: str): return f"https://api.qrserver.com/v1/create-qr-code/?data={quote(data, safe='')}&size=512x512&qzone=2"

def add_user(uid, uname): c_users.update_one({"user_id": uid},{"$set":{"username":uname or ""}},upsert=True)
def get_all_user_ids(): return list(c_users.distinct("user_id"))

def reserve_amount_key(k: str, hard_expire_at: datetime) -> bool:
    try:
        c_locks.insert_one({"amount_key": k,"hard_expire_at": hard_expire_at,"created_at": datetime.now(UTC)})
        return True
    except DuplicateKeyError:
        return False
def release_amount_key(k: str): c_locks.delete_one({"amount_key": k})

def pick_unique_amount(lo: float, hi: float, hard_expire_at: datetime) -> float:
    lo, hi = int(lo), int(hi); ints = list(range(lo, hi+1)); random.shuffle(ints)
    for v in ints:
        if reserve_amount_key(str(v), hard_expire_at): return float(v)
    for base in ints:
        for p in range(1,100):
            key = f"{base}.{p:02d}"
            if reserve_amount_key(key, hard_expire_at): return float(f"{base}.{p:02d}")
    return float(ints[-1])

def _normalize_digits(s: str) -> str:
    out=[]
    for ch in s:
        if unicodedata.category(ch).startswith('M'): continue
        if ch.isdigit():
            try: out.append(str(unicodedata.digit(ch))); continue
            except Exception: pass
        out.append(ch)
    return "".join(out)

# --- Payment parser patterns (simplified coverage) ---
PHONEPE_RE = re.compile(
    r"(?:you['']ve\s*received\s*(?:rs\.?|rupees|₹)|money\s*received|payment\s*received|upi\s*payment\s*received|credited(?:\s*by)?\s*(?:rs\.?|rupees|₹)?|received\s*(?:rs\.?|rupees|₹)|paid\s*you\s*₹)\s*[.:₹\s]*([0-9][0-9,]*(?:\.[0-9]{1,2})?)",
    re.I | re.S
)
AMOUNT_BEFORE_CURRENCY_RE = re.compile(r"(?:received|credited)\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:rupees|rs\.?|₹)\b", re.I | re.S)
GPAY_PAID_YOU_RE = re.compile(r"paid\s*you\s*[₹\s]*([0-9][0-9,]*(?:\.[0-9]{1,2})?)", re.I | re.S)
BHARATPE_BUSINESS_RE = re.compile(r"BharatPe for Business.*?received.*?payment.*?₹\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)", re.I | re.S)

def parse_phonepe_amount(text: str):
    norm = _normalize_digits(text or "")
    for pat in (PHONEPE_RE, AMOUNT_BEFORE_CURRENCY_RE, GPAY_PAID_YOU_RE, BHARATPE_BUSINESS_RE):
        m = pat.search(norm)
        if m:
            try: return float(m.group(1).replace(",", ""))
            except: pass
    return None

# ---------- Robust invite-link helper ----------
def _robust_invite_link(context: CallbackContext, ch_id: int, uid: int = None) -> str:
    name = f"paid-{uid}-{int(time.time())}" if uid else f"gen-{int(time.time())}"
    try:
        cil = context.bot.create_chat_invite_link(ch_id, creates_join_request=True, name=name)
        if cil and getattr(cil, "invite_link", None):
            return cil.invite_link
    except Exception as e:
        log.debug(f"create join-request link failed for {ch_id}: {e}")
    try:
        cil2 = context.bot.create_chat_invite_link(ch_id, name=name)
        if cil2 and getattr(cil2, "invite_link", None):
            return cil2.invite_link
    except Exception as e:
        log.debug(f"create regular invite link failed for {ch_id}: {e}")
    try:
        chat_obj = context.bot.get_chat(ch_id)
        if getattr(chat_obj, "username", None):
            return f"https://t.me/{chat_obj.username}"
        if getattr(chat_obj, "invite_link", None):
            return chat_obj.invite_link
    except Exception as e:
        log.debug(f"get_chat fallback failed for {ch_id}: {e}")
    return None

# --- Force-subscribe wrapper (unchanged logic except safer link creation) ---
def force_subscribe(fn):
    def wrapper(update: Update, context: CallbackContext, *a, **k):
        if (not FORCE_SUBSCRIBE_ENABLED) or (not FORCE_SUBSCRIBE_CHANNEL_IDS) or (update.effective_user.id in ALL_ADMINS):
            return fn(update, context, *a, **k)
        uid = update.effective_user.id
        need=[]
        for ch in FORCE_SUBSCRIBE_CHANNEL_IDS:
            try:
                st = context.bot.get_chat_member(ch, uid).status
                if st not in ("member","administrator","creator"): need.append(ch)
            except: need.append(ch)
        if not need: return fn(update, context, *a, **k)
        context.user_data['pending_command']={'fn':fn,'update':update}
        btns=[]
        for ch in need:
            try:
                chat=context.bot.get_chat(ch)
                link = _robust_invite_link(context, ch, uid)
                if link:
                    btns.append([InlineKeyboardButton(f"Join {chat.title}", url=link)])
                else:
                    log.warning(f"No join link available for {ch}")
            except Exception as e: log.warning(f"Invite link fail {ch}: {e}")
        btns.append([InlineKeyboardButton("✅ I have joined", callback_data="check_join")])
        msg = cfg("force_sub_text","Join required channels to continue.")
        photo = cfg("force_sub_photo_id")
        if photo: update.effective_message.reply_photo(photo=photo, caption=msg, reply_markup=InlineKeyboardMarkup(btns))
        else: update.effective_message.reply_text(msg, reply_markup=InlineKeyboardMarkup(btns))
    return wrapper

def check_join_cb(update: Update, context: CallbackContext):
    q=update.callback_query; uid=q.from_user.id; need=[]
    for ch in FORCE_SUBSCRIBE_CHANNEL_IDS:
        try:
            st=context.bot.get_chat_member(ch, uid).status
            if st not in ("member","administrator","creator"): need.append(ch)
        except: need.append(ch)
    if not need:
        try: q.message.delete()
        except: pass
        q.answer("Thank you!", show_alert=True)
        pend = context.user_data.pop('pending_command', None)
        if pend: return pend['fn'](pend['update'], context)
    else: q.answer("Still not joined all.", show_alert=True)

def _auto_delete_messages(context: CallbackContext):
    data = context.job.context
    chat_id = data["chat_id"]; ids = data["message_ids"]
    for mid in ids:
        try: context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception: pass

def _delete_unpaid_qr(context: CallbackContext):
    data = context.job.context
    if c_sessions.find_one({"key": data["sess_key"]}):
        try: context.bot.delete_message(chat_id=data["chat_id"], message_id=data["qr_message_id"])
        except Exception: pass

# --- Purchase flow (files unchanged in UX; storage behavior enhanced) ---
def start_purchase(ctx: CallbackContext, chat_id: int, uid: int, item_id: str):
    prod = c_products.find_one({"item_id": item_id})
    if not prod: return ctx.bot.send_message(chat_id, "❌ Item not found.")
    
    # Get admin who added the product (for earnings tracking)
    admin_id = prod.get("added_by", OWNER_ID)
    
    mn, mx = prod.get("min_price"), prod.get("max_price")
    if mn is None or mx is None:
        v=float(prod.get("price",0))
        if v == 0:
            deliver_ids = deliver(ctx, uid, item_id, return_ids=True, notify_on_fail=True) or []
            if deliver_ids:
                ctx.job_queue.run_once(
                    _auto_delete_messages,
                    timedelta(minutes=DELETE_AFTER_MINUTES),
                    context={"chat_id": chat_id, "message_ids": deliver_ids},
                    name=f"free_del_{uid}_{int(time.time())}"
                )
            if "channel_id" in prod:
                try:
                    c_orders.update_one(
                        {"user_id": uid, "channel_id": int(prod["channel_id"])},
                        {"$set": {"item_id": item_id, "paid_at": datetime.now(UTC), "status": "free", "admin_id": admin_id}},
                        upsert=True
                    )
                except Exception:
                    pass
            return
        if v<=0: return ctx.bot.send_message(chat_id,"❌ Price not set.")
        mn=mx=v
    else:
        try:
            if float(mn) == float(mx) == 0:
                deliver_ids = deliver(ctx, uid, item_id, return_ids=True, notify_on_fail=True) or []
                if deliver_ids:
                    ctx.job_queue.run_once(
                        _auto_delete_messages,
                        timedelta(minutes=DELETE_AFTER_MINUTES),
                        context={"chat_id": chat_id, "message_ids": deliver_ids},
                        name=f"free_del_{uid}_{int(time.time())}"
                    )
                if "channel_id" in prod:
                    try:
                        c_orders.update_one(
                            {"user_id": uid, "channel_id": int(prod["channel_id"])},
                            {"$set": {"item_id": item_id, "paid_at": datetime.now(UTC), "status": "free", "admin_id": admin_id}},
                            upsert=True
                        )
                    except Exception:
                        pass
                return
        except Exception:
            pass

    created = datetime.now(UTC)
    hard_expire_at = created + timedelta(minutes=PAY_WINDOW_MINUTES)
    amt = pick_unique_amount(mn, mx, datetime.now(UTC) + timedelta(minutes=PAY_WINDOW_MINUTES))
    akey = amount_key(amt)

    chosen_upi = select_upi_for_amount(float(amt)) or UPI_ID
    uri = build_upi_uri(amt, f"order_uid_{uid}", chosen_upi)
    img = qr_url(uri)
    display_amt = fmt_inr(amt)
    caption = (
         f"Pay ₹{display_amt} for the item\n\n"
         f"UPI ID — `{chosen_upi}`\n\n"
         "Instructions:\n"
         "• Scan this QR or copy the UPI ID\n"
         f"• Pay exactly ₹{display_amt} within {PAY_WINDOW_MINUTES} minutes\n"
         "Verification is automatic. Delivery right after payment."
    )
    sent = ctx.bot.send_photo(chat_id=chat_id, photo=img, caption=caption, parse_mode=ParseMode.MARKDOWN)

    sess_key = f"{uid}:{item_id}:{int(time.time())}"
    c_sessions.insert_one({
        "key": sess_key, "user_id": uid, "chat_id": chat_id, "item_id": item_id,
        "amount": float(amt), "amount_key": akey, "upi_id": chosen_upi,
        "created_at": datetime.now(UTC), "admin_id": admin_id,
        "hard_expire_at": datetime.now(UTC) + timedelta(minutes=PAY_WINDOW_MINUTES, seconds=GRACE_SECONDS),
        "qr_message_id": sent.message_id,
    })

    qr_timeout_mins = int(cfg("qr_unpaid_delete_minutes", PAY_WINDOW_MINUTES))
    ctx.job_queue.run_once(
        _delete_unpaid_qr,
        timedelta(minutes=qr_timeout_mins, seconds=1),
        context={"sess_key": sess_key, "chat_id": chat_id, "qr_message_id": sent.message_id},
        name=f"qr_expire_{uid}_{int(time.time())}"
    )

def deliver(ctx: CallbackContext, uid: int, item_id: str, return_ids: bool = False, notify_on_fail: bool = False):
    """
    Deliver product:
      - Files: copy messages with multi-storage failover (main → backups).
      - Channel: create a request-to-join invite link (fallback regular/public/existing) and DM it.
      - notify_on_fail: if True, tell user to contact support when link generation fails.
    """
    prod = c_products.find_one({"item_id": item_id})
    if not prod:
        try: ctx.bot.send_message(uid, "❌ Item missing.")
        except Exception as e: log.error(f"Notify missing item failed (to {uid}): {e}")
        return [] if return_ids else None

    # Channel product
    if "channel_id" in prod:
        ch_id = prod["channel_id"]

        # Capture channel title/username NOW
        ch_title = None; ch_username = None
        try:
            chat_obj = ctx.bot.get_chat(ch_id)
            ch_title = (chat_obj.title or None)
            ch_username = (chat_obj.username or None)
        except Exception as e:
            log.debug(f"get_chat failed while preparing link for {ch_id}: {e}")

        # Robust invite link (no export)
        link = None
        try:
            link = _robust_invite_link(ctx, ch_id, uid)
        except Exception as e:
            log.warning(f"Robust invite link creation failed for {ch_id}: {e}")

        if not link:
            log.warning(f"All link strategies failed for {ch_id}")
            if notify_on_fail:
                sup = cfg("support_contact")
                txt = "⚠️ This Channel is not available right now. Please contact support"
                txt += f" {sup}." if sup else "."
                try: ctx.bot.send_message(uid, txt)
                except Exception as ee: log.error(f"Notify link-missing failed (to {uid}): {ee}")
            return [] if return_ids else None

        # Persist captured title/username
        try:
            c_orders.update_one(
                {"user_id": uid, "channel_id": int(ch_id)},
                {"$set": {
                    "channel_title_at_purchase": ch_title,
                    "channel_username_at_purchase": ch_username,
                    "channel_title_captured_at": datetime.now(UTC)
                }},
                upsert=True
            )
        except Exception as e:
            log.warning(f"Order title upsert failed for user {uid}, ch {ch_id}: {e}")

        # Link DM
        try:
            txt = (
                f"🔗 <b>Join:</b> <a href=\"{html.escape(link)}\">{html.escape(link)}</a>\n"
                f"<i>(this link auto-deletes soon)</i>"
            )
            m = ctx.bot.send_message(
                uid,
                txt,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Join now", url=link)]])
            )
            return [m.message_id] if return_ids else None
        except Exception as e:
            log.error(f"Send channel link failed (to {uid}): {e}")
            if notify_on_fail:
                sup = cfg("support_contact")
                txt = "⚠️ This Channel is not available right now. Please contact support"
                txt += f" {sup}." if sup else "."
                try: ctx.bot.send_message(uid, txt)
                except Exception as ee: log.error(f"Notify link send-fail (to {uid}): {ee}")
            return [] if return_ids else None

    # Files product — multi-storage variants & failover
    def _copy_variant(from_ch, msg_id):
        try:
            m = ctx.bot.copy_message(chat_id=uid, from_chat_id=int(from_ch), message_id=int(msg_id), protect_content=PROTECT_CONTENT_ENABLED)
            if int(from_ch) == int(get_main_storage_channel()):
                _record_storage_success_on_main()
            return m.message_id
        except Exception as e:
            log.error(f"Copy to user failed from {from_ch}/{msg_id}: {e}")
            _record_storage_failure(ctx, int(from_ch))
            return None

    msg_ids = []
    storage_list = get_storage_channels()
    main_storage = storage_list[0]

    for f in prod.get("files", []):
        variants = []
        primary = (int(f.get("channel_id")), int(f.get("message_id")))
        backups = [(int(b.get("channel_id")), int(b.get("message_id"))) for b in (f.get("backups") or []) if b.get("channel_id") and b.get("message_id")]

        main_variant = None
        if primary[0] == main_storage:
            main_variant = primary
        else:
            for b in backups:
                if b[0] == main_storage:
                    main_variant = b; break

        if main_variant: variants.append(main_variant)
        if primary not in variants: variants.append(primary)
        for b in backups:
            if b not in variants:
                variants.append(b)

        delivered = False
        for ch_id, mid in variants:
            m_id = _copy_variant(ch_id, mid)
            if m_id:
                msg_ids.append(m_id)
                delivered = True
                break

        if not delivered:
            log.error(f"All storage variants failed for a file of item {item_id}")

    try: ctx.bot.send_message(uid, "⚠️ Files auto-delete here in 30 minutes. Save now.")
    except Exception as e: log.error(f"Warn send fail (to {uid}): {e}")

    return msg_ids if return_ids else None

# ---- Payment listener ----
def on_channel_post(update: Update, context: CallbackContext):
    msg = update.channel_post
    if not msg or msg.chat_id != PAYMENT_NOTIF_CHANNEL_ID: return
    text = msg.text or msg.caption or ""; low = text.lower()

    if not any(k in low for k in (
        "phonepe business","phonepe","gpay","google pay","slice","bharatpe",
        "money received","payment received","upi payment received",
        "received rs","received ₹","rupees","paid you ₹","credited"
    )): return

    amt = parse_phonepe_amount(text)
    if amt is None: return

    ts = (msg.date or datetime.now(UTC)); ts = ts if ts.tzinfo else ts.replace(tzinfo=UTC); ts = ts.astimezone(UTC)

    akey = amount_key(amt)
    try: c_paylog.insert_one({"key": akey, "ts": ts, "raw": text[:500]})
    except: pass

    matches = list(c_sessions.find({"amount_key": akey, "created_at": {"$lte": ts}, "hard_expire_at": {"$gte": ts}}))
    for s in matches:
        qr_mid = s.get("qr_message_id")
        if qr_mid:
            try: context.bot.delete_message(chat_id=s["chat_id"], message_id=qr_mid)
            except Exception as e: log.debug(f"Delete QR failed: {e}")

        try:
            confirm_msg = context.bot.send_message(s["chat_id"], "✅ Payment received. Delivering your item…")
            confirm_msg_id = confirm_msg.message_id
        except Exception as e:
            log.warning(f"Notify user fail: {e}")
            confirm_msg_id = None

        ids_to_delete = []
        if confirm_msg_id: ids_to_delete.append(confirm_msg_id)

        deliver_ids = deliver(context, s["user_id"], s["item_id"], return_ids=True, notify_on_fail=False)
        ids_to_delete.extend(deliver_ids or [])

        prod = c_products.find_one({"item_id": s["item_id"]}) or {}
        order_id = gen_order_id()
        receipt_msg_id = None
        link_msg_id = (deliver_ids[0] if deliver_ids else None)

        # Record earning for the admin
        admin_id = s.get("admin_id", OWNER_ID)
        if admin_id and amt > 0:
            record_earning(admin_id, amt, order_id, s["item_id"], s["user_id"])

        if "channel_id" in prod:
            try:
                c_orders.update_one(
                    {"user_id": s["user_id"], "channel_id": int(prod["channel_id"])},
                    {"$set": {
                        "item_id": s["item_id"], "paid_at": ts, "status": "paid",
                        "order_id": order_id, "amount": float(s.get("amount", 0.0)),
                        "admin_id": admin_id,
                        "receipt_message_id": None, "link_message_id": link_msg_id
                    }},
                    upsert=True
                )
            except Exception as e:
                log.error(f"Order upsert failed: {e}")

            try:
                receipt_text = (
                    "🧾 *Receipt*\n"
                    f"*Order ID:* `{order_id}` _(Tap to copy)_\n"
                    "\n"
                    "_Join via the link I sent . Keep this message._"
                )
                r = context.bot.send_message(s["user_id"], receipt_text, parse_mode=ParseMode.MARKDOWN)
                receipt_msg_id = r.message_id
                c_orders.update_one(
                    {"user_id": s["user_id"], "channel_id": int(prod["channel_id"])},
                    {"$set": {"receipt_message_id": receipt_msg_id}}
                )
            except Exception as e:
                log.error(f"Send receipt failed: {e}")

            if not deliver_ids:
                sup = cfg("support_contact")
                txt = "⚠️ This Channel is not available right now. Please contact support"
                txt += f" {sup}." if sup else "."
                try: context.bot.send_message(s["user_id"], txt)
                except Exception as e: log.error(f"Notify support contact failed: {e}")

        used_upi = s.get("upi_id")
        if used_upi:
            try: _bump_usage(used_upi); _bump_amount(used_upi, s.get("amount", 0.0))
            except Exception as e: log.warning(f"UPI usage/amount bump failed for {used_upi}: {e}")

        if ids_to_delete:
            context.job_queue.run_once(
                _auto_delete_messages,
                timedelta(minutes=DELETE_AFTER_MINUTES),
                context={"chat_id": s["chat_id"], "message_ids": ids_to_delete},
                name=f"del_{s['user_id']}_{int(time.time())}"
            )

        c_sessions.delete_one({"_id": s["_id"]})
        release_amount_key(akey)

# ---- Auto-approve join-requests for paid buyers ----
def on_join_request(update: Update, context: CallbackContext):
    req = update.chat_join_request
    if not req: return
    uid = req.from_user.id; ch_id = req.chat.id
    has_access = c_orders.find_one({"user_id": uid, "channel_id": ch_id})
    if has_access:
        try: context.bot.approve_chat_join_request(ch_id, uid)
        except Exception as e: log.error(f"Approve join failed: {e}")

# --- Simple stats and toggles (unchanged) ---
def stats(update, context):
    if update.effective_user.id not in ALL_ADMINS: return
    
    # Basic stats
    users = c_users.count_documents({}); 
    sessions = c_sessions.count_documents({})
    products = c_products.count_documents({})
    orders = c_orders.count_documents({})
    
    message = f"📊 *Bot Statistics*\n\n"
    message += f"• Users: `{users}`\n"
    message += f"• Pending sessions: `{sessions}`\n"
    message += f"• Products: `{products}`\n"
    message += f"• Total orders: `{orders}`\n\n"
    
    # If owner, show earnings for all admins
    if update.effective_user.id == OWNER_ID:
        message += "💰 *Admin Earnings Breakdown*\n\n"
        
        total_all_admins = 0.0
        today_all_admins = 0.0
        week_all_admins = 0.0
        
        # Get earnings for each admin
        for admin_id in ALL_ADMINS:
            earnings = get_admin_earnings(admin_id)
            total_all_admins += earnings['total']
            today_all_admins += earnings['today']
            week_all_admins += earnings['week']
            
            admin_label = "👑 Owner" if admin_id == OWNER_ID else "👤 Admin"
            message += f"{admin_label} `{admin_id}`:\n"
            message += f"  • Today: ₹{earnings['today']:.2f}\n"
            message += f"  • Week: ₹{earnings['week']:.2f}\n"
            message += f"  • Total: ₹{earnings['total']:.2f}\n\n"
        
        message += "📈 *Overall Earnings*\n"
        message += f"• Today Total: ₹{today_all_admins:.2f}\n"
        message += f"• Week Total: ₹{week_all_admins:.2f}\n"
        message += f"• Grand Total: ₹{total_all_admins:.2f}"
    
    update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

def protect_on(update, context):
    if update.effective_user.id not in ALL_ADMINS: return
    global PROTECT_CONTENT_ENABLED; PROTECT_CONTENT_ENABLED = True
    update.message.reply_text("Content protection ON.")
def protect_off(update, context):
    if update.effective_user.id not in ALL_ADMINS: return
    global PROTECT_CONTENT_ENABLED; PROTECT_CONTENT_ENABLED = False
    update.message.reply_text("Content protection OFF.")

# ---- QR timeout config (unchanged) ----
def qr_timeout_show(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS: return
    mins = cfg("qr_unpaid_delete_minutes", PAY_WINDOW_MINUTES)
    update.message.reply_text(f"QR auto-delete if unpaid: {mins} minutes.")

def set_qr_timeout(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS: return
    if not context.args: return update.message.reply_text("Usage: /set_qr_timeout <minutes>")
    try:
        mins = int(float(context.args[0]))
        if mins < 1 or mins > 180: return update.message.reply_text("Choose 1–180 minutes.")
    except Exception:
        return update.message.reply_text("Invalid number. Example: /set_qr_timeout 5")
    set_cfg("qr_unpaid_delete_minutes", mins)
    update.message.reply_text(f"QR auto-delete timeout set to {mins} minutes.")

# ---- Earnings command for admins ----
def earnings_cmd(update: Update, context: CallbackContext):
    """Show earnings for the admin who issued the command"""
    admin_id = update.effective_user.id
    if admin_id not in ALL_ADMINS:
        return
    
    earnings = get_admin_earnings(admin_id)
    
    message = (
        f"💰 *Your Earnings*\n\n"
        f"• *Today:* ₹{earnings['today']:.2f}\n"
        f"• *This Week:* ₹{earnings['week']:.2f}\n"
        f"• *Total:* ₹{earnings['total']:.2f}\n\n"
        f"_Note: Earnings are tracked from payments of products you added._"
    )
    
    update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

# ---- Product add (files) ----
GET_PRODUCT_FILES, PRICE, GET_BROADCAST_FILES, GET_BROADCAST_TEXT, BROADCAST_CONFIRM = range(5)

def add_product_start(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS: return
    context.user_data['new_files']=[]
    if update.message.effective_attachment:
        try:
            chs = get_storage_channels()
            main_id = chs[0]; backups = chs[1:]
            fwd = context.bot.forward_message(main_id, update.message.chat_id, update.message.message_id)
            rec = {"channel_id": fwd.chat_id, "message_id": fwd.message_id, "backups": []}
            for bch in backups:
                try:
                    cm = context.bot.copy_message(bch, update.message.chat_id, update.message.message_id)
                    rec["backups"].append({"channel_id": cm.chat_id, "message_id": cm.message_id})
                    time.sleep(0.1)
                except Exception as e:
                    log.error(f"Mirror to backup {bch} failed: {e}")
            context.user_data['new_files'].append(rec)
            update.message.reply_text("✅ First file added. Send more or /done.")
        except Exception as e:
            log.error(f"Store fail on first file: {e}"); update.message.reply_text("Failed to store first file.")
    else:
        update.message.reply_text("Send product files now. Use /done when finished.")
    return GET_PRODUCT_FILES

def get_product_files(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS: return GET_PRODUCT_FILES
    if not update.message.effective_attachment:
        update.message.reply_text("Not a file. Send again or /done."); return GET_PRODUCT_FILES
    try:
        chs = get_storage_channels()
        main_id = chs[0]; backups = chs[1:]
        fwd = context.bot.forward_message(main_id, update.message.chat_id, update.message.message_id)
        rec = {"channel_id": fwd.chat_id, "message_id": fwd.message_id, "backups": []}
        for bch in backups:
            try:
                cm = context.bot.copy_message(bch, update.message.chat_id, update.message.message_id)
                rec["backups"].append({"channel_id": cm.chat_id, "message_id": cm.message_id})
                time.sleep(0.1)
            except Exception as e:
                log.error(f"Mirror to backup {bch} failed: {e}")
        context.user_data['new_files'].append(rec)
        update.message.reply_text("✅ Added. Send more or /done."); return GET_PRODUCT_FILES
    except Exception as e:
        log.error(str(e)); update.message.reply_text("Store failed."); return ConversationHandler.END

def finish_adding_files(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS: return ConversationHandler.END
    if not context.user_data.get('new_files'):
        update.message.reply_text("No files yet. Send one or /cancel."); return GET_PRODUCT_FILES
    update.message.reply_text("Now send price or range (10 or 10-30)."); return PRICE

# ---- Product add (channel) ----
# Strict matcher (conversation entry), and lenient finder (for storage input):
CHANNEL_REF_RE = re.compile(r"^\s*(?:-100\d{5,}|@[\w\d_]{5,}|https?://t\.me/[\w\d_+/]+)\s*$")
CHANNEL_REF_FINDER = re.compile(r"(-100\d{5,}|@[\w\d_]{5,}|https?://t\.me/[\w\d_+/]+)")

def _get_bot_id(context: CallbackContext) -> int:
    bid = context.bot_data.get("__bot_id__")
    if bid: return bid
    me = context.bot.get_me(); context.bot_data["__bot_id__"] = me.id
    return me.id

def _resolve_channel(context: CallbackContext, ref: str):
    ref = ref.strip()
    if ref.startswith("-100") and ref[4:].isdigit():
        chat = context.bot.get_chat(int(ref))
    else:
        key = re.search(r"t\.me/([^/?\s]+)", ref).group(1) if ref.startswith("http") else ref
        chat = context.bot.get_chat(key)
    return chat.id

def _bot_is_admin(context: CallbackContext, chat_id: int) -> bool:
    try:
        bot_id = _get_bot_id(context)
        st = context.bot.get_chat_member(chat_id, bot_id).status
        return st in ("administrator","creator")
    except Exception as e:
        log.info(f"Admin check failed for {chat_id}: {e}"); return False

def add_channel_start(update: Update, context: CallbackContext):
    """Entry point for adding a *product* channel or intercepting storage-add flow."""
    if update.effective_user.id not in ALL_ADMINS: return
    text = (update.message.text or "").strip()
    # If we're awaiting a STORAGE BACKUP reference, accept lenient patterns here:
    if context.user_data.get("__await_storage_add__", False):
        m = CHANNEL_REF_FINDER.search(text)
        if not m:
            update.message.reply_text("❌ Send a valid channel — numeric `-100...`, `@username`, or `https://t.me/...`.")
            return ConversationHandler.END
        text = m.group(0)

    if not CHANNEL_REF_RE.match(text): 
        return

    # STORAGE BACKUP intercept (non-blocking auto-resync)
    if context.user_data.pop("__await_storage_add__", False):
        try:
            ch_id = _resolve_channel(context, text)
        except (BadRequest, Unauthorized) as e:
            update.message.reply_text(f"❌ I couldn't access that channel: {e}")
            return ConversationHandler.END
        if not _bot_is_admin(context, ch_id):
            update.message.reply_text("❌ I'm not an admin there. Add me and try again.")
            return ConversationHandler.END
        chs = get_storage_channels()
        if ch_id in chs:
            update.message.reply_text("Already in storage list. 🔁 Starting resync…")
        else:
            set_storage_channels(chs + [ch_id])
            update.message.reply_text(f"✅ Added `{ch_id}` as backup.\n🔁 Starting resync…", parse_mode=ParseMode.MARKDOWN)
        context.job_queue.run_once(_resync_job_to_chat, when=1, context={"chat_id": update.effective_chat.id})
        return ConversationHandler.END

    # (original product-channel add flow)
    try: ch_id = _resolve_channel(context, text)
    except (BadRequest, Unauthorized) as e:
        update.message.reply_text(f"❌ I couldn't access that channel: {e}"); return
    if not _bot_is_admin(context, ch_id):
        update.message.reply_text("❌ I'm not an admin there. Add me and try again."); return
    context.user_data.clear(); context.user_data["channel_id"] = ch_id
    update.message.reply_text("Channel recognized. Now send price or range (10 or 10-30).")
    return PRICE

def get_price(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS: return ConversationHandler.END
    t = update.message.text.strip()
    try:
        if "-" in t:
            a, b = t.split("-", 1); mn, mx = float(a), float(b); assert mx >= mn and mn >= 0
        else:
            v = float(t); assert v >= 0; mn = mx = v
    except:
        update.message.reply_text("Invalid. Send like 10 or 10-30."); return PRICE

    item_id = f"item_{int(time.time())}"
    admin_id = update.effective_user.id  # Get the admin who is adding the product
    
    if "channel_id" in context.user_data:
        doc = {
            "item_id": item_id, 
            "min_price": mn, 
            "max_price": mx, 
            "channel_id": int(context.user_data["channel_id"]),
            "added_by": admin_id  # Track which admin added this product
        }
        if mn == mx: doc["price"] = mn
        c_products.insert_one(doc)
        link = f"https://t.me/{context.bot.username}?start={item_id}"
        update.message.reply_text(f"✅ Channel product added.\nLink:\n`{link}`", parse_mode=ParseMode.MARKDOWN)
        context.user_data.clear(); return ConversationHandler.END

    if not context.user_data.get('new_files'):
        update.message.reply_text("No files yet. Send a file or /cancel."); return PRICE
    doc = {
        "item_id": item_id, 
        "min_price": mn, 
        "max_price": mx, 
        "files": context.user_data['new_files'],
        "added_by": admin_id  # Track which admin added this product
    }
    if mn == mx: doc["price"] = mn
    c_products.insert_one(doc)
    link = f"https://t.me/{context.bot.username}?start={item_id}"
    update.message.reply_text(f"✅ Product added.\nLink:\n`{link}`", parse_mode=ParseMode.MARKDOWN)
    context.user_data.clear(); return ConversationHandler.END

def cancel_conv(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS: return ConversationHandler.END
    context.user_data.clear(); update.message.reply_text("Canceled."); return ConversationHandler.END

# ---- Broadcast ----
def bc_start(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS: return
    context.user_data['b_files'] = []; context.user_data['b_text'] = None
    update.message.reply_text("Send files for broadcast. /done when finished.")
    return GET_BROADCAST_FILES

def bc_files(update, context):
    if update.effective_user.id not in ALL_ADMINS: return GET_BROADCAST_FILES
    if update.message.effective_attachment:
        context.user_data['b_files'].append(update.message); update.message.reply_text("File added. /done when finished.")
    else:
        update.message.reply_text("Send a file or /done.")
    return GET_BROADCAST_FILES

def bc_done_files(update, context):
    if update.effective_user.id not in ALL_ADMINS: return ConversationHandler.END
    update.message.reply_text("Now send the text (or /skip)."); return GET_BROADCAST_TEXT

def bc_text(update, context):
    if update.effective_user.id not in ALL_ADMINS: return ConversationHandler.END
    context.user_data['b_text'] = update.message.text; return bc_confirm(update, context)
def bc_skip(update, context): 
    if update.effective_user.id not in ALL_ADMINS: return ConversationHandler.END
    return bc_confirm(update, context)

def bc_confirm(update, context):
    if update.effective_user.id not in ALL_ADMINS: return ConversationHandler.END
    total = c_users.count_documents({})
    buttons = [[InlineKeyboardButton("✅ Send", callback_data="send_bc")],
               [InlineKeyboardButton("❌ Cancel", callback_data="cancel_bc")]]
    update.message.reply_text(f"Broadcast to {total} users. Proceed?", reply_markup=InlineKeyboardMarkup(buttons))
    return BROADCAST_CONFIRM

def bc_send(update, context):
    q = update.callback_query; q.answer(); q.edit_message_text("Broadcasting…")
    files = context.user_data.get('b_files', []); text = context.user_data.get('b_text'); ok = fail = 0
    for uid in get_all_user_ids():
        try:
            for m in files: context.bot.copy_message(uid, m.chat_id, m.message_id); time.sleep(0.1)
            if text: context.bot.send_message(uid, text)
            ok += 1
        except Exception as e: log.error(e); fail += 1
    q.message.reply_text(f"Done. Sent:{ok} Fail:{fail}"); context.user_data.clear(); return ConversationHandler.END

def on_cb(update: Update, context: CallbackContext):
    q = update.callback_query; q.answer()
    if q.data == "check_join": return check_join_cb(update, context)

# === UPI settings panes (unchanged render) ===
(UPI_ADD_UPI, UPI_ADD_MIN, UPI_ADD_MAX, UPI_ADD_LIMIT, UPI_ADD_MAIN,
 UPI_EDIT_NAME, UPI_EDIT_MIN, UPI_EDIT_MAX, UPI_EDIT_LIMIT) = range(100, 109)

def _force_status_text():
    f = cfg("force_upi")
    if f and isinstance(f, dict) and f.get("upi"):
        rt = "yes" if f.get("respect_txn") else "no"
        ra = "yes" if f.get("respect_amount") else "no"
        nm = None
        for u in get_upi_pool():
            if u.get("upi") == f["upi"]:
                nm = u.get("name"); break
        label = f"`{f['upi']}`" + (f" ({nm})" if nm else "")
        when = ""
        set_at = f.get("set_at")
        if set_at:
            try:
                dt = datetime.fromisoformat(set_at)
                if not dt.tzinfo: dt = dt.replace(tzinfo=UTC)
                when = " • set: " + dt.astimezone(IST).strftime("%Y-%m-%d %I:%M %p IST")
            except Exception: pass
        return f"*Forced UPI:* {label}  •  respect max-txns: *{rt}*  •  respect amount: *{ra}*{when}"
    return "*Forced UPI:* none"

def _render_settings_text():
    pool = get_upi_pool()
    lines = ["*Current UPI Configuration* (resets daily at 12:00 AM IST)\n"]
    sup = cfg("support_contact")
    if sup:
        lines.append(f"*Support contact:* `{sup}`")
    else:
        lines.append("*Support contact:* not set")
    lines.append("")
    lines.append(_force_status_text())
    lines.append("")
    if not pool:
        lines.append("No UPI IDs configured yet. Tap ➕ Add UPI.")
        return "\n".join(lines)
    f = cfg("force_upi")
    forced_upi = f.get("upi") if isinstance(f, dict) else None
    fr_txn = "yes" if (isinstance(f, dict) and f.get("respect_txn")) else "no"
    fr_amt = "yes" if (isinstance(f, dict) and f.get("respect_amount")) else "no"
    for i, u in enumerate(pool, 1):
        st = _refresh_state_for_today(u); used = st.get("count", 0); dmax = st.get("daily_max")
        rng  = f"{u.get('min_amt', 'none')} – {u.get('max_amt', 'none')}"
        lim_label = "none"
        if u.get("rand_min") is not None and u.get("rand_max") is not None:
            lim_label = f"{int(u['rand_min'])}-{int(u['rand_max'])} (today: {dmax if dmax is not None else '∞'})"
        elif u.get("max_txn") is not None:
            lim_label = f"{int(u['max_txn'])}"
        nm = u.get("name") or "—"
        amt_today = st.get("amt_today", 0.0); amt_yday = st.get("amt_yday", 0.0); amt_all = st.get("amt_all", 0.0)
        is_forced = (forced_upi == u['upi'])
        header = f"{i}. `{u['upi']}` {'(MAIN)' if u.get('main') else ''}{' (FORCED)' if is_forced else ''}\n"
        forced_line = (f"\n   • FORCED NOW — respect max-txns: {fr_txn}; respect amount: {fr_amt}") if is_forced else ""
        lines.append(
            header +
            f"   • name: {nm}\n"
            f"   • amount range: {rng}\n"
            f"   • daily limit: {lim_label} | used today: {used}/{dmax if dmax is not None else '∞'}\n"
            f"   • collected: today ₹{amt_today:.2f} | yesterday ₹{amt_yday:.2f} | all-time ₹{amt_all:.2f}"
            + forced_line
        )
    return "\n".join(lines)

def _settings_keyboard():
    pool = get_upi_pool()
    rows = [
        [InlineKeyboardButton("➕ Add UPI", callback_data="upi:add")],
        [InlineKeyboardButton("⚡ Force UPI", callback_data="upi:force"),
         InlineKeyboardButton("🧹 Clear Force", callback_data="upi:force_clear")],
        [InlineKeyboardButton("🆘 Support Contact", callback_data="cfg:support")],
        [InlineKeyboardButton("🔄 Reset Today Counts", callback_data="upi:reset")],
        [InlineKeyboardButton("📦 Storage Channels", callback_data="storage:menu")]
    ]
    for idx, u in enumerate(pool):
        rows.append([
            InlineKeyboardButton("⭐ Main",  callback_data=f"upi:main:{idx}"),
            InlineKeyboardButton("✏️ Edit",  callback_data=f"upi:edit:{idx}"),
            InlineKeyboardButton("🗑️ Delete",callback_data=f"upi:del:{idx}")
        ])
    return InlineKeyboardMarkup(rows)

def settings_cmd(update: Update, context: CallbackContext):
    # Only owner can access settings
    if update.effective_user.id != OWNER_ID:
        update.message.reply_text("❌ Only the bot owner can access settings.")
        return
    update.message.reply_text(_render_settings_text(), parse_mode=ParseMode.MARKDOWN, reply_markup=_settings_keyboard())

def _settings_refresh(chat_id, context):
    try:
        context.bot.send_message(chat_id, _render_settings_text(), parse_mode=ParseMode.MARKDOWN, reply_markup=_settings_keyboard())
    except Exception as e:
        log.error(f"settings refresh failed: {e}")

# === Storage settings pane ===
def _render_storage_text(context: CallbackContext):
    chs = get_storage_channels()
    titles = _storage_titles(context, chs)
    lines = ["*Storage (Database) Channels)*\n"]
    if not chs:
        lines.append("No storage channels configured.")
        return "\n".join(lines)
    main = titles[0]
    lines.append(f"• *MAIN:* `{main[0]}`" + (f" — {main[1]}" if main[1] else ""))
    if len(titles) > 1:
        lines.append("*Backups:*")
        for cid, t in titles[1:]:
            lines.append(f"  • `{cid}`" + (f" — {t}" if t else ""))
    else:
        lines.append("_No backups configured._")
    lines.append("\nUse the buttons below to add/remove/promote or resync.")
    return "\n".join(lines)

def _storage_keyboard(context: CallbackContext):
    chs = get_storage_channels()
    rows = [
        [InlineKeyboardButton("➕ Add backup", callback_data="storage:add"),
         InlineKeyboardButton("🔁 Resync All", callback_data="storage:resync")]
    ]
    for idx, cid in enumerate(chs):
        if idx == 0:
            rows.append([InlineKeyboardButton(f"⭐ Main ({cid})", callback_data="noop")])
        else:
            rows.append([
                InlineKeyboardButton(f"⭐ Make Main ({cid})", callback_data=f"storage:main:{idx}"),
                InlineKeyboardButton(f"🗑️ Remove ({cid})",    callback_data=f"storage:del:{idx}")
            ])
    return InlineKeyboardMarkup(rows)

def storage_menu_cb(update: Update, context: CallbackContext):
    # Only owner can access storage settings
    if update.effective_user.id != OWNER_ID: return
    q = update.callback_query; q.answer()
    q.message.reply_text(_render_storage_text(context), parse_mode=ParseMode.MARKDOWN, reply_markup=_storage_keyboard(context))

def storage_resync_cb(update: Update, context: CallbackContext):
    # Only owner can access storage settings
    if update.effective_user.id != OWNER_ID: return
    q = update.callback_query; q.answer("Resync started.")
    context.job_queue.run_once(_resync_job_to_chat, when=1, context={"chat_id": q.message.chat_id})

def storage_add_cb(update: Update, context: CallbackContext):
    # Only owner can access storage settings
    if update.effective_user.id != OWNER_ID: return
    q = update.callback_query; q.answer()
    context.user_data["__await_storage_add__"] = True
    q.message.reply_text("Send the *channel reference* for backup (numeric `-100...`, `@username`, or `https://t.me/...`).", parse_mode=ParseMode.MARKDOWN)

def storage_make_main_cb(update: Update, context: CallbackContext):
    # Only owner can access storage settings
    if update.effective_user.id != OWNER_ID: return
    q = update.callback_query; q.answer()
    try:
        idx = int((q.data or "").split(":")[-1])
    except Exception:
        return q.message.reply_text("Invalid selection.")
    chs = get_storage_channels()
    if not (0 <= idx < len(chs)):
        return q.message.reply_text("Invalid selection.")
    if idx == 0:
        return q.message.reply_text("Already main.")
    new_order = [chs[idx]] + chs[:idx] + chs[idx+1:]
    set_storage_channels(new_order)
    q.message.reply_text("✅ Main storage updated. 🔁 Checking copies…")
    # Safety: run a quick resync to ensure main + backups all have copies
    context.job_queue.run_once(_resync_job_to_chat, when=1, context={"chat_id": q.message.chat_id})
    q.message.reply_text(_render_storage_text(context), parse_mode=ParseMode.MARKDOWN, reply_markup=_storage_keyboard(context))

def storage_del_cb(update: Update, context: CallbackContext):
    # Only owner can access storage settings
    if update.effective_user.id != OWNER_ID: return
    q = update.callback_query; q.answer()
    try:
        idx = int((q.data or "").split(":")[-1])
    except Exception:
        return q.message.reply_text("Invalid selection.")
    chs = get_storage_channels()
    if not (0 <= idx < len(chs)):
        return q.message.reply_text("Invalid selection.")
    if idx == 0 and len(chs) > 1:
        return q.message.reply_text("❌ Cannot remove MAIN. Make another channel main first.")
    if idx == 0 and len(chs) == 1:
        return q.message.reply_text("❌ At least one storage channel is required.")
    removed = chs[idx]
    new_list = chs[:idx] + chs[idx+1:]
    set_storage_channels(new_list)
    q.message.reply_text(f"🗑️ Removed backup `{removed}`.", parse_mode=ParseMode.MARKDOWN)
    q.message.reply_text(_render_storage_text(context), parse_mode=ParseMode.MARKDOWN, reply_markup=_storage_keyboard(context))

# === Support contact capture and admin text router ===
def cfg_support_cb(update: Update, context: CallbackContext):
    # Only owner can access settings
    if update.effective_user.id != OWNER_ID: return
    q = update.callback_query; q.answer()
    context.user_data["__await_support__"] = True
    sup = cfg("support_contact")
    current = f"`{sup}`" if sup else "not set"
    q.message.reply_text(
        "Send the support contact now (e.g., `@helpdesk` or a numeric user ID). "
        "Send `clear` to remove. Current: " + current,
        parse_mode=ParseMode.MARKDOWN
    )

def admin_text_router(update: Update, context: CallbackContext):
    """Admin text router:
       0) Storage backup add (lenient capture, with auto-resync) - OWNER ONLY
       1) Support contact capture - OWNER ONLY
       2) Order ID lookup - ALL ADMINS
    """
    user_id = update.effective_user.id
    
    # Check if user is admin
    if user_id not in ALL_ADMINS:
        return

    t = (update.message.text or "").strip()

    # (0) Storage add (lenient capture; auto-resync) - OWNER ONLY
    if context.user_data.pop("__await_storage_add__", False):
        if user_id != OWNER_ID:
            update.message.reply_text("❌ Only the bot owner can modify storage settings.")
            return
        m = CHANNEL_REF_FINDER.search(t)
        if not m:
            update.message.reply_text("❌ Send a valid channel — numeric `-100...`, `@username`, or `https://t.me/...`.")
            return
        ref = m.group(0)
        try:
            ch_id = _resolve_channel(context, ref)
        except Exception as e:
            update.message.reply_text(f"❌ I couldn't access that channel: {e}")
            return
        if not _bot_is_admin(context, ch_id):
            update.message.reply_text("❌ I'm not an admin in that channel. Add me and try again.")
            return
        chs = get_storage_channels()
        if ch_id in chs:
            update.message.reply_text("Already in storage list. 🔁 Starting resync…")
        else:
            set_storage_channels(chs + [ch_id])
            update.message.reply_text(f"✅ Added `{ch_id}` as backup.\n🔁 Starting resync…", parse_mode=ParseMode.MARKDOWN)
        context.job_queue.run_once(_resync_job_to_chat, when=1, context={"chat_id": update.effective_chat.id})
        return

    # (1) Support contact setter - OWNER ONLY
    if context.user_data.pop("__await_support__", False):
        if user_id != OWNER_ID:
            update.message.reply_text("❌ Only the bot owner can modify support contact.")
            return
        if t.lower() in ("clear", "none", "-", "remove"):
            set_cfg("support_contact", None)
            update.message.reply_text("✅ Support contact cleared.")
        else:
            set_cfg("support_contact", t)
            update.message.reply_text(f"✅ Support contact set to: {t}")
        return

    # (2) Admin receipt lookup by Order ID - ALL ADMINS
    m = ORDER_ID_PATTERN.search(t)
    if not m:
        return
    oid = m.group(0)

    doc = c_orders.find_one({"order_id": oid})
    if not doc:
        update.message.reply_text("❌ Order not found for that ID.")
        return

    ch_title_stored = (doc.get("channel_title_at_purchase") or "").strip() or None
    ch_id = doc.get("channel_id")
    ch_title_live = None
    if not ch_title_stored and ch_id:
        try:
            ch = context.bot.get_chat(ch_id)
            ch_title_live = ch.title or None
        except Exception:
            ch_title_live = None

    ch_title = ch_title_stored or ch_title_live or None
    ch_label = f"`{ch_title}`" if ch_title else "Unknown"

    paid_at = doc.get("paid_at") or datetime.now(UTC)
    if not paid_at.tzinfo:
        paid_at = paid_at.replace(tzinfo=UTC)
    dt_ist = paid_at.astimezone(IST).strftime("%Y-%m-%d %I:%M %p IST")
    amt_disp = fmt_inr(float(doc.get("amount", 0.0)))
    uid = doc.get("user_id")
    admin_id = doc.get("admin_id", "Unknown")

    txt = (
        "🧾 *Admin check receipt*\n\n"
        f"*Order ID:* `{oid}`\n"
        f"*Date:* {dt_ist}\n"
        f"*Amount:* ₹{amt_disp}\n"
        f"*Channel:* " + (f"`{ch_title}` (`{ch_id}`)" if ch_id else "Unknown") +
        f"\n*Buyer ID:* `{uid}`" +
        f"\n*Admin ID:* `{admin_id}`"
    )
    m = update.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)
    context.job_queue.run_once(
        _auto_delete_messages,
        timedelta(minutes=10),
        context={"chat_id": m.chat_id, "message_ids": [m.message_id]},
        name=f"admrec_{int(time.time())}"
    )

# === UPI add/edit flows (unchanged) ===
def addupi_cmd(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return
    context.user_data.clear()
    context.user_data["__mode__"] = "add"
    update.message.reply_text("Send the UPI ID to add (e.g., dexar@slc).")
    return UPI_ADD_UPI

def addupi_cb_entry(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return ConversationHandler.END
    q = update.callback_query; q.answer()
    context.user_data.clear(); context.user_data["__mode__"]="add"
    q.message.reply_text("Send the UPI ID to add (e.g., dexar@slc).")
    return UPI_ADD_UPI

def upi_add__upi(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return ConversationHandler.END
    upi = (update.message.text or "").strip()
    if not upi or "@" not in upi:
        update.message.reply_text("Send a valid UPI ID (looks like name@bank).")
        return UPI_ADD_UPI
    context.user_data["new_upi"] = upi
    update.message.reply_text("Send *minimum amount* or `none`.", parse_mode=ParseMode.MARKDOWN)
    return UPI_ADD_MIN

def upi_add__min(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return ConversationHandler.END
    t = (update.message.text or "").strip().lower()
    val = None if t in ("none","-","na","n/a") else t
    try:
        context.user_data["min_amt"] = (None if val is None else float(val))
    except:
        update.message.reply_text("Invalid. Send a number or `none`.")
        return UPI_ADD_MIN
    update.message.reply_text("Send *maximum amount* or `none`.", parse_mode=ParseMode.MARKDOWN)
    return UPI_ADD_MAX

def upi_add__max(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return ConversationHandler.END
    t = (update.message.text or "").strip().lower()
    val = None if t in ("none","-","na","n/a") else t
    try:
        max_amt = (None if val is None else float(val))
    except:
        update.message.reply_text("Invalid. Send a number or `none`.")
        return UPI_ADD_MAX
    context.user_data["max_amt"] = max_amt
    update.message.reply_text("Send *daily transaction limit*:\n• `none` (no cap)\n• `7` (fixed)\n• `5-10` (random daily pick)", parse_mode=ParseMode.MARKDOWN)
    return UPI_ADD_LIMIT

def upi_add__limit(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return ConversationHandler.END
    t = (update.message.text or "").strip().lower().replace(" ", "")
    mx = None; rmin = None; rmax = None
    try:
        if t in ("none","-","na","n/a"):
            pass
        elif "-" in t:
            a,b = t.split("-",1); rmin = int(float(a)); rmax = int(float(b))
            if rmax < rmin: rmin, rmax = rmax, rmin
        else:
            mx = int(float(t));  mx = max(mx, 0)
    except:
        update.message.reply_text("Invalid. Send `none`, a number like `5`, or a range like `5-10`.")
        return UPI_ADD_LIMIT

    context.user_data["max_txn"] = mx
    context.user_data["rand_min"] = rmin
    context.user_data["rand_max"] = rmax
    update.message.reply_text("Make this the *MAIN* UPI? Reply `yes` or `no`.", parse_mode=ParseMode.MARKDOWN)
    return UPI_ADD_MAIN

def upi_add__main(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return ConversationHandler.END
    ans = (update.message.text or "").strip().lower()
    make_main = ans in ("y","yes","true","1")
    new_upi = context.user_data.get("new_upi")
    if not new_upi:
        update.message.reply_text("Session expired. Please run /addupi again.")
        return ConversationHandler.END
    pool = get_upi_pool()
    entry = {"upi": new_upi, "name": None,
             "min_amt": context.user_data.get("min_amt"),
             "max_amt": context.user_data.get("max_amt"),
             "max_txn": context.user_data.get("max_txn"),
             "rand_min": context.user_data.get("rand_min"),
             "rand_max": context.user_data.get("rand_max"),
             "main": make_main}
    pool.append(entry); set_upi_pool(pool); context.user_data.clear()
    update.message.reply_text(f"Added `{entry['upi']}`.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

def edit_cb_entry(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return ConversationHandler.END
    q = update.callback_query; q.answer()
    parts = (q.data or "").split(":")
    try: idx = int(parts[2])
    except Exception:
        q.message.reply_text("Invalid selection."); return ConversationHandler.END
    pool = get_upi_pool()
    if idx < 0 or idx >= len(pool):
        q.message.reply_text("Invalid selection."); return ConversationHandler.END
    context.user_data.clear(); context.user_data["__mode__"] = "edit"; context.user_data["edit_idx"] = idx
    u = pool[idx]
    q.message.reply_text(
        f"Editing `{u['upi']}`.\nSend *display name* or type `skip` to leave unchanged (current: {(u.get('name') or '—')}).",
        parse_mode=ParseMode.MARKDOWN
    ); return UPI_EDIT_NAME

def upi_edit__name(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return ConversationHandler.END
    pool = get_upi_pool(); idx = context.user_data.get("edit_idx", -1)
    if idx < 0 or idx >= len(pool):
        update.message.reply_text("Edit session expired. Try /settings again."); return ConversationHandler.END
    t = (update.message.text or "").strip()
    if t.lower() != "skip": pool[idx]["name"] = t if t else None; set_upi_pool(pool)
    update.message.reply_text(f"Send *minimum amount* or `none` (current: {pool[idx].get('min_amt','none')}).", parse_mode=ParseMode.MARKDOWN)
    return UPI_EDIT_MIN

def upi_edit__min(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return ConversationHandler.END
    pool = get_upi_pool(); idx = context.user_data.get("edit_idx", -1)
    if idx < 0 or idx >= len(pool):
        update.message.reply_text("Edit session expired. Try /settings again."); return ConversationHandler.END
    t = (update.message.text or "").strip().lower()
    try: pool[idx]["min_amt"] = None if t in ("none","-","na","n/a") else float(t)
    except:
        update.message.reply_text("Invalid. Send a number or `none`."); return UPI_EDIT_MIN
    set_upi_pool(pool)
    update.message.reply_text(f"Send *maximum amount* or `none` (current: {pool[idx].get('max_amt','none')}).", parse_mode=ParseMode.MARKDOWN)
    return UPI_EDIT_MAX

def upi_edit__max(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return ConversationHandler.END
    pool = get_upi_pool(); idx = context.user_data.get("edit_idx", -1)
    if idx < 0 or idx >= len(pool):
        update.message.reply_text("Edit session expired. Try /settings again."); return ConversationHandler.END
    t = (update.message.text or "").strip().lower()
    try: pool[idx]["max_amt"] = None if t in ("none","-","na","n/a") else float(t)
    except:
        update.message.reply_text("Invalid. Send a number or `none`."); return UPI_EDIT_MAX
    set_upi_pool(pool)
    update.message.reply_text("Send *daily transaction limit*:\n• `none`\n• `7`\n• `5-10`", parse_mode=ParseMode.MARKDOWN)
    return UPI_EDIT_LIMIT

def upi_edit__limit(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return ConversationHandler.END
    pool = get_upi_pool(); idx = context.user_data.get("edit_idx", -1)
    if idx < 0 or idx >= len(pool):
        update.message.reply_text("Edit session expired. Try /settings again."); return ConversationHandler.END
    t = (update.message.text or "").strip().lower().replace(" ", "")
    mx = None; rmin = None; rmax = None
    try:
        if t in ("none","-","na","n/a"): pass
        elif "-" in t:
            a,b = t.split("-",1); rmin = int(float(a)); rmax = int(float(b))
            if rmax < rmin: rmin, rmax = rmax, rmin
        else:
            mx = int(float(t)); mx = max(mx, 0)
    except:
        update.message.reply_text("Invalid. Send `none`, `5`, or `5-10`."); return UPI_EDIT_LIMIT
    pool[idx]["max_txn"] = mx; pool[idx]["rand_min"] = rmin; pool[idx]["rand_max"] = rmax
    set_upi_pool(pool); context.user_data.clear(); update.message.reply_text("Updated."); return ConversationHandler.END

def upi_cb(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return
    q = update.callback_query; q.answer()
    data = q.data or ""; pool = get_upi_pool()

    if data == "upi:reset":
        for u in pool:
            st = _refresh_state_for_today(u)
            c_upi_state.update_one({"upi": u["upi"]}, {"$set": {"count": 0, "amt_today": 0.0}}, upsert=True)
        q.message.reply_text("✅ Today's counts reset."); return
    if data == "upi:force_clear":
        set_cfg("force_upi", None); q.message.reply_text("✅ Force UPI cleared."); return
    if data == "upi:force":
        q.message.reply_text("Send the UPI you want to force **as a reply** to this message.\n\nFormat: `upi: <upi_id>`\nOptional flags: `respect_txn=1`, `respect_amount=1`", parse_mode=ParseMode.MARKDOWN); return
    if data.startswith("upi:main:"):
        idx = int(data.split(":")[-1])
        if 0 <= idx < len(pool):
            for i, u in enumerate(pool): u["main"] = (i == idx); set_upi_pool(pool)
            q.message.reply_text(f"✅ `{pool[idx]['upi']}` set as MAIN.", parse_mode=ParseMode.MARKDOWN)
        return
    if data.startswith("upi:del:"):
        idx = int(data.split(":")[-1])
        if 0 <= idx < len(pool):
            removed = pool.pop(idx); set_upi_pool(pool)
            q.message.reply_text(f"🗑️ Deleted `{removed['upi']}`.", parse_mode=ParseMode.MARKDOWN)
        return

def cmd_start(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    add_user(uid, update.effective_user.username)
    msg = update.message or (update.callback_query and update.callback_query.message)
    chat_id = msg.chat_id
    if context.args: return start_purchase(context, chat_id, uid, context.args[0])
    photo = cfg("welcome_photo_id"); text = cfg("welcome_text", "Welcome!")
    (msg.reply_photo(photo=photo, caption=text) if photo else msg.reply_text(text))

def main():
    # default seeds
    set_cfg("welcome_text", cfg("welcome_text", "Welcome!"))
    set_cfg("force_sub_text", cfg("force_sub_text", "Join required channels to continue."))
    if cfg("qr_unpaid_delete_minutes") is None: set_cfg("qr_unpaid_delete_minutes", PAY_WINDOW_MINUTES)
    if cfg("upi_pool") is None:
        set_upi_pool([{"upi": UPI_ID, "name": None, "min_amt": None, "max_amt": None, "max_txn": None,
                       "rand_min": None, "rand_max": None, "main": True}])
    if cfg("force_upi") is None: set_cfg("force_upi", None)

    # seed storage channel list if missing
    if not cfg("storage_channels"):
        set_storage_channels([int(STORAGE_CHANNEL_ID)])

    # clear webhook (if any)
    os.system(f'curl -s "https://api.telegram.org/bot{TOKEN}/deleteWebhook" >/dev/null')

    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher
    admin = Filters.user(ALL_ADMINS)
    owner = Filters.user(OWNER_ID)

    # Add product flows
    add_conv = ConversationHandler(
        entry_points=[MessageHandler((Filters.document | Filters.video | Filters.photo) & admin, add_product_start)],
        states={
            GET_PRODUCT_FILES: [MessageHandler((Filters.document | Filters.video | Filters.photo) & ~Filters.command, get_product_files),
                               CommandHandler('done', finish_adding_files, filters=admin)],
            PRICE: [MessageHandler(Filters.text & ~Filters.command, get_price)]
        },
        fallbacks=[CommandHandler('cancel', cancel_conv, filters=admin)]
    )

    add_channel_conv = ConversationHandler(
        entry_points=[MessageHandler(Filters.regex(CHANNEL_REF_RE) & ~Filters.command & admin, add_channel_start)],
        states={PRICE: [MessageHandler(Filters.text & ~Filters.command, get_price)]},
        fallbacks=[CommandHandler('cancel', cancel_conv, filters=admin)],
        name="add_channel_conv",
        persistent=False
    )

    dp.add_handler(add_conv, group=0)
    dp.add_handler(add_channel_conv, group=0)

    # Broadcast & misc
    dp.add_handler(CommandHandler("broadcast", bc_start, filters=admin))
    dp.add_handler(CommandHandler("start", cmd_start))
    dp.add_handler(CommandHandler("stats", stats, filters=admin))
    dp.add_handler(CommandHandler("earning", earnings_cmd, filters=admin))  # New earnings command
    dp.add_handler(CommandHandler("qr_timeout", qr_timeout_show, filters=admin))
    dp.add_handler(CommandHandler("set_qr_timeout", set_qr_timeout, filters=admin))
    dp.add_handler(CommandHandler("protect_on", protect_on, filters=admin))
    dp.add_handler(CommandHandler("protect_off", protect_off, filters=admin))
    dp.add_handler(CallbackQueryHandler(on_cb, pattern="^(check_join)$"))

    # Payments + join requests
    dp.add_handler(MessageHandler(Filters.update.channel_post & Filters.chat(PAYMENT_NOTIF_CHANNEL_ID) & Filters.text, on_channel_post))
    dp.add_handler(ChatJoinRequestHandler(on_join_request))

    # Settings and UPI management (owner only)
    dp.add_handler(CommandHandler("settings", settings_cmd, filters=owner))
    dp.add_handler(CommandHandler("addupi", addupi_cmd, filters=owner))

    # UPI add/edit flows and buttons (owner only)
    add_upi_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(addupi_cb_entry, pattern=r"^upi:add$"), CommandHandler("addupi", addupi_cmd, filters=owner)],
        states={
            UPI_ADD_UPI:   [MessageHandler(Filters.text & ~Filters.command, upi_add__upi)],
            UPI_ADD_MIN:   [MessageHandler(Filters.text & ~Filters.command, upi_add__min)],
            UPI_ADD_MAX:   [MessageHandler(Filters.text & ~Filters.command, upi_add__max)],
            UPI_ADD_LIMIT: [MessageHandler(Filters.text & ~Filters.command, upi_add__limit)],
            UPI_ADD_MAIN:  [MessageHandler(Filters.text & ~Filters.command, upi_add__main)],
        },
        fallbacks=[CommandHandler('cancel', cancel_conv, filters=owner)],
        name="add_upi_conv",
        persistent=False
    )
    dp.add_handler(add_upi_conv, group=0)

    edit_upi_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_cb_entry, pattern=r"^upi:edit:\d+$")],
        states={
            UPI_EDIT_NAME: [MessageHandler(Filters.text & ~Filters.command, upi_edit__name)],
            UPI_EDIT_MIN:  [MessageHandler(Filters.text & ~Filters.command, upi_edit__min)],
            UPI_EDIT_MAX:  [MessageHandler(Filters.text & ~Filters.command, upi_edit__max)],
            UPI_EDIT_LIMIT:[MessageHandler(Filters.text & ~Filters.command, upi_edit__limit)],
        },
        fallbacks=[CommandHandler('cancel', cancel_conv, filters=owner)],
        name="edit_upi_conv",
        persistent=False
    )
    dp.add_handler(edit_upi_conv, group=0)

    # Config buttons & admin-text router
    dp.add_handler(CallbackQueryHandler(cfg_support_cb, pattern=r"^cfg:support$"), group=1)
    dp.add_handler(CallbackQueryHandler(upi_cb, pattern=r"^upi:(reset|force|force_clear|main:\d+|del:\d+)$"), group=1)

    # Storage management UI (owner only)
    dp.add_handler(CallbackQueryHandler(storage_menu_cb, pattern=r"^storage:menu$"), group=1)
    dp.add_handler(CallbackQueryHandler(storage_add_cb, pattern=r"^storage:add$"), group=1)
    dp.add_handler(CallbackQueryHandler(storage_resync_cb, pattern=r"^storage:resync$"), group=1)
    dp.add_handler(CallbackQueryHandler(storage_make_main_cb, pattern=r"^storage:main:\d+$"), group=1)
    dp.add_handler(CallbackQueryHandler(storage_del_cb, pattern=r"^storage:del:\d+$"), group=1)

    # Text router (also handles storage add leniently)
    dp.add_handler(MessageHandler(Filters.text & admin, admin_text_router), group=2)

    # Startup auto-resync (non-blocking)
    updater.job_queue.run_once(_resync_all_job, when=2)

    logging.info("Bot running…"); updater.start_polling(); updater.idle()

if __name__ == "__main__":
    main()
