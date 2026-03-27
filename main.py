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

import os, logging, time, random, re, unicodedata, html, threading, queue
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from telegram import Update, ParseMode, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import (
    Updater, CommandHandler, MessageHandler, Filters, CallbackContext,
    ConversationHandler, CallbackQueryHandler, ChatJoinRequestHandler,
    DispatcherHandlerStop
)
from telegram.error import BadRequest, Unauthorized, RetryAfter, TimedOut, NetworkError
from telegram.utils.request import Request

from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

logging.basicConfig(format="%(asctime)s %(levelname)s:%(name)s: %(message)s", level=logging.INFO)
log = logging.getLogger("upi-mongo-bot")

# === Bot token (updated as requested) ===
TOKEN = "7303696543:AAEzn6YVIhNmDz6jeKkW9vRfrpzoylIsU78"

# === Owner and Admins (updated as requested) ===
OWNER_ID = 7381642564  # Owner has full access including /settings
ADMIN_IDS = [5860915865, 7223414109, 6053105336, 7748361879, 7276257621]  # Admins have all features except /settings
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
SPECIAL_POST_DELETE_MINUTES = 30
SPECIAL_POST_REPEAT_HOURS = 6
SPECIAL_POST_DEFAULT_REPEAT_CYCLES = 15
SPECIAL_POST_SWEEP_BATCH_SIZE = 20

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
c_broadcast_deletes = mdb["broadcast_deletes"]
c_special_post_cycles = mdb["special_post_cycles"]

# Free broadcast tuning. Keep below Telegram's free global send cap.
BROADCAST_FREE_MESSAGES_PER_SEC = 28.0
BROADCAST_WORKERS = 16
BROADCAST_PROGRESS_UPDATE_SECONDS = 5.0
BROADCAST_REQUEST_TIMEOUT = 20.0
BROADCAST_DELETE_BATCH_SIZE = 20

_broadcast_state_lock = threading.Lock()
_broadcast_state = {"active": False}

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
c_broadcast_deletes.create_index([("delete_at", ASCENDING)])
c_special_post_cycles.create_index([("user_id", ASCENDING), ("item_id", ASCENDING)], unique=True)
c_special_post_cycles.create_index([("next_send_at", ASCENDING)])

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


# === Bot start-link (admin edit) helpers ===
BOT_START_LINK_RE = re.compile(
    r"(?:https?://)?(?:t|telegram)\.me/([A-Za-z0-9_]{5,})/?(?:\?start=([A-Za-z0-9_:-]+))",
    re.I,
)

def _clear_edit_link_state(context: CallbackContext):
    for k in (
        "__edit_link_item_id__", "__edit_link_prod__", "__edit_link_changes__",
        "__await_edit_channel__", "__await_edit_price__", "__await_edit_files__",
        "__await_edit_free_file_text__",
        "__edit_new_files__"
    ):
        context.user_data.pop(k, None)

def _clear_owner_settings_capture_state(context: CallbackContext):
    for k in (
        "__await_support__",
        "__await_storage_add__",
        "__await_start_post__",
        "__await_start_message_delete__",
        "__await_default_free_file_text__",
    ):
        context.user_data.pop(k, None)

def _clear_special_post_state(context: CallbackContext):
    for k in (
        "__await_special_post__",
        "__await_special_post_cycles_manual__",
        "__await_special_post_repeat_minutes__",
        "__await_special_post_delete_minutes__",
        "__special_post_item_id__",
        "__special_post_record__",
        "__special_post_cycles__",
        "__special_post_repeat_minutes__",
        "__special_post_delete_minutes__",
    ):
        context.user_data.pop(k, None)

def _is_editable_link_product(prod: dict) -> bool:
    return bool(prod) and ("channel_id" in prod or "files" in prod)

def _is_special_post_product(prod: dict) -> bool:
    return bool(_normalize_stored_message((prod or {}).get("special_post_record")))

def _special_post_cycle_count(value=None) -> int:
    if value is None:
        value = SPECIAL_POST_DEFAULT_REPEAT_CYCLES
    try:
        cycles = int(value)
    except Exception:
        cycles = SPECIAL_POST_DEFAULT_REPEAT_CYCLES
    return max(cycles, 0)

def _special_post_repeat_hours_value(value=None) -> int:
    if value is None:
        value = SPECIAL_POST_REPEAT_HOURS
    try:
        hours = int(float(value))
    except Exception:
        hours = SPECIAL_POST_REPEAT_HOURS
    return max(hours, 1)

def _special_post_repeat_minutes_value(value=None) -> int:
    if value is None:
        value = SPECIAL_POST_REPEAT_HOURS * 60
    try:
        minutes = int(float(value))
    except Exception:
        minutes = SPECIAL_POST_REPEAT_HOURS * 60
    return max(minutes, 1)

def _special_post_delete_minutes_value(value=None) -> int:
    if value is None:
        value = SPECIAL_POST_DELETE_MINUTES
    try:
        minutes = int(float(value))
    except Exception:
        minutes = SPECIAL_POST_DELETE_MINUTES
    return max(minutes, 1)

def _special_post_repeat_minutes(prod: dict = None) -> int:
    prod = prod or {}
    if prod.get("special_post_repeat_minutes") is not None:
        return _special_post_repeat_minutes_value(prod.get("special_post_repeat_minutes"))
    if prod.get("special_post_repeat_hours") is not None:
        return _special_post_repeat_minutes_value(_special_post_repeat_hours_value(prod.get("special_post_repeat_hours")) * 60)
    return _special_post_repeat_minutes_value()

def _special_post_delete_minutes(prod: dict = None) -> int:
    return _special_post_delete_minutes_value((prod or {}).get("special_post_delete_minutes"))

def _special_post_repeat_minutes_from_state(context: CallbackContext) -> int:
    return _special_post_repeat_minutes_value(context.user_data.get("__special_post_repeat_minutes__"))

def _special_post_delete_minutes_from_state(context: CallbackContext) -> int:
    return _special_post_delete_minutes_value(context.user_data.get("__special_post_delete_minutes__"))

def _special_post_builder_text(context: CallbackContext) -> str:
    rec = _normalize_stored_message(context.user_data.get("__special_post_record__"))
    cycles = _special_post_cycle_count(context.user_data.get("__special_post_cycles__"))
    repeat_minutes = _special_post_repeat_minutes_from_state(context)
    delete_minutes = _special_post_delete_minutes_from_state(context)
    item_id = (context.user_data.get("__special_post_item_id__") or "").strip()
    lines = [f"<b>{'Edit' if item_id else 'Create'} Special Post</b>"]
    if item_id:
        lines.append(f"<b>Item:</b> <code>{html.escape(item_id)}</code>")
    lines.append(f"<b>Post:</b> {'captured ✅' if rec else 'waiting for your post'}")
    lines.append("<b>Price:</b> Free / instant delivery")
    lines.append(f"<b>Repeat cycles:</b> {cycles}")
    lines.append(f"<b>Resend timer:</b> every {repeat_minutes} minute{'s' if repeat_minutes != 1 else ''}")
    lines.append(f"<b>Delete timer:</b> {delete_minutes} minute{'s' if delete_minutes != 1 else ''}")
    lines.append("Photo, video, caption, text, and embedded hyperlinks are preserved.")
    lines.append("")
    lines.append("Use the buttons below to change cycles and timers.")
    lines.append("")
    if rec:
        lines.append("Send another post to replace the current one, or press Save.")
    else:
        lines.append("Send or forward the post now.")
    return "\n".join(lines)

def _special_post_builder_keyboard(context: CallbackContext):
    cycles = _special_post_cycle_count(context.user_data.get("__special_post_cycles__"))
    repeat_minutes = _special_post_repeat_minutes_from_state(context)
    delete_minutes = _special_post_delete_minutes_from_state(context)
    has_post = bool(_normalize_stored_message(context.user_data.get("__special_post_record__")))
    is_edit = bool((context.user_data.get("__special_post_item_id__") or "").strip())
    rows = [[
        InlineKeyboardButton(f"🔁 Cycles: {cycles}", callback_data="specialpost:cycles"),
        InlineKeyboardButton(f"⏱ Resend: {repeat_minutes}m", callback_data="specialpost:resend"),
    ], [
        InlineKeyboardButton(f"🗑 Delete: {delete_minutes}m", callback_data="specialpost:delete"),
    ]]
    if has_post:
        rows.append([InlineKeyboardButton("✅ Save" if is_edit else "✅ Create Link", callback_data="specialpost:save")])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="specialpost:cancel")])
    return InlineKeyboardMarkup(rows)

def _special_post_cycles_text(context: CallbackContext) -> str:
    cycles = _special_post_cycle_count(context.user_data.get("__special_post_cycles__"))
    repeat_minutes = _special_post_repeat_minutes_from_state(context)
    delete_minutes = _special_post_delete_minutes_from_state(context)
    return (
        "<b>Special Post Repeat Cycles</b>\n"
        f"Current: <b>{cycles}</b>\n"
        f"After the first free delivery, the same post is sent again every {repeat_minutes} minute{'s' if repeat_minutes != 1 else ''}.\n"
        f"Each copy auto-deletes after {delete_minutes} minute{'s' if delete_minutes != 1 else ''}."
    )

def _special_post_cycles_keyboard(context: CallbackContext):
    current = _special_post_cycle_count(context.user_data.get("__special_post_cycles__"))

    def _label(n: int) -> str:
        return f"✅ {n}" if n == current else str(n)

    rows = [
        [
            InlineKeyboardButton(_label(1), callback_data="specialpost:setcycles:1"),
            InlineKeyboardButton(_label(5), callback_data="specialpost:setcycles:5"),
            InlineKeyboardButton(_label(10), callback_data="specialpost:setcycles:10"),
        ],
        [
            InlineKeyboardButton(_label(15), callback_data="specialpost:setcycles:15"),
            InlineKeyboardButton(_label(20), callback_data="specialpost:setcycles:20"),
            InlineKeyboardButton(_label(30), callback_data="specialpost:setcycles:30"),
        ],
        [InlineKeyboardButton("⌨️ Set Manually", callback_data="specialpost:cycles_manual")],
        [InlineKeyboardButton("⬅️ Back", callback_data="specialpost:back")],
    ]
    return InlineKeyboardMarkup(rows)

def _open_special_post_builder(update: Update, context: CallbackContext, prod: dict = None, item_id: str = None):
    _clear_owner_settings_capture_state(context)
    _clear_special_post_state(context)
    context.user_data["__await_special_post__"] = True
    context.user_data["__special_post_cycles__"] = _special_post_cycle_count((prod or {}).get("special_post_cycles"))
    context.user_data["__special_post_repeat_minutes__"] = _special_post_repeat_minutes(prod)
    context.user_data["__special_post_delete_minutes__"] = _special_post_delete_minutes(prod)
    if item_id:
        context.user_data["__special_post_item_id__"] = item_id
    if prod and _is_special_post_product(prod):
        context.user_data["__special_post_record__"] = _normalize_stored_message(prod.get("special_post_record"))

    target_msg = update.message or (update.callback_query and update.callback_query.message)
    if not target_msg:
        return
    target_msg.reply_text(
        _special_post_builder_text(context),
        parse_mode=ParseMode.HTML,
        reply_markup=_special_post_builder_keyboard(context),
        disable_web_page_preview=True,
    )

def _open_edit_link_menu(update: Update, context: CallbackContext, prod: dict, item_id: str):
    _clear_edit_link_state(context)
    context.user_data["__edit_link_item_id__"] = item_id
    context.user_data["__edit_link_changes__"] = {}
    txt, kb = _edit_link_menu(prod, {})
    update.message.reply_text(
        txt,
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
        disable_web_page_preview=True,
    )
    raise DispatcherHandlerStop()

def _free_file_text_state(prod: dict, changes: dict = None):
    saved_free_file_text = (prod.get("free_file_text") or "").strip()
    default_free_file_text = (cfg("default_free_file_text") or "").strip()
    has_pending = isinstance(changes, dict) and "free_file_text" in changes

    previous_free_file_text = saved_free_file_text or default_free_file_text
    previous_source = "custom" if saved_free_file_text else ("default" if default_free_file_text else "off")

    pending_free_file_text = None
    effective_free_file_text = previous_free_file_text
    effective_source = previous_source

    if has_pending:
        pending_free_file_text = (changes.get("free_file_text") or "").strip()
        if pending_free_file_text:
            effective_free_file_text = pending_free_file_text
            effective_source = "custom"
        elif default_free_file_text:
            effective_free_file_text = default_free_file_text
            effective_source = "default"
        else:
            effective_free_file_text = ""
            effective_source = "off"

    return {
        "previous_text": previous_free_file_text,
        "previous_source": previous_source,
        "pending_text": pending_free_file_text,
        "has_pending": has_pending,
        "effective_text": effective_free_file_text,
        "effective_source": effective_source,
        "default_text": default_free_file_text,
    }

def _free_file_text_preview(text: str, limit: int = 280) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    if len(text) > limit:
        text = text[: limit - 3].rstrip() + "..."
    return f"<pre>{html.escape(text)}</pre>"

def _free_file_text_alert_text(text: str, source: str = "custom", limit: int = 180) -> str:
    text = (text or "").strip()
    if not text:
        return "Previous text: OFF"
    compact = " ".join(text.split())
    if len(compact) > limit:
        compact = compact[: limit - 3].rstrip() + "..."
    if source == "default":
        return f"Previous default text:\n{compact}"
    return f"Previous text:\n{compact}"

def _edit_link_menu(prod: dict, changes: dict):
    is_channel = "channel_id" in prod
    cur_mn = prod.get("min_price"); cur_mx = prod.get("max_price")
    cur_price = cur_mn if (cur_mn is not None and cur_mx is not None and float(cur_mn) == float(cur_mx)) else None

    # effective values after pending changes
    eff_mn = changes.get("min_price", cur_mn)
    eff_mx = changes.get("max_price", cur_mx)
    eff_price = eff_mn if (eff_mn is not None and eff_mx is not None and float(eff_mn) == float(eff_mx)) else None

    lines = []
    lines.append("✏️ <b>Edit your link</b>")
    lines.append(f"<b>Item:</b> <code>{html.escape(str(prod.get('item_id')))}</code>")
    lines.append(f"<b>Type:</b> {'Channel' if is_channel else 'Files'}")

    if is_channel:
        cur_ch = prod.get("channel_id")
        eff_ch = changes.get("channel_id", cur_ch)
        lines.append(f"<b>Channel ID:</b> <code>{eff_ch}</code>" + (" ✅" if eff_ch == cur_ch else " ✨"))
    else:
        cur_n = len(prod.get("files") or [])
        eff_n = len(changes.get("files") or prod.get("files") or [])
        lines.append(f"<b>Files:</b> {eff_n}" + (" ✅" if eff_n == cur_n else " ✨"))
        free_text_state = _free_file_text_state(prod, changes)
        eff_free_file_text = free_text_state["effective_text"]
        free_text_changed = free_text_state["has_pending"]
        free_text_status = "set" if eff_free_file_text else "OFF"
        if eff_free_file_text and free_text_state["effective_source"] == "default":
            free_text_status += " (default)"
        lines.append(
            "<b>Free File Text:</b> "
            + free_text_status
            + (" ✨" if free_text_changed else " ✅")
            + " <i>(free file links only)</i>"
        )
        if eff_free_file_text:
            preview_label = "<b>Text Preview:</b>"
            if free_text_changed:
                preview_label = "<b>Pending Text Preview:</b>"
            elif free_text_state["effective_source"] == "default":
                preview_label = "<b>Default Text Preview:</b>"
            lines.append(preview_label)
            lines.append(_free_file_text_preview(eff_free_file_text))

    if eff_mn is None or eff_mx is None:
        lines.append("<b>Price:</b> Not set")
    else:
        if eff_price is not None:
            lines.append(f"<b>Price:</b> ₹{fmt_inr(float(eff_price))}" + (" ✅" if cur_price == eff_price else " ✨"))
        else:
            lines.append(
                f"<b>Price:</b> ₹{fmt_inr(float(eff_mn))}-₹{fmt_inr(float(eff_mx))}"
                + (" ✅" if (cur_mn, cur_mx) == (eff_mn, eff_mx) else " ✨")
            )

    # buttons
    btns = []
    if is_channel:
        btns.append([
            InlineKeyboardButton("🔁 Change Channel", callback_data="editln:ch"),
            InlineKeyboardButton("✅ Keep Channel", callback_data="editln:ch_keep"),
        ])
    else:
        btns.append([
            InlineKeyboardButton("🔁 Change Files", callback_data="editln:files"),
            InlineKeyboardButton("✅ Keep Files", callback_data="editln:files_keep"),
        ])
        btns.append([
            InlineKeyboardButton("📝 Free File Text", callback_data="editln:freetext"),
            InlineKeyboardButton("🗑 Clear Text", callback_data="editln:freetext_clear"),
        ])

    btns.append([
        InlineKeyboardButton("💰 Change Price", callback_data="editln:price"),
        InlineKeyboardButton("✅ Keep Price", callback_data="editln:price_keep"),
    ])
    btns.append([
        InlineKeyboardButton("✅ Update Link", callback_data="editln:save"),
        InlineKeyboardButton("❌ Cancel", callback_data="editln:cancel"),
    ])
    return "\n".join(lines), InlineKeyboardMarkup(btns)

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

def _normalize_stored_message(rec):
    if not isinstance(rec, dict):
        return None
    try:
        primary_ch = int(rec.get("channel_id"))
        primary_mid = int(rec.get("message_id"))
    except Exception:
        return None
    out = {"channel_id": primary_ch, "message_id": primary_mid, "backups": []}
    seen = {(primary_ch, primary_mid)}
    for b in rec.get("backups") or []:
        try:
            ch_id = int(b.get("channel_id"))
            msg_id = int(b.get("message_id"))
        except Exception:
            continue
        if (ch_id, msg_id) in seen:
            continue
        out["backups"].append({"channel_id": ch_id, "message_id": msg_id})
        seen.add((ch_id, msg_id))
    return out

def _store_message_record(context: CallbackContext, src_chat_id: int, src_msg_id: int):
    chs = get_storage_channels()
    main_id = chs[0]
    backups = chs[1:]
    fwd = context.bot.forward_message(main_id, src_chat_id, src_msg_id)
    rec = {"channel_id": int(fwd.chat_id), "message_id": int(fwd.message_id), "backups": []}
    for bch in backups:
        try:
            cm = context.bot.copy_message(bch, src_chat_id, src_msg_id)
            rec["backups"].append({"channel_id": int(cm.chat_id), "message_id": int(cm.message_id)})
            time.sleep(0.1)
        except Exception as e:
            log.error(f"Mirror to backup {bch} failed: {e}")
    return rec

def _copy_stored_message_to_chat(context: CallbackContext, chat_id: int, rec, protect_content: bool = False):
    rec = _normalize_stored_message(rec)
    if not rec:
        return None

    storage_list = get_storage_channels()
    main_storage = storage_list[0]
    primary = (int(rec.get("channel_id")), int(rec.get("message_id")))
    backups = [
        (int(b.get("channel_id")), int(b.get("message_id")))
        for b in (rec.get("backups") or [])
        if b.get("channel_id") and b.get("message_id")
    ]

    variants = []
    main_variant = None
    if primary[0] == main_storage:
        main_variant = primary
    else:
        for b in backups:
            if b[0] == main_storage:
                main_variant = b
                break

    if main_variant:
        variants.append(main_variant)
    if primary not in variants:
        variants.append(primary)
    for b in backups:
        if b not in variants:
            variants.append(b)

    for from_ch, msg_id in variants:
        try:
            m = context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=int(from_ch),
                message_id=int(msg_id),
                protect_content=protect_content,
            )
            if int(from_ch) == int(main_storage):
                _record_storage_success_on_main()
            return int(m.message_id)
        except Exception as e:
            log.error(f"Copy stored message failed from {from_ch}/{msg_id}: {e}")
            _record_storage_failure(context, int(from_ch))
    return None

def _start_post_record():
    return _normalize_stored_message(cfg("start_post_record"))

def _start_message_delete_minutes() -> int:
    try:
        mins = int(cfg("start_message_delete_minutes", 0) or 0)
    except Exception:
        mins = 0
    return max(mins, 0)

def _queue_auto_delete(context: CallbackContext, chat_id: int, message_ids, minutes: int, prefix: str = "autodel"):
    ids = [int(mid) for mid in (message_ids or []) if mid]
    if minutes <= 0 or not ids:
        return
    context.job_queue.run_once(
        _auto_delete_messages,
        timedelta(minutes=minutes),
        context={"chat_id": int(chat_id), "message_ids": ids},
        name=f"{prefix}_{int(chat_id)}_{int(time.time())}",
    )

def _queue_persisted_delete(chat_id: int, message_ids, minutes: int):
    ids = [int(mid) for mid in (message_ids or []) if mid]
    if minutes <= 0 or not ids:
        return
    delete_at = datetime.now(UTC) + timedelta(minutes=minutes)
    created_at = datetime.now(UTC)
    docs = [
        {
            "chat_id": int(chat_id),
            "message_id": int(mid),
            "delete_at": delete_at,
            "created_at": created_at,
        }
        for mid in ids
    ]
    if not docs:
        return
    try:
        c_broadcast_deletes.insert_many(docs, ordered=False)
    except Exception as e:
        log.warning(f"Queue persisted delete failed for {chat_id}: {e}")

def _deliver_special_post_copy(context: CallbackContext, uid: int, prod: dict):
    sent_mid = _copy_stored_message_to_chat(
        context,
        uid,
        prod.get("special_post_record"),
        protect_content=PROTECT_CONTENT_ENABLED,
    )
    if not sent_mid:
        return []
    msg_ids = [int(sent_mid)]
    _queue_persisted_delete(uid, msg_ids, _special_post_delete_minutes(prod))
    return msg_ids

def _schedule_special_post_cycles(uid: int, item_id: str, prod: dict):
    cycles = _special_post_cycle_count((prod or {}).get("special_post_cycles"))
    if cycles <= 0:
        c_special_post_cycles.delete_one({"user_id": int(uid), "item_id": item_id})
        return
    now = datetime.now(UTC)
    c_special_post_cycles.update_one(
        {"user_id": int(uid), "item_id": item_id},
        {
            "$set": {
                "user_id": int(uid),
                "item_id": item_id,
                "remaining_cycles": cycles,
                "next_send_at": now + timedelta(minutes=_special_post_repeat_minutes(prod)),
                "updated_at": now,
            },
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )

def _deliver_special_post(context: CallbackContext, uid: int, item_id: str, prod: dict = None):
    prod = prod or c_products.find_one({"item_id": item_id})
    if not _is_special_post_product(prod):
        try:
            context.bot.send_message(uid, "❌ Special post not found.")
        except Exception as e:
            log.error(f"Special post missing notify failed (to {uid}): {e}")
        return []

    sent_ids = _deliver_special_post_copy(context, uid, prod)
    if not sent_ids:
        try:
            context.bot.send_message(uid, "⚠️ This special post is not available right now.")
        except Exception as e:
            log.error(f"Special post unavailable notify failed (to {uid}): {e}")
        return []

    _schedule_special_post_cycles(uid, item_id, prod)
    return sent_ids

def _save_new_product(update: Update, context: CallbackContext, mn: float, mx: float, free_file_text: str = None):
    item_id = f"item_{int(time.time())}"
    admin_id = update.effective_user.id

    if "channel_id" in context.user_data:
        doc = {
            "item_id": item_id,
            "min_price": mn,
            "max_price": mx,
            "channel_id": int(context.user_data["channel_id"]),
            "added_by": admin_id
        }
        if mn == mx:
            doc["price"] = mn
        c_products.insert_one(doc)
        link = f"https://t.me/{context.bot.username}?start={item_id}"
        update.message.reply_text(f"✅ Channel product added.\nLink:\n`{link}`", parse_mode=ParseMode.MARKDOWN)
        context.user_data.clear()
        return ConversationHandler.END

    doc = {
        "item_id": item_id,
        "min_price": mn,
        "max_price": mx,
        "files": context.user_data["new_files"],
        "added_by": admin_id
    }
    free_file_text = (free_file_text or "").strip()
    if mn == mx:
        doc["price"] = mn
    if free_file_text:
        doc["free_file_text"] = free_file_text

    c_products.insert_one(doc)
    link = f"https://t.me/{context.bot.username}?start={item_id}"
    update.message.reply_text(f"✅ Product added.\nLink:\n`{link}`", parse_mode=ParseMode.MARKDOWN)
    context.user_data.clear()
    return ConversationHandler.END

def _is_free_product(prod: dict) -> bool:
    try:
        mn = prod.get("min_price")
        mx = prod.get("max_price")
        if mn is not None and mx is not None:
            return float(mn) == 0 and float(mx) == 0
        return float(prod.get("price", 0) or 0) == 0
    except Exception:
        return False

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

def _process_broadcast_deletes(context: CallbackContext):
    # Keep deletes out of the way while a free broadcast is actively sending.
    if _broadcast_snapshot().get("active"):
        return

    now = datetime.now(UTC)
    docs = list(
        c_broadcast_deletes.find({"delete_at": {"$lte": now}})
        .sort("delete_at", ASCENDING)
        .limit(BROADCAST_DELETE_BATCH_SIZE)
    )
    if not docs:
        return

    done_ids = []
    for doc in docs:
        try:
            context.bot.delete_message(
                chat_id=int(doc["chat_id"]),
                message_id=int(doc["message_id"]),
            )
            done_ids.append(doc["_id"])
        except RetryAfter:
            break
        except (TimedOut, NetworkError):
            continue
        except Exception:
            done_ids.append(doc["_id"])

    if done_ids:
        c_broadcast_deletes.delete_many({"_id": {"$in": done_ids}})

def _process_special_post_cycles(context: CallbackContext):
    now = datetime.now(UTC)
    docs = list(
        c_special_post_cycles.find({"next_send_at": {"$lte": now}})
        .sort("next_send_at", ASCENDING)
        .limit(SPECIAL_POST_SWEEP_BATCH_SIZE)
    )
    if not docs:
        return

    for doc in docs:
        uid = int(doc.get("user_id", 0) or 0)
        item_id = (doc.get("item_id") or "").strip()
        remaining = _special_post_cycle_count(doc.get("remaining_cycles", 0))

        if uid <= 0 or not item_id or remaining <= 0:
            c_special_post_cycles.delete_one({"_id": doc["_id"]})
            continue

        prod = c_products.find_one({"item_id": item_id})
        if not _is_special_post_product(prod):
            c_special_post_cycles.delete_one({"_id": doc["_id"]})
            continue

        sent_ids = _deliver_special_post_copy(context, uid, prod)
        if not sent_ids:
            log.warning("special post resend failed user=%s item=%s; cycle removed", uid, item_id)
            c_special_post_cycles.delete_one({"_id": doc["_id"]})
            continue

        remaining -= 1
        if remaining <= 0:
            c_special_post_cycles.delete_one({"_id": doc["_id"]})
            continue

        c_special_post_cycles.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "remaining_cycles": remaining,
                    "next_send_at": now + timedelta(minutes=_special_post_repeat_minutes(prod)),
                    "last_sent_at": now,
                    "updated_at": now,
                }
            },
        )

def _delete_unpaid_qr(context: CallbackContext):
    data = context.job.context
    if c_sessions.find_one({"key": data["sess_key"]}):
        try: context.bot.delete_message(chat_id=data["chat_id"], message_id=data["qr_message_id"])
        except Exception: pass

# --- Purchase flow (files unchanged in UX; storage behavior enhanced) ---
def start_purchase(ctx: CallbackContext, chat_id: int, uid: int, item_id: str):
    prod = c_products.find_one({"item_id": item_id})
    if not prod: return ctx.bot.send_message(chat_id, "❌ Item not found.")

    if _is_special_post_product(prod):
        _deliver_special_post(ctx, uid, item_id, prod)
        return
    
    # Get admin who added the product (for earnings tracking)
    admin_id = prod.get("added_by", OWNER_ID)
    
    mn, mx = prod.get("min_price"), prod.get("max_price")
    if mn is None or mx is None:
        v=float(prod.get("price",0))
        if v == 0:
            deliver_ids = deliver(ctx, uid, item_id, return_ids=True, notify_on_fail=True, is_free=True) or []
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
                deliver_ids = deliver(ctx, uid, item_id, return_ids=True, notify_on_fail=True, is_free=True) or []
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

def deliver(
    ctx: CallbackContext,
    uid: int,
    item_id: str,
    return_ids: bool = False,
    notify_on_fail: bool = False,
    is_free: bool = False,
):
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
    free_file_text = ""
    if is_free and _is_free_product(prod):
        free_file_text = (prod.get("free_file_text") or "").strip()
        if not free_file_text:
            free_file_text = (cfg("default_free_file_text") or "").strip()

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
                if free_file_text:
                    try:
                        txt_msg = ctx.bot.send_message(uid, free_file_text)
                        msg_ids.append(txt_msg.message_id)
                    except Exception as e:
                        log.error(f"Free file text send failed (to {uid}): {e}")
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

def broadcast_delete_show(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS:
        return
    mins = _broadcast_delete_minutes()
    if mins <= 0:
        update.message.reply_text("Broadcast auto-delete: OFF.")
    else:
        update.message.reply_text(f"Broadcast auto-delete: {mins} minutes.")

def set_broadcast_delete(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS:
        return
    if not context.args:
        return update.message.reply_text("Usage: /set_broadcast_delete <minutes|off>")

    raw = (context.args[0] or "").strip().lower()
    if raw in ("off", "0", "none", "disable"):
        set_cfg("broadcast_delete_minutes", 0)
        return update.message.reply_text("Broadcast auto-delete turned OFF.")

    try:
        mins = int(float(raw))
    except Exception:
        return update.message.reply_text("Invalid number. Example: /set_broadcast_delete 30")

    if mins < 1 or mins > 10080:
        return update.message.reply_text("Choose 1–10080 minutes, or use /set_broadcast_delete off.")

    set_cfg("broadcast_delete_minutes", mins)
    update.message.reply_text(f"Broadcast auto-delete set to {mins} minutes.")

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
GET_PRODUCT_FILES, PRICE, GET_FREE_FILE_TEXT, GET_BROADCAST_FILES, GET_BROADCAST_TEXT, GET_BROADCAST_TARGET_USERS, BROADCAST_CONFIRM = range(7)

def _clear_broadcast_state(context: CallbackContext):
    for key in ("b_files", "b_text", "b_text_msg", "b_target_mode", "b_target_ids", "__broadcast_mode__"):
        context.user_data.pop(key, None)

def _render_admin_start_panel(user_id: int) -> str:
    lines = ["<b>Admin Quick Actions</b>"]
    lines.append(f"Broadcast auto-delete: <b>{'OFF' if _broadcast_delete_minutes() <= 0 else f'{_broadcast_delete_minutes()} min'}</b>")
    lines.append(f"Special post repeats: <b>{SPECIAL_POST_DEFAULT_REPEAT_CYCLES}</b> by default")
    lines.append("Modes: all users or specific user IDs.")
    if user_id == OWNER_ID:
        lines.append("Owner settings are also available below.")
    return "\n".join(lines)

def _admin_start_keyboard(user_id: int):
    rows = [[
        InlineKeyboardButton("📢 Broadcast All", callback_data="admin:broadcast"),
        InlineKeyboardButton("🎯 Specific Users", callback_data="admin:broadcast_specific"),
    ], [
        InlineKeyboardButton("🌟 Special Post", callback_data="admin:specialpost"),
    ], [
        InlineKeyboardButton("🗑 Auto-Delete", callback_data="admin:bcdel"),
    ]]
    if user_id == OWNER_ID:
        rows.append([InlineKeyboardButton("⚙️ Settings", callback_data="admin:settings")])
    return InlineKeyboardMarkup(rows)

def _broadcast_delete_menu_text() -> str:
    mins = _broadcast_delete_minutes()
    current = "OFF" if mins <= 0 else f"{mins} minutes"
    return (
        "<b>Broadcast Auto-Delete</b>\n"
        f"Current timer: <b>{current}</b>\n"
        "This applies to future broadcasts.\n"
        "Choose a preset below or use <code>/set_broadcast_delete &lt;minutes|off&gt;</code> for a custom value."
    )

def _broadcast_delete_menu_keyboard(user_id: int):
    rows = [
        [
            InlineKeyboardButton("15m", callback_data="bcdel:set:15"),
            InlineKeyboardButton("30m", callback_data="bcdel:set:30"),
            InlineKeyboardButton("60m", callback_data="bcdel:set:60"),
        ],
        [
            InlineKeyboardButton("3h", callback_data="bcdel:set:180"),
            InlineKeyboardButton("24h", callback_data="bcdel:set:1440"),
            InlineKeyboardButton("OFF", callback_data="bcdel:set:0"),
        ],
        [InlineKeyboardButton("⬅️ Back", callback_data="admin:startmenu")],
    ]
    if user_id == OWNER_ID:
        rows.append([InlineKeyboardButton("⚙️ Settings", callback_data="admin:settings")])
    return InlineKeyboardMarkup(rows)

def _broadcast_snapshot():
    with _broadcast_state_lock:
        return dict(_broadcast_state)

def _broadcast_try_start(admin_id: int, total: int, message_count: int, target_label: str):
    with _broadcast_state_lock:
        if _broadcast_state.get("active"):
            return False, dict(_broadcast_state)
        _broadcast_state.clear()
        _broadcast_state.update({
            "active": True,
            "admin_id": int(admin_id),
            "total": int(total),
            "processed": 0,
            "sent": 0,
            "failed": 0,
            "dead_removed": 0,
            "message_count": int(message_count),
            "target_label": target_label,
            "started_at": datetime.now(UTC).isoformat(),
            "started_monotonic": time.monotonic(),
        })
        return True, dict(_broadcast_state)

def _broadcast_mark_progress(**kwargs):
    with _broadcast_state_lock:
        if not _broadcast_state.get("active"):
            return
        _broadcast_state.update(kwargs)

def _broadcast_finish():
    with _broadcast_state_lock:
        _broadcast_state.clear()
        _broadcast_state["active"] = False

def _fmt_duration(seconds: float) -> str:
    seconds = max(int(seconds), 0)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

def _render_broadcast_status(snapshot: dict) -> str:
    total = int(snapshot.get("total", 0) or 0)
    processed = int(snapshot.get("processed", 0) or 0)
    sent = int(snapshot.get("sent", 0) or 0)
    failed = int(snapshot.get("failed", 0) or 0)
    dead_removed = int(snapshot.get("dead_removed", 0) or 0)
    message_count = int(snapshot.get("message_count", 0) or 0)
    target_label = snapshot.get("target_label", "all users")
    started_monotonic = float(snapshot.get("started_monotonic", time.monotonic()) or time.monotonic())
    elapsed = max(time.monotonic() - started_monotonic, 0.001)
    rate = processed / elapsed
    remaining = max(total - processed, 0)
    eta = _fmt_duration(remaining / rate) if rate > 0 else "calculating…"
    return (
        "📢 Broadcast running\n"
        f"Target: {target_label}\n"
        f"Users: {processed}/{total}\n"
        f"Sent: {sent}\n"
        f"Failed: {failed}\n"
        f"Dead users removed: {dead_removed}\n"
        f"Messages per user: {message_count}\n"
        f"Rate: {rate:.1f} users/sec\n"
        f"ETA: {eta}"
    )

def _render_broadcast_done(snapshot: dict) -> str:
    started_monotonic = float(snapshot.get("started_monotonic", time.monotonic()) or time.monotonic())
    elapsed = max(time.monotonic() - started_monotonic, 0.0)
    return (
        "✅ Broadcast finished\n"
        f"Target: {snapshot.get('target_label', 'all users')}\n"
        f"Users processed: {int(snapshot.get('processed', 0) or 0)}/{int(snapshot.get('total', 0) or 0)}\n"
        f"Delivered: {int(snapshot.get('sent', 0) or 0)}\n"
        f"Failed: {int(snapshot.get('failed', 0) or 0)}\n"
        f"Dead users removed: {int(snapshot.get('dead_removed', 0) or 0)}\n"
        f"Duration: {_fmt_duration(elapsed)}"
    )

def _broadcast_make_bot():
    req = Request(
        con_pool_size=BROADCAST_WORKERS + 4,
        connect_timeout=5.0,
        read_timeout=BROADCAST_REQUEST_TIMEOUT,
    )
    return Bot(token=TOKEN, request=req)

def _broadcast_delete_dead_users(user_ids):
    ids = list(user_ids)
    if not ids:
        return 0
    removed = 0
    for i in range(0, len(ids), 1000):
        chunk = ids[i:i + 1000]
        removed += c_users.delete_many({"user_id": {"$in": chunk}}).deleted_count
    return removed

def _broadcast_delete_minutes() -> int:
    try:
        mins = int(cfg("broadcast_delete_minutes", DELETE_AFTER_MINUTES) or 0)
    except Exception:
        mins = DELETE_AFTER_MINUTES
    return max(mins, 0)

def _queue_broadcast_deletes(chat_id: int, message_ids):
    mins = _broadcast_delete_minutes()
    if mins <= 0:
        return
    _queue_persisted_delete(chat_id, message_ids, mins)

def _broadcast_is_dead_user_error(err: Exception) -> bool:
    msg = str(err).lower()
    return any(
        needle in msg for needle in (
            "bot was blocked by the user",
            "user is deactivated",
            "chat not found",
            "user not found",
        )
    )

class _BroadcastRateLimiter:
    def __init__(self, rate_per_sec: float):
        self.interval = 1.0 / max(rate_per_sec, 1.0)
        self.lock = threading.Lock()
        self.next_slot = time.monotonic()

    def acquire(self):
        sleep_for = 0.0
        with self.lock:
            now = time.monotonic()
            if now < self.next_slot:
                sleep_for = self.next_slot - now
                self.next_slot += self.interval
            else:
                self.next_slot = now + self.interval
        if sleep_for > 0:
            time.sleep(sleep_for)

def _broadcast_api_call(limiter: _BroadcastRateLimiter, func, *args, **kwargs):
    for attempt in range(4):
        limiter.acquire()
        try:
            return func(*args, timeout=BROADCAST_REQUEST_TIMEOUT, **kwargs)
        except RetryAfter as e:
            wait_s = float(getattr(e, "retry_after", 1) or 1)
            time.sleep(wait_s + 0.1)
        except (TimedOut, NetworkError):
            if attempt == 3:
                raise
            time.sleep(0.4 * (attempt + 1))
    raise RuntimeError("broadcast api call retries exhausted")

def _broadcast_send_to_user(bot: Bot, limiter: _BroadcastRateLimiter, uid: int, files, text):
    sent_message_ids = []
    try:
        for item in files:
            copied = _broadcast_api_call(
                limiter,
                bot.copy_message,
                chat_id=uid,
                from_chat_id=item["chat_id"],
                message_id=item["message_id"],
            )
            sent_message_ids.append(int(copied.message_id))
        if text:
            if isinstance(text, dict) and text.get("chat_id") and text.get("message_id"):
                copied_text = _broadcast_api_call(
                    limiter,
                    bot.copy_message,
                    chat_id=uid,
                    from_chat_id=int(text["chat_id"]),
                    message_id=int(text["message_id"]),
                )
                sent_message_ids.append(int(copied_text.message_id))
            else:
                sent = _broadcast_api_call(limiter, bot.send_message, chat_id=uid, text=str(text))
                sent_message_ids.append(int(sent.message_id))
        return True, False, sent_message_ids
    except Exception as e:
        if _broadcast_is_dead_user_error(e):
            return False, True, sent_message_ids
        log.warning(f"Broadcast failed for {uid}: {e}")
        return False, False, sent_message_ids

def _broadcast_run(admin_chat_id: int, status_chat_id: int, status_message_id: int, files, text, target_user_ids=None):
    bot = _broadcast_make_bot()
    dead_ids = set()
    stats = {"processed": 0, "sent": 0, "failed": 0}
    stats_lock = threading.Lock()
    limiter = _BroadcastRateLimiter(BROADCAST_FREE_MESSAGES_PER_SEC)
    q = queue.Queue()

    try:
        user_ids = list(target_user_ids) if target_user_ids is not None else get_all_user_ids()
        total = len(user_ids)
        _broadcast_mark_progress(total=total)

        if total == 0:
            snapshot = _broadcast_snapshot()
            try:
                bot.edit_message_text(
                    "✅ Broadcast finished\nUsers processed: 0/0\nDelivered: 0\nFailed: 0\nDead users removed: 0\nDuration: 0s",
                    chat_id=status_chat_id,
                    message_id=status_message_id,
                )
            except Exception:
                pass
            return

        for uid in user_ids:
            q.put(uid)

        def worker_loop():
            while True:
                try:
                    uid = q.get_nowait()
                except queue.Empty:
                    return

                ok, dead, sent_ids = _broadcast_send_to_user(bot, limiter, uid, files, text)
                if sent_ids:
                    _queue_broadcast_deletes(uid, sent_ids)
                with stats_lock:
                    stats["processed"] += 1
                    if ok:
                        stats["sent"] += 1
                    else:
                        stats["failed"] += 1
                    if dead:
                        dead_ids.add(uid)
                    _broadcast_mark_progress(
                        processed=stats["processed"],
                        sent=stats["sent"],
                        failed=stats["failed"],
                    )
                q.task_done()

        threads = [
            threading.Thread(target=worker_loop, name=f"broadcast-{i}", daemon=True)
            for i in range(BROADCAST_WORKERS)
        ]
        for t in threads:
            t.start()

        last_update = 0.0
        while any(t.is_alive() for t in threads):
            now = time.monotonic()
            if now - last_update >= BROADCAST_PROGRESS_UPDATE_SECONDS:
                try:
                    bot.edit_message_text(
                        _render_broadcast_status(_broadcast_snapshot()),
                        chat_id=status_chat_id,
                        message_id=status_message_id,
                    )
                except Exception:
                    pass
                last_update = now
            time.sleep(0.5)

        for t in threads:
            t.join()

        removed = _broadcast_delete_dead_users(dead_ids)
        _broadcast_mark_progress(dead_removed=removed)

        try:
            bot.edit_message_text(
                _render_broadcast_done(_broadcast_snapshot()),
                chat_id=status_chat_id,
                message_id=status_message_id,
            )
        except Exception:
            bot.send_message(status_chat_id, _render_broadcast_done(_broadcast_snapshot()))
    except Exception as e:
        log.error(f"Broadcast worker crashed: {e}")
        try:
            bot.send_message(admin_chat_id, f"❌ Broadcast crashed: {e}")
        except Exception:
            pass
    finally:
        _broadcast_finish()

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

    if "channel_id" in context.user_data:
        return _save_new_product(update, context, mn, mx)

    if not context.user_data.get('new_files'):
        update.message.reply_text("No files yet. Send a file or /cancel."); return PRICE

    if float(mn) == 0 and float(mx) == 0:
        context.user_data["pending_price"] = {"min_price": mn, "max_price": mx}
        update.message.reply_text(
            "Send the text to show after each file for this free link.\n"
            "Use /skip to leave it empty."
        )
        return GET_FREE_FILE_TEXT

    return _save_new_product(update, context, mn, mx)

def get_free_file_text(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS:
        return ConversationHandler.END
    pending = context.user_data.get("pending_price") or {}
    if not context.user_data.get("new_files") or not pending:
        context.user_data.clear()
        update.message.reply_text("Session expired. Start again.")
        return ConversationHandler.END

    free_file_text = (update.message.text or "").strip()
    if not free_file_text:
        update.message.reply_text("Send a non-empty text, or use /skip.")
        return GET_FREE_FILE_TEXT

    return _save_new_product(
        update,
        context,
        float(pending["min_price"]),
        float(pending["max_price"]),
        free_file_text=free_file_text,
    )

def skip_free_file_text(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS:
        return ConversationHandler.END
    pending = context.user_data.get("pending_price") or {}
    if not context.user_data.get("new_files") or not pending:
        context.user_data.clear()
        update.message.reply_text("Session expired. Start again.")
        return ConversationHandler.END
    return _save_new_product(
        update,
        context,
        float(pending["min_price"]),
        float(pending["max_price"]),
    )

def cancel_conv(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS: return ConversationHandler.END
    context.user_data.clear(); update.message.reply_text("Canceled."); return ConversationHandler.END

# ---- Broadcast ----
def bc_start(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS: return ConversationHandler.END
    snap = _broadcast_snapshot()
    if snap.get("active"):
        q = update.callback_query
        if q:
            q.answer("Broadcast already running.")
            q.message.reply_text(_render_broadcast_status(snap))
        else:
            update.message.reply_text(_render_broadcast_status(snap))
        return ConversationHandler.END
    _clear_broadcast_state(context)
    context.user_data['b_files'] = []
    context.user_data['b_text'] = None
    context.user_data['b_text_msg'] = None
    mode = "all"
    q = update.callback_query
    if q and q.data == "admin:broadcast_specific":
        mode = "specific"
    elif update.message and (update.message.text or "").split()[0].lower().startswith("/broadcast_specific"):
        mode = "specific"
    context.user_data["b_target_mode"] = mode
    context.user_data["b_target_ids"] = None
    context.user_data["__broadcast_mode__"] = True
    if q:
        q.answer()
        q.message.reply_text(
            "Send files for broadcast. /done when finished."
            if mode == "all"
            else "Send files for broadcast to specific users. /done when finished."
        )
    else:
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
    context.user_data['b_text'] = update.message.text
    context.user_data['b_text_msg'] = {
        "chat_id": int(update.message.chat_id),
        "message_id": int(update.message.message_id),
    }
    if context.user_data.get("b_target_mode") == "specific":
        update.message.reply_text("Now send the target user IDs separated by spaces, commas, or new lines.")
        return GET_BROADCAST_TARGET_USERS
    return bc_confirm(update, context)
def bc_skip(update, context): 
    if update.effective_user.id not in ALL_ADMINS: return ConversationHandler.END
    if context.user_data.get("b_target_mode") == "specific":
        update.message.reply_text("Now send the target user IDs separated by spaces, commas, or new lines.")
        return GET_BROADCAST_TARGET_USERS
    return bc_confirm(update, context)

def bc_target_ids(update, context):
    if update.effective_user.id not in ALL_ADMINS:
        return ConversationHandler.END
    raw = (update.message.text or "").strip()
    ids = []
    seen = set()
    for token in re.findall(r"\d+", raw):
        try:
            uid = int(token)
        except Exception:
            continue
        if uid <= 0 or uid in seen:
            continue
        seen.add(uid)
        ids.append(uid)
    if not ids:
        update.message.reply_text("No valid user IDs found. Send numeric user IDs separated by spaces, commas, or new lines.")
        return GET_BROADCAST_TARGET_USERS
    context.user_data["b_target_ids"] = ids
    return bc_confirm(update, context)

def bc_confirm(update, context):
    if update.effective_user.id not in ALL_ADMINS: return ConversationHandler.END
    snap = _broadcast_snapshot()
    if snap.get("active"):
        update.message.reply_text(_render_broadcast_status(snap))
        _clear_broadcast_state(context)
        return ConversationHandler.END
    mode = context.user_data.get("b_target_mode", "all")
    target_ids = context.user_data.get("b_target_ids") or []
    total = len(target_ids) if mode == "specific" else c_users.count_documents({})
    message_count = len(context.user_data.get("b_files") or []) + (1 if context.user_data.get("b_text") else 0)
    delete_mins = _broadcast_delete_minutes()
    delete_text = "OFF" if delete_mins <= 0 else f"{delete_mins} min"
    buttons = [[InlineKeyboardButton("✅ Send", callback_data="send_bc")],
               [InlineKeyboardButton("❌ Cancel", callback_data="cancel_bc")]]
    update.message.reply_text(
        (
            f"Broadcast to all users: {total}.\n"
            if mode != "specific" else
            f"Broadcast to specific users: {total}.\n"
        )
        + f"Messages per user: {message_count}\nAuto-delete: {delete_text}\nProceed?",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return BROADCAST_CONFIRM

def bc_send(update, context):
    q = update.callback_query
    q.answer()

    files = [
        {"chat_id": int(m.chat_id), "message_id": int(m.message_id)}
        for m in (context.user_data.get("b_files") or [])
    ]
    text = context.user_data.get("b_text_msg") or context.user_data.get("b_text")
    target_mode = context.user_data.get("b_target_mode", "all")
    target_ids = list(context.user_data.get("b_target_ids") or [])
    message_count = len(files) + (1 if text else 0)

    if message_count <= 0:
        q.edit_message_text("Nothing to broadcast.")
        _clear_broadcast_state(context)
        return ConversationHandler.END

    if target_mode == "specific" and not target_ids:
        q.edit_message_text("No target user IDs provided.")
        _clear_broadcast_state(context)
        return ConversationHandler.END

    total = len(target_ids) if target_mode == "specific" else c_users.count_documents({})
    target_label = f"specific users ({total})" if target_mode == "specific" else "all users"
    started, snap = _broadcast_try_start(q.from_user.id, total, message_count, target_label)
    if not started:
        q.edit_message_text(_render_broadcast_status(snap))
        _clear_broadcast_state(context)
        return ConversationHandler.END

    q.edit_message_text("Broadcast started in background.")
    status_msg = q.message.reply_text(_render_broadcast_status(_broadcast_snapshot()))
    worker = threading.Thread(
        target=_broadcast_run,
        args=(q.message.chat_id, status_msg.chat_id, status_msg.message_id, files, text, target_ids if target_mode == "specific" else None),
        name="broadcast-runner",
        daemon=True,
    )
    worker.start()
    _clear_broadcast_state(context)
    return ConversationHandler.END

def bc_cancel(update, context):
    q = update.callback_query
    q.answer("Broadcast cancelled.")
    q.edit_message_text("Broadcast cancelled.")
    _clear_broadcast_state(context)
    return ConversationHandler.END

def on_cb(update: Update, context: CallbackContext):
    q = update.callback_query
    data = q.data or ""
    if data == "check_join":
        return check_join_cb(update, context)
    if q.from_user.id not in ALL_ADMINS:
        q.answer("Not allowed.", show_alert=True)
        return
    if data == "admin:startmenu":
        q.answer()
        q.edit_message_text(
            _render_admin_start_panel(q.from_user.id),
            parse_mode=ParseMode.HTML,
            reply_markup=_admin_start_keyboard(q.from_user.id),
        )
        return
    if data == "admin:bcdel":
        q.answer()
        q.message.reply_text(
            _broadcast_delete_menu_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=_broadcast_delete_menu_keyboard(q.from_user.id),
        )
        return
    if data.startswith("bcdel:set:"):
        try:
            mins = int(data.split(":")[-1])
        except Exception:
            q.answer("Invalid value.", show_alert=True)
            return
        set_cfg("broadcast_delete_minutes", max(mins, 0))
        q.answer("Broadcast auto-delete updated.")
        q.edit_message_text(
            _broadcast_delete_menu_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=_broadcast_delete_menu_keyboard(q.from_user.id),
        )
        return
    if data == "admin:settings" and q.from_user.id == OWNER_ID:
        q.answer()
        q.message.reply_text(
            _render_settings_text(),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_settings_keyboard()
        )
        return

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
    start_post = _start_post_record()
    start_delete_mins = _start_message_delete_minutes()
    default_free_file_text = (cfg("default_free_file_text") or "").strip()
    if sup:
        lines.append(f"*Support contact:* `{sup}`")
    else:
        lines.append("*Support contact:* not set")
    lines.append(f"*Startup post:* {'configured' if start_post else 'not set'}")
    lines.append(f"*Start message auto-delete:* {'OFF' if start_delete_mins <= 0 else f'{start_delete_mins} min'}")
    lines.append(f"*Default free file text:* {'set' if default_free_file_text else 'OFF'}")
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
        [InlineKeyboardButton("📌 Startup Post", callback_data="cfg:startpost"),
         InlineKeyboardButton("⏱ Start Msg Delete", callback_data="cfg:startmsgdel")],
        [InlineKeyboardButton("📝 Default Free Text", callback_data="cfg:defaultfreetext")],
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
    _clear_owner_settings_capture_state(context)
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
    _clear_owner_settings_capture_state(context)
    context.user_data["__await_support__"] = True
    sup = cfg("support_contact")
    current = f"`{sup}`" if sup else "not set"
    q.message.reply_text(
        "Send the support contact now (e.g., `@helpdesk` or a numeric user ID). "
        "Send `clear` to remove. Current: " + current,
        parse_mode=ParseMode.MARKDOWN
    )

def cfg_start_post_cb(update: Update, context: CallbackContext):
    if update.effective_user.id != OWNER_ID:
        return
    q = update.callback_query
    q.answer()
    _clear_owner_settings_capture_state(context)
    context.user_data["__await_start_post__"] = True
    current = "configured" if _start_post_record() else "not set"
    q.message.reply_text(
        "Send or forward the message/post to show on `/start`.\n"
        "Send `clear` to remove it.\n"
        f"Current: {current}",
        parse_mode=ParseMode.MARKDOWN,
    )

def cfg_start_message_delete_cb(update: Update, context: CallbackContext):
    if update.effective_user.id != OWNER_ID:
        return
    q = update.callback_query
    q.answer()
    _clear_owner_settings_capture_state(context)
    context.user_data["__await_start_message_delete__"] = True
    mins = _start_message_delete_minutes()
    current = "OFF" if mins <= 0 else f"{mins} minutes"
    q.message.reply_text(
        "Send the auto-delete time for the start message in minutes.\n"
        "Use `off` to disable it. Allowed range: `1-10080`.\n"
        f"Current: {current}",
        parse_mode=ParseMode.MARKDOWN,
    )

def cfg_default_free_file_text_cb(update: Update, context: CallbackContext):
    if update.effective_user.id != OWNER_ID:
        return
    q = update.callback_query
    q.answer()
    _clear_owner_settings_capture_state(context)
    context.user_data["__await_default_free_file_text__"] = True
    current = "set" if (cfg("default_free_file_text") or "").strip() else "OFF"
    q.message.reply_text(
        "Send the default text to show after each file for free file links.\n"
        "This is used for older links or any free file link without its own custom text.\n"
        "Send `clear` to turn it off.\n"
        f"Current: {current}",
        parse_mode=ParseMode.MARKDOWN,
    )

def start_post_capture_router(update: Update, context: CallbackContext):
    if update.effective_user.id != OWNER_ID:
        return
    if not context.user_data.get("__await_start_post__", False):
        return

    msg = update.message
    if not msg:
        return

    text = (msg.text or "").strip()
    low = text.lower()
    if text.startswith("/") and low not in ("clear", "/clear"):
        return

    if low in ("clear", "none", "-", "remove", "/clear"):
        context.user_data.pop("__await_start_post__", None)
        set_cfg("start_post_record", None)
        msg.reply_text("✅ Startup post cleared.")
        _settings_refresh(msg.chat_id, context)
        raise DispatcherHandlerStop()

    if text.isdigit() and not msg.effective_attachment:
        msg.reply_text(
            "That looks like a number, not a startup post. "
            "Send actual text/media for the startup post, or use `Start Msg Delete` for minutes.",
            parse_mode=ParseMode.MARKDOWN,
        )
        raise DispatcherHandlerStop()

    try:
        rec = _store_message_record(context, msg.chat_id, msg.message_id)
        set_cfg("start_post_record", rec)
        context.user_data.pop("__await_start_post__", None)
        mins = _start_message_delete_minutes()
        msg.reply_text(f"✅ Startup post saved. Auto-delete: {'OFF' if mins <= 0 else f'{mins} min'}.")
        _settings_refresh(msg.chat_id, context)
    except Exception as e:
        log.error(f"Startup post capture failed: {e}")
        msg.reply_text(
            "❌ Failed to save that post. Send or forward it again, or send `clear` to remove the current one.",
            parse_mode=ParseMode.MARKDOWN,
        )
    raise DispatcherHandlerStop()

def special_post_capture_router(update: Update, context: CallbackContext):
    if update.effective_user.id not in ALL_ADMINS:
        return
    if not context.user_data.get("__await_special_post__", False):
        return

    msg = update.message
    if not msg:
        return

    text = (msg.text or "").strip()
    low = text.lower()

    if context.user_data.pop("__await_special_post_cycles_manual__", False):
        try:
            cycles = int(float(text))
        except Exception:
            msg.reply_text("Send the cycle count as a number, like `15`.", parse_mode=ParseMode.MARKDOWN)
            context.user_data["__await_special_post_cycles_manual__"] = True
            raise DispatcherHandlerStop()
        if cycles < 0 or cycles > 1000:
            msg.reply_text("Choose a cycle count between `0` and `1000`.", parse_mode=ParseMode.MARKDOWN)
            context.user_data["__await_special_post_cycles_manual__"] = True
            raise DispatcherHandlerStop()
        context.user_data["__special_post_cycles__"] = cycles
        msg.reply_text(
            _special_post_builder_text(context),
            parse_mode=ParseMode.HTML,
            reply_markup=_special_post_builder_keyboard(context),
            disable_web_page_preview=True,
        )
        raise DispatcherHandlerStop()

    if context.user_data.pop("__await_special_post_repeat_minutes__", False):
        try:
            minutes = int(float(text))
        except Exception:
            msg.reply_text("Send the resend timer in minutes, like `360`.", parse_mode=ParseMode.MARKDOWN)
            context.user_data["__await_special_post_repeat_minutes__"] = True
            raise DispatcherHandlerStop()
        if minutes < 1 or minutes > 10080:
            msg.reply_text("Choose a resend timer between `1` and `10080` minutes.", parse_mode=ParseMode.MARKDOWN)
            context.user_data["__await_special_post_repeat_minutes__"] = True
            raise DispatcherHandlerStop()
        context.user_data["__special_post_repeat_minutes__"] = minutes
        msg.reply_text(
            _special_post_builder_text(context),
            parse_mode=ParseMode.HTML,
            reply_markup=_special_post_builder_keyboard(context),
            disable_web_page_preview=True,
        )
        raise DispatcherHandlerStop()

    if context.user_data.pop("__await_special_post_delete_minutes__", False):
        try:
            minutes = int(float(text))
        except Exception:
            msg.reply_text("Send the delete timer in minutes, like `30`.", parse_mode=ParseMode.MARKDOWN)
            context.user_data["__await_special_post_delete_minutes__"] = True
            raise DispatcherHandlerStop()
        if minutes < 1 or minutes > 10080:
            msg.reply_text("Choose a delete timer between `1` and `10080` minutes.", parse_mode=ParseMode.MARKDOWN)
            context.user_data["__await_special_post_delete_minutes__"] = True
            raise DispatcherHandlerStop()
        context.user_data["__special_post_delete_minutes__"] = minutes
        msg.reply_text(
            _special_post_builder_text(context),
            parse_mode=ParseMode.HTML,
            reply_markup=_special_post_builder_keyboard(context),
            disable_web_page_preview=True,
        )
        raise DispatcherHandlerStop()

    if text.startswith("/") and low not in ("clear", "/clear"):
        return

    if low in ("clear", "none", "-", "remove", "/clear"):
        context.user_data.pop("__special_post_record__", None)
        msg.reply_text(
            _special_post_builder_text(context),
            parse_mode=ParseMode.HTML,
            reply_markup=_special_post_builder_keyboard(context),
            disable_web_page_preview=True,
        )
        raise DispatcherHandlerStop()

    try:
        rec = _store_message_record(context, msg.chat_id, msg.message_id)
        context.user_data["__special_post_record__"] = rec
        msg.reply_text(
            _special_post_builder_text(context),
            parse_mode=ParseMode.HTML,
            reply_markup=_special_post_builder_keyboard(context),
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.error(f"Special post capture failed: {e}")
        msg.reply_text("❌ Failed to save that special post. Send or forward it again.")
    raise DispatcherHandlerStop()

def special_post_cb(update: Update, context: CallbackContext):
    q = update.callback_query
    if not q:
        return
    if q.from_user.id not in ALL_ADMINS:
        q.answer("Not allowed.", show_alert=True)
        return

    data = q.data or ""
    if data == "admin:specialpost":
        q.answer()
        _open_special_post_builder(update, context)
        return

    if not data.startswith("specialpost:"):
        return

    if not context.user_data.get("__await_special_post__", False):
        q.answer("No active special post builder.", show_alert=True)
        return

    action = data.split(":", 1)[1]

    def _edit_builder():
        try:
            q.message.edit_text(
                _special_post_builder_text(context),
                parse_mode=ParseMode.HTML,
                reply_markup=_special_post_builder_keyboard(context),
                disable_web_page_preview=True,
            )
        except Exception:
            q.message.reply_text(
                _special_post_builder_text(context),
                parse_mode=ParseMode.HTML,
                reply_markup=_special_post_builder_keyboard(context),
                disable_web_page_preview=True,
            )

    if action == "cycles":
        q.answer()
        try:
            q.message.edit_text(
                _special_post_cycles_text(context),
                parse_mode=ParseMode.HTML,
                reply_markup=_special_post_cycles_keyboard(context),
                disable_web_page_preview=True,
            )
        except Exception:
            q.message.reply_text(
                _special_post_cycles_text(context),
                parse_mode=ParseMode.HTML,
                reply_markup=_special_post_cycles_keyboard(context),
                disable_web_page_preview=True,
            )
        return

    if action == "resend":
        q.answer()
        context.user_data.pop("__await_special_post_delete_minutes__", None)
        context.user_data.pop("__await_special_post_cycles_manual__", None)
        context.user_data["__await_special_post_repeat_minutes__"] = True
        q.message.reply_text(
            "Send the resend timer in minutes.\n"
            "Allowed range: `1-10080`.\n"
            f"Current: `{_special_post_repeat_minutes_from_state(context)}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if action == "delete":
        q.answer()
        context.user_data.pop("__await_special_post_repeat_minutes__", None)
        context.user_data.pop("__await_special_post_cycles_manual__", None)
        context.user_data["__await_special_post_delete_minutes__"] = True
        q.message.reply_text(
            "Send the delete timer in minutes.\n"
            "Allowed range: `1-10080`.\n"
            f"Current: `{_special_post_delete_minutes_from_state(context)}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if action.startswith("setcycles:"):
        try:
            cycles = int(action.split(":")[-1])
        except Exception:
            q.answer("Invalid cycle count.", show_alert=True)
            return
        context.user_data["__special_post_cycles__"] = _special_post_cycle_count(cycles)
        q.answer("Cycle count updated.")
        _edit_builder()
        return

    if action == "cycles_manual":
        q.answer()
        context.user_data.pop("__await_special_post_repeat_minutes__", None)
        context.user_data.pop("__await_special_post_delete_minutes__", None)
        context.user_data["__await_special_post_cycles_manual__"] = True
        q.message.reply_text(
            "Send the cycle count manually.\n"
            "Allowed range: `0-1000`.\n"
            f"Current: `{_special_post_cycle_count(context.user_data.get('__special_post_cycles__'))}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if action == "back":
        q.answer()
        context.user_data.pop("__await_special_post_cycles_manual__", None)
        context.user_data.pop("__await_special_post_repeat_minutes__", None)
        context.user_data.pop("__await_special_post_delete_minutes__", None)
        _edit_builder()
        return

    if action == "cancel":
        q.answer("Cancelled.")
        _clear_special_post_state(context)
        try:
            q.message.edit_text("❌ Special post creation cancelled.")
        except Exception:
            pass
        return

    if action == "save":
        rec = _normalize_stored_message(context.user_data.get("__special_post_record__"))
        if not rec:
            q.answer("Send or forward the post first.", show_alert=True)
            return

        item_id = (context.user_data.get("__special_post_item_id__") or "").strip()
        is_edit = bool(item_id)
        if not item_id:
            item_id = f"item_{int(time.time())}"
        cycles = _special_post_cycle_count(context.user_data.get("__special_post_cycles__"))
        repeat_minutes = _special_post_repeat_minutes_from_state(context)
        delete_minutes = _special_post_delete_minutes_from_state(context)
        doc = {
            "min_price": 0.0,
            "max_price": 0.0,
            "price": 0.0,
            "special_post_record": rec,
            "special_post_cycles": cycles,
            "special_post_repeat_minutes": repeat_minutes,
            "special_post_delete_minutes": delete_minutes,
            "added_by": q.from_user.id,
        }
        try:
            if is_edit:
                c_products.update_one({"item_id": item_id}, {"$set": doc})
            else:
                c_products.insert_one({"item_id": item_id, **doc})
            _clear_special_post_state(context)
            link = f"https://t.me/{context.bot.username}?start={item_id}"
            q.answer("Special post updated." if is_edit else "Special post created.")
            q.message.edit_text(
                ("✅ Special post updated.\n" if is_edit else "✅ Special post created.\n")
                + f"Repeat cycles: {cycles}\n"
                + f"Resend timer: {repeat_minutes} minute{'s' if repeat_minutes != 1 else ''}\n"
                + f"Delete timer: {delete_minutes} minute{'s' if delete_minutes != 1 else ''}\n"
                + f"Link:\n`{link}`",
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )
        except Exception as e:
            log.error(f"Special post create failed: {e}")
            q.answer("Failed to create special post.", show_alert=True)
        return

def admin_text_router(update: Update, context: CallbackContext):
    """Admin text router:
       0) Storage backup add (lenient capture, with auto-resync) - OWNER ONLY
       1) Support contact capture - OWNER ONLY
       2) Start message auto-delete capture - OWNER ONLY
       3) Default free file text capture - OWNER ONLY
       4) Order ID lookup - ALL ADMINS
    """
    user_id = update.effective_user.id
    
    # Check if user is admin
    if user_id not in ALL_ADMINS:
        return
    if context.user_data.get("__broadcast_mode__"):
        return

    t = (update.message.text or "").strip()

    # (2) Edit-link input capture (only for links created by the same admin)
    if context.user_data.get("__edit_link_item_id__"):
        item_id = context.user_data.get("__edit_link_item_id__")
        prod = c_products.find_one({"item_id": item_id}) if item_id else None
        if not _is_editable_link_product(prod):
            # stale/invalid session
            _clear_edit_link_state(context)
        else:
            changes = context.user_data.setdefault("__edit_link_changes__", {})
            # waiting for channel ref
            if context.user_data.pop("__await_edit_channel__", False):
                if "channel_id" not in prod:
                    update.message.reply_text("❌ This item is not a channel product.")
                elif not CHANNEL_REF_RE.match(t):
                    update.message.reply_text("Invalid channel. Send like `-100...` or `@username` or `https://t.me/...`.", parse_mode=ParseMode.MARKDOWN)
                    context.user_data["__await_edit_channel__"] = True
                    raise DispatcherHandlerStop()
                else:
                    try:
                        ch_id = _resolve_channel(context, t)
                    except (BadRequest, Unauthorized) as e:
                        update.message.reply_text(f"❌ I couldn't access that channel: {e}")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    if not _bot_is_admin(context, ch_id):
                        update.message.reply_text("❌ I'm not an admin there. Add me and try again.")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    changes["channel_id"] = int(ch_id)
                    txt, kb = _edit_link_menu(prod, changes)
                    update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

            # waiting for price
            if context.user_data.pop("__await_edit_price__", False):
                try:
                    if "-" in t:
                        a, b = t.split("-", 1)
                        mn, mx = float(a), float(b)
                        assert mx >= mn and mn >= 0
                    else:
                        v = float(t); assert v >= 0
                        mn = mx = v
                except Exception:
                    update.message.reply_text("Invalid. Send like 10 or 10-30.")
                    context.user_data["__await_edit_price__"] = True
                    raise DispatcherHandlerStop()
                changes["min_price"] = float(mn)
                changes["max_price"] = float(mx)
                txt, kb = _edit_link_menu(prod, changes)
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

            if context.user_data.pop("__await_edit_free_file_text__", False):
                if "files" not in prod:
                    update.message.reply_text("❌ This item is not a files product.")
                else:
                    if t.lower() in ("clear", "none", "-", "remove"):
                        changes["free_file_text"] = None
                    else:
                        changes["free_file_text"] = t
                    txt, kb = _edit_link_menu(prod, changes)
                    update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

    # (3) Detect a bot start-link and open edit menu
    m = BOT_START_LINK_RE.search(t)
    if m:
        bot_un = (m.group(1) or "").lstrip("@")
        payload = m.group(2) or ""
        try:
            me = context.bot.get_me()
            my_un = (me.username or "").lower()
        except Exception:
            my_un = (getattr(context.bot, "username", "") or "").lower()
        if bot_un.lower() == my_un and payload.startswith("item_"):
            prod = c_products.find_one({"item_id": payload})
            log.info(
                "admin bot-link received admin=%s bot=%s payload=%s found=%s editable=%s special=%s",
                user_id,
                bot_un,
                payload,
                bool(prod),
                _is_editable_link_product(prod),
                _is_special_post_product(prod),
            )
            if not prod:
                update.message.reply_text("❌ Link not found.")
                raise DispatcherHandlerStop()
            if _is_special_post_product(prod):
                _open_special_post_builder(update, context, prod, payload)
                raise DispatcherHandlerStop()
            if not _is_editable_link_product(prod):
                update.message.reply_text("❌ This link can't be edited here.")
                raise DispatcherHandlerStop()
            _open_edit_link_menu(update, context, prod, payload)

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

    # (2) Start message auto-delete setter - OWNER ONLY
    if context.user_data.pop("__await_start_message_delete__", False):
        if user_id != OWNER_ID:
            update.message.reply_text("❌ Only the bot owner can modify the start message timer.")
            return
        raw = t.lower()
        if raw in ("off", "0", "none", "disable"):
            set_cfg("start_message_delete_minutes", 0)
            update.message.reply_text("✅ Start message auto-delete turned OFF.")
            _settings_refresh(update.effective_chat.id, context)
            return
        try:
            mins = int(float(t))
        except Exception:
            update.message.reply_text("Invalid value. Send minutes like `30`, or `off`.", parse_mode=ParseMode.MARKDOWN)
            context.user_data["__await_start_message_delete__"] = True
            return
        if mins < 1 or mins > 10080:
            update.message.reply_text("Choose 1–10080 minutes, or send `off`.", parse_mode=ParseMode.MARKDOWN)
            context.user_data["__await_start_message_delete__"] = True
            return
        set_cfg("start_message_delete_minutes", mins)
        update.message.reply_text(f"✅ Start message auto-delete set to {mins} minutes.")
        _settings_refresh(update.effective_chat.id, context)
        return

    # (3) Default free file text setter - OWNER ONLY
    if context.user_data.pop("__await_default_free_file_text__", False):
        if user_id != OWNER_ID:
            update.message.reply_text("❌ Only the bot owner can modify the default free file text.")
            return
        if t.lower() in ("clear", "none", "-", "remove", "off", "disable"):
            set_cfg("default_free_file_text", None)
            update.message.reply_text("✅ Default free file text turned OFF.")
            _settings_refresh(update.effective_chat.id, context)
            return
        set_cfg("default_free_file_text", t)
        update.message.reply_text("✅ Default free file text saved.")
        _settings_refresh(update.effective_chat.id, context)
        return

    # (4) Admin receipt lookup by Order ID - ALL ADMINS
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


def editln_cb(update: Update, context: CallbackContext):
    q = update.callback_query
    if not q:
        return
    uid = q.from_user.id
    if uid not in ALL_ADMINS:
        q.answer()
        return

    data = q.data or ""
    if not data.startswith("editln:"):
        return

    item_id = context.user_data.get("__edit_link_item_id__")
    if not item_id:
        q.answer("No active edit.", show_alert=True)
        return

    prod = c_products.find_one({"item_id": item_id})
    if not prod:
        _clear_edit_link_state(context)
        q.answer("Item not found anymore.", show_alert=True)
        try:
            q.message.edit_text("❌ Item not found anymore.")
        except Exception:
            pass
        return

    if not _is_editable_link_product(prod):
        _clear_edit_link_state(context)
        q.answer("This link can't be edited here.", show_alert=True)
        return

    changes = context.user_data.setdefault("__edit_link_changes__", {})
    action = data.split(":", 1)[1]

    # Common: show menu
    def _show_menu():
        txt, kb = _edit_link_menu(prod, changes)
        try:
            q.message.edit_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            # fallback (e.g., message not editable)
            q.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)

    if action == "cancel":
        q.answer()
        _clear_edit_link_state(context)
        try:
            q.message.edit_text("❌ Cancelled.")
        except Exception:
            pass
        return

    if action == "ch":
        q.answer()
        context.user_data["__await_edit_channel__"] = True
        q.message.reply_text("Send the new channel ID / @username / t.me link.")
        return

    if action == "ch_keep":
        q.answer()
        changes.pop("channel_id", None)
        context.user_data.pop("__await_edit_channel__", None)
        _show_menu()
        return

    if action == "files":
        q.answer()
        context.user_data["__await_edit_files__"] = True
        context.user_data["__edit_new_files__"] = []
        q.message.reply_text(
            "Send the new files now (one by one). When finished, press ✅ Done.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Done", callback_data="editln:files_done")],
                [InlineKeyboardButton("❌ Cancel", callback_data="editln:cancel")]
            ])
        )
        return

    if action == "files_keep":
        q.answer()
        changes.pop("files", None)
        context.user_data.pop("__await_edit_files__", None)
        context.user_data.pop("__edit_new_files__", None)
        _show_menu()
        return

    if action == "files_done":
        new_files = context.user_data.get("__edit_new_files__") or []
        if not new_files:
            q.answer("Send at least one file first.", show_alert=True)
            return
        q.answer()
        changes["files"] = new_files
        context.user_data.pop("__await_edit_files__", None)
        context.user_data.pop("__edit_new_files__", None)
        _show_menu()
        return

    if action == "price":
        q.answer()
        context.user_data["__await_edit_price__"] = True
        q.message.reply_text("Send new price like `10` or `10-30`.", parse_mode=ParseMode.MARKDOWN)
        return

    if action == "price_keep":
        q.answer()
        for k in ("min_price", "max_price"):
            changes.pop(k, None)
        context.user_data.pop("__await_edit_price__", None)
        _show_menu()
        return

    if action == "back":
        q.answer()
        context.user_data.pop("__await_edit_free_file_text__", None)
        _show_menu()
        return

    if action == "freetext":
        if "files" not in prod:
            q.answer("Only available for file links.", show_alert=True)
            return
        context.user_data["__await_edit_free_file_text__"] = True
        free_text_state = _free_file_text_state(prod, changes)
        q.answer(
            _free_file_text_alert_text(
                free_text_state["previous_text"],
                free_text_state["previous_source"],
            ),
            show_alert=True,
        )
        log.info(
            "editln freetext item=%s admin=%s previous_len=%s source=%s pending=%s",
            item_id,
            uid,
            len(free_text_state["previous_text"] or ""),
            free_text_state["previous_source"],
            free_text_state["has_pending"],
        )

        prompt_lines = ["Send the text to show after each file when this link is free.", ""]
        if free_text_state["previous_text"]:
            previous_label = "<b>Previous text:</b>"
            if free_text_state["previous_source"] == "default":
                previous_label = "<b>Previous text (default):</b>"
            prompt_lines.extend([
                previous_label,
                f"<pre>{html.escape(free_text_state['previous_text'])}</pre>",
            ])
        else:
            prompt_lines.append("<b>Previous text:</b> OFF")

        if free_text_state["has_pending"]:
            prompt_lines.append("")
            if free_text_state["pending_text"]:
                prompt_lines.extend([
                    "<b>Pending new text:</b>",
                    f"<pre>{html.escape(free_text_state['pending_text'])}</pre>",
                ])
            elif free_text_state["default_text"]:
                prompt_lines.extend([
                    "<b>Pending result:</b> Default text will be used.",
                    f"<pre>{html.escape(free_text_state['default_text'])}</pre>",
                ])
            else:
                prompt_lines.append("<b>Pending result:</b> OFF")

        prompt_lines.append("")
        if free_text_state["default_text"]:
            prompt_lines.append(
                "Send <code>clear</code> to remove this link's custom text and use the default text."
            )
        else:
            prompt_lines.append("Send <code>clear</code> to remove it.")

        prompt_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Back", callback_data="editln:back")],
            [InlineKeyboardButton("❌ Cancel", callback_data="editln:cancel")],
        ])
        prompt_text = "\n".join(prompt_lines)

        try:
            q.message.edit_text(
                prompt_text,
                parse_mode=ParseMode.HTML,
                reply_markup=prompt_keyboard,
                disable_web_page_preview=True,
            )
        except Exception:
            q.message.reply_text(
                prompt_text,
                parse_mode=ParseMode.HTML,
                reply_markup=prompt_keyboard,
                disable_web_page_preview=True,
            )
        return

    if action == "freetext_clear":
        if "files" not in prod:
            q.answer("Only available for file links.", show_alert=True)
            return
        q.answer()
        changes["free_file_text"] = None
        context.user_data.pop("__await_edit_free_file_text__", None)
        _show_menu()
        return

    if action == "save":
        set_doc = {}
        unset_doc = {}

        if "channel_id" in changes:
            set_doc["channel_id"] = int(changes["channel_id"])
        if "files" in changes:
            set_doc["files"] = changes["files"]

        if "min_price" in changes and "max_price" in changes:
            mn = float(changes["min_price"]); mx = float(changes["max_price"])
            set_doc["min_price"] = mn
            set_doc["max_price"] = mx
            if mn == mx:
                set_doc["price"] = mn
            else:
                unset_doc["price"] = ""
        if "free_file_text" in changes:
            free_file_text = (changes.get("free_file_text") or "").strip()
            if free_file_text:
                set_doc["free_file_text"] = free_file_text
            else:
                unset_doc["free_file_text"] = ""
        # If price not changed, keep existing price field as-is.

        if not set_doc and not unset_doc:
            q.answer("Nothing changed.", show_alert=True)
            _show_menu()
            return

        q.answer()
        update = {}
        if set_doc: update["$set"] = set_doc
        if unset_doc: update["$unset"] = unset_doc

        try:
            c_products.update_one({"item_id": item_id}, update)
            _clear_edit_link_state(context)
            link = f"https://t.me/{context.bot.username}?start={item_id}"
            q.message.reply_text(f"✅ Updated.\nLink:\n`{link}`", parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
            try:
                q.message.edit_text("✅ Updated. (menu closed)")
            except Exception:
                pass
        except Exception as e:
            log.error(f"Edit link save failed for {item_id}: {e}")
            q.message.reply_text(f"❌ Failed to update: {e}")
        return


def admin_edit_files_router(update: Update, context: CallbackContext):
    """Captures files for edit-link flow so it doesn't trigger add-product."""
    uid = update.effective_user.id
    if uid not in ALL_ADMINS:
        return
    if not context.user_data.get("__await_edit_files__", False):
        return

    item_id = context.user_data.get("__edit_link_item_id__")
    prod = c_products.find_one({"item_id": item_id}) if item_id else None
    if (not prod) or ("files" not in prod):
        update.message.reply_text("❌ This item is not a files product.")
        context.user_data.pop("__await_edit_files__", None)
        context.user_data.pop("__edit_new_files__", None)
        raise DispatcherHandlerStop()

    if not update.message.effective_attachment:
        update.message.reply_text("Not a file. Send again.")
        raise DispatcherHandlerStop()

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

        context.user_data.setdefault("__edit_new_files__", []).append(rec)
        n = len(context.user_data["__edit_new_files__"])
        update.message.reply_text(
            f"✅ Added {n} file(s). Send more or press ✅ Done.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Done", callback_data="editln:files_done")],
                [InlineKeyboardButton("❌ Cancel", callback_data="editln:cancel")]
            ])
        )
    except Exception as e:
        log.error(f"Edit link file capture failed: {e}")
        update.message.reply_text(f"❌ Failed to capture file: {e}")

    raise DispatcherHandlerStop()


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
    t = (update.message.text or "").strip()

    # (2) Edit-link input capture (only for links created by the same admin)
    if context.user_data.get("__edit_link_item_id__"):
        item_id = context.user_data.get("__edit_link_item_id__")
        prod = c_products.find_one({"item_id": item_id}) if item_id else None
        if (not prod) or (int(prod.get("added_by", 0) or 0) != int(user_id)):
            # stale/invalid session
            _clear_edit_link_state(context)
        else:
            changes = context.user_data.setdefault("__edit_link_changes__", {})
            # waiting for channel ref
            if context.user_data.pop("__await_edit_channel__", False):
                if "channel_id" not in prod:
                    update.message.reply_text("❌ This item is not a channel product.")
                elif not CHANNEL_REF_RE.match(t):
                    update.message.reply_text("Invalid channel. Send like `-100...` or `@username` or `https://t.me/...`.", parse_mode=ParseMode.MARKDOWN)
                    context.user_data["__await_edit_channel__"] = True
                    raise DispatcherHandlerStop()
                else:
                    try:
                        ch_id = _resolve_channel(context, t)
                    except (BadRequest, Unauthorized) as e:
                        update.message.reply_text(f"❌ I couldn't access that channel: {e}")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    if not _bot_is_admin(context, ch_id):
                        update.message.reply_text("❌ I'm not an admin there. Add me and try again.")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    changes["channel_id"] = int(ch_id)
                    txt, kb = _edit_link_menu(prod, changes)
                    update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

            # waiting for price
            if context.user_data.pop("__await_edit_price__", False):
                try:
                    if "-" in t:
                        a, b = t.split("-", 1)
                        mn, mx = float(a), float(b)
                        assert mx >= mn and mn >= 0
                    else:
                        v = float(t); assert v >= 0
                        mn = mx = v
                except Exception:
                    update.message.reply_text("Invalid. Send like 10 or 10-30.")
                    context.user_data["__await_edit_price__"] = True
                    raise DispatcherHandlerStop()
                changes["min_price"] = float(mn)
                changes["max_price"] = float(mx)
                txt, kb = _edit_link_menu(prod, changes)
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

    # (3) Detect a bot start-link and open edit menu
    m = BOT_START_LINK_RE.search(t)
    if m:
        bot_un = (m.group(1) or "").lstrip("@")
        payload = m.group(2) or ""
        try:
            me = context.bot.get_me()
            my_un = (me.username or "").lower()
        except Exception:
            my_un = (getattr(context.bot, "username", "") or "").lower()
        if bot_un.lower() == my_un and payload.startswith("item_"):
            prod = c_products.find_one({"item_id": payload})
            if prod and int(prod.get("added_by", 0) or 0) == int(user_id) and ("channel_id" in prod or "files" in prod):
                _clear_edit_link_state(context)
                context.user_data["__edit_link_item_id__"] = payload
                context.user_data["__edit_link_changes__"] = {}
                txt, kb = _edit_link_menu(prod, {})
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop().lower()
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
    t = (update.message.text or "").strip()

    # (2) Edit-link input capture (only for links created by the same admin)
    if context.user_data.get("__edit_link_item_id__"):
        item_id = context.user_data.get("__edit_link_item_id__")
        prod = c_products.find_one({"item_id": item_id}) if item_id else None
        if (not prod) or (int(prod.get("added_by", 0) or 0) != int(user_id)):
            # stale/invalid session
            _clear_edit_link_state(context)
        else:
            changes = context.user_data.setdefault("__edit_link_changes__", {})
            # waiting for channel ref
            if context.user_data.pop("__await_edit_channel__", False):
                if "channel_id" not in prod:
                    update.message.reply_text("❌ This item is not a channel product.")
                elif not CHANNEL_REF_RE.match(t):
                    update.message.reply_text("Invalid channel. Send like `-100...` or `@username` or `https://t.me/...`.", parse_mode=ParseMode.MARKDOWN)
                    context.user_data["__await_edit_channel__"] = True
                    raise DispatcherHandlerStop()
                else:
                    try:
                        ch_id = _resolve_channel(context, t)
                    except (BadRequest, Unauthorized) as e:
                        update.message.reply_text(f"❌ I couldn't access that channel: {e}")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    if not _bot_is_admin(context, ch_id):
                        update.message.reply_text("❌ I'm not an admin there. Add me and try again.")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    changes["channel_id"] = int(ch_id)
                    txt, kb = _edit_link_menu(prod, changes)
                    update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

            # waiting for price
            if context.user_data.pop("__await_edit_price__", False):
                try:
                    if "-" in t:
                        a, b = t.split("-", 1)
                        mn, mx = float(a), float(b)
                        assert mx >= mn and mn >= 0
                    else:
                        v = float(t); assert v >= 0
                        mn = mx = v
                except Exception:
                    update.message.reply_text("Invalid. Send like 10 or 10-30.")
                    context.user_data["__await_edit_price__"] = True
                    raise DispatcherHandlerStop()
                changes["min_price"] = float(mn)
                changes["max_price"] = float(mx)
                txt, kb = _edit_link_menu(prod, changes)
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

    # (3) Detect a bot start-link and open edit menu
    m = BOT_START_LINK_RE.search(t)
    if m:
        bot_un = (m.group(1) or "").lstrip("@")
        payload = m.group(2) or ""
        try:
            me = context.bot.get_me()
            my_un = (me.username or "").lower()
        except Exception:
            my_un = (getattr(context.bot, "username", "") or "").lower()
        if bot_un.lower() == my_un and payload.startswith("item_"):
            prod = c_products.find_one({"item_id": payload})
            if prod and int(prod.get("added_by", 0) or 0) == int(user_id) and ("channel_id" in prod or "files" in prod):
                _clear_edit_link_state(context)
                context.user_data["__edit_link_item_id__"] = payload
                context.user_data["__edit_link_changes__"] = {}
                txt, kb = _edit_link_menu(prod, {})
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop().lower()
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
    t = (update.message.text or "").strip()

    # (2) Edit-link input capture (only for links created by the same admin)
    if context.user_data.get("__edit_link_item_id__"):
        item_id = context.user_data.get("__edit_link_item_id__")
        prod = c_products.find_one({"item_id": item_id}) if item_id else None
        if (not prod) or (int(prod.get("added_by", 0) or 0) != int(user_id)):
            # stale/invalid session
            _clear_edit_link_state(context)
        else:
            changes = context.user_data.setdefault("__edit_link_changes__", {})
            # waiting for channel ref
            if context.user_data.pop("__await_edit_channel__", False):
                if "channel_id" not in prod:
                    update.message.reply_text("❌ This item is not a channel product.")
                elif not CHANNEL_REF_RE.match(t):
                    update.message.reply_text("Invalid channel. Send like `-100...` or `@username` or `https://t.me/...`.", parse_mode=ParseMode.MARKDOWN)
                    context.user_data["__await_edit_channel__"] = True
                    raise DispatcherHandlerStop()
                else:
                    try:
                        ch_id = _resolve_channel(context, t)
                    except (BadRequest, Unauthorized) as e:
                        update.message.reply_text(f"❌ I couldn't access that channel: {e}")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    if not _bot_is_admin(context, ch_id):
                        update.message.reply_text("❌ I'm not an admin there. Add me and try again.")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    changes["channel_id"] = int(ch_id)
                    txt, kb = _edit_link_menu(prod, changes)
                    update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

            # waiting for price
            if context.user_data.pop("__await_edit_price__", False):
                try:
                    if "-" in t:
                        a, b = t.split("-", 1)
                        mn, mx = float(a), float(b)
                        assert mx >= mn and mn >= 0
                    else:
                        v = float(t); assert v >= 0
                        mn = mx = v
                except Exception:
                    update.message.reply_text("Invalid. Send like 10 or 10-30.")
                    context.user_data["__await_edit_price__"] = True
                    raise DispatcherHandlerStop()
                changes["min_price"] = float(mn)
                changes["max_price"] = float(mx)
                txt, kb = _edit_link_menu(prod, changes)
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

    # (3) Detect a bot start-link and open edit menu
    m = BOT_START_LINK_RE.search(t)
    if m:
        bot_un = (m.group(1) or "").lstrip("@")
        payload = m.group(2) or ""
        try:
            me = context.bot.get_me()
            my_un = (me.username or "").lower()
        except Exception:
            my_un = (getattr(context.bot, "username", "") or "").lower()
        if bot_un.lower() == my_un and payload.startswith("item_"):
            prod = c_products.find_one({"item_id": payload})
            if prod and int(prod.get("added_by", 0) or 0) == int(user_id) and ("channel_id" in prod or "files" in prod):
                _clear_edit_link_state(context)
                context.user_data["__edit_link_item_id__"] = payload
                context.user_data["__edit_link_changes__"] = {}
                txt, kb = _edit_link_menu(prod, {})
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                # Stop other handlers (especially add_channel_conv) from treating this as a new product flow.
                raise DispatcherHandlerStop()
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

    # (2) Edit-link input capture (only for links created by the same admin)
    if context.user_data.get("__edit_link_item_id__"):
        item_id = context.user_data.get("__edit_link_item_id__")
        prod = c_products.find_one({"item_id": item_id}) if item_id else None
        if (not prod) or (int(prod.get("added_by", 0) or 0) != int(user_id)):
            # stale/invalid session
            _clear_edit_link_state(context)
        else:
            changes = context.user_data.setdefault("__edit_link_changes__", {})
            # waiting for channel ref
            if context.user_data.pop("__await_edit_channel__", False):
                if "channel_id" not in prod:
                    update.message.reply_text("❌ This item is not a channel product.")
                elif not CHANNEL_REF_RE.match(t):
                    update.message.reply_text("Invalid channel. Send like `-100...` or `@username` or `https://t.me/...`.", parse_mode=ParseMode.MARKDOWN)
                    context.user_data["__await_edit_channel__"] = True
                    raise DispatcherHandlerStop()
                else:
                    try:
                        ch_id = _resolve_channel(context, t)
                    except (BadRequest, Unauthorized) as e:
                        update.message.reply_text(f"❌ I couldn't access that channel: {e}")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    if not _bot_is_admin(context, ch_id):
                        update.message.reply_text("❌ I'm not an admin there. Add me and try again.")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    changes["channel_id"] = int(ch_id)
                    txt, kb = _edit_link_menu(prod, changes)
                    update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

            # waiting for price
            if context.user_data.pop("__await_edit_price__", False):
                try:
                    if "-" in t:
                        a, b = t.split("-", 1)
                        mn, mx = float(a), float(b)
                        assert mx >= mn and mn >= 0
                    else:
                        v = float(t); assert v >= 0
                        mn = mx = v
                except Exception:
                    update.message.reply_text("Invalid. Send like 10 or 10-30.")
                    context.user_data["__await_edit_price__"] = True
                    raise DispatcherHandlerStop()
                changes["min_price"] = float(mn)
                changes["max_price"] = float(mx)
                txt, kb = _edit_link_menu(prod, changes)
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

    # (3) Detect a bot start-link and open edit menu
    m = BOT_START_LINK_RE.search(t)
    if m:
        bot_un = (m.group(1) or "").lstrip("@")
        payload = m.group(2) or ""
        try:
            me = context.bot.get_me()
            my_un = (me.username or "").lower()
        except Exception:
            my_un = (getattr(context.bot, "username", "") or "").lower()
        if bot_un.lower() == my_un and payload.startswith("item_"):
            prod = c_products.find_one({"item_id": payload})
            if prod and int(prod.get("added_by", 0) or 0) == int(user_id) and ("channel_id" in prod or "files" in prod):
                _clear_edit_link_state(context)
                context.user_data["__edit_link_item_id__"] = payload
                context.user_data["__edit_link_changes__"] = {}
                txt, kb = _edit_link_menu(prod, {})
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()
    if t.lower() != "skip": pool[idx]["name"] = t if t else None; set_upi_pool(pool)
    update.message.reply_text(f"Send *minimum amount* or `none` (current: {pool[idx].get('min_amt','none')}).", parse_mode=ParseMode.MARKDOWN)
    return UPI_EDIT_MIN

def upi_edit__min(update: Update, context: CallbackContext):
    # Only owner can access UPI settings
    if update.effective_user.id != OWNER_ID: return ConversationHandler.END
    pool = get_upi_pool(); idx = context.user_data.get("edit_idx", -1)
    if idx < 0 or idx >= len(pool):
        update.message.reply_text("Edit session expired. Try /settings again."); return ConversationHandler.END
    t = (update.message.text or "").strip()

    # (2) Edit-link input capture (only for links created by the same admin)
    if context.user_data.get("__edit_link_item_id__"):
        item_id = context.user_data.get("__edit_link_item_id__")
        prod = c_products.find_one({"item_id": item_id}) if item_id else None
        if (not prod) or (int(prod.get("added_by", 0) or 0) != int(user_id)):
            # stale/invalid session
            _clear_edit_link_state(context)
        else:
            changes = context.user_data.setdefault("__edit_link_changes__", {})
            # waiting for channel ref
            if context.user_data.pop("__await_edit_channel__", False):
                if "channel_id" not in prod:
                    update.message.reply_text("❌ This item is not a channel product.")
                elif not CHANNEL_REF_RE.match(t):
                    update.message.reply_text("Invalid channel. Send like `-100...` or `@username` or `https://t.me/...`.", parse_mode=ParseMode.MARKDOWN)
                    context.user_data["__await_edit_channel__"] = True
                    raise DispatcherHandlerStop()
                else:
                    try:
                        ch_id = _resolve_channel(context, t)
                    except (BadRequest, Unauthorized) as e:
                        update.message.reply_text(f"❌ I couldn't access that channel: {e}")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    if not _bot_is_admin(context, ch_id):
                        update.message.reply_text("❌ I'm not an admin there. Add me and try again.")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    changes["channel_id"] = int(ch_id)
                    txt, kb = _edit_link_menu(prod, changes)
                    update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

            # waiting for price
            if context.user_data.pop("__await_edit_price__", False):
                try:
                    if "-" in t:
                        a, b = t.split("-", 1)
                        mn, mx = float(a), float(b)
                        assert mx >= mn and mn >= 0
                    else:
                        v = float(t); assert v >= 0
                        mn = mx = v
                except Exception:
                    update.message.reply_text("Invalid. Send like 10 or 10-30.")
                    context.user_data["__await_edit_price__"] = True
                    raise DispatcherHandlerStop()
                changes["min_price"] = float(mn)
                changes["max_price"] = float(mx)
                txt, kb = _edit_link_menu(prod, changes)
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

    # (3) Detect a bot start-link and open edit menu
    m = BOT_START_LINK_RE.search(t)
    if m:
        bot_un = (m.group(1) or "").lstrip("@")
        payload = m.group(2) or ""
        try:
            me = context.bot.get_me()
            my_un = (me.username or "").lower()
        except Exception:
            my_un = (getattr(context.bot, "username", "") or "").lower()
        if bot_un.lower() == my_un and payload.startswith("item_"):
            prod = c_products.find_one({"item_id": payload})
            if prod and int(prod.get("added_by", 0) or 0) == int(user_id) and ("channel_id" in prod or "files" in prod):
                _clear_edit_link_state(context)
                context.user_data["__edit_link_item_id__"] = payload
                context.user_data["__edit_link_changes__"] = {}
                txt, kb = _edit_link_menu(prod, {})
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop().lower()
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
    t = (update.message.text or "").strip()

    # (2) Edit-link input capture (only for links created by the same admin)
    if context.user_data.get("__edit_link_item_id__"):
        item_id = context.user_data.get("__edit_link_item_id__")
        prod = c_products.find_one({"item_id": item_id}) if item_id else None
        if (not prod) or (int(prod.get("added_by", 0) or 0) != int(user_id)):
            # stale/invalid session
            _clear_edit_link_state(context)
        else:
            changes = context.user_data.setdefault("__edit_link_changes__", {})
            # waiting for channel ref
            if context.user_data.pop("__await_edit_channel__", False):
                if "channel_id" not in prod:
                    update.message.reply_text("❌ This item is not a channel product.")
                elif not CHANNEL_REF_RE.match(t):
                    update.message.reply_text("Invalid channel. Send like `-100...` or `@username` or `https://t.me/...`.", parse_mode=ParseMode.MARKDOWN)
                    context.user_data["__await_edit_channel__"] = True
                    raise DispatcherHandlerStop()
                else:
                    try:
                        ch_id = _resolve_channel(context, t)
                    except (BadRequest, Unauthorized) as e:
                        update.message.reply_text(f"❌ I couldn't access that channel: {e}")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    if not _bot_is_admin(context, ch_id):
                        update.message.reply_text("❌ I'm not an admin there. Add me and try again.")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    changes["channel_id"] = int(ch_id)
                    txt, kb = _edit_link_menu(prod, changes)
                    update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

            # waiting for price
            if context.user_data.pop("__await_edit_price__", False):
                try:
                    if "-" in t:
                        a, b = t.split("-", 1)
                        mn, mx = float(a), float(b)
                        assert mx >= mn and mn >= 0
                    else:
                        v = float(t); assert v >= 0
                        mn = mx = v
                except Exception:
                    update.message.reply_text("Invalid. Send like 10 or 10-30.")
                    context.user_data["__await_edit_price__"] = True
                    raise DispatcherHandlerStop()
                changes["min_price"] = float(mn)
                changes["max_price"] = float(mx)
                txt, kb = _edit_link_menu(prod, changes)
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

    # (3) Detect a bot start-link and open edit menu
    m = BOT_START_LINK_RE.search(t)
    if m:
        bot_un = (m.group(1) or "").lstrip("@")
        payload = m.group(2) or ""
        try:
            me = context.bot.get_me()
            my_un = (me.username or "").lower()
        except Exception:
            my_un = (getattr(context.bot, "username", "") or "").lower()
        if bot_un.lower() == my_un and payload.startswith("item_"):
            prod = c_products.find_one({"item_id": payload})
            if prod and int(prod.get("added_by", 0) or 0) == int(user_id) and ("channel_id" in prod or "files" in prod):
                _clear_edit_link_state(context)
                context.user_data["__edit_link_item_id__"] = payload
                context.user_data["__edit_link_changes__"] = {}
                txt, kb = _edit_link_menu(prod, {})
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop().lower()
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
    t = (update.message.text or "").strip()

    # (2) Edit-link input capture (only for links created by the same admin)
    if context.user_data.get("__edit_link_item_id__"):
        item_id = context.user_data.get("__edit_link_item_id__")
        prod = c_products.find_one({"item_id": item_id}) if item_id else None
        if (not prod) or (int(prod.get("added_by", 0) or 0) != int(user_id)):
            # stale/invalid session
            _clear_edit_link_state(context)
        else:
            changes = context.user_data.setdefault("__edit_link_changes__", {})
            # waiting for channel ref
            if context.user_data.pop("__await_edit_channel__", False):
                if "channel_id" not in prod:
                    update.message.reply_text("❌ This item is not a channel product.")
                elif not CHANNEL_REF_RE.match(t):
                    update.message.reply_text("Invalid channel. Send like `-100...` or `@username` or `https://t.me/...`.", parse_mode=ParseMode.MARKDOWN)
                    context.user_data["__await_edit_channel__"] = True
                    raise DispatcherHandlerStop()
                else:
                    try:
                        ch_id = _resolve_channel(context, t)
                    except (BadRequest, Unauthorized) as e:
                        update.message.reply_text(f"❌ I couldn't access that channel: {e}")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    if not _bot_is_admin(context, ch_id):
                        update.message.reply_text("❌ I'm not an admin there. Add me and try again.")
                        context.user_data["__await_edit_channel__"] = True
                        raise DispatcherHandlerStop()
                    changes["channel_id"] = int(ch_id)
                    txt, kb = _edit_link_menu(prod, changes)
                    update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

            # waiting for price
            if context.user_data.pop("__await_edit_price__", False):
                try:
                    if "-" in t:
                        a, b = t.split("-", 1)
                        mn, mx = float(a), float(b)
                        assert mx >= mn and mn >= 0
                    else:
                        v = float(t); assert v >= 0
                        mn = mx = v
                except Exception:
                    update.message.reply_text("Invalid. Send like 10 or 10-30.")
                    context.user_data["__await_edit_price__"] = True
                    raise DispatcherHandlerStop()
                changes["min_price"] = float(mn)
                changes["max_price"] = float(mx)
                txt, kb = _edit_link_menu(prod, changes)
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                raise DispatcherHandlerStop()

    # (3) Detect a bot start-link and open edit menu
    m = BOT_START_LINK_RE.search(t)
    if m:
        bot_un = (m.group(1) or "").lstrip("@")
        payload = m.group(2) or ""
        try:
            me = context.bot.get_me()
            my_un = (me.username or "").lower()
        except Exception:
            my_un = (getattr(context.bot, "username", "") or "").lower()
        if bot_un.lower() == my_un and payload.startswith("item_"):
            prod = c_products.find_one({"item_id": payload})
            if prod and int(prod.get("added_by", 0) or 0) == int(user_id) and ("channel_id" in prod or "files" in prod):
                _clear_edit_link_state(context)
                context.user_data["__edit_link_item_id__"] = payload
                context.user_data["__edit_link_changes__"] = {}
                txt, kb = _edit_link_menu(prod, {})
                update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                # Stop other handlers (especially add_channel_conv) from treating this as a new product flow.
                raise DispatcherHandlerStop()
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
    if context.args:
        item_id = (context.args[0] or "").strip()
        if uid in ALL_ADMINS and item_id.startswith("item_"):
            prod = c_products.find_one({"item_id": item_id})
            if _is_special_post_product(prod):
                log.info("admin start-link special-post edit redirect admin=%s item=%s", uid, item_id)
                _open_special_post_builder(update, context, prod, item_id)
                return
            if _is_editable_link_product(prod):
                log.info("admin start-link edit redirect admin=%s item=%s", uid, item_id)
                _open_edit_link_menu(update, context, prod, item_id)
                return
        return start_purchase(context, chat_id, uid, item_id)

    sent_ids = []
    start_post = _start_post_record()

    if start_post:
        sent_mid = _copy_stored_message_to_chat(
            context,
            chat_id,
            start_post,
            protect_content=PROTECT_CONTENT_ENABLED,
        )
        if sent_mid:
            sent_ids.append(sent_mid)
        else:
            log.warning("Configured startup post could not be delivered; falling back to welcome message.")

    if not sent_ids:
        photo = cfg("welcome_photo_id")
        text = cfg("welcome_text", "Welcome!")
        reply_markup = _admin_start_keyboard(uid) if uid in ALL_ADMINS else None
        if photo:
            sent = msg.reply_photo(photo=photo, caption=text, reply_markup=reply_markup)
        else:
            sent = msg.reply_text(text, reply_markup=reply_markup)
        sent_ids.append(sent.message_id)
    elif uid in ALL_ADMINS:
        msg.reply_text(
            _render_admin_start_panel(uid),
            parse_mode=ParseMode.HTML,
            reply_markup=_admin_start_keyboard(uid),
        )

    _queue_auto_delete(context, chat_id, sent_ids, _start_message_delete_minutes(), prefix="startmsg")

def main():
    # default seeds
    set_cfg("welcome_text", cfg("welcome_text", "Welcome!"))
    set_cfg("force_sub_text", cfg("force_sub_text", "Join required channels to continue."))
    if cfg("qr_unpaid_delete_minutes") is None: set_cfg("qr_unpaid_delete_minutes", PAY_WINDOW_MINUTES)
    if cfg("broadcast_delete_minutes") is None: set_cfg("broadcast_delete_minutes", DELETE_AFTER_MINUTES)
    if cfg("start_message_delete_minutes") is None: set_cfg("start_message_delete_minutes", 0)
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

    dp.add_handler(MessageHandler(Filters.all & admin, special_post_capture_router), group=-3)
    dp.add_handler(MessageHandler(Filters.all & owner, start_post_capture_router), group=-2)

    # Edit-link: capture admin files without triggering add-product
    dp.add_handler(MessageHandler((Filters.document | Filters.video | Filters.photo) & admin, admin_edit_files_router), group=-1)

    broadcast_conv = ConversationHandler(
        entry_points=[
            CommandHandler("broadcast", bc_start, filters=admin),
            CommandHandler("broadcast_specific", bc_start, filters=admin),
            CallbackQueryHandler(bc_start, pattern=r"^admin:broadcast(?:_specific)?$")
        ],
        states={
            GET_BROADCAST_FILES: [
                MessageHandler((Filters.document | Filters.video | Filters.photo) & ~Filters.command, bc_files),
                CommandHandler('done', bc_done_files, filters=admin)
            ],
            GET_BROADCAST_TEXT: [
                MessageHandler(Filters.text & ~Filters.command, bc_text),
                CommandHandler('skip', bc_skip, filters=admin)
            ],
            GET_BROADCAST_TARGET_USERS: [
                MessageHandler(Filters.text & ~Filters.command, bc_target_ids)
            ],
            BROADCAST_CONFIRM: [
                CallbackQueryHandler(bc_send, pattern=r"^send_bc$"),
                CallbackQueryHandler(bc_cancel, pattern=r"^cancel_bc$")
            ]
        },
        fallbacks=[CommandHandler('cancel', cancel_conv, filters=admin)],
        name="broadcast_conv",
        persistent=False
    )

    # Add product flows
    add_conv = ConversationHandler(
        entry_points=[MessageHandler((Filters.document | Filters.video | Filters.photo) & admin, add_product_start)],
        states={
            GET_PRODUCT_FILES: [MessageHandler((Filters.document | Filters.video | Filters.photo) & ~Filters.command, get_product_files),
                               CommandHandler('done', finish_adding_files, filters=admin)],
            PRICE: [MessageHandler(Filters.text & ~Filters.command, get_price)],
            GET_FREE_FILE_TEXT: [
                MessageHandler(Filters.text & ~Filters.command, get_free_file_text),
                CommandHandler('skip', skip_free_file_text, filters=admin),
            ],
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

    dp.add_handler(broadcast_conv, group=0)
    dp.add_handler(add_conv, group=0)
    dp.add_handler(add_channel_conv, group=0)

    # Broadcast & misc
    dp.add_handler(CommandHandler("start", cmd_start))
    dp.add_handler(CommandHandler("stats", stats, filters=admin))
    dp.add_handler(CommandHandler("earning", earnings_cmd, filters=admin))  # New earnings command
    dp.add_handler(CommandHandler("qr_timeout", qr_timeout_show, filters=admin))
    dp.add_handler(CommandHandler("set_qr_timeout", set_qr_timeout, filters=admin))
    dp.add_handler(CommandHandler("broadcast_delete", broadcast_delete_show, filters=admin))
    dp.add_handler(CommandHandler("set_broadcast_delete", set_broadcast_delete, filters=admin))
    dp.add_handler(CommandHandler("protect_on", protect_on, filters=admin))
    dp.add_handler(CommandHandler("protect_off", protect_off, filters=admin))
    dp.add_handler(CallbackQueryHandler(on_cb, pattern=r"^(check_join|admin:settings|admin:startmenu|admin:bcdel|bcdel:set:\d+)$"))

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
    dp.add_handler(CallbackQueryHandler(editln_cb, pattern=r"^editln:(ch|ch_keep|files|files_keep|files_done|price|price_keep|back|freetext|freetext_clear|save|cancel)$"), group=1)
    dp.add_handler(CallbackQueryHandler(special_post_cb, pattern=r"^(admin:specialpost|specialpost:(cycles|setcycles:\d+|cycles_manual|resend|delete|back|save|cancel))$"), group=1)
    dp.add_handler(CallbackQueryHandler(cfg_support_cb, pattern=r"^cfg:support$"), group=1)
    dp.add_handler(CallbackQueryHandler(cfg_start_post_cb, pattern=r"^cfg:startpost$"), group=1)
    dp.add_handler(CallbackQueryHandler(cfg_start_message_delete_cb, pattern=r"^cfg:startmsgdel$"), group=1)
    dp.add_handler(CallbackQueryHandler(cfg_default_free_file_text_cb, pattern=r"^cfg:defaultfreetext$"), group=1)
    dp.add_handler(CallbackQueryHandler(upi_cb, pattern=r"^upi:(reset|force|force_clear|main:\d+|del:\d+)$"), group=1)

    # Storage management UI (owner only)
    dp.add_handler(CallbackQueryHandler(storage_menu_cb, pattern=r"^storage:menu$"), group=1)
    dp.add_handler(CallbackQueryHandler(storage_add_cb, pattern=r"^storage:add$"), group=1)
    dp.add_handler(CallbackQueryHandler(storage_resync_cb, pattern=r"^storage:resync$"), group=1)
    dp.add_handler(CallbackQueryHandler(storage_make_main_cb, pattern=r"^storage:main:\d+$"), group=1)
    dp.add_handler(CallbackQueryHandler(storage_del_cb, pattern=r"^storage:del:\d+$"), group=1)

    # Text router (must run BEFORE add_channel_conv so edit-link inputs don't get treated as new products)
    dp.add_handler(MessageHandler(Filters.text & admin, admin_text_router), group=-1)

    # Startup auto-resync (non-blocking)
    updater.job_queue.run_once(_resync_all_job, when=2)
    updater.job_queue.run_repeating(_process_broadcast_deletes, interval=1, first=10, name="broadcast_delete_sweeper")
    updater.job_queue.run_repeating(_process_special_post_cycles, interval=30, first=30, name="special_post_cycle_sweeper")

    logging.info("Bot running…"); updater.start_polling(); updater.idle()

if __name__ == "__main__":
    main()
