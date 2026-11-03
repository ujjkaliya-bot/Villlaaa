"""
bot.py - Main Telegram Bot: OTP / Session Store
Database: MongoDB (Motor) via mongo_compat - all DB calls are async.
"""
import os
import re
import csv
import html
import time
import asyncio
import logging
import zipfile
import shutil
from datetime import datetime

import aiohttp
from telethon import TelegramClient, events, Button
from telethon.errors import (
    SessionPasswordNeededError,
    MessageNotModifiedError,
    UserNotParticipantError,
    ChatAdminRequiredError,
    AuthKeyUnregisteredError,
    AuthKeyDuplicatedError,
    UserDeactivatedBanError,
)
from telethon.tl.types import (
    ReplyKeyboardMarkup,
    KeyboardButtonRow,
    KeyboardButton,
    KeyboardButtonWebView,
)
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.tl.functions.account import GetPasswordRequest

# ─── Project Imports ──────────────────────────────────────────────────────────
from config import (
    API_ID, API_HASH, BOT_TOKEN, ADMIN_ID,
    LOG_CHANNEL_ID, CHECK_CHANNELS, JOIN_URLS,
    PAYMENT_LOG_FULL_ID, PAYMENT_LOG_MASKED_ID,
    UPI_ID, UPI_MID,
    BANK_NAME, BANK_HOLDER, BANK_ACCOUNT_NO, BANK_IFSC,
    TERMS_URL, AUTO_CANCEL_SECONDS, OTP_REGEX,
    START_IMAGE, DEPOSIT_IMAGE, PROFILE_IMAGE, STATS_IMAGE, SUPPORT_IMAGE, BUY_IMAGE,
    P_YES, P_NO, P_PKG, P_MONEY, P_USDT, P_INR, P_TG,
    P_GIFT, P_STATS, P_CARD, P_USERS, P_CAL, P_PC, P_EYE,
    P_UPI, P_CW, P_ON, P_OFF, P_ID, P_KEY, P_GLOBE,
    P_CART, P_STORE, P_OTP, P_2FA, P_FLAG, P_PHONE,
    P_WAIT, P_TIME, P_WARN, P_DOC, P_SOS, P_ASST, P_ACC,
)
from database import (
    get_pool, get_user_lock, init_db_pool, setup_db, keepalive_loop,
    ensure_user, is_user_banned, update_balance,
    is_bot_online, get_usdt_rate, get_support_url, to_usd, get_maint_msg,
    is_referral_enabled, is_images_enabled, get_stock_layout,
    get_welcome_msg, is_autopay_enabled, get_autopay_config,
)
from helpers import (
    is_admin, has_perm,
    check_channel_joined,
    COUNTRY_CODES,
    get_flag_by_country_name, get_country_info,
    delete_session_files,
    mask_phone, mask_id, mask_otp, mask_username,
)
from addstock_devices import run_addstock_flow, register_device_handlers, bot_session_pending_logout
from demo import register_demo_handlers
from email_verify import fetch_recent_credits

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Bot Client ───────────────────────────────────────────────────────────────
os.makedirs("sessions", exist_ok=True)
session_name = f"sessions/bot_{BOT_TOKEN.split(':')[0]}"
bot = TelegramClient(session_name, API_ID, API_HASH)
bot.parse_mode = 'html'

# ─── Shared HTTP Session ──────────────────────────────────────────────────────
_http_session: aiohttp.ClientSession = None

def get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15),
            connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300),
        )
    return _http_session

# ─── In-Memory State ─────────────────────────────────────────────────────────
active_orders:      dict = {}
waiting_proof:      dict = {}
deposit_input:      dict = {}
admin_dep_state:    dict = {}
user_spam_cooldown: dict = {}
session_buy_state:  dict = {}
custom_dep_amt:     dict = {}
autopay_input:      dict = {}
autopay_state:      dict = {}
add_session_file_state: dict = {}

# ─── Image cache (avoids re-fetching banner images on every message) ──────────
_image_cache: dict = {}

# ─── Bot-me cache (avoids calling bot.get_me() on every request) ─────────────
_bot_me = None

async def get_bot_me():
    global _bot_me
    if _bot_me is None:
        _bot_me = await bot.get_me()
    return _bot_me

# ─── Entity cache (avoids calling bot.get_entity() on every menu load) ────────
_entity_cache: dict = {}  # uid -> (entity, timestamp)
_ENTITY_TTL = 300  # seconds

async def get_cached_entity(uid: int):
    import time
    now = time.time()
    if uid in _entity_cache:
        entity, ts = _entity_cache[uid]
        if now - ts < _ENTITY_TTL:
            return entity
    try:
        entity = await bot.get_entity(uid)
        _entity_cache[uid] = (entity, now)
        return entity
    except Exception:
        return None

async def get_cached_image(url: str):
    """Upload image once to Telegram and reuse the file_id thereafter.
    Works for both remote URLs and local file paths on the VPS."""
    if url not in _image_cache:
        try:
            msg = await bot.send_file("me", url)
            _image_cache[url] = msg.media
        except Exception:
            _image_cache[url] = None
    return _image_cache[url]


async def get_banner_image(key: str, default_url: str) -> str:
    """Return the active banner image for `key`: a custom VPS-uploaded image
    if the admin set one via the Banner Images panel, else the default URL
    from config.py. Stored in the `settings` table so it's shared across
    multiple bot instances using the same database."""
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT value FROM settings WHERE key=$1", f"banner_{key}")
        if row and row["value"] and os.path.exists(row["value"]):
            return row["value"]
    except Exception as exc:
        logger.warning(f"get_banner_image lookup failed for {key}: {exc}")
    return default_url


# =============================================================================
#  LOG CHANNEL HELPERS
# =============================================================================
def _get_purchase_log_channel() -> int:
    """FULL (private/admin) log channel."""
    return PAYMENT_LOG_FULL_ID

def _get_payment_log_channel() -> int:
    """FULL (private/admin) log channel. Admin Accept/Reject buttons and
    screenshots always go here, never to the masked channel."""
    return PAYMENT_LOG_FULL_ID

def _get_masked_log_channel() -> int:
    """MASKED (public/proof) log channel, 0 = disabled."""
    return int(PAYMENT_LOG_MASKED_ID or 0)


async def send_dual_log(full_msg: str, masked_msg: str):
    """Send the complete log to the FULL channel and the masked copy to the
    MASKED channel. One channel failing never blocks the other."""
    try:
        await bot.send_message(_get_payment_log_channel(), full_msg)
    except Exception as exc:
        logger.error(f"FULL log channel send failed: {exc}")
    masked_id = _get_masked_log_channel()
    if masked_id:
        try:
            await bot.send_message(masked_id, masked_msg)
        except Exception as exc:
            logger.error(f"MASKED log channel send failed: {exc}")


# =============================================================================
#  IMAGE HELPER
# =============================================================================
async def send_with_image(chat_id: int, image: str, caption: str, buttons=None):
    if image and await is_images_enabled():
        try:
            media = await get_cached_image(image)
            if media:
                return await bot.send_file(chat_id, media, caption=caption, buttons=buttons)
            return await bot.send_file(chat_id, image, caption=caption, buttons=buttons)
        except Exception as exc:
            logger.warning(f"send_with_image failed ({exc}), falling back to text.")
    return await bot.send_message(chat_id, caption, buttons=buttons)


async def get_persistent_menu(uid: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton("🛒 Buy Account"), KeyboardButton("👤 My Profile")],
        [KeyboardButton("📁 Buy Sessions")],
        [KeyboardButton("💰 Deposit"), KeyboardButton("📊 My Stats")],
        [KeyboardButton("📞 Support")],
    ]
    if await is_admin(uid):
        rows.append([KeyboardButton("🔐 Admin Panel")])
    return ReplyKeyboardMarkup([KeyboardButtonRow(r) for r in rows], resize=True)


async def send_main_menu(event, uid: int):
    if not await is_bot_online() and not await is_admin(uid):
        maint_text = await get_maint_msg()
        msg = f"{P_OFF} <b>Maintenance Mode</b>\n\n{maint_text}"
        if isinstance(event, events.CallbackQuery.Event):
            try:
                await event.edit(msg)
            except Exception:
                await bot.send_message(uid, msg)
        else:
            await bot.send_message(uid, msg)
        return

    try:
        sender = await get_cached_entity(uid)
        first_name = (sender.first_name if sender else None) or "there"
    except Exception:
        first_name = "there"

    msg = await get_welcome_msg(first_name)
    inline_btns = None

    if isinstance(event, events.CallbackQuery.Event):
        try:
            await event.delete()
        except Exception:
            pass
        _start_img = await get_banner_image("start", START_IMAGE)
        await send_with_image(uid, _start_img, msg, buttons=inline_btns)
        await bot.send_message(uid, ".", buttons=await get_persistent_menu(uid))
    else:
        _start_img = await get_banner_image("start", START_IMAGE)
        await send_with_image(uid, _start_img, msg, buttons=inline_btns)
        await bot.send_message(uid, ".", buttons=await get_persistent_menu(uid))


def format_payment_buttons(buttons: list) -> list:
    res = []
    for i in range(0, len(buttons), 2):
        res.append(buttons[i:i + 2])
    return res


def get_keypad() -> list:
    return [
        [Button.inline("1", "kp_1"), Button.inline("2", "kp_2"), Button.inline("3", "kp_3")],
        [Button.inline("4", "kp_4"), Button.inline("5", "kp_5"), Button.inline("6", "kp_6")],
        [Button.inline("7", "kp_7"), Button.inline("8", "kp_8"), Button.inline("9", "kp_9")],
        [Button.inline("🔙 Del", "kp_del"), Button.inline("0", "kp_0"), Button.inline("✅ Confirm", "kp_done")],
        [Button.inline("❌ Cancel", "cancel_action")],
    ]


def get_bank_keypad() -> list:
    return [
        [Button.inline("1", "bkp_1"), Button.inline("2", "bkp_2"), Button.inline("3", "bkp_3")],
        [Button.inline("4", "bkp_4"), Button.inline("5", "bkp_5"), Button.inline("6", "bkp_6")],
        [Button.inline("7", "bkp_7"), Button.inline("8", "bkp_8"), Button.inline("9", "bkp_9")],
        [Button.inline("🔙 Del", "bkp_del"), Button.inline("0", "bkp_0"), Button.inline("✅ Confirm", "bkp_done")],
        [Button.inline("❌ Cancel", "cancel_action")],
    ]


def get_autopay_keypad() -> list:
    return [
        [Button.inline("1", "akp_1"), Button.inline("2", "akp_2"), Button.inline("3", "akp_3")],
        [Button.inline("4", "akp_4"), Button.inline("5", "akp_5"), Button.inline("6", "akp_6")],
        [Button.inline("7", "akp_7"), Button.inline("8", "akp_8"), Button.inline("9", "akp_9")],
        [Button.inline("🔙 Del", "akp_del"), Button.inline("0", "akp_0"), Button.inline("✅ Confirm", "akp_done")],
        [Button.inline("❌ Cancel", "cancel_action")],
    ]


def get_admin_custom_keypad(dep_id: int) -> list:
    d = dep_id
    return [
        [Button.inline("1", f"dkp|{d}|1"), Button.inline("2", f"dkp|{d}|2"), Button.inline("3", f"dkp|{d}|3")],
        [Button.inline("4", f"dkp|{d}|4"), Button.inline("5", f"dkp|{d}|5"), Button.inline("6", f"dkp|{d}|6")],
        [Button.inline("7", f"dkp|{d}|7"), Button.inline("8", f"dkp|{d}|8"), Button.inline("9", f"dkp|{d}|9")],
        [Button.inline("🔙 Del", f"dkp|{d}|del"), Button.inline("0", f"dkp|{d}|0"), Button.inline("✅ Confirm", f"dkp|{d}|conf")],
        [Button.inline("❌ Cancel", f"dkp|{d}|cancel")],
    ]


# =============================================================================
#  LOGGING / NOTIFICATIONS
# =============================================================================
async def process_referral_bonus(uid: int, amount: int):
    if not await is_referral_enabled():
        return
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT referred_by FROM users WHERE user_id=$1", uid)
    ref = row["referred_by"] if row else None
    if not ref:
        return
    async with pool.acquire() as conn:
        pct_row = await conn.fetchrow("SELECT value FROM settings WHERE key='ref_percent'")
    pct = float(pct_row["value"]) if pct_row else 3.0
    if pct > 0:
        bonus = int(amount * (pct / 100))
        if bonus > 0:
            await update_balance(ref, bonus)
            try:
                await bot.send_message(
                    ref,
                    f"{P_GIFT} <b>Referral Bonus!</b>\nYour referral <code>{uid}</code> deposited {P_INR}{amount}.\n"
                    f"{P_YES} You earned <b>{P_INR}{bonus}</b>!",
                )
            except Exception:
                pass


async def log_primary_deposit(uid: int, amt: int, method: str):
    try:
        try:
            user = await get_cached_entity(int(uid))
            raw_username = user.username if user and user.username else None
        except Exception:
            raw_username = None
        now = datetime.now()
        t, d = now.strftime('%H:%M:%S'), now.strftime('%Y-%m-%d')

        full = (
            f"{P_YES} <b>NEW DEPOSIT SUCCESSFUL</b>\n\n"
            f"{P_ACC} <b>ID :</b> <code>{uid}</code>\n"
            f"👤 <b>Username :</b> @{html.escape(raw_username) if raw_username else 'NoUsername'}\n"
            f"{P_MONEY} <b>Amount :</b> {P_INR}{amt}\n"
            f"{P_CARD} <b>Method :</b> {method}\n"
            f"{P_TIME} <b>Time :</b> {t}\n"
            f"{P_CAL} <b>Date :</b> {d}\n\n"
            f"<i>Thanks for depositing in Fresh Tg!</i>"
        )
        masked = (
            f"{P_YES} <b>NEW DEPOSIT SUCCESSFUL</b>\n\n"
            f"{P_ACC} <b>ID :</b> <code>{mask_id(uid)}</code>\n"
            f"👤 <b>Username :</b> @{html.escape(mask_username(raw_username))}\n"
            f"{P_MONEY} <b>Amount :</b> {P_INR}{amt}\n"
            f"{P_CARD} <b>Method :</b> {method}\n"
            f"{P_TIME} <b>Time :</b> {t}\n"
            f"{P_CAL} <b>Date :</b> {d}\n\n"
            f"<i>Thanks for depositing in Fresh Tg!</i>"
        )
        await send_dual_log(full, masked)
    except Exception as exc:
        logger.error(f"log_primary_deposit error: {exc}")


async def log_primary_purchase(uid: int, country: str, price: int, amount: int, year: int, qty: int, phone: str = None, otp: str = None, twofa: str = None):
    try:
        now = datetime.now()
        t, d = now.strftime('%H:%M:%S'), now.strftime('%Y-%m-%d')
        me = await get_bot_me()
        bot_username = f"@{me.username}" if me and me.username else ""

        has_2fa = bool(twofa and twofa != "None")

        def build(num_txt: str, id_txt: str, otp_txt: str, twofa_txt: str, title: str) -> str:
            return (
                f"<b>{title}</b>\n\n"
                f"<blockquote>"
                f"🌍 <b>Country :</b> {country}\n"
                f"📅 <b>Year :</b> {year}\n"
                f"📦 <b>Qty :</b> {qty}"
                f"</blockquote>\n\n"
                f"<blockquote>"
                f"📱 <b>Num :</b> <code>{num_txt}</code>\n"
                f"🔑 <b>OTP :</b> <code>{otp_txt}</code>\n"
                f"🔒 <b>2FA :</b> <code>{twofa_txt}</code>"
                f"</blockquote>\n\n"
                f"<blockquote>"
                f"💰 <b>Price :</b> {P_INR}{price}\n"
                f"⏰ <b>Time :</b> {t}\n"
                f"📅 <b>Date :</b> {d}"
                f"</blockquote>\n\n"
                f"🆔 <b>ID :</b> <code>{id_txt}</code>\n\n"
                f"✦ {bot_username} ✦"
            )

        if phone:
            p_txt = str(phone).strip()
            full_num = p_txt if p_txt.startswith("+") else f"+{p_txt}"
        else:
            full_num = "N/A"

        full = build(
            full_num, str(uid), str(otp) if otp else "N/A",
            twofa if has_2fa else "Disabled", "🛍️ New Number Purchased",
        )
        masked = build(
            mask_phone(phone), mask_id(uid), mask_otp(otp),
            "Enabled" if has_2fa else "Disabled", "🛍️ New Number Purchased",
        )
        await send_dual_log(full, masked)
    except Exception as exc:
        logger.error(f"log_primary_purchase error: {exc}")


# =============================================================================
#  DEPOSIT HANDLERS
# =============================================================================
async def deposit_menu(event, edit: bool = False):
    msg = (
        f"{P_CARD} <b>Select Payment Method:</b>\n\n"
        f"{P_ON} Choose Automatic for instant credit.\n"
        f"{P_WAIT} Choose Manual for other methods."
    )
    pool = get_pool()
    async with pool.acquire() as conn:
        customs = await conn.fetch("SELECT name FROM custom_payments")
    flat_buttons = [Button.inline(f"💳 {r['name']}", f"depm_{r['name']}") for r in customs]
    top_btns = []
    if await is_autopay_enabled():
        top_btns.append([Button.inline("🤖 Auto Payment (Instant)", "dep_autopay")])
    btns = top_btns + format_payment_buttons(flat_buttons)
    if edit and isinstance(event, events.CallbackQuery.Event):
        try:
            await event.edit(msg, buttons=btns)
            return
        except Exception:
            pass
    _deposit_img = await get_banner_image("deposit", DEPOSIT_IMAGE)
    await send_with_image(event.chat_id, _deposit_img, msg, buttons=btns)


async def manual_deposit_init(event, method: str):
    uid = event.sender_id
    deposit_input[uid] = {'step': 'wait_amt', 'method': method}
    await event.edit(
        f"{P_CARD} <b>{method} Deposit</b>\n\n"
        f"👇 Reply to this message with the <b>AMOUNT</b> in ₹ (INR) you want to deposit.",
        buttons=[[Button.inline("❌ Cancel", "cancel_action")]],
    )


async def init_upi_keypad(event):
    uid = event.sender_id
    deposit_input[uid] = {'step': 'upi_keypad', 'val': '0'}
    await event.edit(
        f"{P_KEY} <b>ENTER AMOUNT IN INR (Min ₹1)</b>\n\n{P_MONEY} <code>₹0</code>",
        buttons=get_keypad(),
    )


async def keypad_logic(event):
    uid = event.sender_id
    action = event.data.decode().replace("kp_", "")
    curr = deposit_input.get(uid, {}).get('val', "0")
    if action.isdigit():
        curr = action if curr == "0" else curr + action
        if len(curr) > 5:
            curr = curr[:5]
    elif action == "del":
        curr = curr[:-1] or "0"
    elif action == "done":
        amt = int(curr)
        if amt < 1:
            return await event.answer("⚠️ Minimum Deposit is ₹1", alert=True)
        return await show_upi_qr(event, amt)
    deposit_input[uid] = {'step': 'upi_keypad', 'val': curr}
    try:
        await event.edit(
            f"{P_KEY} <b>ENTER AMOUNT IN INR</b>\n\n{P_MONEY} <code>₹{curr}</code>",
            buttons=get_keypad(),
        )
    except MessageNotModifiedError:
        pass


async def show_upi_qr(event, amount: int):
    uid = event.sender_id
    order_id = f"ORDER_{uid}_{int(time.time())}"
    await event.edit(f"{P_WAIT} Generating Payment QR...")
    url = f"https://redoxng.in/api/genqr.php?upi={UPI_ID}&amount={amount}&name=Pay&orderId={order_id}"
    try:
        session = get_http_session()
        async with session.get(url) as resp:
            data = await resp.json(content_type=None)
        if not data.get("success"):
            return await event.edit(f"{P_NO} Failed to generate QR. Please try again.")
        qr_url = data['url']
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO upi_orders (order_id, user_id, amount, status) VALUES ($1,$2,$3,$4)",
                order_id, uid, amount, "pending",
            )
        msg = (
            f"{P_UPI} <b>AUTOMATIC UPI PAYMENT</b>\n\n"
            f"{P_MONEY} Amount: <code>₹{amount}</code>\n"
            f"{P_ID} Order ID: <code>{order_id}</code>\n\n"
            f"👇 Scan the QR below.\nClick <b>✅ Check Payment</b> after paying."
        )
        await event.delete()
        sent_msg = await bot.send_file(
            uid, qr_url, caption=msg,
            buttons=[
                [Button.inline("✅ Check Payment", f"check_upi_{order_id}")],
                [Button.inline("❌ Cancel", "cancel_action")],
            ],
        )
        asyncio.create_task(auto_check_upi_task(uid, order_id, sent_msg))
    except Exception as exc:
        logger.error(f"show_upi_qr error: {exc}")
        await bot.send_message(uid, f"{P_NO} Error generating QR: {exc}")


async def auto_check_upi_task(uid: int, order_id: str, msg_to_edit):
    pool = get_pool()
    for _ in range(60):
        await asyncio.sleep(10)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT amount, status FROM upi_orders WHERE order_id=$1", order_id
            )
        if not row or row["status"] != 'pending':
            break
        url = f"https://redoxng.in/api/verify.php?orderId={order_id}&mid={UPI_MID}"
        try:
            session = get_http_session()
            async with session.get(url) as resp:
                data = await resp.json(content_type=None)
            if data.get("success") and data.get("status") == "TXN_SUCCESS":
                amt = int(float(data.get("amount", row["amount"])))
                async with get_user_lock(uid):
                    async with pool.acquire() as conn:
                        check = await conn.fetchrow(
                            "SELECT status FROM upi_orders WHERE order_id=$1", order_id
                        )
                        if check and check["status"] == 'pending':
                            prev = await conn.fetchrow(
                                "SELECT balance FROM users WHERE user_id=$1", uid
                            )
                            prev_bal = prev["balance"] if prev else 0
                            rev_row = await conn.fetchrow(
                                "SELECT value FROM settings WHERE key='upi_revenue'"
                            )
                            curr_rev = float(rev_row["value"]) if rev_row else 0.0
                            # Batch all updates in one transaction
                            await conn.execute("""
                                UPDATE users SET
                                    balance = balance + $1,
                                    total_deposited = total_deposited + $1
                                WHERE user_id=$2
                            """, amt, uid)
                            await conn.execute(
                                "UPDATE upi_orders SET status='success' WHERE order_id=$1",
                                order_id,
                            )
                            await conn.execute(
                                "INSERT INTO settings (key, value) VALUES ('upi_revenue', $1) ON CONFLICT (key) DO UPDATE SET value=$1",
                                str(curr_rev + amt),
                            )
                            await process_referral_bonus(uid, amt)
                            await log_primary_deposit(uid, amt, "UPI Automatic")
                            usd_amt = await to_usd(amt)
                            usd_prev = await to_usd(prev_bal)
                            usd_new = await to_usd(prev_bal + amt)
                            user_msg = (
                                f"{P_YES} <b>UPI Deposit Approved!</b>\n\n"
                                f"{P_MONEY} <b>Amount Added:</b> ${usd_amt:.2f} (₹{amt})\n"
                                f"📉 <b>Previous Balance:</b> ${usd_prev:.2f} (₹{prev_bal})\n"
                                f"📈 <b>New Balance:</b> ${usd_new:.2f} (₹{prev_bal + amt})"
                            )
                            await bot.send_message(uid, user_msg)
                            try:
                                await bot.edit_message(uid, msg_to_edit.id, f"{P_YES} <b>Payment Successful! ₹{amt} added to your wallet.</b>")
                            except Exception:
                                pass
                break
        except Exception:
            pass


async def verify_upi_payment(event, order_id: str):
    uid = event.sender_id
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT amount, status FROM upi_orders WHERE order_id=$1", order_id
        )
    if not row:
        return await event.answer("❌ Order not found.", alert=True)
    if row["status"] == 'success':
        return await event.answer("✅ Already credited!", alert=True)

    url = f"https://redoxng.in/api/verify.php?orderId={order_id}&mid={UPI_MID}"
    try:
        session = get_http_session()
        async with session.get(url) as resp:
            data = await resp.json(content_type=None)
        if data.get("success") and data.get("status") == "TXN_SUCCESS":
            amt = int(float(data.get("amount", row["amount"])))
            async with get_user_lock(uid):
                async with pool.acquire() as conn:
                    check = await conn.fetchrow(
                        "SELECT status FROM upi_orders WHERE order_id=$1", order_id
                    )
                    if check and check["status"] == 'success':
                        return await event.answer("✅ Already credited!", alert=True)
                    prev = await conn.fetchrow(
                        "SELECT balance FROM users WHERE user_id=$1", uid
                    )
                    prev_bal = prev["balance"] if prev else 0
                    rev_row = await conn.fetchrow(
                        "SELECT value FROM settings WHERE key='upi_revenue'"
                    )
                    curr_rev = float(rev_row["value"]) if rev_row else 0.0
                    await conn.execute("""
                        UPDATE users SET
                            balance = balance + $1,
                            total_deposited = total_deposited + $1
                        WHERE user_id=$2
                    """, amt, uid)
                    await conn.execute(
                        "UPDATE upi_orders SET status='success' WHERE order_id=$1", order_id
                    )
                    await conn.execute(
                        "INSERT INTO settings (key, value) VALUES ('upi_revenue', $1) ON CONFLICT (key) DO UPDATE SET value=$1",
                        str(curr_rev + amt),
                    )
            await process_referral_bonus(uid, amt)
            await log_primary_deposit(uid, amt, "UPI Automatic")
            try:
                await event.delete()
            except Exception:
                pass
            usd_amt = await to_usd(amt)
            usd_prev = await to_usd(prev_bal)
            usd_new = await to_usd(prev_bal + amt)
            user_msg = (
                f"{P_YES} <b>UPI Deposit Approved!</b>\n\n"
                f"{P_MONEY} <b>Amount Added:</b> ${usd_amt:.2f} (₹{amt})\n"
                f"📉 <b>Previous Balance:</b> ${usd_prev:.2f} (₹{prev_bal})\n"
                f"📈 <b>New Balance:</b> ${usd_new:.2f} (₹{prev_bal + amt})"
            )
            await bot.send_message(uid, user_msg)
        else:
            await event.answer("⏳ Payment not received yet. Check again in 1 min.", alert=True)
    except Exception:
        await event.answer("❌ Gateway Error. Try again later.", alert=True)


# ─── Bank Transfer (Auto UTR Verify via Email) ────────────────────────────────

bank_match_lock = asyncio.Lock()


async def init_bank_keypad(event):
    uid = event.sender_id
    deposit_input[uid] = {'step': 'bank_keypad', 'val': '0'}
    await event.edit(
        f"{P_KEY} <b>ENTER AMOUNT IN INR (Min ₹1)</b>\n\n{P_MONEY} <code>₹0</code>",
        buttons=get_bank_keypad(),
    )


async def bank_keypad_logic(event):
    uid = event.sender_id
    action = event.data.decode().replace("bkp_", "")
    curr = deposit_input.get(uid, {}).get('val', "0")
    if action.isdigit():
        curr = action if curr == "0" else curr + action
        if len(curr) > 5:
            curr = curr[:5]
    elif action == "del":
        curr = curr[:-1] or "0"
    elif action == "done":
        amt = int(curr)
        if amt < 1:
            return await event.answer("⚠️ Minimum Deposit is ₹1", alert=True)
        return await show_bank_details(event, amt)
    deposit_input[uid] = {'step': 'bank_keypad', 'val': curr}
    try:
        await event.edit(
            f"{P_KEY} <b>ENTER AMOUNT IN INR</b>\n\n{P_MONEY} <code>₹{curr}</code>",
            buttons=get_bank_keypad(),
        )
    except MessageNotModifiedError:
        pass


async def show_bank_details(event, amount: int):
    uid = event.sender_id
    order_id = f"BANK_{uid}_{int(time.time())}"
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO bank_orders (order_id, user_id, amount, status) VALUES ($1,$2,$3,$4)",
            order_id, uid, amount, "pending",
        )
    msg = (
        f"{P_UPI} <b>BANK TRANSFER (AUTO VERIFY)</b>\n\n"
        f"{P_MONEY} Amount: <code>₹{amount}</code>\n"
        f"{P_ID} Order ID: <code>{order_id}</code>\n\n"
        f"🏦 <b>Bank:</b> {BANK_NAME}\n"
        f"👤 <b>Account Holder:</b> {BANK_HOLDER}\n"
        f"🔢 <b>Account No:</b> <code>{BANK_ACCOUNT_NO}</code>\n"
        f"🏷️ <b>IFSC:</b> <code>{BANK_IFSC}</code>\n\n"
        f"👇 Pay the <b>exact amount</b> via IMPS/UPI/NEFT to the above account.\n"
        f"Once paid, tap <b>✅ Check Payment</b> — we auto-detect your transfer from the bank's email alert (usually within 1-2 minutes)."
    )
    buttons = [
        [Button.inline("✅ Check Payment", f"check_bank_{order_id}")],
        [Button.inline("❌ Cancel", "cancel_action")],
    ]
    try:
        sent_msg = await event.edit(msg, buttons=buttons)
    except Exception:
        sent_msg = await bot.send_message(uid, msg, buttons=buttons)
    asyncio.create_task(auto_check_bank_task(uid, order_id, sent_msg))


async def try_match_bank_credit(amount: float, utr: str):
    """
    Attempt to match an email-detected bank credit to the oldest pending
    bank_order with the same amount.
    Returns:
      {'status': 'matched', 'order_id': ..., 'user_id': ..., 'amount': ...}
      {'status': 'duplicate', 'used_order_id': ...}
      {'status': 'nomatch'}
    """
    pool = get_pool()
    amt_int = int(round(amount))
    async with bank_match_lock:
        async with pool.acquire() as conn:
            already_used = await conn.fetchrow(
                "SELECT order_id FROM bank_orders WHERE utr=$1 AND status='success'", utr
            )
            if already_used:
                return {"status": "duplicate", "used_order_id": already_used["order_id"]}
            order = await conn.fetchrow(
                "SELECT order_id, user_id FROM bank_orders "
                "WHERE status='pending' AND amount=$1 ORDER BY date ASC LIMIT 1",
                amt_int,
            )
            if not order:
                return {"status": "nomatch"}
            await conn.execute(
                "UPDATE bank_orders SET status='success', utr=$1 WHERE order_id=$2",
                utr, order["order_id"],
            )
        return {"status": "matched", "order_id": order["order_id"], "user_id": order["user_id"], "amount": amt_int}


async def cancel_bank_order(order_id: str):
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE bank_orders SET status='cancelled' WHERE order_id=$1",
            order_id,
        )


async def credit_bank_order(matched: dict, msg_to_edit=None):
    uid = matched["user_id"]
    amt = matched["amount"]
    pool = get_pool()
    async with get_user_lock(uid):
        async with pool.acquire() as conn:
            prev = await conn.fetchrow("SELECT balance FROM users WHERE user_id=$1", uid)
            prev_bal = prev["balance"] if prev else 0
            await conn.execute("""
                UPDATE users SET
                    balance = balance + $1,
                    total_deposited = total_deposited + $1
                WHERE user_id=$2
            """, amt, uid)
    await process_referral_bonus(uid, amt)
    await log_primary_deposit(uid, amt, "Bank Transfer Automatic")
    usd_amt = await to_usd(amt)
    usd_prev = await to_usd(prev_bal)
    usd_new = await to_usd(prev_bal + amt)
    user_msg = (
        f"{P_YES} <b>Bank Deposit Approved!</b>\n\n"
        f"{P_MONEY} <b>Amount Added:</b> ${usd_amt:.2f} (₹{amt})\n"
        f"📉 <b>Previous Balance:</b> ${usd_prev:.2f} (₹{prev_bal})\n"
        f"📈 <b>New Balance:</b> ${usd_new:.2f} (₹{prev_bal + amt})"
    )
    await bot.send_message(uid, user_msg)
    if msg_to_edit:
        try:
            await bot.edit_message(uid, msg_to_edit.id, f"{P_YES} <b>Payment Successful! ₹{amt} added to your wallet.</b>")
        except Exception:
            pass


async def auto_check_bank_task(uid: int, order_id: str, msg_to_edit):
    pool = get_pool()
    for _ in range(40):  # ~40 * 15s ≈ 10 minutes
        await asyncio.sleep(15)
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT status FROM bank_orders WHERE order_id=$1", order_id)
        if not row or row["status"] != 'pending':
            break
        try:
            credits = await fetch_recent_credits(hours=3)
            for c in credits:
                matched = await try_match_bank_credit(c["amount"], c["utr"])
                if not matched or matched.get("status") == "nomatch":
                    continue
                if matched["status"] == "duplicate":
                    await cancel_bank_order(order_id)
                    try:
                        await bot.send_message(
                            uid,
                            "❌ This UTR has already been used for another successful deposit. "
                            "Your pending payment has been cancelled. Please re-check your transfer details or contact support."
                        )
                    except Exception:
                        pass
                    return
                if matched.get("order_id") == order_id:
                    await credit_bank_order(matched, msg_to_edit)
                    return
                else:
                    # A different pending order matched this same scan — credit it too.
                    await credit_bank_order(matched)
        except Exception as exc:
            logger.error(f"auto_check_bank_task error: {exc}")


async def verify_bank_payment(event, order_id: str):
    uid = event.sender_id
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status FROM bank_orders WHERE order_id=$1", order_id)
    if not row:
        return await event.answer("❌ Order not found.", alert=True)
    if row["status"] == 'success':
        return await event.answer("✅ Already credited!", alert=True)

    await event.answer("🔍 Checking your inbox for the payment...", alert=False)
    try:
        credits = await fetch_recent_credits(hours=3)
        for c in credits:
            matched = await try_match_bank_credit(c["amount"], c["utr"])
            if not matched or matched.get("status") == "nomatch":
                continue
            if matched["status"] == "duplicate":
                await cancel_bank_order(order_id)
                return await event.answer(
                    "❌ This UTR has already been used for another successful deposit. "
                    "Your pending payment has been cancelled. Please re-check your transfer details or contact support.",
                    alert=True,
                )
            if matched["order_id"] == order_id:
                try:
                    await event.delete()
                except Exception:
                    pass
                await credit_bank_order(matched)
                return
            else:
                await credit_bank_order(matched)
        await event.answer("⏳ Not received yet. We'll keep auto-checking — try again shortly.", alert=True)
    except Exception as exc:
        logger.error(f"verify_bank_payment error: {exc}")
        await event.answer("❌ Error checking payment. Try again shortly.", alert=True)


# ─── Auto Payment (Admin QR + UTR + Screenshot + 60s Email Auto-Verify) ───────

async def init_autopay_keypad(event):
    uid = event.sender_id
    autopay_input[uid] = {'val': '0'}
    await event.edit(
        f"{P_KEY} <b>ENTER AMOUNT IN INR (Min ₹1)</b>\n\n{P_MONEY} <code>₹0</code>",
        buttons=get_autopay_keypad(),
    )


async def autopay_keypad_logic(event):
    uid = event.sender_id
    action = event.data.decode().replace("akp_", "")
    curr = autopay_input.get(uid, {}).get('val', "0")
    if action.isdigit():
        curr = action if curr == "0" else curr + action
        if len(curr) > 6:
            curr = curr[:6]
    elif action == "del":
        curr = curr[:-1] or "0"
    elif action == "done":
        amt = int(curr)
        if amt < 1:
            return await event.answer("⚠️ Minimum Deposit is ₹1", alert=True)
        autopay_input.pop(uid, None)
        return await show_autopay_qr(event, amt)
    autopay_input[uid] = {'val': curr}
    try:
        await event.edit(
            f"{P_KEY} <b>ENTER AMOUNT IN INR</b>\n\n{P_MONEY} <code>₹{curr}</code>",
            buttons=get_autopay_keypad(),
        )
    except MessageNotModifiedError:
        pass


async def show_autopay_qr(event, amount: int):
    uid = event.sender_id
    cfg = await get_autopay_config()
    autopay_state[uid] = {'step': 'wait_utr', 'amount': amount}
    cap = (
        cfg["caption"]
        + f"\n\n{P_MONEY} <b>Amount to Pay:</b> {P_INR}{amount}\n\n"
        f"👇 After paying, <b>reply here with the UTR / UPI Ref No.</b> from your payment confirmation."
    )
    btns = [[Button.inline("❌ Cancel", "cancel_action")]]
    try:
        await event.delete()
    except Exception:
        pass

    qr = cfg.get("qr_file_id", "")
    sent = False
    if qr.startswith("tg:"):
        # format: tg:{photo_id}:{msg_id}:{channel_id}
        try:
            parts = qr.split(":")
            msg_id   = int(parts[2])
            chat_id  = int(parts[3])
            src_msg  = await bot.get_messages(chat_id, ids=msg_id)
            if src_msg and src_msg.media:
                await bot.send_file(uid, src_msg.media, caption=cap, buttons=btns)
                sent = True
        except Exception as exc:
            logger.warning(f"show_autopay_qr tg fetch failed: {exc}")
    elif qr and os.path.exists(qr):
        # legacy local path fallback
        try:
            await bot.send_file(uid, qr, caption=cap, buttons=btns)
            sent = True
        except Exception:
            pass

    if not sent:
        await bot.send_message(uid, cap, buttons=btns)


async def autopay_global_checker():
    """
    Single global background task — runs every 30s forever.
    Scans Gmail ONCE per cycle and matches ALL pending Auto Payment deposits.
    Far more reliable than per-user tasks — no timeout on email matching.
    """
    pool = get_pool()

    def _norm(raw: str) -> str:
        return re.sub(r'[^A-Z0-9]', '', raw.upper())

    async def _utr_already_used(conn, utr_norm: str) -> bool:
        duplicate = await conn.fetchrow(
            "SELECT 1 FROM deposits "
            "WHERE status='approved' AND regexp_replace(upper(utr), '[^A-Z0-9]', '', 'g')=$1 LIMIT 1",
            utr_norm,
        )
        if duplicate:
            return True
        duplicate = await conn.fetchrow(
            "SELECT 1 FROM bank_orders "
            "WHERE status='success' AND regexp_replace(upper(utr), '[^A-Z0-9]', '', 'g')=$1 LIMIT 1",
            utr_norm,
        )
        return bool(duplicate)

    while True:
        try:
            await asyncio.sleep(30)

            async with pool.acquire() as conn:
                pending = await conn.fetch(
                    "SELECT id, user_id, amount, utr FROM deposits "
                    "WHERE method_name='Auto Payment' AND status='pending' AND utr IS NOT NULL"
                )
            if not pending:
                continue

            try:
                credits = await fetch_recent_credits(hours=1)
            except Exception as exc:
                logger.error(f"autopay_global_checker email fetch error: {exc}")
                continue

            if not credits:
                continue

            for dep in pending:
                dep_id   = dep["id"]
                uid      = dep["user_id"]
                amount   = dep["amount"]
                utr_norm = _norm(dep["utr"] or "")
                if not utr_norm:
                    continue

                async with pool.acquire() as conn:
                    if await _utr_already_used(conn, utr_norm):
                        await conn.execute(
                            "UPDATE deposits SET status='rejected' WHERE id=$1", dep_id
                        )
                        try:
                            await bot.send_message(
                                uid,
                                "❌ This UTR has already been used for a successful deposit. "
                                "Your auto-payment request has been cancelled. Please verify your payment reference and try again."
                            )
                        except Exception:
                            pass
                        continue

                for c in credits:
                    email_utr_norm = _norm(c["utr"])
                    utr_matched = (
                        email_utr_norm == utr_norm or
                        utr_norm in email_utr_norm or
                        email_utr_norm in utr_norm
                    )
                    amt_matched = abs(float(c["amount"]) - amount) <= 1.0

                    if not (utr_matched and amt_matched):
                        continue

                    prev_bal = 0
                    async with get_user_lock(uid):
                        async with pool.acquire() as conn:
                            chk = await conn.fetchrow(
                                "SELECT status FROM deposits WHERE id=$1", dep_id
                            )
                            if not chk or chk["status"] != 'pending':
                                break
                            prev_row = await conn.fetchrow(
                                "SELECT balance FROM users WHERE user_id=$1", uid
                            )
                            prev_bal = prev_row["balance"] if prev_row else 0
                            await conn.execute(
                                "UPDATE users SET balance=balance+$1, total_deposited=total_deposited+$1 WHERE user_id=$2",
                                amount, uid,
                            )
                            await conn.execute(
                                "UPDATE deposits SET status='approved' WHERE id=$1", dep_id
                            )

                    await process_referral_bonus(uid, amount)
                    await log_primary_deposit(uid, amount, "Auto Payment")

                    usd_amt  = await to_usd(amount)
                    usd_prev = await to_usd(prev_bal)
                    usd_new  = await to_usd(prev_bal + amount)

                    try:
                        await bot.send_message(
                            uid,
                            f"{P_YES} <b>Payment Auto-Verified!</b>\n\n"
                            f"{P_MONEY} <b>Amount Added:</b> ${usd_amt:.2f} (₹{amount})\n"
                            f"📉 <b>Previous Balance:</b> ${usd_prev:.2f} (₹{prev_bal})\n"
                            f"📈 <b>New Balance:</b> ${usd_new:.2f} (₹{prev_bal + amount})"
                        )
                    except Exception as exc:
                        logger.error(f"autopay_global_checker notify error uid={uid}: {exc}")

                    logger.info(f"autopay_global_checker: AUTO-VERIFIED dep_id={dep_id} uid={uid} amount={amount} utr={dep['utr']}")
                    break

        except Exception as exc:
            logger.error(f"autopay_global_checker loop error: {exc}")


async def autopay_verify_task(uid: int, dep_id: int, amount: int, utr: str, countdown_msg, log_msg):
    """
    Per-user countdown display only — actual matching is done by autopay_global_checker.
    Shows 3-min countdown, then tells user background checking continues.
    """
    pool = get_pool()
    deadline = time.time() + 180
    last_edit = 0

    while time.time() < deadline:
        await asyncio.sleep(5)

        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT status FROM deposits WHERE id=$1", dep_id)

        if not row or row["status"] != 'pending':
            return  # global checker already credited it

        remaining = max(0, int(deadline - time.time()))
        if time.time() - last_edit >= 10:
            last_edit = time.time()
            try:
                await countdown_msg.edit(
                    f"{P_WAIT} <b>Auto-verifying your payment...</b>\n\n"
                    f"⏳ <b>{remaining}s</b> remaining\n\n"
                    f"<i>Checking for your bank confirmation email...</i>"
                )
            except Exception:
                pass

    # 3 min elapsed — check if already verified
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status FROM deposits WHERE id=$1", dep_id)
    if not row or row["status"] != 'pending':
        return

    # Still pending — tell user background checker continues
    try:
        await countdown_msg.edit(
            f"{P_WARN} <b>Still verifying in background...</b>\n\n"
            f"Your payment of {P_INR}{amount} (UTR: <code>{utr}</code>) is being checked every 30 seconds.\n\n"
            f"✅ You'll be <b>automatically notified</b> once verified.\n"
            f"If not verified within 10 minutes, contact support or wait for admin approval."
        )
    except Exception:
        pass



# =============================================================================
#  BUYING FLOW
# =============================================================================
async def show_countries(event, flow: str, page: int = 1):
    limit = 10
    offset = (page - 1) * limit
    pool = get_pool()
    async with pool.acquire() as conn:
        total_row = await conn.fetchrow(
            "SELECT COUNT(*) FROM (SELECT DISTINCT country_name, account_year FROM stock WHERE available=1) t"
        )
        total = total_row[0] if total_row else 0
        rows = await conn.fetch(
            "SELECT MAX(country_icon), country_name, COUNT(*) FROM stock "
            "WHERE available=1 GROUP BY country_name ORDER BY country_name ASC LIMIT $1 OFFSET $2",
            limit, offset,
        )

    if not rows and page == 1:
        err_msg = f"{P_NO} <b>Stock is Empty right now. Check back later!</b>"
        if isinstance(event, events.CallbackQuery.Event):
            return await event.edit(err_msg)
        else:
            return await event.respond(err_msg)

    title = f"🛒 <b>SELECT PRODUCT (SESSIONS)</b>" if flow == 'bulk' else f"🛒 <b>SELECT PRODUCT (ACCOUNTS)</b>"
    rate = await get_usdt_rate()

    country_lines = ""
    btn_list = []
    pool = get_pool()
    async with pool.acquire() as conn:
        for row in rows:
            icon, name, cnt = row[0], row[1], row[2]
            yr_rows = await conn.fetch(
                "SELECT account_year, MIN(price), COUNT(*) FROM stock WHERE available=1 AND country_name LIKE $1 "
                "GROUP BY account_year ORDER BY account_year DESC",
                f"{name}%",
            )
            for yr in yr_rows:
                year_val, min_price, yr_cnt = yr[0], yr[1], yr[2]
                usd_price = round(float(min_price) / float(rate), 2) if rate else 0
                display_name = f"{name} {year_val}"
                country_lines += f"• {icon} {display_name}: <b>${usd_price} ({P_INR}{min_price})</b> - Stock: {yr_cnt}\n"
                usd = usd_price
                btn_label = f"{icon} {display_name} - ₹{min_price} (${usd})"
                btn_data = f"bc|{flow}|{name[:18]}|{year_val}"
                btn_list.append(Button.inline(btn_label, btn_data))

    msg = (
        f"{title}\n{'━' * 28}\n\n"
        f"<blockquote>⚡ <b>Rate: 1 USDT = {P_INR}{rate}</b></blockquote>\n\n"
        f"<blockquote>{country_lines}</blockquote>\n"
        f"<i>Select an item to view details:</i>"
    )

    layout = await get_stock_layout()
    if layout == "one_by_one":
        btns = [[btn] for btn in btn_list]
    else:
        btns = [btn_list[j:j+2] for j in range(0, len(btn_list), 2)]
    nav = []
    if page > 1:
        nav.append(Button.inline("Prev", f"pg_c|{flow}|{page - 1}"))
    if offset + limit < total:
        nav.append(Button.inline("Next", f"pg_c|{flow}|{page + 1}"))
    if nav:
        btns.append(nav)
    btns.append([Button.inline("❌ Cancel", "cancel_action")])

    if isinstance(event, events.CallbackQuery.Event):
        await event.edit(msg, buttons=btns)
    else:
        _buy_img = await get_banner_image("buy", BUY_IMAGE)
        await send_with_image(event.chat_id, _buy_img, msg, buttons=btns)


async def show_years(event, flow: str, country: str, year_filter: str = None):
    pool = get_pool()
    async with pool.acquire() as conn:
        if year_filter:
            rows = await conn.fetch(
                "SELECT account_year, price, COUNT(*) FROM stock "
                "WHERE available=1 AND country_name LIKE $1 AND account_year=$2 GROUP BY account_year, price ORDER BY account_year DESC",
                f"{country}%", int(year_filter),
            )
        else:
            rows = await conn.fetch(
                "SELECT account_year, price, COUNT(*) FROM stock "
                "WHERE available=1 AND country_name LIKE $1 GROUP BY account_year, price ORDER BY account_year DESC",
                f"{country}%",
            )
    if not rows:
        return await event.answer("❌ Out of stock for this country.", alert=True)

    uid = event.sender_id
    async with pool.acquire() as conn:
        disc_row = await conn.fetchrow("SELECT discount FROM users WHERE user_id=$1", uid)
    discount = disc_row["discount"] if disc_row else 0

    rate = await get_usdt_rate()
    flag = await get_flag_by_country_name(country)
    btns = []
    for row in rows:
        y, p, c = row[0], row[1], row[2]
        disp_p = p if discount == 0 else int(p * (100 - discount) / 100)
        disc_text = f" (-{discount}%)" if discount > 0 else ""
        usd2 = round(float(disp_p) / float(rate), 2) if rate else 0
        btns.append([Button.inline(f"{flag} {country} {y} - ₹{disp_p}{disc_text} (${usd2}) | {c} left", f"by|{flow}|{country}|{y}|{p}")])
    btns.append([Button.inline("🔙 Back to Menu", f"pg_c|{flow}|1")])

    if year_filter:
        # When year is pre-selected, show confirm screen directly if only 1 row
        msg = f"{P_CAL} <b>{country} {year_filter}</b>\n{P_GLOBE} Select price tier:\n\n"
    else:
        msg = f"{P_CAL} <b>Select Account Year</b>\n{P_GLOBE} Country: <b>{country}</b>\n\n"
    await event.edit(msg, buttons=btns)


async def confirm_purchase(event, country: str, year: str, price_str: str):
    uid = event.sender_id
    base_price = int(price_str)
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT balance, discount FROM users WHERE user_id=$1", uid
        )
        phone_row = await conn.fetchrow(
            "SELECT phone FROM stock WHERE country_name LIKE $1 AND account_year=$2 AND price=$3 AND available=1 LIMIT 1",
            f"{country}%", int(year), base_price,
        )
    bal = row["balance"] if row else 0
    discount = row["discount"] if row else 0
    final_price = base_price if discount == 0 else int(base_price * (100 - discount) / 100)

    if bal < final_price:
        needed = final_price - bal
        return await event.edit(
            f"{P_NO} <b>Insufficient Balance!</b>\n\n"
            f"{P_MONEY} <b>Price:</b> {P_INR}{final_price}\n"
            f"{P_CARD} <b>Your Balance:</b> {P_INR}{bal}\n"
            f"{P_WARN} <b>You need</b> {P_INR}{needed} <b>more.</b>\n\nPlease add funds and try again.",
            buttons=[[Button.inline("💰 Add Balance", "dep_menu")], [Button.inline("❌ Cancel", "cancel_action")]],
        )

    spam_phone = phone_row["phone"] if phone_row else None
    report_button = [Button.inline("🛡️ Check Spam", f"check_spam|{country}|{year}|{base_price}|{spam_phone}")] if spam_phone else None

    msg = (
        f"{P_CART} <b>Confirm Your Purchase</b>\n\n"
        f"{P_FLAG} <b>Country:</b> {country}\n"
        f"{P_CAL} <b>Year:</b> {year}\n"
        f"{P_MONEY} <b>Final Price:</b> {P_INR}{final_price}\n"
        f"{P_CARD} <b>Your Balance:</b> {P_INR}{bal}\n\n"
        f"<blockquote>⚠️ <b>Please Read Before Buying</b>\n"
        f"• Our service is to provide the OTP only.\n"
        f"• We are <b>not responsible</b> if the account gets banned or limited after purchase.\n"
        f"• No refunds once the OTP is delivered.\n"
        f"• By clicking <b>Yes, Buy Now</b> you agree to our <a href=\"{TERMS_URL}\">Terms & Conditions</a>.</blockquote>\n\n"
        f"❓ Do you want to proceed?"
    )
    btns = [
        [Button.inline("✅ Yes, Buy Now", f"buy_cf|{country}|{year}|{base_price}")],
    ]
    if report_button:
        btns.append(report_button)
    btns.append([Button.inline("❌ No, Cancel", "cancel_action")])
    await event.edit(msg, buttons=btns)


async def fetch_spam_report(event, country: str, year: str, price_str: str, phone: str = None):
    pool = get_pool()
    session_file = None
    async with pool.acquire() as conn:
        if phone:
            row = await conn.fetchrow(
                "SELECT session_file FROM stock WHERE phone=$1 AND available=1", phone
            )
        else:
            row = await conn.fetchrow(
                "SELECT session_file FROM stock WHERE country_name LIKE $1 AND account_year=$2 AND price=$3 AND available=1 LIMIT 1",
                f"{country}%", int(year), int(price_str),
            )
        if row:
            session_file = row["session_file"]

    if not session_file:
        await event.answer("⚠️ Unable to find account session for spam check.", alert=True)
        return await confirm_purchase(event, country, year, price_str)

    clean_sess = session_file[:-8] if session_file.endswith(".session") else session_file
    client = TelegramClient(clean_sess, API_ID, API_HASH)

    def _extract_text(msg):
        if not msg:
            return ""
        return (getattr(msg, "text", None) or getattr(msg, "message", None) or getattr(msg, "caption", None) or "").strip()

    try:
        await event.edit(f"{P_WAIT} <b>Checking spam report via @SpamBot...</b>")
        await client.connect()
        if not await client.is_user_authorized():
            raise Exception("Account session is not authorized")

        spam_entity = await client.get_entity("spambot")
        await client.send_message(spam_entity, "/start")
        await asyncio.sleep(2)

        report_text = ""
        async for message in client.iter_messages(spam_entity, limit=3):
            report_text = _extract_text(message)
            if report_text:
                break

        if not report_text:
            report_text = "No response from @SpamBot."

        lower_report = report_text.lower()
        if any(phrase in lower_report for phrase in [
            "not limited", "not restricted", "no restrictions",
            "your account is not limited", "no limit",
        ]):
            status_text = "Account is clean — no spam restrictions detected"
        elif any(phrase in lower_report for phrase in [
            "limited", "restricted", "spamban", "spam ban",
            "temporarily limited", "your account is limited",
        ]):
            status_text = f"Account has restrictions: {report_text[:400]}"
        else:
            status_text = f"Bot reply: {report_text[:400]}"

        await bot.send_message(
            event.chat_id,
            f"{P_FLAG} <b>@SpamBot Report</b>\n\n{status_text}",
        )
    except FloodWaitError as exc:
        await asyncio.sleep(exc.seconds + 1)
        return await fetch_spam_report(event, country, year, price_str, phone)
    except Exception as exc:
        logger.warning(f"fetch_spam_report failed: {exc}")
        await event.answer("⚠️ Could not fetch spam report right now.", alert=True)
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
        await confirm_purchase(event, country, year, price_str)


async def process_purchase(event, country: str, year_str: str, price_str: str):
    uid = event.sender_id
    base_price = int(price_str)
    pool = get_pool()

    async with get_user_lock(uid):
        async with pool.acquire() as conn:
            user_row = await conn.fetchrow(
                "SELECT balance, discount FROM users WHERE user_id=$1", uid
            )
            discount = user_row["discount"] if user_row else 0
            final_price = base_price if discount == 0 else int(base_price * (100 - discount) / 100)

            stock_row = await conn.fetchrow(
                "SELECT phone, session_file, country_icon, account_year, twofa "
                "FROM stock WHERE country_name LIKE $1 AND account_year=$2 AND price=$3 AND available=1 LIMIT 1",
                f"{country}%", int(year_str), base_price,
            )
            if not stock_row:
                return await event.answer("❌ Sold out! Another user just bought this account.", alert=True)

            phone = stock_row["phone"]
            sess = stock_row["session_file"]
            c_icon = stock_row["country_icon"]
            actual_year = stock_row["account_year"]
            twofa_pass = stock_row["twofa"]
            actual_bal = user_row["balance"] if user_row else 0

            if actual_bal < final_price:
                needed = final_price - actual_bal
                await event.answer(f"❌ Insufficient Balance! Need ₹{final_price}", alert=True)
                return await event.edit(
                    f"{P_NO} <b>Insufficient Balance!</b>\n\n"
                    f"{P_MONEY} <b>Required:</b> {P_INR}{final_price}\n"
                    f"{P_CARD} <b>Your Balance:</b> {P_INR}{actual_bal}\n"
                    f"{P_WARN} <b>You need</b> {P_INR}{needed} <b>more.</b>\n\nPlease add funds and try again.",
                    buttons=[[Button.inline("💰 Add Balance", "dep_menu")], [Button.inline("❌ Cancel", "cancel_action")]],
                )
            await conn.execute(
                "UPDATE users SET balance = balance - $1 WHERE user_id=$2", final_price, uid
            )
            await conn.execute(
                "UPDATE stock SET available=0 WHERE phone=$1", phone
            )

    await event.edit(f"🔄 <b>Fetching Number (+{phone})...</b>")
    clean_sess = sess[:-8] if sess.endswith(".session") else sess
    client = TelegramClient(clean_sess, API_ID, API_HASH)

    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise Exception("Session dead")
    except Exception:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM stock WHERE phone=$1", phone)
            await conn.execute(
                "UPDATE users SET balance = balance + $1 WHERE user_id=$2", final_price, uid
            )
        await client.disconnect()
        delete_session_files(sess)
        return await event.edit(f"{P_NO} <b>Account Invalid.</b> Money refunded. Try buying another.")

    msg = (
        f"{P_YES} <b>Order Active!</b>\n\n"
        f"{P_PHONE} <b>Phone:</b> <code>{phone}</code>\n"
        f"{P_FLAG} <b>Country:</b> {c_icon} {country}\n\n"
        f"<blockquote>🔻 INSTRUCTIONS:\n"
        f"1. Open Telegram & Add Account\n"
        f"2. Enter the number above.\n"
        f"3. {P_WAIT} Please wait! The bot is actively listening for your OTP.</blockquote>\n\n"
        f"<i>⚠️ Cancel only available after 2 minutes. If no OTP in 10 minutes, auto-refund happens.</i>"
    )
    order_start = time.time()
    sent_msg = await event.edit(msg, buttons=[[Button.inline("❌ Cancel Order", f"cancel_order|{phone}")]])

    active_orders[phone] = {
        'uid': uid, 'client': client, 'sess': sess, 'start_time': order_start,
        'paid': False, 'price': final_price, 'country': country, 'year': actual_year,
        'c_icon': c_icon, 'twofa': twofa_pass, 'msg_id': sent_msg.id,
    }
    asyncio.create_task(auto_otp_task(phone))


async def auto_otp_task(phone: str):
    if phone not in active_orders:
        return
    order = active_orders[phone]
    client = order['client']
    start_time = order['start_time']
    uid = order['uid']
    msg_id = order['msg_id']
    pool = get_pool()

    while time.time() - start_time < AUTO_CANCEL_SECONDS:
        if phone not in active_orders:
            return
        try:
            msgs = await client.get_messages(777000, limit=5)
            code = None
            for m in msgs:
                if m.date.timestamp() > start_time - 10:
                    if m.message and re.search(OTP_REGEX, m.message) and "Login detected" not in m.message:
                        code = re.search(OTP_REGEX, m.message).group()
                        break

            if code:
                if not order['paid']:
                    try:
                        async with get_user_lock(uid):
                            async with pool.acquire() as conn:
                                await conn.execute(
                                    "INSERT INTO orders (user_id, country, year, price, phone, otp) VALUES ($1,$2,$3,$4,$5,$6)",
                                    uid, order['country'], order['year'], order['price'], phone, code,
                                )
                                await conn.execute("DELETE FROM stock WHERE phone=$1", phone)
                        order['paid'] = True
                        await log_primary_purchase(uid, order['country'], order['price'], order['price'], order['year'], 1, phone=phone, otp=code, twofa=order.get('twofa'))
                    except Exception as exc:
                        logger.error(f"auto_otp_task DB/log error for {phone}: {exc}")
                        order['paid'] = True  # still mark paid so user isn't charged again / refunded incorrectly

                twofa_pass = order['twofa'] if order['twofa'] != "None" else "None"
                twofa_text = (
                    f"{P_2FA} <b>2FA:</b> <code>{twofa_pass}</code>"
                    if twofa_pass != "None"
                    else f"🔓 <b>2FA:</b> <code>Disabled (No Password)</code>"
                )
                msg_text = (
                    f"{P_YES} <b>Latest OTP Fetched!</b>\n\n"
                    f"{P_PHONE} <b>Phone:</b> <code>{phone}</code>\n"
                    f"{P_FLAG} <b>Country:</b> {order['c_icon']} {order['country']}\n"
                    f"{P_OTP} <b>OTP:</b> <code>{code}</code>\n"
                    f"{twofa_text}"
                )
                btns = [
                    [Button.inline("🔄 Get OTP Again",   f"get_otp_again|{phone}")],
                    [Button.inline("📱 Manage Devices",  f"manage_devices|{phone}")],
                    [Button.inline("🚪 Finish & Logout", f"logout_bot|{phone}")],
                ]
                try:
                    await bot.edit_message(uid, msg_id, msg_text, buttons=btns)
                except MessageNotModifiedError:
                    pass
                except Exception:
                    await bot.send_message(uid, msg_text, buttons=btns)
                # ✅ FIX: Do NOT pop active_orders here.
                # Keep the order alive so "Get OTP Again" and "Manage Devices"
                # buttons keep working. Order is only removed on Finish & Logout.
                order['paid'] = True  # mark as paid so balance isn't refunded on timeout
                return
        except Exception as exc:
            logger.error(f"auto_otp_task loop error for {phone}: {exc}")
        await asyncio.sleep(2)  # reduced from 6 for faster OTP detection

    # Auto-cancel after timeout
    if phone in active_orders and not active_orders[phone]['paid']:
        order = active_orders.pop(phone)
        try:
            await order['client'].disconnect()
        except Exception:
            pass
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance + $1 WHERE user_id=$2", order['price'], uid
            )
            await conn.execute(
                "UPDATE stock SET available=1 WHERE phone=$1", phone
            )
        try:
            await bot.edit_message(
                uid, msg_id,
                f"{P_TIME} <b>Order Expired!</b>\n"
                f"The 10-minute limit for <code>{phone}</code> ran out. "
                f"Your money ({P_INR}{order['price']}) has been automatically refunded.",
            )
        except Exception:
            pass


async def init_session_purchase(event, country: str, year: str, price_str: str):
    uid = event.sender_id
    price = int(price_str)
    pool = get_pool()
    async with pool.acquire() as conn:
        stock_row = await conn.fetchrow(
            "SELECT COUNT(*) FROM stock WHERE country_name LIKE $1 AND account_year=$2 AND price=$3 AND available=1",
            f"{country}%", int(year), price,
        )
    stock = stock_row[0] if stock_row else 0
    if stock == 0:
        return await event.answer("❌ Out of stock!", alert=True)

    session_buy_state[uid] = {'country': country, 'year': year, 'price': price, 'stock': stock}
    async with pool.acquire() as conn:
        disc_row = await conn.fetchrow("SELECT discount FROM users WHERE user_id=$1", uid)
    discount = disc_row["discount"] if disc_row else 0
    p_disp = price if discount == 0 else int(price * (100 - discount) / 100)

    msg = (
        f"{P_STORE} <b>Buy {country} ({year}) Sessions</b>\n\n"
        f"{P_MONEY} <b>Price per session:</b> {P_INR}{p_disp}\n"
        f"{P_PKG} <b>Available Stock:</b> {stock}\n\n"
        f"👇 <b>Reply to this message</b> with the <b>Number of Sessions</b> you want to buy."
    )
    await event.edit(msg, buttons=[[Button.inline("Cancel", "cancel_action")]])


async def process_bulk_sessions(event, uid: int, qty: int, state: dict, final_cost: int):
    country, year, price = state['country'], int(state['year']), int(state['price'])
    await event.respond(f"{P_WAIT} <b>Processing your sessions...</b>")
    pool = get_pool()

    async with get_user_lock(uid):
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT phone, session_file, twofa, account_year FROM stock "
                "WHERE country_name LIKE $1 AND account_year=$2 AND price=$3 AND available=1 LIMIT $4",
                f"{country}%", year, price, qty,
            )
            if len(rows) < qty:
                return await event.respond(f"{P_NO} Stock changed during processing. Purchase Cancelled.")
            bal_row = await conn.fetchrow("SELECT balance FROM users WHERE user_id=$1", uid)
            user_bal = bal_row["balance"] if bal_row else 0
            if user_bal < final_cost:
                return await event.respond(f"{P_NO} Insufficient Balance! Purchase Cancelled.")
            await conn.execute(
                "UPDATE users SET balance = balance - $1 WHERE user_id=$2", final_cost, uid
            )
            phones = [r["phone"] for r in rows]
            await conn.execute(
                f"UPDATE stock SET available=0 WHERE phone = ANY($1::text[])", phones
            )
            price_per_acc = final_cost // qty
            await conn.executemany(
                "INSERT INTO orders (user_id, country, price, phone, otp) VALUES ($1,$2,$3,$4,$5)",
                [(uid, country, price_per_acc, p, "SESSION_FILES") for p in phones],
            )

    zip_name = f"sessions_{uid}_{int(time.time())}.zip"
    numbers_txt = ""
    try:
        with zipfile.ZipFile(zip_name, 'w') as zf:
            for row in rows:
                phone, sess_file, twofa_pass, y = row["phone"], row["session_file"], row["twofa"], row["account_year"]
                base_s = sess_file[:-8] if sess_file.endswith(".session") else sess_file
                for ext in ('.session', '.session-wal', '.session-shm', '.session-journal'):
                    src = base_s + ext
                    if os.path.exists(src):
                        zf.write(src, os.path.basename(src))
                pass_text = twofa_pass if twofa_pass != "None" else "No_Password"
                numbers_txt += f"+{phone} | pass:{pass_text}\n"
            numbers_txt += "\n\nPurchased from @Freshtgsales\n"
            zf.writestr("numbers.txt", numbers_txt)
        caption = (
            f"{P_YES} <b>Bulk Purchase Successful!</b>\n\n"
            f"{P_FLAG} Country: {country}\n"
            f"{P_PKG} Quantity: {qty}\n"
            f"{P_CARD} Total Paid: {P_INR}{final_cost}\n\n"
            f"<i>(Sessions are safely provided.)</i>"
        )
        await bot.send_file(uid, zip_name, caption=caption)
        await log_primary_purchase(uid, country, price, final_cost, year, qty)
    except Exception as exc:
        await event.respond(f"{P_WARN} Error creating zip: {exc}")
    finally:
        if os.path.exists(zip_name):
            os.remove(zip_name)


# =============================================================================
#  STATS & PROFILE
# =============================================================================
async def profile_handler(event):
    uid = event.sender_id
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT balance, total_deposited, joined_date, discount FROM users WHERE user_id=$1", uid
        )
    if not row:
        return await bot.send_message(event.chat_id, "⚠️ Error: Please type /start to initialize your account.")
    bal, dep, date, discount = row["balance"], row["total_deposited"], row["joined_date"], row["discount"]
    me = await get_bot_me()
    ref_link = f"https://t.me/{me.username}?start=ref_{uid}"
    disc_msg = f"\n{P_GIFT} Active Discount: <b>{discount}% OFF</b>" if discount > 0 else ""
    usd_bal = await to_usd(bal)
    usd_dep = await to_usd(dep)
    ref_section = ""
    if await is_referral_enabled():
        ref_section = (
            f"{P_USERS} <b>Your Referral Link:</b>\n<code>{ref_link}</code>\n\n"
            f"<i>(Share this link with your friends to earn bonuses!)</i>"
        )
    msg = (
        f"{P_ACC} <b>USER PROFILE</b>\n\n"
        f"{P_ID} User ID: <code>{uid}</code>\n"
        f"{P_MONEY} Balance: <code>${usd_bal:.2f} (₹{bal})</code>\n"
        f"{P_CARD} Deposited: <code>${usd_dep:.2f} (₹{dep})</code>{disc_msg}\n"
        f"{P_CAL} Joined: {str(date)[:10]}\n\n"
        f"{ref_section}"
    )
    _profile_img = await get_banner_image("profile", PROFILE_IMAGE)
    await send_with_image(event.chat_id, _profile_img, msg)


async def stats_handler(event, is_callback: bool = False):
    uid = event.sender_id
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT total_deposited FROM users WHERE user_id=$1", uid)
        if not row:
            return
        o_row = await conn.fetchrow("SELECT COUNT(*), SUM(price) FROM orders WHERE user_id=$1", uid)
    dep = row["total_deposited"]
    total_orders = o_row[0] if o_row else 0
    spent = o_row[1] if o_row and o_row[1] else 0
    usd_spent = await to_usd(spent)
    usd_dep = await to_usd(dep)
    msg = (
        f"{P_STATS} <b>My Statistics</b>\n\n"
        f"{P_CART} <b>Accounts Bought:</b> {total_orders}\n"
        f"{P_MONEY} <b>Total Spent:</b>\n${usd_spent:.2f}\n"
        f"{P_CARD} <b>Total Deposited:</b>\n${usd_dep:.2f}"
    )
    btns = [
        [Button.inline("View Purchase Logs", "page_purchases_1")],
    ]
    if await is_referral_enabled():
        btns.append([Button.inline("Referral Logs", "view_referrals")])
    if is_callback:
        try:
            await event.edit(msg, buttons=btns)
        except MessageNotModifiedError:
            pass
    else:
        _stats_img = await get_banner_image("stats", STATS_IMAGE)
        await send_with_image(event.chat_id, _stats_img, msg, buttons=btns)


async def send_purchase_page(event, uid: int, page: int):
    limit = 5
    offset = (page - 1) * limit
    pool = get_pool()
    async with pool.acquire() as conn:
        t_row = await conn.fetchrow("SELECT COUNT(*) FROM orders WHERE user_id=$1", uid)
        total = t_row[0] if t_row else 0
        rows = await conn.fetch(
            "SELECT phone, date FROM orders WHERE user_id=$1 ORDER BY id DESC LIMIT $2 OFFSET $3",
            uid, limit, offset,
        )
    msg = f"{P_DOC} <b>Purchase History (Page {page})</b>\n\n"
    if not rows:
        msg += "No purchases found."
    else:
        for row in rows:
            ph, d = row["phone"], row["date"]
            try:
                d_str = d.strftime("%a %b %d %H:%M:%S %Y") if hasattr(d, 'strftime') else str(d)
            except Exception:
                d_str = str(d)
            msg += f"{P_PHONE} {ph}\n{P_CAL} {d_str}\n────────────────\n"
    nav = []
    if page > 1:
        nav.append(Button.inline("Prev", f"page_purchases_{page - 1}"))
    nav.append(Button.inline("Back", "back_to_stats"))
    if offset + limit < total:
        nav.append(Button.inline("Next", f"page_purchases_{page + 1}"))
    await event.edit(msg, buttons=[nav])


async def view_referrals(event):
    pool = get_pool()
    async with pool.acquire() as conn:
        refs = await conn.fetch("SELECT user_id FROM users WHERE referred_by=$1", event.sender_id)
    await event.answer(f"👥 You have referred {len(refs)} user(s).", alert=True)


# =============================================================================
#  ADMIN PANEL
# =============================================================================
async def admin_panel_handler(event):
    uid = event.sender_id
    if not await is_admin(uid):
        return
    status_text = "🟢 Bot is ON" if await is_bot_online() else "🔴 Bot is OFF"
    btns = []
    if uid == ADMIN_ID or await has_perm(uid, 'p_settings'):
        btns.append([Button.inline(f"Status: {status_text}", "adm_togglebot")])
    r1 = []
    if uid == ADMIN_ID or await has_perm(uid, 'p_add_stock'):
        r1.extend([Button.inline("Add Single Acc", "adm_addstock"), Button.inline("Add via Session File", "adm_addsessfile")])
    if r1:
        btns.append(r1)
    r1b = []
    if uid == ADMIN_ID or await has_perm(uid, 'p_add_stock'):
        r1b.append(Button.inline("Add ZIP", "adm_addzip"))
    if r1b:
        btns.append(r1b)
    r2 = []
    if uid == ADMIN_ID or await has_perm(uid, 'p_manage_stock'):
        r2.extend([Button.inline("Manage Stock", "adm_managestock"), Button.inline("Auto Price", "adm_autoprice")])
    if r2:
        btns.append(r2)
    r3 = []
    if uid == ADMIN_ID or await has_perm(uid, 'p_stats'):
        r3.extend([Button.inline("Statistics", "adm_stats"), Button.inline("Broadcast", "adm_bcast")])
        r3.append(Button.inline("User Info", "adm_userinfo"))
    if r3:
        btns.append(r3)
    r4 = []
    if uid == ADMIN_ID or await has_perm(uid, 'p_bal'):
        r4.extend([Button.inline("Change Balance", "adm_bal"), Button.inline("Ban User", "adm_ban")])
    if r4:
        btns.append(r4)
    r5 = []
    if uid == ADMIN_ID or await has_perm(uid, 'p_settings'):
        r5.extend([Button.inline("Discount", "adm_discount"), Button.inline("Ref %", "adm_refpct")])
        btns.append(r5)
        btns.append([Button.inline("Support URL", "adm_supporturl"), Button.inline("Payments", "adm_payments")])
        btns.append([Button.inline("🤖 Auto Payment", "adm_autopay")])
        btns.append([Button.inline("✏️ Set Welcome Msg", "adm_setwelcome"), Button.inline("🖼 Banner Images", "adm_banners")])
        btns.append([Button.inline("Set USDT Rate", "adm_usdtrate")])
        # ── New toggles ──────────────────────────────────────────────────────
        ref_status   = "🟢 ON" if await is_referral_enabled() else "🔴 OFF"
        img_status   = "🟢 ON" if await is_images_enabled()   else "🔴 OFF"
        layout_val   = await get_stock_layout()
        layout_label = "Side‑by‑Side" if layout_val == "side_by_side" else "One‑by‑One"
        btns.append([
            Button.inline(f"Referral: {ref_status}", "adm_toggle_referral"),
            Button.inline(f"Images: {img_status}",   "adm_toggle_images"),
        ])
        btns.append([Button.inline(f"Stock Layout: {layout_label}", "adm_toggle_layout")])
        # ─────────────────────────────────────────────────────────────────────
        btns.append([Button.inline("Backup Users", "adm_backupusr"), Button.inline("Restore Users", "adm_restoreusr")])
    if uid == ADMIN_ID or await has_perm(uid, 'p_stats'):
        btns.append([Button.inline("🔍 Search User", "adm_searchuser")])
    if uid == ADMIN_ID or await has_perm(uid, 'p_manage_stock'):
        btns.append([Button.inline("📦 View Stock", "adm_viewstock|1")])
        btns.append([Button.inline("🧊 Frozen & Dead Session Check", "adm_stockhealth")])
    if uid == ADMIN_ID or await has_perm(uid, 'p_settings'):
        btns.append([Button.inline("🔧 Maintenance Mode", "adm_maintenance")])
    if uid == ADMIN_ID or await has_perm(uid, 'p_manage_stock'):
        btns.append([Button.inline("📥 Download Sessions ZIP", "adm_dl_sessions_zip"), Button.inline("🗄 Download DB Backup", "adm_dl_db_backup")])
    if uid == ADMIN_ID:
        btns.append([Button.inline("Manage Admins", "adm_manageadmins")])
    if uid == ADMIN_ID or await has_perm(uid, 'p_demo'):
        btns.append([Button.inline("🎬 View Demo", "adm_demo")])
    await bot.send_message(event.chat_id, f"{P_PC} <b>ADVANCED ADMIN DASHBOARD</b>", buttons=btns)


async def manage_admins_menu(event):
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM admins")
    msg = f"{P_USERS} <b>Manage Sub-Admins</b>\n\n"
    for row in rows:
        msg += f"{P_ACC} <code>{row['user_id']}</code>\n"
    btns = [
        [Button.inline("Add Admin", "adm_addadmin"), Button.inline("Edit Admin", "adm_editadminreq")],
        [Button.inline("Back", "adm_adminmain")],
    ]
    await event.edit(msg, buttons=btns)


async def edit_admin_menu(event, target_id):
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT p_add_stock, p_manage_stock, p_stats, p_bal, p_settings, p_demo FROM admins WHERE user_id=$1",
            target_id,
        )
    if not row:
        return await event.answer("Admin not found", alert=True)
    p = ["✅" if row[i] == 1 else "❌" for i in range(6)]
    btns = [
        [Button.inline(f"Add Stock: {p[0]}", f"adm_tglperm|{target_id}|p_add_stock")],
        [Button.inline(f"Manage Stock: {p[1]}", f"adm_tglperm|{target_id}|p_manage_stock")],
        [Button.inline(f"Stats & Bcast: {p[2]}", f"adm_tglperm|{target_id}|p_stats")],
        [Button.inline(f"Bal & Users: {p[3]}", f"adm_tglperm|{target_id}|p_bal")],
        [Button.inline(f"Settings: {p[4]}", f"adm_tglperm|{target_id}|p_settings")],
        [Button.inline(f"Demo Mode: {p[5]}", f"adm_tglperm|{target_id}|p_demo")],
        [Button.inline("Remove Admin", f"adm_deladmin|{target_id}")],
        [Button.inline("Back", "adm_manageadmins")],
    ]
    await event.edit(f"✏️ <b>Editing Admin:</b> <code>{target_id}</code>", buttons=btns)


async def send_manage_stock_page(event, page: int):
    limit = 10
    offset = (page - 1) * limit
    pool = get_pool()
    async with pool.acquire() as conn:
        all_rows = await conn.fetch("SELECT DISTINCT country_name FROM stock ORDER BY country_name")
    total = len(all_rows)
    countries = all_rows[offset:offset + limit]
    btns = []
    for row in countries:
        flag = await get_flag_by_country_name(row["country_name"])
        btns.append([Button.inline(f"{flag} {row['country_name']}", f"adm_msc|{row['country_name']}")])
    nav = []
    if page > 1:
        nav.append(Button.inline("Prev", f"adm_mspg|{page - 1}"))
    if offset + limit < total:
        nav.append(Button.inline("Next", f"adm_mspg|{page + 1}"))
    if nav:
        btns.append(nav)
    btns.append([Button.inline("Back", "adm_adminmain")])
    await event.edit(f"{P_DOC} <b>Manage Stock</b> (Page {page})\nSelect a country to edit:", buttons=btns)


async def send_manage_stock_country(event, c_name: str):
    pool = get_pool()
    async with pool.acquire() as conn:
        years = await conn.fetch(
            "SELECT DISTINCT account_year FROM stock WHERE country_name=$1 ORDER BY account_year DESC", c_name
        )
    flag = await get_flag_by_country_name(c_name)
    btns = [
        [Button.inline("Edit Country Name", f"adm_msedit|name|{c_name}"), Button.inline("Edit Flag", f"adm_msedit|flag|{c_name}")],
        [Button.inline("Edit Common Price (All Years)", f"adm_msedit|cprice|{c_name}")],
    ]
    y_btns = [Button.inline(f"{row['account_year']}", f"adm_msedit|yprice|{c_name}|{row['account_year']}") for row in years]
    for i in range(0, len(y_btns), 3):
        btns.append(y_btns[i:i + 3])
    btns.append([Button.inline("Back", "adm_mspg|1")])
    await event.edit(f"{flag} <b>Managing: {c_name}</b>\nSelect an option:", buttons=btns)


async def send_autoprice_page(event, page: int):
    limit = 10
    offset = (page - 1) * limit
    pool = get_pool()
    async with pool.acquire() as conn:
        stock_countries = await conn.fetch("SELECT DISTINCT country_name FROM stock")
        custom_countries = await conn.fetch("SELECT DISTINCT name FROM custom_countries")
    c_list = set(c for c, _ in COUNTRY_CODES.values())
    for row in stock_countries:
        c_list.add(row["country_name"])
    for row in custom_countries:
        c_list.add(row["name"])
    c_list = sorted(c_list)
    total = len(c_list)
    countries = c_list[offset:offset + limit]
    btns = []
    for c in countries:
        flag = await get_flag_by_country_name(c)
        btns.append([Button.inline(f"{flag} {c}", f"adm_apc|{c}")])
    nav = []
    if page > 1:
        nav.append(Button.inline("Prev", f"adm_appg|{page - 1}"))
    if offset + limit < total:
        nav.append(Button.inline("Next", f"adm_appg|{page + 1}"))
    if nav:
        btns.append(nav)
    btns.append([Button.inline("Add Custom Country", "adm_ap_add_country")])
    btns.append([Button.inline("Back", "adm_adminmain")])
    await event.edit(f"{P_ASST} <b>Auto Price Setup</b> (Page {page})\nSelect a country:", buttons=btns)


async def send_autoprice_country(event, c_name: str):
    flag = await get_flag_by_country_name(c_name)
    btns = [[Button.inline("Set Common Price", f"adm_apset|{c_name}|Common")]]
    y_btns = [Button.inline(f"{y}", f"adm_apset|{c_name}|{y}") for y in range(2024, 1999, -1)]
    for i in range(0, len(y_btns), 4):
        btns.append(y_btns[i:i + 4])
    btns.append([Button.inline("Back", "adm_appg|1")])
    await event.edit(f"{flag} <b>Auto Price: {c_name}</b>\nSelect year or Common:", buttons=btns)


# =============================================================================
#  STOCK HEALTH
# =============================================================================
async def _check_session_health(phone: str, sess_file: str):
    clean_sess = sess_file[:-8] if sess_file.endswith(".session") else sess_file
    client = TelegramClient(clean_sess, API_ID, API_HASH)
    status = 'healthy'
    try:
        await client.connect()
        try:
            authorized = await client.is_user_authorized()
        except (AuthKeyUnregisteredError, AuthKeyDuplicatedError, UserDeactivatedBanError):
            status = 'banned'
            await _safe_disconnect(client)
            return status
        if not authorized:
            status = 'dead'
            await _safe_disconnect(client)
            return status
        try:
            await asyncio.wait_for(client.get_dialogs(limit=1), timeout=10)
            status = 'healthy'
        except (AuthKeyUnregisteredError, AuthKeyDuplicatedError, UserDeactivatedBanError):
            status = 'banned'
        except asyncio.TimeoutError:
            status = 'frozen'
        except Exception:
            status = 'frozen'
        if status in ('frozen', 'dead'):
            try:
                await asyncio.wait_for(client.log_out(), timeout=8)
            except Exception:
                pass
    except (AuthKeyUnregisteredError, AuthKeyDuplicatedError, UserDeactivatedBanError):
        status = 'banned'
    except Exception:
        status = 'dead'
    finally:
        await _safe_disconnect(client)
    return status


async def _safe_disconnect(client):
    try:
        await client.disconnect()
    except Exception:
        pass


# =============================================================================
#  ADMIN ACTIONS HANDLER
# =============================================================================
async def admin_actions(event):
    data_full = event.data.decode()
    if not data_full.startswith("adm_"):
        return
    uid = event.sender_id
    action_data = data_full[4:]
    chat = event.chat_id
    pool = get_pool()

    if action_data == "adminmain":
        await event.delete()
        class FakeEvent:
            chat_id = chat
            sender_id = uid
        return await admin_panel_handler(FakeEvent())

    if action_data == "searchuser" and await has_perm(uid, 'p_stats'):
        async with bot.conversation(chat, timeout=60) as conv:
            await conv.send_message(f"{P_ACC} <b>Enter User ID to search:</b>")
            try:
                reply = await conv.get_response()
                t_uid_str = reply.text.strip()
                if not t_uid_str.lstrip('-').isdigit():
                    return await bot.send_message(chat, f"{P_NO} Invalid ID.")
                t_uid = int(t_uid_str)
            except asyncio.TimeoutError:
                return await bot.send_message(chat, f"{P_NO} Timed out.")
        async with pool.acquire() as conn:
            u_row = await conn.fetchrow(
                "SELECT balance, total_deposited, joined_date, banned, discount, referred_by FROM users WHERE user_id=$1", t_uid
            )
            if not u_row:
                return await bot.send_message(chat, f"{P_NO} User <code>{t_uid}</code> not found.")
            o_row = await conn.fetchrow("SELECT COUNT(*), SUM(price) FROM orders WHERE user_id=$1", t_uid)
            dep_row = await conn.fetchrow("SELECT COUNT(*), SUM(amount) FROM upi_orders WHERE user_id=$1 AND status='success'", t_uid)
            ref_count = (await conn.fetchrow("SELECT COUNT(*) FROM users WHERE referred_by=$1", t_uid))[0]
        bal, dep, joined, is_banned, disc, ref_by = u_row["balance"], u_row["total_deposited"], u_row["joined_date"], u_row["banned"], u_row["discount"], u_row["referred_by"]
        o_count = o_row[0] or 0
        o_spent = o_row[1] or 0
        d_count = dep_row[0] or 0
        d_total = dep_row[1] or 0
        ban_status = f"{P_OFF} Banned" if is_banned else f"{P_ON} Active"
        msg = (
            f"🔍 <b>USER SEARCH RESULT</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"{P_ID} <b>User ID:</b> <code>{t_uid}</code>\n"
            f"🔰 <b>Status:</b> {ban_status}\n"
            f"{P_CAL} <b>Joined:</b> {joined}\n\n"
            f"<b>💰 Wallet</b>\n"
            f"  ├ Balance: {P_INR}{bal}\n"
            f"  ├ Total Deposited: {P_INR}{dep}\n"
            f"  └ UPI Deposits: {d_count} txns ({P_INR}{d_total})\n\n"
            f"<b>{P_CART} Purchases</b>\n"
            f"  ├ Total Orders: {o_count}\n"
            f"  └ Total Spent: {P_INR}{o_spent}\n\n"
            f"<b>{P_GIFT} Referrals</b>\n"
            f"  ├ Referred By: {ref_by or 'None'}\n"
            f"  ├ Users Referred: {ref_count}\n"
            f"  └ Discount: {disc}%"
        )
        btns = [
            [Button.inline(f"{'🔓 Unban' if is_banned else '🔨 Ban'} User", f"adm_toggleban|{t_uid}")],
            [Button.inline("💸 Change Balance", f"adm_setbal|{t_uid}")],
            [Button.inline("🔙 Back to Admin", "adm_adminmain")],
        ]
        return await bot.send_message(chat, msg, buttons=btns)

    if action_data.startswith("toggleban|") and await has_perm(uid, 'p_bal'):
        t_uid = int(action_data.split("|")[1])
        async with pool.acquire() as conn:
            cur_status = await conn.fetchrow("SELECT banned FROM users WHERE user_id=$1", t_uid)
            if not cur_status:
                return await event.answer("User not found", alert=True)
            new_ban = 0 if cur_status["banned"] else 1
            await conn.execute("UPDATE users SET banned=$1 WHERE user_id=$2", new_ban, t_uid)
        label = "Banned" if new_ban else "Unbanned"
        await event.answer(f"✅ User {label}!", alert=True)
        return await bot.send_message(chat, f"{'🔨' if new_ban else '🔓'} User <code>{t_uid}</code> has been <b>{label}</b>.")

    if action_data.startswith("setbal|") and await has_perm(uid, 'p_bal'):
        t_uid = int(action_data.split("|")[1])
        async with bot.conversation(chat, timeout=60) as conv:
            await conv.send_message(f"{P_MONEY} <b>Enter new balance for <code>{t_uid}</code>:</b>")
            try:
                reply = await conv.get_response()
                new_bal = int(reply.text.strip())
            except (asyncio.TimeoutError, ValueError):
                return await bot.send_message(chat, f"{P_NO} Invalid or timed out.")
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET balance=$1 WHERE user_id=$2", new_bal, t_uid)
        await bot.send_message(chat, f"{P_YES} Balance for <code>{t_uid}</code> set to {P_INR}{new_bal}.")
        try:
            await bot.send_message(t_uid, f"{P_MONEY} <b>Your balance has been updated to {P_INR}{new_bal} by admin.</b>")
        except Exception:
            pass
        return

    if action_data == "maintenance" and await has_perm(uid, 'p_settings'):
        async with pool.acquire() as conn:
            current = await conn.fetchrow("SELECT value FROM settings WHERE key='bot_status'")
            maint_msg_row = await conn.fetchrow("SELECT value FROM settings WHERE key='maint_msg'")
        is_on = current and current["value"] == 'on'
        status_label = "🟢 Bot is LIVE" if is_on else "🔴 Bot is in MAINTENANCE"
        btns = [
            [Button.inline("🔴 Enable Maintenance" if is_on else "🟢 Go Live", "adm_togglemaint")],
            [Button.inline("✏️ Set Maintenance Message", "adm_maintmsg")],
            [Button.inline("🔙 Back", "adm_adminmain")],
        ]
        current_msg = maint_msg_row["value"] if maint_msg_row else "Bot is under maintenance. Please check back later."
        return await bot.send_message(
            chat,
            f"🔧 <b>MAINTENANCE MODE</b>\n\nCurrent status: <b>{status_label}</b>\n\n📝 <b>Current message:</b>\n<i>{current_msg}</i>",
            buttons=btns,
        )

    if action_data == "togglemaint" and await has_perm(uid, 'p_settings'):
        async with pool.acquire() as conn:
            current = await conn.fetchrow("SELECT value FROM settings WHERE key='bot_status'")
            new_status = 'off' if (current and current["value"] == 'on') else 'on'
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ('bot_status', $1) ON CONFLICT (key) DO UPDATE SET value=$1",
                new_status,
            )
        label = "🟢 Bot is now LIVE" if new_status == 'on' else "🔴 Maintenance mode ENABLED"
        await event.answer(label, alert=True)
        return await bot.send_message(chat, label)

    if action_data == "maintmsg" and await has_perm(uid, 'p_settings'):
        async with bot.conversation(chat, timeout=60) as conv:
            await conv.send_message(f"{P_DOC} <b>Enter new maintenance message:</b>")
            try:
                reply = await conv.get_response()
                new_msg = reply.text.strip()
            except asyncio.TimeoutError:
                return await bot.send_message(chat, f"{P_NO} Timed out.")
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ('maint_msg', $1) ON CONFLICT (key) DO UPDATE SET value=$1",
                new_msg,
            )
        return await bot.send_message(chat, f"{P_YES} Maintenance message updated!")

    if action_data == "togglebot" and await has_perm(uid, 'p_settings'):
        new_status = 'off' if await is_bot_online() else 'on'
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ('bot_status', $1) ON CONFLICT (key) DO UPDATE SET value=$1",
                new_status,
            )
        await event.answer(f"Bot turned {new_status.upper()}", alert=True)
        class FakeEvent:
            chat_id = chat
            sender_id = uid
        await admin_panel_handler(FakeEvent())
        await event.delete()
        return

    if action_data == "toggle_referral" and await has_perm(uid, 'p_settings'):
        new_val = 'off' if await is_referral_enabled() else 'on'
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ('referral_enabled', $1) ON CONFLICT (key) DO UPDATE SET value=$1",
                new_val,
            )
        await event.answer(f"Referral system turned {new_val.upper()}", alert=True)
        class FakeEvent:
            chat_id = chat
            sender_id = uid
        await admin_panel_handler(FakeEvent())
        await event.delete()
        return

    if action_data == "toggle_images" and await has_perm(uid, 'p_settings'):
        new_val = 'off' if await is_images_enabled() else 'on'
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ('images_enabled', $1) ON CONFLICT (key) DO UPDATE SET value=$1",
                new_val,
            )
        await event.answer(f"Banner images turned {new_val.upper()}", alert=True)
        class FakeEvent:
            chat_id = chat
            sender_id = uid
        await admin_panel_handler(FakeEvent())
        await event.delete()
        return

    if action_data == "toggle_layout" and await has_perm(uid, 'p_settings'):
        current = await get_stock_layout()
        new_val = 'one_by_one' if current == 'side_by_side' else 'side_by_side'
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ('stock_layout', $1) ON CONFLICT (key) DO UPDATE SET value=$1",
                new_val,
            )
        label = "Side-by-Side" if new_val == "side_by_side" else "One-by-One"
        await event.answer(f"Stock layout changed to {label}", alert=True)
        class FakeEvent:
            chat_id = chat
            sender_id = uid
        await admin_panel_handler(FakeEvent())
        await event.delete()
        return

    elif action_data == "dl_sessions_zip" and (uid == ADMIN_ID or await has_perm(uid, 'p_manage_stock')):
        await event.answer("⏳ Preparing sessions ZIP...", alert=False)
        await send_sessions_backup(target_uid=uid)
        return

    elif action_data == "dl_db_backup" and (uid == ADMIN_ID or await has_perm(uid, 'p_manage_stock')):
        await event.answer("⏳ Preparing DB backup...", alert=False)
        await send_database_backup(target_uid=uid)
        return

    elif action_data.startswith("viewstock|") and (uid == ADMIN_ID or await has_perm(uid, 'p_manage_stock')):
        page = int(action_data.split("|")[1])
        limit = 8
        offset = (page - 1) * limit
        async with pool.acquire() as conn:
            total = (await conn.fetchrow("SELECT COUNT(*) FROM stock WHERE available=1"))[0]
            rows = await conn.fetch(
                "SELECT phone, country_name, account_year, price FROM stock WHERE available=1 ORDER BY country_name, phone LIMIT $1 OFFSET $2",
                limit, offset,
            )
        if not rows:
            return await event.edit("📦 <b>Stock is empty.</b>", buttons=[[Button.inline("🔙 Back", "adm_adminmain")]])
        lines = [f"📦 <b>Stock Viewer</b> — Page {page} ({total} total)\n"]
        btns = []
        for row in rows:
            flag = await get_flag_by_country_name(row["country_name"])
            lines.append(f"{flag} {row['country_name']} | {row['account_year']} | ₹{row['price']} | <code>+{row['phone']}</code>")
            btns.append([Button.inline(f"🗑 Remove +{row['phone']} ({row['country_name']})", f"adm_rmstock|{row['phone']}")])
        nav = []
        if page > 1:
            nav.append(Button.inline("⬅ Prev", f"adm_viewstock|{page - 1}"))
        if offset + limit < total:
            nav.append(Button.inline("Next ➡", f"adm_viewstock|{page + 1}"))
        if nav:
            btns.append(nav)
        btns.append([Button.inline("🔙 Back", "adm_adminmain")])
        return await event.edit("\n".join(lines), buttons=btns)

    elif action_data.startswith("rmstock|") and (uid == ADMIN_ID or await has_perm(uid, 'p_manage_stock')):
        phone = action_data.split("|")[1]
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT session_file, country_name, account_year FROM stock WHERE phone=$1 AND available=1", phone
            )
        if not row:
            return await event.answer("❌ Account not found or already removed.", alert=True)
        sess_file, country, year = row["session_file"], row["country_name"], row["account_year"]
        clean_sess = sess_file[:-8] if sess_file.endswith(".session") else sess_file
        client = TelegramClient(clean_sess, API_ID, API_HASH)
        try:
            await client.connect()
            try:
                authorized = await client.is_user_authorized()
            except (AuthKeyUnregisteredError, AuthKeyDuplicatedError, UserDeactivatedBanError):
                authorized = False
            if authorized:
                try:
                    await asyncio.wait_for(client.log_out(), timeout=8)
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            await _safe_disconnect(client)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM stock WHERE phone=$1", phone)
        delete_session_files(sess_file)
        flag = await get_flag_by_country_name(country)
        return await event.answer(f"✅ Removed +{phone} ({flag} {country} {year}) and logged out.", alert=True)

    elif action_data == "stockhealth" and (uid == ADMIN_ID or await has_perm(uid, 'p_manage_stock')):
        async with pool.acquire() as conn:
            all_stock = await conn.fetch("SELECT phone, session_file, country_name FROM stock WHERE available=1")
        total_in_stock = len(all_stock)
        frozen_phones, dead_phones, banned_phones = [], [], []
        await event.edit(f"<b>🔍 Checking {total_in_stock} accounts in stock...</b>\n<i>Please wait.</i>")
        for row in all_stock:
            health = await _check_session_health(row["phone"], row["session_file"])
            if health == 'frozen':
                frozen_phones.append((row["phone"], row["country_name"], row["session_file"]))
            elif health == 'banned':
                banned_phones.append((row["phone"], row["country_name"], row["session_file"]))
            elif health == 'dead':
                dead_phones.append((row["phone"], row["country_name"], row["session_file"]))
        total_bad = len(frozen_phones) + len(dead_phones) + len(banned_phones)
        healthy = total_in_stock - total_bad
        lines = [
            "🧊 <b>Stock Health Report</b>\n",
            f"📦 <b>Total Checked:</b> {total_in_stock}",
            f"🧊 <b>Frozen:</b> {len(frozen_phones)}",
            f"💀 <b>Dead:</b> {len(dead_phones)}",
            f"🚫 <b>Banned:</b> {len(banned_phones)}",
            f"✅ <b>Healthy:</b> {healthy}",
        ]
        for label, lst in [("🧊 Frozen", frozen_phones), ("💀 Dead", dead_phones), ("🚫 Banned", banned_phones)]:
            if lst:
                lines.append(f"\n{label}:")
                for p, c, _ in lst[:10]:
                    lines.append(f"  • <code>+{p}</code> ({c})")
                if len(lst) > 10:
                    lines.append(f"  ... and {len(lst) - 10} more")
        btns = []
        if total_bad > 0:
            btns.append([Button.inline(f"🗑 Remove All Bad ({total_bad} accounts)", "adm_cleanup_bad_stock")])
        btns.append([Button.inline("🔙 Back", "adm_adminmain")])
        return await event.edit("\n".join(lines), buttons=btns)

    elif action_data == "cleanup_bad_stock" and (uid == ADMIN_ID or await has_perm(uid, 'p_manage_stock')):
        async with pool.acquire() as conn:
            all_stock = await conn.fetch("SELECT phone, session_file FROM stock WHERE available=1")
        removed = 0
        for row in all_stock:
            health = await _check_session_health(row["phone"], row["session_file"])
            if health != 'healthy':
                async with pool.acquire() as conn:
                    await conn.execute("DELETE FROM stock WHERE phone=$1", row["phone"])
                delete_session_files(row["session_file"])
                removed += 1
        return await event.edit(
            f"{P_YES} <b>Cleanup Complete!</b>\n\n🗑 Removed <b>{removed}</b> bad accounts.",
            buttons=[[Button.inline("🔙 Back", "adm_adminmain")]],
        )

    elif action_data == "stats" and await has_perm(uid, 'p_stats'):
        async with pool.acquire() as conn:
            u = (await conn.fetchrow("SELECT COUNT(*) FROM users"))[0]
            s = (await conn.fetchrow("SELECT COUNT(*) FROM stock WHERE available=1"))[0]
            r_row = await conn.fetchrow("SELECT value FROM settings WHERE key='upi_revenue'")
            total_bal = (await conn.fetchrow("SELECT SUM(balance) FROM users"))[0] or 0
            o_row = await conn.fetchrow("SELECT COUNT(*), SUM(price) FROM orders")
        r = r_row["value"] if r_row else "0"
        total_orders = o_row[0] if o_row else 0
        total_spent = (o_row[1] if o_row and o_row[1] else 0)
        msg = (
            f"{P_STATS} <b>ADVANCED STATS</b>\n\n"
            f"{P_USERS} <b>Total Users:</b> {u}\n"
            f"{P_PKG} <b>Accounts in Stock:</b> {s}\n"
            f"{P_MONEY} <b>Total UPI Revenue:</b> {P_INR}{r}\n\n"
            f"{P_CARD} <b>Overall Users Balance:</b> {P_INR}{total_bal}\n"
            f"{P_CART} <b>Total Accounts Sold:</b> {total_orders}\n"
            f"{P_USDT} <b>Overall Sales Amount:</b> {P_INR}{total_spent}"
        )
        return await event.edit(msg, buttons=[[Button.inline("Back", "adm_adminmain")]])

    elif action_data == "payments" and await has_perm(uid, 'p_settings'):
        btns = [
            [Button.inline("Add Payment Method", "adm_addpay")],
            [Button.inline("Remove Payment Method", "adm_delpay")],
            [Button.inline("Back to Admin", "adm_adminmain")],
        ]
        return await event.edit(f"{P_CARD} <b>Manage Payment Methods</b>", buttons=btns)

    elif action_data == "autopay" and await has_perm(uid, 'p_settings'):
        enabled = await is_autopay_enabled()
        cfg = await get_autopay_config()
        status_label = f"{P_ON} Auto Payment: ON" if enabled else f"{P_OFF} Auto Payment: OFF"
        qr_val = cfg["qr_file_id"]
        qr_status = "✅ Set" if (qr_val.startswith("tg:") or (qr_val and os.path.exists(qr_val))) else "⚪️ Not Set"
        btns = [
            [Button.inline(status_label, "adm_autopay_toggle")],
            [Button.inline(f"🖼️ Upload QR ({qr_status})", "adm_autopay_qr")],
            [Button.inline("📝 Edit QR Caption", "adm_autopay_caption")],
            [Button.inline("🔙 Back to Admin", "adm_adminmain")],
        ]
        return await event.edit(
            f"🤖 <b>Auto Payment Settings</b>\n\n"
            f"Users pay via this QR, enter their UTR, send a screenshot, "
            f"and the bot auto-checks your bank email for confirmation (60s timeout, "
            f"then falls back to your manual approval).\n\n"
            f"<b>Current Caption Preview:</b>\n{cfg['caption']}",
            buttons=btns,
        )

    elif action_data == "autopay_toggle" and await has_perm(uid, 'p_settings'):
        enabled = await is_autopay_enabled()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ('autopay_enabled', $1) "
                "ON CONFLICT (key) DO UPDATE SET value=$1",
                "off" if enabled else "on",
            )
        await event.answer(f"Auto Payment turned {'OFF' if enabled else 'ON'}!", alert=True)
        class FakeEvent:
            chat_id = chat
            sender_id = uid
            data = b"adm_autopay"
            async def edit(self, *a, **kw):
                return await event.edit(*a, **kw)
            async def answer(self, *a, **kw):
                pass
            async def delete(self, *a, **kw):
                pass
        return await admin_actions(FakeEvent())

    elif action_data == "banners" and await has_perm(uid, 'p_settings'):
        labels = {
            "start": "🏠 Start Menu", "deposit": "💰 Deposit", "buy": "🛒 Buy Account",
            "profile": "👤 Profile", "stats": "📊 Stats", "support": "🆘 Support",
        }
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM settings WHERE key LIKE 'banner_%'")
        custom_set = {r["key"].replace("banner_", "") for r in rows if r["value"]}
        btns = []
        for key, label in labels.items():
            tag = "✅" if key in custom_set else "⚪️"
            btns.append([Button.inline(f"{tag} {label}", f"adm_setbanner|{key}")])
        btns.append([Button.inline("♻️ Reset All to Default", "adm_resetbanners")])
        btns.append([Button.inline("🔙 Back to Admin", "adm_adminmain")])
        return await event.edit(
            f"{P_DOC} <b>Banner Images</b>\n\n"
            f"✅ = Custom image set (stored on VPS)\n"
            f"⚪️ = Using default config.py image\n\n"
            f"Tap a banner to upload a new image for it.",
            buttons=btns,
        )

    elif action_data.startswith("setbanner|") and await has_perm(uid, 'p_settings'):
        banner_key = action_data.split("|")[1]
        async with bot.conversation(chat, timeout=120) as conv:
            await conv.send_message(f"{P_DOC} <b>Send the new image for this banner:</b>\n\n<i>(Type /cancel to abort)</i>")
            resp = await conv.get_response()
            if resp.text == "/cancel":
                return await conv.send_message(f"{P_WARN} Cancelled.")
            if not resp.photo:
                return await conv.send_message(f"{P_NO} That's not a photo. Please try again from the Banner Images menu.")
            os.makedirs("banners", exist_ok=True)
            banner_path = f"banners/{banner_key}_{int(time.time())}.jpg"
            await bot.download_media(resp, banner_path)
            async with pool.acquire() as conn:
                old_row = await conn.fetchrow("SELECT value FROM settings WHERE key=$1", f"banner_{banner_key}")
                await conn.execute(
                    "INSERT INTO settings (key, value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2",
                    f"banner_{banner_key}", banner_path,
                )
            # Clean up the old file + cached file_id so the new image is used immediately
            if old_row and old_row["value"] and os.path.exists(old_row["value"]):
                try:
                    os.remove(old_row["value"])
                except Exception:
                    pass
                _image_cache.pop(old_row["value"], None)
            await conv.send_message(f"{P_YES} Banner image updated! It will appear on the next message sent.")

    elif action_data == "resetbanners" and await has_perm(uid, 'p_settings'):
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM settings WHERE key LIKE 'banner_%'")
            await conn.execute("DELETE FROM settings WHERE key LIKE 'banner_%'")
        for r in rows:
            if r["value"] and os.path.exists(r["value"]):
                try:
                    os.remove(r["value"])
                except Exception:
                    pass
                _image_cache.pop(r["value"], None)
        return await event.edit(f"{P_YES} All banners reset to default config.py images.", buttons=[[Button.inline("🔙 Back", "adm_adminmain")]])

    elif action_data == "setwelcome" and await has_perm(uid, 'p_settings'):
        async with pool.acquire() as conn:
            cur = await conn.fetchrow("SELECT value FROM settings WHERE key='welcome_msg'")
        cur_msg = cur["value"] if cur and cur["value"] else "(using default)"
        async with bot.conversation(chat, timeout=180) as conv:
            await conv.send_message(
                f"✏️ <b>Set Welcome Message</b>\n\n"
                f"<b>Current:</b>\n<blockquote>{cur_msg}</blockquote>\n\n"
                f"Send the new welcome message. You can use HTML formatting and the placeholder "
                f"<code>{{name}}</code> which will be replaced with the user's first name.\n\n"
                f"<i>Type /cancel to keep current.</i>"
            )
            r = await conv.get_response()
            if r.text == "/cancel":
                return await conv.send_message(f"{P_WARN} Cancelled. Welcome message unchanged.")
            new_msg = r.text.strip()
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO settings (key, value) VALUES ('welcome_msg', $1) ON CONFLICT (key) DO UPDATE SET value=$1",
                    new_msg,
                )
            await conv.send_message(
                f"{P_YES} <b>Welcome message updated!</b>\n\n"
                f"<b>Preview:</b>\n<blockquote>{new_msg.replace('{name}', 'John')}</blockquote>"
            )

    elif action_data == "manageadmins" and uid == ADMIN_ID:
        return await manage_admins_menu(event)

    elif action_data == "demo" and (uid == ADMIN_ID or await has_perm(uid, 'p_demo')):
        msg = (
            "🎬 <b>DEMO MODE</b>\n\n"
            "This is a simulated walkthrough of the full purchase flow.\n"
            "No real data, sessions, or users are touched.\n\n"
            "A demo purchase log will be sent to the log channel at the end.\n\n"
            "Tap ▶️ Start Demo to begin!"
        )
        btns = [
            [Button.inline("▶️ Start Demo", "demo_step_1")],
            [Button.inline("❌ Cancel",     "adm_adminmain")],
        ]
        await event.edit(msg, buttons=btns)
        return

    elif action_data.startswith("tglperm|") and uid == ADMIN_ID:
        _, t_id, p_name = action_data.split("|")
        allowed_perms = {"p_add_stock", "p_manage_stock", "p_stats", "p_bal", "p_settings", "p_demo"}
        if p_name not in allowed_perms:
            return await event.answer("❌ Invalid permission.", alert=True)
        async with pool.acquire() as conn:
            await conn.execute(
                f"UPDATE admins SET {p_name} = CASE WHEN {p_name}=1 THEN 0 ELSE 1 END WHERE user_id=$1", int(t_id)
            )
        return await edit_admin_menu(event, int(t_id))

    elif action_data.startswith("deladmin|") and uid == ADMIN_ID:
        t_id = int(action_data.split("|")[1])
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM admins WHERE user_id=$1", t_id)
        await event.answer("✅ Admin Removed", alert=True)
        return await manage_admins_menu(event)

    elif action_data == "managestock" and await has_perm(uid, 'p_manage_stock'):
        return await send_manage_stock_page(event, 1)
    elif action_data.startswith("mspg|") and await has_perm(uid, 'p_manage_stock'):
        return await send_manage_stock_page(event, int(action_data.split("|")[1]))
    elif action_data.startswith("msc|") and await has_perm(uid, 'p_manage_stock'):
        return await send_manage_stock_country(event, action_data.split("|")[1])
    elif action_data == "autoprice" and await has_perm(uid, 'p_manage_stock'):
        return await send_autoprice_page(event, 1)
    elif action_data.startswith("appg|") and await has_perm(uid, 'p_manage_stock'):
        return await send_autoprice_page(event, int(action_data.split("|")[1]))
    elif action_data.startswith("apc|") and await has_perm(uid, 'p_manage_stock'):
        return await send_autoprice_country(event, action_data.split("|")[1])

    elif action_data == "backupusr" and await has_perm(uid, 'p_settings'):
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM users")
        with open("users_backup.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if rows:
                w.writerow(rows[0].keys())
                w.writerows([list(r.values()) for r in rows])
        await bot.send_file(chat, "users_backup.csv", caption=f"{P_USERS} <b>Users Backup CSV</b>")
        os.remove("users_backup.csv")
        return await event.answer("✅ Backup Generated!", alert=True)

    # ── Conversation-based admin actions ──────────────────────────────────────
    async with bot.conversation(chat, timeout=600) as conv:
        async def get_reply(txt: str):
            await conv.send_message(txt + "\n\n<i>(Type /cancel to abort)</i>")
            resp = await conv.get_response()
            if resp.text == "/cancel":
                raise ValueError("Cancelled")
            return resp

        async def select_account_year(prompt: str = None):
            import datetime as _dt
            current_year = _dt.datetime.now().year
            year_options = list(range(current_year, current_year - 6, -1))
            year_buttons = []
            row = []
            for i, yr in enumerate(year_options):
                row.append(Button.inline(str(yr), f"_yearpick_{uid}_{yr}"))
                if len(row) == 3 or i == len(year_options) - 1:
                    year_buttons.append(row)
                    row = []
            year_buttons.append([Button.inline("✏️ Other Year", f"_yearpick_{uid}_custom")])
            prompt_text = prompt or "Select Account Year:"
            year_msg = await conv.send_message(
                f"{P_CAL} <b>{prompt_text}</b>",
                buttons=year_buttons,
            )
            try:
                year_event = await conv.wait_event(
                    events.CallbackQuery(
                        chats=conv.chat_id,
                        func=lambda e: e.sender_id == uid and e.data.decode().startswith(f"_yearpick_{uid}_"),
                    ),
                    timeout=60,
                )
                await year_event.answer()
                choice = year_event.data.decode().split("_")[-1]
                if choice == "custom":
                    await year_msg.edit(f"{P_CAL} <b>Enter custom account year:</b>\n<i>Example: 2025</i>")
                    year_resp = await conv.get_response()
                    if year_resp.text.strip().lower() == "/cancel":
                        raise ValueError("Cancelled")
                    try:
                        year = int(year_resp.text.strip())
                        if year < 1900 or year > current_year + 5:
                            raise ValueError()
                    except Exception:
                        raise ValueError("Invalid year")
                else:
                    year = int(choice)
                await year_msg.edit(f"{P_CAL} <b>Account Year:</b> {year}")
                return year
            except asyncio.TimeoutError:
                raise

        try:
            if action_data == "ap_add_country" and await has_perm(uid, 'p_manage_stock'):
                code = (await get_reply(f"{P_PHONE} <b>Enter Country Calling Code (without +):</b>")).text.replace("+", "").strip()
                flag = html.escape((await get_reply(f"{P_FLAG} <b>Enter Country Flag Emoji:</b>")).text.strip())
                name = html.escape((await get_reply(f"{P_GLOBE} <b>Enter Country Name:</b>")).text.strip())
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO custom_countries (code, name, flag) VALUES ($1,$2,$3) ON CONFLICT (code) DO UPDATE SET name=$2, flag=$3",
                        code, name, flag,
                    )
                await conv.send_message(f"{P_YES} Custom Country Added: {flag} {name} (+{code})")

            elif action_data == "userinfo" and await has_perm(uid, 'p_stats'):
                t_uid = int((await get_reply(f"{P_ACC} <b>Enter User ID:</b>")).text)
                async with pool.acquire() as conn:
                    u_row = await conn.fetchrow(
                        "SELECT balance, total_deposited, joined_date, banned, discount FROM users WHERE user_id=$1", t_uid
                    )
                    if not u_row:
                        return await conv.send_message(f"{P_NO} User not found.")
                    o_row = await conn.fetchrow("SELECT COUNT(*), SUM(price) FROM orders WHERE user_id=$1", t_uid)
                    up_row = await conn.fetchrow("SELECT SUM(amount) FROM upi_orders WHERE user_id=$1 AND status='success'", t_uid)
                bal = u_row["balance"]; dep = u_row["total_deposited"]; joined = u_row["joined_date"]; is_banned = u_row["banned"]; disc = u_row["discount"]
                o_count = o_row[0] or 0; o_spent = o_row[1] or 0; u_upi = up_row[0] if up_row and up_row[0] else 0
                await conv.send_message(
                    f"{P_ACC} <b>USER INFO:</b> <code>{t_uid}</code>\n\n"
                    f"{P_MONEY} Balance: {P_INR}{bal}\n"
                    f"{P_CARD} Total Deposited: {P_INR}{dep}\n"
                    f"{P_UPI} UPI Deposited: {P_INR}{u_upi}\n"
                    f"{P_CART} Total Orders: {o_count}\n"
                    f"{P_USDT} Total Spent: {P_INR}{o_spent}\n"
                    f"{P_GIFT} Discount: {disc}%\n"
                    f"{P_CAL} Joined: {joined}\n"
                    f"{P_OFF} Banned: {'Yes' if is_banned else 'No'}"
                )

            elif action_data == "addadmin" and uid == ADMIN_ID:
                new_ad = int((await get_reply(f"{P_ACC} <b>Enter User ID for new Admin:</b>")).text)
                async with pool.acquire() as conn:
                    await conn.execute("INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT DO NOTHING", new_ad)
                await conv.send_message(f"{P_YES} Admin added!")
                class FakeEv:
                    async def edit(self, t, buttons=None): await bot.send_message(chat, t, buttons=buttons)
                    async def answer(self, t, alert=False): pass
                await edit_admin_menu(FakeEv(), new_ad)

            elif action_data == "editadminreq" and uid == ADMIN_ID:
                t_id = int((await get_reply(f"{P_ACC} <b>Enter User ID to edit:</b>")).text)
                class FakeEv:
                    async def edit(self, t, buttons=None): await bot.send_message(chat, t, buttons=buttons)
                    async def answer(self, t, alert=False): pass
                await edit_admin_menu(FakeEv(), t_id)

            elif action_data.startswith("msedit|") and await has_perm(uid, 'p_manage_stock'):
                parts = action_data.split("|")
                action, c_name = parts[1], parts[2]
                async with pool.acquire() as conn:
                    if action == "name":
                        new_name = html.escape((await get_reply(f"{P_DOC} <b>Enter NEW Name for {c_name}:</b>")).text)
                        await conn.execute("UPDATE stock SET country_name=$1 WHERE country_name=$2", new_name, c_name)
                        await conn.execute("UPDATE auto_prices SET country=$1 WHERE country=$2", new_name, c_name)
                        await conv.send_message(f"{P_YES} Renamed '{c_name}' → '{new_name}'!")
                    elif action == "flag":
                        new_flag = html.escape((await get_reply(f"{P_FLAG} <b>Enter NEW Flag Emoji for {c_name}:</b>")).text)
                        await conn.execute("UPDATE stock SET country_icon=$1 WHERE country_name=$2", new_flag, c_name)
                        await conv.send_message(f"{P_YES} Flag updated to {new_flag} for '{c_name}'!")
                    elif action == "cprice":
                        new_p = int((await get_reply(f"{P_MONEY} <b>Enter NEW Common Price for all {c_name} accounts:</b>")).text)
                        await conn.execute("UPDATE stock SET price=$1 WHERE country_name=$2", new_p, c_name)
                        await conv.send_message(f"{P_YES} All '{c_name}' accounts updated to {P_INR}{new_p}!")
                    elif action == "yprice":
                        year = parts[3]
                        new_p = int((await get_reply(f"{P_MONEY} <b>Enter NEW Price for {c_name} ({year}):</b>")).text)
                        await conn.execute("UPDATE stock SET price=$1 WHERE country_name=$2 AND account_year=$3", new_p, c_name, int(year))
                        await conv.send_message(f"{P_YES} '{c_name}' ({year}) updated to {P_INR}{new_p}!")

            elif action_data.startswith("apset|") and await has_perm(uid, 'p_manage_stock'):
                parts = action_data.split("|")
                c_name, year = parts[1], parts[2]
                new_p = int((await get_reply(f"{P_ASST} <b>Enter Auto-Price for {c_name} ({year}):</b>\n<i>(Enter 0 to remove)</i>")).text)
                async with pool.acquire() as conn:
                    if new_p == 0:
                        await conn.execute("DELETE FROM auto_prices WHERE country=$1 AND year=$2", c_name, year)
                        await conv.send_message(f"{P_YES} Auto-Price for {c_name} ({year}) removed!")
                    else:
                        await conn.execute(
                            "INSERT INTO auto_prices (country, year, price) VALUES ($1,$2,$3) ON CONFLICT (country, year) DO UPDATE SET price=$3",
                            c_name, year, new_p,
                        )
                        await conv.send_message(f"{P_YES} Auto-Price for {c_name} ({year}) set to {P_INR}{new_p}!")

            elif action_data == "addpay" and await has_perm(uid, 'p_settings'):
                name = html.escape((await get_reply(f"{P_CARD} <b>Enter Payment Method Name:</b>")).text)
                qr_msg = await get_reply(f"📸 <b>Send QR Code Image:</b>\n<i>(Or type <code>skip</code> if no QR needed)</i>")
                qr_path = ""
                if qr_msg.photo:
                    qr_path = f"qr_{int(time.time())}.jpg"
                    await bot.download_media(qr_msg, qr_path)
                cap_msg = (await get_reply(f"{P_DOC} <b>Enter Payment Caption:</b>")).text
                cap_msg = html.escape(cap_msg).replace("&lt;code&gt;", "<code>").replace("&lt;/code&gt;", "</code>")
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO custom_payments (name, caption, qr_file_id) VALUES ($1,$2,$3)",
                        name, cap_msg, qr_path,
                    )
                await conv.send_message(f"{P_YES} Payment Method '{name}' added!")

            elif action_data == "autopay_qr" and await has_perm(uid, 'p_settings'):
                qr_msg = await get_reply(f"📸 <b>Send the new Auto Payment QR Code Image:</b>")
                if not qr_msg.photo:
                    await conv.send_message(f"{P_NO} That wasn't an image. Aborted — try again from the menu.")
                else:
                    # Upload to Telegram and save the file_id — never touches disk so it
                    # survives restarts/redeployments permanently.
                    sent = await bot.send_file(
                        _get_payment_log_channel(),
                        qr_msg.media,
                        caption="[autopay_qr_storage]",
                    )
                    tg_file_id = sent.photo.id if sent.photo else str(sent.id)
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO settings (key, value) VALUES ('autopay_qr_file_id', $1) "
                            "ON CONFLICT (key) DO UPDATE SET value=$1",
                            f"tg:{tg_file_id}:{sent.id}:{_get_payment_log_channel()}",
                        )
                    await conv.send_message(f"{P_YES} Auto Payment QR saved permanently to Telegram!")

            elif action_data == "autopay_caption" and await has_perm(uid, 'p_settings'):
                cap_msg = (await get_reply(
                    f"{P_DOC} <b>Enter new Auto Payment caption/instructions</b> "
                    f"(shown to users above the QR — HTML <code>&lt;b&gt;</code>/<code>&lt;code&gt;</code> tags allowed):"
                )).text
                cap_msg = html.escape(cap_msg).replace("&lt;b&gt;", "<b>").replace("&lt;/b&gt;", "</b>") \
                                               .replace("&lt;code&gt;", "<code>").replace("&lt;/code&gt;", "</code>")
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO settings (key, value) VALUES ('autopay_caption', $1) "
                        "ON CONFLICT (key) DO UPDATE SET value=$1",
                        cap_msg,
                    )
                await conv.send_message(f"{P_YES} Auto Payment caption updated!")

            elif action_data == "delpay" and await has_perm(uid, 'p_settings'):
                async with pool.acquire() as conn:
                    rows = await conn.fetch("SELECT id, name FROM custom_payments")
                if not rows:
                    return await conv.send_message(f"{P_NO} No custom payment methods.")
                msg = f"{P_DOC} <b>Reply with the ID of the method to delete:</b>\n\n"
                for r in rows:
                    msg += f"ID: {r['id']} - {r['name']}\n"
                del_id = (await get_reply(msg)).text
                try:
                    del_id = int(del_id)
                    async with pool.acquire() as conn:
                        file_path = await conn.fetchrow("SELECT qr_file_id FROM custom_payments WHERE id=$1", del_id)
                        if file_path and file_path["qr_file_id"] and os.path.exists(file_path["qr_file_id"]):
                            os.remove(file_path["qr_file_id"])
                        await conn.execute("DELETE FROM custom_payments WHERE id=$1", del_id)
                    await conv.send_message(f"{P_YES} Deleted!")
                except Exception:
                    await conv.send_message(f"{P_NO} Invalid ID.")

            elif action_data == "addzip" and await has_perm(uid, 'p_add_stock'):
                resp = await get_reply(f"{P_PKG} <b>Send the ZIP file containing <code>.session</code> files:</b>")
                if not resp.file or not resp.file.name.endswith('.zip'):
                    return await conv.send_message(f"{P_NO} Invalid file.")
                await conv.send_message(f"{P_WAIT} <b>Extracting & Scanning Accounts...</b>")
                zip_path = await bot.download_media(resp, "temp_sessions.zip")
                extracted_dir = f"temp_extracted_{int(time.time())}"
                os.makedirs(extracted_dir, exist_ok=True)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extracted_dir)
                groups = {}
                for file in os.listdir(extracted_dir):
                    if not file.endswith(".session"):
                        continue
                    sess_path = os.path.join(extracted_dir, file)
                    clean_path = sess_path[:-8]
                    try:
                        client = TelegramClient(clean_path, API_ID, API_HASH)
                        await client.connect()
                        if not await client.is_user_authorized():
                            await client.disconnect()
                            continue
                        me = await client.get_me()
                        phone = getattr(me, 'phone', None)
                        if not phone:
                            await client.disconnect()
                            continue
                        c_name, c_icon = await get_country_info(phone)
                        pwd = await client(GetPasswordRequest())
                        has_2fa = pwd.has_password
                        await client.disconnect()
                        key = (c_name, has_2fa)
                        if key not in groups:
                            groups[key] = []
                        groups[key].append({"phone": phone, "path": clean_path, "c_icon": c_icon})
                    except Exception as exc:
                        logger.error(f"Scan error: {exc}")
                for key in list(groups.keys()):
                    if key[0] == "Unknown":
                        sample_phone = groups[key][0]["phone"]
                        await conv.send_message(f"{P_WARN} Country not recognized for +{sample_phone}!")
                        new_icon = html.escape((await get_reply(f"{P_FLAG} <b>Enter Country Flag Emoji:</b>")).text)
                        new_name = html.escape((await get_reply(f"{P_GLOBE} <b>Enter Country Name:</b>")).text)
                        new_key = (new_name, key[1])
                        groups[new_key] = groups.pop(key)
                        for acc in groups[new_key]:
                            acc["c_icon"] = new_icon
                success = 0
                for (c_name, has_2fa), accs in groups.items():
                    c_icon = accs[0]["c_icon"]
                    twofa_pass = "None"
                    if has_2fa:
                        twofa_pass = html.escape((await get_reply(f"{P_2FA} <b>Enter 2FA Password for {len(accs)}x {c_name}:</b>")).text)
                    try:
                        year = await select_account_year(f"Select Account Year for {len(accs)}x {c_name}:")
                    except asyncio.TimeoutError:
                        await conv.send_message(f"{P_NO} Year selection timed out. Bulk upload aborted.")
                        break
                    except ValueError:
                        await conv.send_message(f"{P_NO} Invalid year. Bulk upload aborted.")
                        break
                    async with pool.acquire() as conn:
                        auto_row = await conn.fetchrow(
                            "SELECT price FROM auto_prices WHERE country=$1 AND year=$2", c_name, str(year)
                        )
                        if not auto_row:
                            auto_row = await conn.fetchrow(
                                "SELECT price FROM auto_prices WHERE country=$1 AND year='Common'", c_name
                            )
                        if auto_row:
                            price = auto_row["price"]
                            await conv.send_message(f"⚡ Auto-Price: {len(accs)}x {c_name} ({year}) @ {P_INR}{price}")
                        else:
                            existing_price = await conn.fetchrow(
                                "SELECT price FROM stock WHERE country_name=$1 AND account_year=$2 LIMIT 1", c_name, int(year)
                            )
                            if existing_price:
                                price = existing_price["price"]
                                await conv.send_message(f"⚡ Auto-detected: {len(accs)}x {c_name} ({year}) @ {P_INR}{price}")
                            else:
                                price = int((await get_reply(f"📌 Found {len(accs)}x {c_name} ({year}).\n{P_MONEY} Enter Price (₹):")).text)
                        for acc in accs:
                            perm_base = f"sessions/{acc['phone']}"
                            for ext in ('.session', '.session-wal', '.session-shm', '.session-journal'):
                                if os.path.exists(acc['path'] + ext):
                                    shutil.move(acc['path'] + ext, perm_base + ext)
                            await conn.execute(
                                "INSERT INTO stock (phone, session_file, country_name, country_icon, account_year, category, price, available, twofa) "
                                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (phone) DO UPDATE SET "
                                "session_file=$2, country_name=$3, country_icon=$4, account_year=$5, category=$6, price=$7, available=$8, twofa=$9",
                                acc['phone'], perm_base + ".session", c_name, c_icon, year, 'Good', price, 1, twofa_pass,
                            )
                            success += 1
                os.remove(zip_path)
                shutil.rmtree(extracted_dir)
                await conv.send_message(f"{P_YES} <b>Bulk Upload Complete!</b>\n{P_ON} Added: {success}")

            elif action_data == "addstock" and await has_perm(uid, 'p_add_stock'):
                await run_addstock_flow(conv, uid, bot)

            elif action_data == "addsessfile" and await has_perm(uid, 'p_add_stock'):
                while True:
                    await conv.send_message(
                        f"📂 <b>Add Account via Session File</b>\n\n"
                        f"Send the <code>.session</code> file for the account you want to add.\n"
                        f"<i>(Type /cancel to abort)</i>"
                    )
                    resp = await conv.get_response()
                    if resp.text and resp.text.strip() == "/cancel":
                        raise ValueError("Cancelled")
                    if not resp.file or not (resp.file.name or "").endswith(".session"):
                        await conv.send_message(f"{P_NO} Please send a valid <code>.session</code> file.")
                        break
                    await conv.send_message(f"{P_WAIT} <b>Scanning session file...</b>")
                    temp_sess_path = f"temp_upload_{uid}_{int(time.time())}.session"
                    await bot.download_media(resp, temp_sess_path)
                    clean_path = temp_sess_path[:-8]
                    c_name = "Unknown"; c_icon = "🌍"; phone = None; has_2fa = False; twofa_pass = "None"
                    try:
                        client = TelegramClient(clean_path, API_ID, API_HASH)
                        await client.connect()
                        try:
                            authorized = await client.is_user_authorized()
                        except (AuthKeyUnregisteredError, AuthKeyDuplicatedError, UserDeactivatedBanError):
                            await _safe_disconnect(client)
                            if os.path.exists(temp_sess_path):
                                os.remove(temp_sess_path)
                            await conv.send_message(f"{P_NO} This session is <b>banned/revoked</b>.")
                            break
                        if not authorized:
                            await _safe_disconnect(client)
                            if os.path.exists(temp_sess_path):
                                os.remove(temp_sess_path)
                            await conv.send_message(f"{P_NO} Session is <b>not authorized</b> (dead).")
                            break
                        me = await client.get_me()
                        phone = getattr(me, 'phone', None)
                        if not phone:
                            await _safe_disconnect(client)
                            if os.path.exists(temp_sess_path):
                                os.remove(temp_sess_path)
                            await conv.send_message(f"{P_NO} Could not get phone number from session.")
                            break
                        try:
                            pwd_info = await client(GetPasswordRequest())
                            has_2fa = pwd_info.has_password
                        except Exception:
                            has_2fa = False
                        c_name, c_icon = await get_country_info(phone)
                    except Exception as exc:
                        if os.path.exists(temp_sess_path):
                            os.remove(temp_sess_path)
                        await conv.send_message(f"{P_NO} Error reading session: {exc}")
                        break
                    await conv.send_message(
                        f"✅ <b>Session Valid!</b>\n\n"
                        f"{P_PHONE} Phone: <code>+{phone}</code>\n"
                        f"{P_GLOBE} Detected Country: <b>{c_icon} {c_name}</b>\n"
                        f"{P_2FA} 2FA: {'Yes' if has_2fa else 'No'}"
                    )
                    if c_name == "Unknown":
                        c_icon = html.escape((await get_reply(f"{P_FLAG} <b>Enter Country Flag Emoji:</b>")).text)
                        c_name = html.escape((await get_reply(f"{P_GLOBE} <b>Enter Country Name:</b>")).text)
                    if has_2fa:
                        twofa_pass = html.escape((await get_reply(f"{P_2FA} <b>Enter 2FA Password:</b>")).text)
                    try:
                        year = await select_account_year(f"Select Account Year for {c_name}:")
                    except asyncio.TimeoutError:
                        await conv.send_message(f"{P_NO} Year selection timed out. Account not saved.")
                        break
                    except ValueError:
                        await conv.send_message(f"{P_NO} Invalid year. Account not saved.")
                        break
                    async with pool.acquire() as conn:
                        auto_row = await conn.fetchrow(
                            "SELECT price FROM auto_prices WHERE country=$1 AND year=$2", c_name, str(year)
                        )
                        if not auto_row:
                            auto_row = await conn.fetchrow(
                                "SELECT price FROM auto_prices WHERE country=$1 AND year='Common'", c_name
                            )
                        if auto_row:
                            price = auto_row["price"]
                            await conv.send_message(f"⚡ Auto-Price: {P_INR}{price} for {c_name} ({year})")
                        else:
                            existing_price = await conn.fetchrow(
                                "SELECT price FROM stock WHERE country_name=$1 AND account_year=$2 LIMIT 1", c_name, int(year)
                            )
                            if existing_price:
                                price = existing_price["price"]
                                await conv.send_message(f"⚡ Auto-detected Price: {P_INR}{price} for {c_name} ({year})")
                            else:
                                price = int((await get_reply(f"{P_MONEY} <b>Enter Price (₹):</b>")).text)
                    try:
                        await _safe_disconnect(client)
                    except Exception:
                        pass
                    perm_base = f"sessions/{phone}"
                    if os.path.exists(temp_sess_path):
                        shutil.move(temp_sess_path, perm_base + ".session")
                    for ext in ('-wal', '-shm', '-journal'):
                        src = clean_path + ".session" + ext
                        if os.path.exists(src):
                            shutil.move(src, perm_base + ".session" + ext)
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO stock (phone, session_file, country_name, country_icon, account_year, category, price, available, twofa) "
                            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (phone) DO UPDATE SET "
                            "session_file=$2, country_name=$3, country_icon=$4, account_year=$5, category=$6, price=$7, available=$8, twofa=$9",
                            phone, perm_base + ".session", c_name, c_icon, year, 'Good', price, 1, twofa_pass,
                        )
                    success_msg = await conv.send_message(
                        f"{P_YES} <b>Account Added Successfully!</b>\n\n"
                        f"{P_PHONE} Phone: <code>+{phone}</code>\n"
                        f"{P_GLOBE} Country: {c_icon} {c_name}\n"
                        f"{P_CAL} Year: {year}\n"
                        f"{P_MONEY} Price: {P_INR}{price}\n"
                        f"{P_2FA} 2FA: {twofa_pass if has_2fa else 'None'}\n\nWhat would you like to do next?",
                        buttons=[
                            [Button.inline("➕ Add Another Session File", b"addsessfile_again")],
                            [Button.inline("✅ Done", b"addsessfile_done")],
                        ],
                    )
                    try:
                        click = await conv.wait_event(
                            events.CallbackQuery(
                                chats=conv.chat_id,
                                func=lambda e: e.sender_id == uid and e.data in (b"addsessfile_again", b"addsessfile_done"),
                            ),
                            timeout=60,
                        )
                        await click.answer()
                        if click.data == b"addsessfile_done":
                            await conv.send_message("✅ Done! All sessions saved.")
                            break
                        await conv.send_message("🔄 <b>Send the next session file:</b>")
                    except Exception:
                        break

            elif action_data == "supporturl" and await has_perm(uid, 'p_settings'):
                url = (await get_reply("🔗 Enter new Support URL (must start with https://).")).text
                if not url.startswith("http"):
                    url = "https://" + url.replace("@", "t.me/")
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO settings (key, value) VALUES ('support_url', $1) "
                        "ON CONFLICT (key) DO UPDATE SET value=$1",
                        url,
                    )
                await conv.send_message(f"{P_YES} Support URL updated.")

            elif action_data == "bcast" and await has_perm(uid, 'p_stats'):
                txt = (await get_reply(f"{P_DOC} <b>Message to broadcast:</b>")).text
                btn_name = (await get_reply("🔘 <b>Button Name (or 'skip'):</b>")).text
                btn_url = (await get_reply("🔗 <b>URL:</b>")).text if btn_name.lower() != 'skip' else None
                btns = [[Button.url(btn_name, btn_url)]] if btn_url else None
                async with pool.acquire() as conn:
                    users = await conn.fetch("SELECT user_id FROM users")
                s, f = 0, 0
                await conv.send_message(f"{P_TG} Broadcasting...")
                for row in users:
                    try:
                        await bot.send_message(int(row["user_id"]), txt, buttons=btns, parse_mode='html')
                        s += 1
                    except Exception:
                        f += 1
                    await asyncio.sleep(0.1)
                await conv.send_message(f"{P_YES} Done! Sent: {s} | Failed: {f}")

            elif action_data == "bal" and await has_perm(uid, 'p_bal'):
                t_uid = int((await get_reply(f"{P_ACC} <b>User ID:</b>")).text)
                amt = int((await get_reply(f"{P_MONEY} <b>Amount (negative to deduct):</b>")).text)
                await update_balance(t_uid, amt)
                await conv.send_message(f"{P_YES} Added {P_INR}{amt} to {t_uid}.")

            elif action_data == "discount" and await has_perm(uid, 'p_settings'):
                t_uid = int((await get_reply(f"{P_ACC} <b>User ID:</b>")).text)
                pct = int((await get_reply(f"{P_GIFT} <b>Discount % (0 to remove):</b>")).text)
                async with pool.acquire() as conn:
                    await conn.execute("UPDATE users SET discount=$1 WHERE user_id=$2", pct, t_uid)
                await conv.send_message(f"{P_YES} User {t_uid} has {pct}% discount.")

            elif action_data == "refpct" and await has_perm(uid, 'p_settings'):
                pct = int((await get_reply(f"{P_USERS} <b>New Referral %:</b>")).text)
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO settings (key, value) VALUES ('ref_percent', $1) ON CONFLICT (key) DO UPDATE SET value=$1",
                        str(pct),
                    )
                await conv.send_message(f"{P_YES} Referral revenue set to {pct}%.")

            elif action_data == "usdtrate" and await has_perm(uid, 'p_settings'):
                r = float((await get_reply(f"{P_USDT} <b>New USDT Rate (INR):</b>")).text)
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO settings (key, value) VALUES ('usdt_rate', $1) ON CONFLICT (key) DO UPDATE SET value=$1",
                        str(r),
                    )
                await conv.send_message(f"{P_YES} Rate set to {r}.")

            elif action_data == "restoreusr" and await has_perm(uid, 'p_settings'):
                resp = await get_reply(f"📤 <b>Send the <code>users_backup.csv</code> file:</b>")
                if not resp.file or not resp.file.name.endswith('.csv'):
                    return await conv.send_message(f"{P_NO} Invalid file.")
                await bot.download_media(resp, "temp_restore.csv")
                with open("temp_restore.csv", "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    next(reader)
                    count = 0
                    async with pool.acquire() as conn:
                        for row in reader:
                            try:
                                await conn.execute(
                                    "INSERT INTO users (user_id, balance, referred_by, total_deposited, joined_date, banned, discount, terms_accepted) "
                                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (user_id) DO UPDATE SET "
                                    "balance=$2, referred_by=$3, total_deposited=$4, joined_date=$5, banned=$6, discount=$7, terms_accepted=$8",
                                    int(row[0]), int(row[1]), int(row[2]) if row[2] else None,
                                    int(row[3]), row[4], int(row[5]), int(row[6]), int(row[7]),
                                )
                                count += 1
                            except Exception:
                                pass
                os.remove("temp_restore.csv")
                await conv.send_message(f"{P_YES} Restored {count} users.")

            elif action_data == "ban" and await has_perm(uid, 'p_bal'):
                t_uid = int((await get_reply(f"{P_ACC} <b>User ID:</b>")).text)
                async with pool.acquire() as conn:
                    is_ban = await conn.fetchrow("SELECT banned FROM users WHERE user_id=$1", t_uid)
                    if not is_ban:
                        return await conv.send_message(f"{P_NO} User not found.")
                    ns = 0 if is_ban["banned"] == 1 else 1
                    await conn.execute("UPDATE users SET banned=$1 WHERE user_id=$2", ns, t_uid)
                await conv.send_message(f"User {t_uid} is {'Banned 🚫' if ns == 1 else 'Unbanned ✅'}.")

        except ValueError:
            await conv.send_message(f"{P_NO} Cancelled.")
        except Exception as exc:
            await conv.send_message(f"{P_NO} Error: {exc}")


# =============================================================================
#  EVENT HANDLERS
# =============================================================================
@bot.on(events.NewMessage(pattern=r"(?i)^/backup"))
async def handle_backup(e):
    if e.sender_id != ADMIN_ID:
        return
    await e.respond("⏳ <b>Creating backup, please wait...</b>")
    await send_sessions_backup()


@bot.on(events.NewMessage(pattern=r"(?i)^/dbbackup"))
async def handle_db_backup(e):
    if e.sender_id != ADMIN_ID:
        return
    await e.respond("⏳ <b>Creating database backup, please wait...</b>")
    await send_database_backup()


@bot.on(events.NewMessage(pattern=r"(?i)^/start"))
async def handle_start(e):
    try:
        uid = e.sender_id
        if not uid:
            return
        await ensure_user(uid)
        if await is_user_banned(uid):
            return
        if not await is_bot_online() and not await is_admin(uid):
            maint_text = await get_maint_msg()
            return await e.respond(f"{P_OFF} <b>Maintenance</b>\n\n{maint_text}")

        session_buy_state.pop(uid, None)
        deposit_input.pop(uid, None)

        text = e.text or ''
        if len(text.split()) > 1:
            start_param = text.split()[1]
            if start_param.startswith("ref_"):
                ref = start_param.replace("ref_", "")
                if ref.isdigit() and int(ref) != uid and await is_referral_enabled():
                    pool = get_pool()
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE users SET referred_by=$1 WHERE user_id=$2 AND referred_by IS NULL",
                            int(ref), uid,
                        )

        is_joined = await check_channel_joined(bot, uid)
        if not is_joined:
            btns = [[Button.url(f"📢 Join Channel {i + 1}", link)] for i, link in enumerate(JOIN_URLS)]
            btns.append([Button.inline("✅ I've Joined – Verify", "verify_join")])
            return await e.respond(
                f"{P_WARN} <b>You must join our channels to use this bot!</b>\n\n"
                f"Please join all channels below, then click <b>✅ I've Joined – Verify</b>.",
                buttons=btns,
            )

        pool = get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT terms_accepted, balance, total_deposited FROM users WHERE user_id=$1", uid
            )
        if not row:
            return
        terms_ok, balance, deposited = row["terms_accepted"], row["balance"], row["total_deposited"]
        if not terms_ok and (balance > 0 or deposited > 0):
            async with pool.acquire() as conn:
                await conn.execute("UPDATE users SET terms_accepted=1 WHERE user_id=$1", uid)
            terms_ok = 1
        if not terms_ok:
            msg = f"{P_DOC} <b>TERMS & CONDITIONS</b>\nPlease read and accept our Terms & Conditions before using the bot."
            btns = [
                [KeyboardButtonWebView(text="📜 Read Terms & Conditions", url=TERMS_URL)],
                [Button.inline("✅ Accept", "tc_accept"), Button.inline("❌ Reject", "tc_reject")],
            ]
            return await e.respond(msg, buttons=btns)

        await send_main_menu(e, uid)
    except Exception as exc:
        logger.error(f"handle_start error: {exc}")


@bot.on(events.NewMessage())
async def handle_all_messages(e):
    try:
        uid = e.sender_id
        if not uid:
            return
        if getattr(e, 'text', None) and e.text.startswith('/'):
            return
        if not await is_bot_online() and not await is_admin(uid):
            maint_text = await get_maint_msg()
            return await e.respond(f"{P_OFF} <b>Maintenance</b>\n\n{maint_text}")

        await ensure_user(uid)
        if await is_user_banned(uid):
            return

        # Auto Payment: Step 1 - capture UTR text reply
        if uid in autopay_state and autopay_state[uid].get('step') == 'wait_utr' and e.text and not e.text.startswith('/'):
            utr_val = re.sub(r'[^A-Za-z0-9]', '', e.text.strip())
            if len(utr_val) < 6:
                return await e.reply(f"{P_WARN} That doesn't look like a valid UTR / UPI Ref No. Please re-check and send again.")
            autopay_state[uid]['utr'] = utr_val
            autopay_state[uid]['step'] = 'wait_ss'
            await e.reply(
                f"{P_YES} UTR Saved: <code>{utr_val}</code>\n\n"
                f"📸 Now send a clear <b>Screenshot</b> of the payment confirmation.",
                buttons=[[Button.inline("❌ Cancel", "cancel_action")]],
            )
            return

        # Auto Payment: Step 2 - capture screenshot, log, and start 60s auto-verify
        _is_autopay_img = bool(e.document and e.document.mime_type and e.document.mime_type.startswith("image/"))
        if uid in autopay_state and autopay_state[uid].get('step') == 'wait_ss' and (e.photo or _is_autopay_img):
            info = autopay_state.pop(uid)
            amount, utr = info['amount'], info['utr']
            pool = get_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO deposits (user_id, amount, method_name, status, utr) VALUES ($1,$2,$3,$4,$5) RETURNING id",
                    uid, amount, "Auto Payment", "pending", utr,
                )
            dep_id = row["id"]

            cap = (
                f"🤖 <b>AUTO PAYMENT REQUEST</b>\n"
                f"{P_ACC} User: <code>{uid}</code>\n"
                f"{P_MONEY} Amount: <b>{P_INR}{amount}</b>\n"
                f"🔢 UTR: <code>{utr}</code>\n"
                f"{P_ID} Ref: <code>{dep_id}</code>\n\n"
                f"⏳ <i>Auto-verifying via bank email (3 min)...</i>"
            )
            btns = [
                [Button.inline(f"✅ Accept (₹{amount})", f"dep_acc|{dep_id}|{uid}|Auto Payment|exact|{amount}"),
                 Button.inline("❌ Reject", f"dep_rej|{dep_id}|{uid}")],
            ]
            log_msg = None
            try:
                log_msg = await bot.send_message(_get_payment_log_channel(), cap, file=e.media, buttons=btns)
            except Exception as exc:
                logger.error(f"Failed to log autopay deposit: {exc}")

            countdown_msg = await e.reply(
                f"{P_WAIT} <b>Auto-verifying your payment...</b>\n\n⏳ <b>180s</b> remaining\n\n"
                f"<i>Checking for your bank confirmation email...</i>"
            )
            asyncio.create_task(autopay_verify_task(uid, dep_id, amount, utr, countdown_msg, log_msg))
            return

        if uid in autopay_state and not e.text:
            return await e.reply(f"{P_WARN} Please send a screenshot (as a photo) of your payment.")

        # Proof / screenshot for manual deposit
        _is_image_doc = bool(e.document and e.document.mime_type and e.document.mime_type.startswith("image/"))
        if uid in waiting_proof and (e.photo or _is_image_doc or (e.text and "http" in e.text)):
            info = waiting_proof.pop(uid)
            final_amt = info['amount']
            pool = get_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO deposits (user_id, amount, method_name, status) VALUES ($1,$2,$3,$4) RETURNING id",
                    uid, final_amt, info['method'], "pending",
                )
            dep_id = row["id"]
            await e.reply(f"{P_YES} Deposit Request Submitted! Wait for Admin Approval.")
            cap = (
                f"🔔 <b>NEW DEPOSIT REQUEST</b>\n"
                f"{P_ACC} User: <code>{uid}</code>\n"
                f"{P_MONEY} Request: <b>{P_INR}{info['amount']}</b>\n"
                f"{P_CARD} Method: {info['method']}\n"
                f"{P_ID} Ref: <code>{dep_id}</code>"
            )
            btns = [
                [Button.inline(f"✅ Accept (₹{final_amt})", f"dep_acc|{dep_id}|{uid}|{info['method']}|exact|{final_amt}"),
                 Button.inline("❌ Reject", f"dep_rej|{dep_id}|{uid}")],
                [Button.inline("📝 Custom Amount", f"dep_acc|{dep_id}|{uid}|{info['method']}|custom|0")],
            ]
            try:
                if e.photo or _is_image_doc:
                    await bot.send_message(_get_payment_log_channel(), cap, file=e.media, buttons=btns)
                else:
                    await bot.send_message(_get_payment_log_channel(), cap + f"\n🔗 Hash: {html.escape(e.text)}", buttons=btns)
            except Exception as exc:
                logger.error(f"Failed to log deposit: {exc}")
            return

        # User is waiting to submit proof but sent something we didn't recognize
        if uid in waiting_proof and not e.text:
            return await e.reply(f"{P_WARN} Please send a clear screenshot (as a photo) or a transaction hash/link.")

        text = e.text or ""
        if not text:
            return

        if any(k in text for k in ("Buy Account", "Buy Sessions", "Deposit", "My Profile", "My Stats", "Support", "Admin Panel")):
            session_buy_state.pop(uid, None)
            deposit_input.pop(uid, None)
            admin_dep_state.pop(uid, None)
            waiting_proof.pop(uid, None)
            autopay_input.pop(uid, None)
            autopay_state.pop(uid, None)

        # Admin deposit rejection reason
        if await is_admin(uid) and uid in admin_dep_state:
            st = admin_dep_state[uid]
            if st['step'] == 'wait_reason':
                t_uid, dep_id, msg_id = st['target_uid'], st['dep_id'], st['msg_id']
                log_chat_id = st.get('log_chat_id') or _get_payment_log_channel()
                pool = get_pool()
                async with pool.acquire() as conn:
                    await conn.execute("UPDATE deposits SET status='rejected' WHERE id=$1", int(dep_id))
                try:
                    await bot.edit_message(log_chat_id, msg_id, f"{P_NO} <b>REJECTED USER {t_uid}</b>\nReason: {html.escape(text)}")
                except Exception as exc:
                    logger.error(f"Failed to edit rejection log message: {exc}")
                await bot.send_message(int(t_uid), f"{P_NO} <b>Deposit Rejected!</b>\n📋 Reason: {html.escape(text)}")
                await e.reply(f"{P_YES} Rejection reason sent.")
                admin_dep_state.pop(uid)
                return

        # Bulk session quantity input
        if uid in session_buy_state:
            state = session_buy_state[uid]
            try:
                qty = int(re.sub(r'[^\d]', '', text))
                if qty < 1:
                    raise ValueError
                if qty > state['stock']:
                    return await e.respond(f"{P_WARN} <b>Not enough stock!</b> Max is {state['stock']}.")
                pool = get_pool()
                async with pool.acquire() as conn:
                    disc_row = await conn.fetchrow("SELECT discount FROM users WHERE user_id=$1", uid)
                discount = disc_row["discount"] if disc_row else 0
                total_cost = qty * state['price']
                if discount > 0:
                    total_cost = int(total_cost * (100 - discount) / 100)
                async with pool.acquire() as conn:
                    bal_row = await conn.fetchrow("SELECT balance FROM users WHERE user_id=$1", uid)
                user_bal = bal_row["balance"] if bal_row else 0
                if user_bal < total_cost:
                    return await e.respond(f"{P_NO} <b>Insufficient Balance!</b>\nYou need {P_INR}{total_cost} to buy {qty} sessions.")
                session_buy_state.pop(uid)
                await process_bulk_sessions(e, uid, qty, state, total_cost)
                return
            except ValueError:
                return await e.respond(f"{P_NO} Please enter a valid number.")

        # Manual deposit amount input
        if uid in deposit_input and deposit_input[uid]['step'] == 'wait_amt':
            try:
                amt = int(re.sub(r'[^\d]', '', text))
                if amt < 10:
                    return await e.reply(f"{P_WARN} Minimum Deposit is ₹10.")
                method = deposit_input[uid]['method']
                waiting_proof[uid] = {'amount': amt, 'method': method}
                deposit_input.pop(uid)
                rate = await get_usdt_rate()
                usdt_amt = round(float(amt) / float(rate), 2)
                rate_text = (
                    f"\n\n{P_MONEY} <b>Amount to Pay:</b> {P_INR}{amt} (~{P_USDT}{usdt_amt} USDT)\n"
                    f"💱 <i>Exchange Rate: {P_INR}{rate} = $1</i>"
                )
                pool = get_pool()
                async with pool.acquire() as conn:
                    row = await conn.fetchrow("SELECT caption, qr_file_id FROM custom_payments WHERE name=$1", method)
                if row:
                    cap = row["caption"] + f"{rate_text}\n\n👇 <b>After paying, send a clear Screenshot here:</b>"
                    btns = [[Button.inline("❌ Cancel", "cancel_action")]]
                    if row["qr_file_id"] and os.path.exists(row["qr_file_id"]):
                        try:
                            await bot.send_file(e.chat_id, row["qr_file_id"], caption=cap, buttons=btns)
                        except Exception:
                            await e.reply(cap, buttons=btns)
                    else:
                        await e.reply(cap, buttons=btns)
                else:
                    await e.reply(
                        f"{P_CARD} <b>{method} Deposit</b>{rate_text}\n\n👇 Send Screenshot here:",
                        buttons=[[Button.inline("❌ Cancel", "cancel_action")]],
                    )
            except ValueError:
                await e.respond(f"{P_NO} Please enter a valid number in {P_INR} (INR).")
            return

        # Main keyboard routing
        if "Buy Account" in text:
            await show_countries(e, 'single', 1)
        elif "Buy Sessions" in text:
            await show_countries(e, 'bulk', 1)
        elif "Deposit" in text:
            await deposit_menu(e)
        elif "My Profile" in text:
            await profile_handler(e)
        elif "My Stats" in text:
            await stats_handler(e)
        elif "Support" in text:
            support_url = await get_support_url()
            support_msg = f"{P_ON} <b>Support</b>\n\n{P_WARN} For support, contact admin."
            support_btns = [
                [Button.url("📩 Support", support_url)],
                [KeyboardButtonWebView(text="📜 Terms & Conditions", url=TERMS_URL)],
                [Button.url("📢 Channel", JOIN_URLS[0] if JOIN_URLS else "https://t.me/")],
            ]
            _support_img = await get_banner_image("support", SUPPORT_IMAGE)
            await send_with_image(e.chat_id, _support_img, support_msg, buttons=support_btns)
        elif "Admin Panel" in text:
            if await is_admin(uid):
                await admin_panel_handler(e)

    except Exception as exc:
        logger.error(f"handle_all_messages error: {exc}")


@bot.on(events.CallbackQuery)
async def handle_callback_query(e):
    try:
        uid = e.sender_id
        if not await is_bot_online() and not await is_admin(uid):
            maint_text = await get_maint_msg()
            await e.answer("⚙️ Bot is under maintenance.", alert=True)
            try:
                await e.edit(f"{P_OFF} <b>Maintenance Mode</b>\n\n{maint_text}")
            except Exception:
                pass
            return
        await ensure_user(uid)

        now = time.time()
        if uid in user_spam_cooldown and now - user_spam_cooldown[uid] < 0.5:
            return await e.answer("⚠️ Please slow down!", alert=True)
        user_spam_cooldown[uid] = now

        if await is_user_banned(uid):
            return await e.answer("🚫 BANNED", alert=True)

        data = e.data.decode()
        pool = get_pool()

        if data == "verify_join":
            if not await check_channel_joined(bot, uid):
                return await e.answer("⚠️ You must join the channels first!", alert=True)
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT terms_accepted, balance, total_deposited FROM users WHERE user_id=$1", uid
                )
            terms_ok = row["terms_accepted"] if row else 0
            if not terms_ok and row and (row["balance"] > 0 or row["total_deposited"] > 0):
                async with pool.acquire() as conn:
                    await conn.execute("UPDATE users SET terms_accepted=1 WHERE user_id=$1", uid)
                terms_ok = 1
            if not terms_ok:
                msg = "📜 <b>TERMS & CONDITIONS</b>\nPlease read and accept our Terms & Conditions before using the bot."
                btns = [
                    [KeyboardButtonWebView(text="📜 Read Terms & Conditions", url=TERMS_URL)],
                    [Button.inline("✅ Accept", "tc_accept"), Button.inline("❌ Reject", "tc_reject")],
                ]
                try:
                    await e.edit(msg, buttons=btns)
                except MessageNotModifiedError:
                    pass
                return
            await send_main_menu(e, uid)

        elif data == "tc_accept":
            async with pool.acquire() as conn:
                await conn.execute("UPDATE users SET terms_accepted=1 WHERE user_id=$1", uid)
            await e.answer("✅ Terms Accepted!", alert=True)
            await send_main_menu(e, uid)

        elif data == "tc_reject":
            try:
                await e.edit(f"{P_NO} You cannot use the bot without accepting the terms.")
            except MessageNotModifiedError:
                pass

        elif data == "cancel_action":
            deposit_input.pop(uid, None)
            waiting_proof.pop(uid, None)
            session_buy_state.pop(uid, None)
            autopay_input.pop(uid, None)
            autopay_state.pop(uid, None)
            try:
                await e.edit(f"{P_NO} <b>Cancelled.</b>")
            except MessageNotModifiedError:
                pass

        elif data.startswith("pg_c|"):
            p = data.split("|")
            await show_countries(e, p[1], int(p[2]))

        elif data.startswith("bc|"):
            p = data.split("|")
            if len(p) >= 4:
                # New format: bc|flow|country|year — go straight to confirm
                await show_years(e, p[1], p[2], year_filter=p[3])
            else:
                # Legacy format: bc|flow|country
                await show_years(e, p[1], p[2])

        elif data.startswith("by|"):
            p = data.split("|")
            if p[1] == 'single':
                await confirm_purchase(e, p[2], p[3], p[4])
            else:
                await init_session_purchase(e, p[2], p[3], p[4])

        elif data.startswith("buy_cf|"):
            p = data.split("|")
            await process_purchase(e, p[1], p[2], p[3])

        elif data.startswith("check_spam|"):
            p = data.split("|")
            spam_phone = p[4] if len(p) > 4 else None
            await fetch_spam_report(e, p[1], p[2], p[3], spam_phone)

        elif data.startswith("check_upi_"):
            order_id = data.replace("check_upi_", "")
            await verify_upi_payment(e, order_id)

        elif data.startswith("check_bank_"):
            order_id = data.replace("check_bank_", "")
            await verify_bank_payment(e, order_id)

        elif data.startswith("kp_"):
            await keypad_logic(e)

        elif data.startswith("bkp_"):
            await bank_keypad_logic(e)

        elif data == "dep_bank":
            await init_bank_keypad(e)

        elif data.startswith("akp_"):
            await autopay_keypad_logic(e)

        elif data == "dep_autopay":
            if not await is_autopay_enabled():
                return await e.answer("⚠️ Auto Payment is currently disabled.", alert=True)
            await init_autopay_keypad(e)

        elif data.startswith("get_otp_again|"):
            phone = data.split("|")[1]
            if phone not in active_orders:
                return await e.answer("⚠️ Session already expired.", alert=True)
            order = active_orders[phone]
            client = order['client']
            start_time = order['start_time']
            await e.answer("🔄 Fetching latest OTP...", alert=False)
            try:
                msgs = await client.get_messages(777000, limit=5)
                latest_code = None
                for m in msgs:
                    if m.date.timestamp() > start_time - 10:
                        if m.message and re.search(OTP_REGEX, m.message) and "Login detected" not in m.message:
                            latest_code = re.search(OTP_REGEX, m.message).group()
                            break
                if latest_code:
                    twofa_text = (
                        f"{P_2FA} <b>2FA:</b> <code>{order['twofa']}</code>"
                        if order['twofa'] != "None"
                        else f"🔓 <b>2FA:</b> <code>Disabled (No Password)</code>"
                    )
                    msg = (
                        f"{P_YES} <b>Latest OTP Fetched!</b>\n\n"
                        f"{P_PHONE} <b>Phone:</b> <code>{phone}</code>\n"
                        f"{P_FLAG} <b>Country:</b> {order['c_icon']} {order['country']}\n"
                        f"{P_OTP} <b>OTP:</b> <code>{latest_code}</code>\n"
                        f"{twofa_text}"
                    )
                    try:
                        await e.edit(msg, buttons=[
                            [Button.inline("🔄 Get OTP Again",   f"get_otp_again|{phone}")],
                            [Button.inline("📱 Manage Devices",  f"manage_devices|{phone}")],
                            [Button.inline("🚪 Finish & Logout", f"logout_bot|{phone}")],
                        ])
                    except MessageNotModifiedError:
                        pass
                else:
                    await e.answer("⏳ No new OTP yet. Try again in a few seconds.", alert=True)
            except Exception:
                await e.answer("❌ Error fetching OTP.", alert=True)

        elif data.startswith("logout_bot|"):
            phone = data.split("|")[1]
            if phone in active_orders:
                order = active_orders.pop(phone)
                # Clear any pending-logout flag set via Manage Devices
                bot_session_pending_logout.pop(phone, None)
                try:
                    await asyncio.wait_for(order['client'].log_out(), timeout=8)
                except Exception:
                    try:
                        await order['client'].disconnect()
                    except Exception:
                        pass
                delete_session_files(order['sess'])
                await e.edit(f"{P_YES} <b>Session Finished successfully.</b>")
            else:
                await e.answer("⚠️ No active order found or already finished.", alert=True)

        elif data.startswith("cancel_order|"):
            phone = data.split("|")[1]
            if phone not in active_orders:
                return await e.answer("⚠️ Order already expired or completed.", alert=True)
            order = active_orders[phone]
            elapsed = time.time() - order['start_time']
            if elapsed < 120:
                remaining = int(120 - elapsed)
                return await e.answer(f"⏳ Please wait {remaining} more seconds before cancelling.", alert=True)
            if order['paid']:
                return await e.answer("✅ OTP already delivered, cannot cancel.", alert=True)
            order = active_orders.pop(phone)
            try:
                await order['client'].disconnect()
            except Exception:
                pass
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE users SET balance = balance + $1 WHERE user_id=$2", order['price'], uid
                )
                await conn.execute("UPDATE stock SET available=1 WHERE phone=$1", phone)
            await e.edit(
                f"{P_YES} <b>Order Cancelled!</b>\n\n"
                f"{P_MONEY} <b>Refunded:</b> {P_INR}{order['price']} to your balance.\n"
                f"{P_PHONE} <b>Phone:</b> <code>{phone}</code>"
            )

        elif data.startswith("page_purchases_"):
            await send_purchase_page(e, uid, int(data.split("_")[2]))
        elif data == "back_to_stats":
            await stats_handler(e, is_callback=True)
        elif data == "view_referrals":
            await view_referrals(e)

        elif data == "dep_menu":
            await deposit_menu(e, edit=True)

        elif data.startswith("depm_"):
            await manual_deposit_init(e, data.replace("depm_", ""))

        elif data.startswith("adm_") and await is_admin(uid):
            await admin_actions(e)

        elif data.startswith("dkp|") and await has_perm(uid, 'p_bal'):
            _, dep_id, action = data.split("|")
            dep_id = int(dep_id)
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT user_id, method_name, status, amount FROM deposits WHERE id=$1", dep_id
                )
            if not row or row["status"] != 'pending':
                return await e.edit(f"{P_WARN} Already processed.")
            t_uid, method, orig_amt = row["user_id"], row["method_name"], row["amount"]
            curr = custom_dep_amt.get(dep_id, "0")
            if action.isdigit():
                curr = action if curr == "0" else curr + action
                if len(curr) > 7:
                    curr = curr[:7]
            elif action == "del":
                curr = curr[:-1] or "0"
            elif action == "cancel":
                btns = [
                    [Button.inline(f"✅ Accept (₹{orig_amt})", f"dep_acc|{dep_id}|{t_uid}|{method}|exact|{orig_amt}"),
                     Button.inline("❌ Reject", f"dep_rej|{dep_id}|{t_uid}")],
                    [Button.inline("📝 Custom Amount", f"dep_acc|{dep_id}|{t_uid}|{method}|custom|0")],
                ]
                return await e.edit(
                    f"🔔 <b>NEW DEPOSIT REQUEST</b>\n{P_ACC} User: <code>{t_uid}</code>\n"
                    f"{P_MONEY} Request: <b>{P_INR}{orig_amt}</b>\n{P_CARD} Method: {method}\n{P_ID} Ref: <code>{dep_id}</code>",
                    buttons=btns,
                )
            elif action == "conf":
                amt = int(curr)
                if amt <= 0:
                    return await e.answer("Amount must be > 0", alert=True)
                async with get_user_lock(t_uid):
                    async with pool.acquire() as conn:
                        prev = await conn.fetchrow("SELECT balance FROM users WHERE user_id=$1", t_uid)
                        prev_bal = prev["balance"] if prev else 0
                        await conn.execute(
                            "UPDATE users SET balance = balance + $1, total_deposited = total_deposited + $1 WHERE user_id=$2",
                            amt, t_uid,
                        )
                        await conn.execute(
                            "UPDATE deposits SET status='approved', amount=$1 WHERE id=$2", amt, dep_id
                        )
                await process_referral_bonus(t_uid, amt)
                _adm2 = await get_cached_entity(uid)
                _adm2_name = f"@{_adm2.username}" if getattr(_adm2, "username", None) else f"Admin {uid}"
                await e.edit(
                    f"{P_YES} <b>Payment Approved!</b>\n\n"
                    f"🆔 <b>User:</b> <code>{t_uid}</code>\n"
                    f"{P_MONEY} <b>Amount:</b> {P_INR}{amt} (Custom)\n"
                    f"👤 <b>Approved By:</b> {_adm2_name}"
                )
                await bot.send_message(
                    int(t_uid),
                    f"{P_YES} <b>Payment Approved!</b>\n\n"
                    f"🆔 <b>User ID:</b> <code>{t_uid}</code>\n"
                    f"{P_MONEY} <b>Amount Added:</b> {P_INR}{amt}\n"
                    f"📉 <b>Previous Balance:</b> {P_INR}{prev_bal}\n"
                    f"📈 <b>New Balance:</b> {P_INR}{prev_bal + amt}\n"
                    f"👤 <b>Approved By:</b> {_adm2_name}",
                )
                await log_primary_deposit(t_uid, amt, method)
                return
            custom_dep_amt[dep_id] = curr
            await e.edit(
                f"{P_KEY} <b>Enter Custom Amount for User {t_uid}:</b>\n\n{P_MONEY} {curr}",
                buttons=get_admin_custom_keypad(dep_id),
            )

        elif data.startswith("dep_acc|") and await has_perm(uid, 'p_bal'):
            p = data.split("|")
            dep_id, t_uid, method, a_type = p[1], int(p[2]), p[3], p[4]
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT status FROM deposits WHERE id=$1", int(dep_id))
            if not row or row["status"] != 'pending':
                return await e.edit(f"{P_WARN} Already processed.")
            if a_type == "exact":
                amt = int(p[5])
                async with get_user_lock(t_uid):
                    async with pool.acquire() as conn:
                        prev = await conn.fetchrow("SELECT balance FROM users WHERE user_id=$1", t_uid)
                        prev_bal = prev["balance"] if prev else 0
                        await conn.execute(
                            "UPDATE users SET balance = balance + $1, total_deposited = total_deposited + $1 WHERE user_id=$2",
                            amt, t_uid,
                        )
                        await conn.execute(
                            "UPDATE deposits SET status='approved', amount=$1 WHERE id=$2", amt, int(dep_id)
                        )
                await process_referral_bonus(t_uid, amt)
                _adm = await get_cached_entity(uid)
                _adm_name = f"@{_adm.username}" if getattr(_adm, "username", None) else f"Admin {uid}"
                user_msg = (
                    f"{P_YES} <b>Payment Approved!</b>\n\n"
                    f"🆔 <b>User ID:</b> <code>{t_uid}</code>\n"
                    f"{P_MONEY} <b>Amount Added:</b> {P_INR}{amt}\n"
                    f"📉 <b>Previous Balance:</b> {P_INR}{prev_bal}\n"
                    f"📈 <b>New Balance:</b> {P_INR}{prev_bal + amt}\n"
                    f"👤 <b>Approved By:</b> {_adm_name}"
                )
                await bot.send_message(int(t_uid), user_msg)
                await log_primary_deposit(t_uid, amt, method)
                try:
                    await e.edit(
                        f"{P_YES} <b>Payment Approved!</b>\n\n"
                        f"🆔 <b>User:</b> <code>{t_uid}</code>\n"
                        f"{P_MONEY} <b>Amount:</b> {P_INR}{amt}\n"
                        f"👤 <b>Approved By:</b> {_adm_name}"
                    )
                except MessageNotModifiedError:
                    pass
            elif a_type == "custom":
                custom_dep_amt[int(dep_id)] = "0"
                await e.edit(
                    f"{P_KEY} <b>Enter Custom Amount for User {t_uid}:</b>\n\n{P_MONEY} 0",
                    buttons=get_admin_custom_keypad(int(dep_id)),
                )

        elif data.startswith("dep_rej|") and await has_perm(uid, 'p_bal'):
            p = data.split("|")
            dep_id, t_uid = p[1], int(p[2])
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT status FROM deposits WHERE id=$1", int(dep_id))
            if not row or row["status"] != 'pending':
                return await e.edit(f"{P_WARN} Already processed.")
            admin_dep_state[uid] = {'target_uid': t_uid, 'dep_id': dep_id, 'step': 'wait_reason', 'msg_id': e.message_id, 'log_chat_id': e.chat_id}
            await bot.send_message(uid, f"{P_WARN} Reply to this message with the REASON for rejecting user <code>{t_uid}</code>:")
            try:
                await e.answer("Check your bot PMs to enter the reason.", alert=True)
            except Exception:
                pass

    except Exception as exc:
        logger.error(f"callback error: {exc}")


# =============================================================================
#  SESSION + DATABASE BACKUP
# =============================================================================
async def send_sessions_backup(target_uid: int = None):
    if target_uid is None:
        target_uid = ADMIN_ID
    zip_name = f"sessions_backup_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.zip"
    try:
        if not os.path.exists("sessions") or not os.listdir("sessions"):
            await bot.send_message(target_uid, "⚠️ <b>Backup:</b> sessions/ folder is empty.")
            return
        session_files = [f for f in os.listdir("sessions") if f.endswith(".session")]
        if not session_files:
            await bot.send_message(target_uid, "⚠️ <b>Backup:</b> No .session files found.")
            return
        with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zf:
            for fname in session_files:
                fpath = os.path.join("sessions", fname)
                if os.path.isfile(fpath):
                    zf.write(fpath, fname)
        size_kb = round(os.path.getsize(zip_name) / 1024, 1)
        caption = (
            f"📦 <b>Sessions Backup</b>\n\n"
            f"📅 Date: <code>{datetime.now().strftime('%Y-%m-%d %H:%M')}</code>\n"
            f"📁 Session Files: <code>{len(session_files)}</code>\n"
            f"💾 Size: <code>{size_kb} KB</code>\n\n"
            f"<i>Keep this safe — do not share!</i>"
        )
        await bot.send_file(target_uid, zip_name, caption=caption)
        logger.info(f"✅ Sessions backup sent to {target_uid} ({size_kb} KB)")
    except Exception as exc:
        logger.error(f"Backup error: {exc}")
        try:
            await bot.send_message(target_uid, f"❌ <b>Backup Failed!</b>\nError: {exc}")
        except Exception:
            pass
    finally:
        if os.path.exists(zip_name):
            os.remove(zip_name)


async def send_database_backup(target_uid: int = None):
    """Export all DB tables to CSV files, zip them, and send to owner."""
    if target_uid is None:
        target_uid = ADMIN_ID

    tables = ["users", "stock", "orders", "deposits", "upi_orders",
              "settings", "admins", "auto_prices", "custom_countries", "custom_payments"]

    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    zip_name = f"db_backup_{timestamp}.zip"
    tmp_dir = f"db_backup_tmp_{timestamp}"
    os.makedirs(tmp_dir, exist_ok=True)

    try:
        pool = get_pool()
        total_rows = 0
        exported_tables = []

        async with pool.acquire() as conn:
            for table in tables:
                try:
                    rows = await conn.fetch(f"SELECT * FROM {table}")
                    if not rows:
                        continue
                    csv_path = os.path.join(tmp_dir, f"{table}.csv")
                    with open(csv_path, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow(rows[0].keys())  # header
                        for row in rows:
                            writer.writerow(list(row.values()))
                    total_rows += len(rows)
                    exported_tables.append(f"  • {table}: {len(rows)} rows")
                    logger.info(f"✅ Exported table: {table} ({len(rows)} rows)")
                except Exception as exc:
                    logger.warning(f"⚠️ Skipped table {table}: {exc}")

        if not exported_tables:
            await bot.send_message(target_uid, "⚠️ <b>DB Backup:</b> No data found in any table.")
            return

        with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zf:
            for fname in os.listdir(tmp_dir):
                zf.write(os.path.join(tmp_dir, fname), fname)

        size_kb = round(os.path.getsize(zip_name) / 1024, 1)
        tables_summary = "\n".join(exported_tables)
        caption = (
            f"🗄 <b>Database Backup</b>\n\n"
            f"📅 Date: <code>{datetime.now().strftime('%Y-%m-%d %H:%M')}</code>\n"
            f"📊 Total Rows: <code>{total_rows}</code>\n"
            f"💾 Size: <code>{size_kb} KB</code>\n\n"
            f"<b>Tables:</b>\n{tables_summary}\n\n"
            f"<i>Keep this safe — do not share!</i>"
        )
        await bot.send_file(target_uid, zip_name, caption=caption)
        logger.info(f"✅ DB backup sent to {target_uid} ({size_kb} KB, {total_rows} rows)")

    except Exception as exc:
        logger.error(f"DB Backup error: {exc}")
        try:
            await bot.send_message(target_uid, f"❌ <b>DB Backup Failed!</b>\nError: {exc}")
        except Exception:
            pass
    finally:
        if os.path.exists(zip_name):
            os.remove(zip_name)
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir, ignore_errors=True)


async def daily_backup_loop():
    from datetime import timedelta
    while True:
        now = datetime.now()
        next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
        if now >= next_run:
            next_run = next_run + timedelta(days=1)
        wait_seconds = (next_run - now).total_seconds()
        logger.info(f"⏰ Next backup in {round(wait_seconds/3600, 1)} hours")
        await asyncio.sleep(wait_seconds)
        await send_sessions_backup()
        await send_database_backup()


# =============================================================================
#  ENTRY POINT
# =============================================================================
async def main():
    await init_db_pool()          # ← connect to MongoDB first
    await setup_db()              # ← create indexes
    await bot.start(bot_token=BOT_TOKEN)
    logger.info("✅ OTP Bot started successfully")
    register_device_handlers(bot, active_orders, delete_session_files)
    register_demo_handlers(bot, get_bot_me, LOG_CHANNEL_ID)
    # ─── Speed optimizations: warm up caches at startup ──────────────────────
    await get_bot_me()  # cache bot.get_me() once
    asyncio.create_task(_prewarm_images())  # pre-upload banner images to Telegram
    # ─────────────────────────────────────────────────────────────────────────
    asyncio.create_task(daily_backup_loop())
    asyncio.create_task(keepalive_loop())
    asyncio.create_task(autopay_global_checker())
    await bot.run_until_disconnected()


async def _prewarm_images():
    """Upload all banner images to Telegram once at startup so file_id is cached.
    Uses any custom VPS-uploaded banner if the admin set one, else the config.py default."""
    from config import START_IMAGE, DEPOSIT_IMAGE, PROFILE_IMAGE, STATS_IMAGE, SUPPORT_IMAGE, BUY_IMAGE
    defaults = {
        "start": START_IMAGE, "deposit": DEPOSIT_IMAGE, "profile": PROFILE_IMAGE,
        "stats": STATS_IMAGE, "support": SUPPORT_IMAGE, "buy": BUY_IMAGE,
    }
    for key, default_url in defaults.items():
        url = await get_banner_image(key, default_url)
        await get_cached_image(url)
    logger.info("✅ Image cache warmed up — all banners pre-uploaded.")


if __name__ == '__main__':
    try:
        import uvloop
        uvloop.run(main())
    except ImportError:
        asyncio.run(main())
