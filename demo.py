"""
demo.py - Interactive Demo Mode for Admin Panel
Shows a simulated purchase flow (country → year → confirm → OTP → devices → finish)
without touching any real DB, sessions, or users.
Demo purchase log is sent to the log channel with a 🎬 DEMO tag.
"""

import html
import logging
from datetime import datetime

from telethon import Button, events

logger = logging.getLogger(__name__)

# ─── Fake Data ────────────────────────────────────────────────────────────────

DEMO_PHONE       = "+15551234567"
DEMO_PHONE_MASKED = "+1555•••••67"
DEMO_OTP         = "48291"
DEMO_OTP_MASKED  = "4***1"
DEMO_COUNTRY     = "USA 🇺🇸"
DEMO_YEAR        = "2024"
DEMO_PRICE       = 40
DEMO_2FA         = "demo1234"
DEMO_BALANCE     = 500
DEMO_USER_ID     = "123456789"

DEMO_DEVICES = [
    {"name": "iPhone 14",    "hash": 1001},
    {"name": "Windows PC",   "hash": 1002},
    {"name": "Chrome Web",   "hash": 1003},
]

# ─── Step renderers ───────────────────────────────────────────────────────────

async def _step_countries(event):
    msg = (
        "🎬 <b>DEMO MODE — Step 1/8</b>\n\n"
        "🛒 <b>Select Country</b>\n\n"
        "User sees a list of available countries with price and stock.\n"
        "Each button shows: <code>🇺🇸 USA - ₹40 ($0.48)</code>"
    )
    btns = [
        [Button.inline("🇺🇸 USA - ₹40 ($0.48)",   "demo_step_2"),
         Button.inline("🇬🇧 UK  - ₹60 ($0.72)",   "demo_noop")],
        [Button.inline("🇮🇪 Ireland - ₹55 ($0.66)", "demo_noop"),
         Button.inline("🇨🇦 Canada - ₹50 ($0.60)", "demo_noop")],
        [Button.inline("▶️ Next Step →", "demo_step_2"),
         Button.inline("⏹ Exit Demo",   "demo_exit")],
    ]
    await event.edit(msg, buttons=btns)


async def _step_years(event):
    msg = (
        "🎬 <b>DEMO MODE — Step 2/8</b>\n\n"
        f"📅 <b>Select Account Year</b>\n"
        f"🌎 Country: <b>{DEMO_COUNTRY}</b>\n\n"
        "User sees available years with quantity and price.\n"
        "Discount (if any) is applied automatically here."
    )
    btns = [
        [Button.inline("🇺🇸 USA 2024 - ₹40 ($0.48) | 12 left", "demo_step_3")],
        [Button.inline("🇺🇸 USA 2023 - ₹35 ($0.42) | 5 left",  "demo_noop")],
        [Button.inline("▶️ Next Step →", "demo_step_3"),
         Button.inline("⏹ Exit Demo",   "demo_exit")],
    ]
    await event.edit(msg, buttons=btns)


async def _step_confirm(event):
    msg = (
        "🎬 <b>DEMO MODE — Step 3/8</b>\n\n"
        f"🛒 <b>Confirm Your Purchase</b>\n\n"
        f"🏳️ <b>Country:</b> {DEMO_COUNTRY}\n"
        f"📅 <b>Year:</b> {DEMO_YEAR}\n"
        f"💰 <b>Final Price:</b> ₹{DEMO_PRICE}\n"
        f"💳 <b>Your Balance:</b> ₹{DEMO_BALANCE}\n\n"
        "<blockquote>⚠️ <b>Please Read Before Buying</b>\n"
        "• Our service is to provide the OTP only.\n"
        "• We are not responsible if the account gets banned.\n"
        "• No refunds once OTP is delivered.\n"
        "• By clicking Yes, Buy Now you agree to our Terms.</blockquote>\n\n"
        "❓ Do you want to proceed?"
    )
    btns = [
        [Button.inline("✅ Yes, Buy Now", "demo_step_4")],
        [Button.inline("❌ No, Cancel",   "demo_noop")],
        [Button.inline("▶️ Next Step →",  "demo_step_4"),
         Button.inline("⏹ Exit Demo",    "demo_exit")],
    ]
    await event.edit(msg, buttons=btns)


async def _step_listening(event):
    msg = (
        "🎬 <b>DEMO MODE — Step 4/8</b>\n\n"
        f"✅ <b>Order Active!</b>\n\n"
        f"📱 <b>Phone:</b> <code>{DEMO_PHONE}</code>\n"
        f"🌎 <b>Country:</b> {DEMO_COUNTRY}\n\n"
        "<blockquote>🔻 INSTRUCTIONS:\n"
        "1. Open Telegram &amp; Add Account\n"
        "2. Enter the number above.\n"
        "⏳ Please wait! The bot is actively listening for your OTP.</blockquote>\n\n"
        "<i>⚠️ Cancel only available after 2 minutes. If no OTP in 10 minutes, auto-refund happens.</i>\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🤖 <i>Bot is polling Telegram every 2 seconds for the OTP message from 777000...</i>"
    )
    btns = [
        [Button.inline("❌ Cancel Order", "demo_noop")],
        [Button.inline("▶️ Next Step (OTP Arrives) →", "demo_step_5"),
         Button.inline("⏹ Exit Demo",                  "demo_exit")],
    ]
    await event.edit(msg, buttons=btns)


async def _step_otp(event):
    msg = (
        "🎬 <b>DEMO MODE — Step 5/8</b>\n\n"
        f"✅ <b>Latest OTP Fetched!</b>\n\n"
        f"📱 <b>Phone:</b> <code>{DEMO_PHONE}</code>\n"
        f"🌎 <b>Country:</b> {DEMO_COUNTRY}\n"
        f"🔢 <b>OTP:</b> <code>{DEMO_OTP}</code>\n"
        f"🔐 <b>2FA:</b> <code>{DEMO_2FA}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "ℹ️ <i>Full details shown to user. Masked only in log channel.</i>"
    )
    btns = [
        [Button.inline("🔄 Get OTP Again",   "demo_noop")],
        [Button.inline("📱 Manage Devices",  "demo_step_6")],
        [Button.inline("🚪 Finish & Logout", "demo_step_8")],
        [Button.inline("▶️ Next Step (Manage Devices) →", "demo_step_6"),
         Button.inline("⏹ Exit Demo",                      "demo_exit")],
    ]
    await event.edit(msg, buttons=btns)


async def _step_devices(event, removed=None):
    if removed is None:
        removed = []
    active = [d for d in DEMO_DEVICES if d["hash"] not in removed]

    lines = ["🎬 <b>DEMO MODE — Step 6/8</b>\n\n📱 <b>Active Sessions</b>\n"]
    btns = []
    for d in active:
        tag = " (current — bot session)" if d["hash"] == 1001 else ""
        lines.append(f"• {d['name']}{tag}")

    for d in active:
        if d["hash"] != 1001:  # don't show logout for current/bot session
            btns.append([Button.inline(
                f"🚫 Logout: {d['name']}",
                f"demo_dev_logout|{d['hash']}|{'|'.join(str(r) for r in removed)}"
            )])

    btns.append([Button.inline("🚫 Logout ALL Other Sessions", f"demo_dev_logout_all")])
    btns.append([Button.inline("🔄 Refresh",                   "demo_step_6")])
    btns.append([Button.inline("« Back",                       "demo_step_5")])
    btns.append([Button.inline("▶️ Next Step (Finish) →",      "demo_step_8"),
                 Button.inline("⏹ Exit Demo",                   "demo_exit")])

    msg = "\n".join(lines)
    try:
        await event.edit(msg, buttons=btns)
    except Exception:
        await event.answer("Refreshed!", alert=False)


async def _step_devices_after_logout(event, removed):
    active = [d for d in DEMO_DEVICES if d["hash"] not in removed]
    lines = ["🎬 <b>DEMO MODE — Step 7/8</b>\n\n📱 <b>Active Sessions (Updated)</b>\n"]
    btns = []
    for d in active:
        tag = " (current — bot session)" if d["hash"] == 1001 else ""
        lines.append(f"• {d['name']}{tag}")

    for d in active:
        if d["hash"] != 1001:
            btns.append([Button.inline(
                f"🚫 Logout: {d['name']}",
                f"demo_dev_logout|{d['hash']}|{'|'.join(str(r) for r in removed)}"
            )])

    if not btns:
        lines.append("\n✅ <i>All other devices removed!</i>")

    btns.append([Button.inline("▶️ Next Step (Finish) →", "demo_step_8"),
                 Button.inline("⏹ Exit Demo",              "demo_exit")])
    msg = "\n".join(lines)
    await event.edit(msg, buttons=btns)


async def _step_finish(event, bot, get_bot_me_fn, log_channel_id):
    """Send demo log to log channel and show finish screen."""
    t = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    me = await get_bot_me_fn()
    bot_username = f"@{me.username}" if me and me.username else "@yourbot"

    # Send demo log to log channel
    try:
        log_msg = (
            f"🎬 <b>DEMO MODE</b>\n\n"
            f"<blockquote>🛒 <b>New Number Purchase Successful</b></blockquote>\n\n"
            f"— <b>Country:</b> {DEMO_COUNTRY}\n"
            f"— <b>Year:</b> {DEMO_YEAR}\n"
            f"— <b>Quantity:</b> 1\n\n"
            f"+ <b>Number:</b> <code>{DEMO_PHONE_MASKED}</code>\n"
            f"+ <b>OTP:</b> <code>{DEMO_OTP_MASKED}</code>\n"
            f"+ <b>2FA:</b> <code>{DEMO_2FA}</code>\n"
            f"+ <b>Price:</b> ₹{DEMO_PRICE}\n"
            f"+ <b>Total Paid:</b> ₹{DEMO_PRICE}\n"
            f"+ <b>Time:</b> {t}\n\n"
            f"🆔 <b>User ID:</b> <code>{DEMO_USER_ID}</code>\n\n"
            f"✦ {bot_username} || {bot_username} ✦"
        )
        await bot.send_message(log_channel_id, log_msg)
        log_sent = True
    except Exception as exc:
        logger.error(f"Demo log send error: {exc}")
        log_sent = False

    msg = (
        "🎬 <b>DEMO MODE — Step 8/8</b>\n\n"
        "✅ <b>Session Finished successfully.</b>\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📋 <b>What happened in this demo:</b>\n"
        "1️⃣ User browsed countries &amp; selected USA\n"
        "2️⃣ Selected year 2024, confirmed purchase\n"
        "3️⃣ Bot deducted ₹40 from balance\n"
        "4️⃣ Bot loaded .session file &amp; listened for OTP\n"
        "5️⃣ OTP arrived → shown to user (full), logged (masked)\n"
        "6️⃣ User managed &amp; removed other devices\n"
        "7️⃣ User clicked Finish → bot disconnected its session only\n"
        f"{'8️⃣ ✅ Demo log sent to log channel' if log_sent else '8️⃣ ⚠️ Demo log failed to send'}\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "<i>This was a fully simulated demo. No real data was touched.</i>"
    )
    btns = [[Button.inline("🔁 Restart Demo", "demo_step_1"),
             Button.inline("⏹ Exit Demo",     "demo_exit")]]
    await event.edit(msg, buttons=btns)


# ─── Register handlers ────────────────────────────────────────────────────────

def register_demo_handlers(bot, get_bot_me_fn, log_channel_id):
    """
    Register all demo callback handlers on the bot.
    Call this once in main() after bot.start().
    """

    @bot.on(events.CallbackQuery(pattern=rb"^demo_step_1$"))
    async def cb_demo_step_1(e):
        await e.answer()
        await _step_countries(e)

    @bot.on(events.CallbackQuery(pattern=rb"^demo_step_2$"))
    async def cb_demo_step_2(e):
        await e.answer()
        await _step_years(e)

    @bot.on(events.CallbackQuery(pattern=rb"^demo_step_3$"))
    async def cb_demo_step_3(e):
        await e.answer()
        await _step_confirm(e)

    @bot.on(events.CallbackQuery(pattern=rb"^demo_step_4$"))
    async def cb_demo_step_4(e):
        await e.answer()
        await _step_listening(e)

    @bot.on(events.CallbackQuery(pattern=rb"^demo_step_5$"))
    async def cb_demo_step_5(e):
        await e.answer()
        await _step_otp(e)

    @bot.on(events.CallbackQuery(pattern=rb"^demo_step_6$"))
    async def cb_demo_step_6(e):
        await e.answer()
        await _step_devices(e)

    @bot.on(events.CallbackQuery(pattern=rb"^demo_dev_logout\|(\d+)\|?(.*)$"))
    async def cb_demo_dev_logout(e):
        await e.answer("✅ Device logged out! (Demo)", alert=False)
        parts = e.data.decode().split("|")
        removed_hash = int(parts[1])
        existing = [int(x) for x in parts[2].split("|") if x.strip().isdigit()]
        existing.append(removed_hash)
        await _step_devices_after_logout(e, existing)

    @bot.on(events.CallbackQuery(pattern=rb"^demo_dev_logout_all$"))
    async def cb_demo_dev_logout_all(e):
        await e.answer("✅ All other sessions removed! (Demo)", alert=True)
        # Remove all except current (hash 1001)
        removed = [d["hash"] for d in DEMO_DEVICES if d["hash"] != 1001]
        await _step_devices_after_logout(e, removed)

    @bot.on(events.CallbackQuery(pattern=rb"^demo_step_8$"))
    async def cb_demo_step_8(e):
        await e.answer()
        await _step_finish(e, bot, get_bot_me_fn, log_channel_id)

    @bot.on(events.CallbackQuery(pattern=rb"^demo_noop$"))
    async def cb_demo_noop(e):
        await e.answer("🎬 This is a demo button — tap ▶️ Next Step to continue.", alert=False)

    @bot.on(events.CallbackQuery(pattern=rb"^demo_exit$"))
    async def cb_demo_exit(e):
        await e.answer("Demo exited.", alert=False)
        await e.edit("⏹ <b>Demo Mode exited.</b>\n\nUse /admin to return to the admin panel.")

    logger.info("✅ Demo handlers registered.")
