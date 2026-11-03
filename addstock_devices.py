"""
addstock_devices.py
───────────────────
Two self-contained feature modules that plug into bot.py:

  1. run_addstock_flow(conv, uid, bot, db, cur)
       • Fixes the "connection already closed" bug (client.disconnect was
         called BEFORE the year/price get_reply prompts).
       • After a successful add the admin gets two inline buttons:
           ➕ Add Another Number   /   ✅ Done

  2. Callback handlers (register with bot via register_device_handlers)
       • manage_devices|<phone>  – lists all active sessions on the account
       • dev_logout|<phone>|<hash> – terminates one specific session
       • dev_logout_all|<phone>   – terminates ALL other sessions
       • dev_logout_bot|<phone>   – logs out the bot session fully

These handlers are meant to appear on the post-OTP purchase message
alongside the existing "Finish & Logout" button.

─── Integration into bot.py ───────────────────────────────────────────────────
1.  Add this at the top of bot.py:
        from addstock_devices import run_addstock_flow, register_device_handlers

2.  Inside admin_actions(), replace the `elif action_data == "addstock"` block:
        elif action_data == "addstock" and has_perm(uid, 'p_add_stock'):
            await run_addstock_flow(conv, uid, bot, db, cur)

3.  After the bot object is created (but before bot.run_until_disconnected):
        register_device_handlers(bot)

4.  In the post-OTP buttons (auto_otp_task), add the new button:
        btns = [
            [Button.inline("🔄 Get OTP Again",   f"get_otp_again|{phone}")],
            [Button.inline("📱 Manage Devices",  f"manage_devices|{phone}")],
            [Button.inline("🚪 Finish & Logout", f"logout_bot|{phone}")],
        ]
───────────────────────────────────────────────────────────────────────────────
"""

import asyncio
import html
import logging
import os

from telethon import TelegramClient, Button, events
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.account import (
    GetAuthorizationsRequest,
    ResetAuthorizationRequest,
)

# These will be set by register_device_handlers
from config import API_ID, API_HASH, P_YES, P_NO, P_WARN, P_PHONE, P_OTP
from config import P_2FA, P_FLAG, P_GLOBE, P_CAL, P_MONEY, P_INR
from helpers import get_country_info
from database import get_pool

logger = logging.getLogger(__name__)

# Tracks phones where the user clicked "logout bot session" inside Manage Devices.
# The bot session isn't actually terminated yet — it stays alive until the user
# clicks "Finish & Logout" (logout_bot|<phone>) in bot.py.
# Format: { phone: True }
bot_session_pending_logout: dict = {}


# ─────────────────────────────────────────────────────────────────────────────
#  1.  ADDSTOCK FLOW  (bug-fixed + "add another" loop)
# ─────────────────────────────────────────────────────────────────────────────

async def run_addstock_flow(conv, uid: int, bot, db=None, cur=None):
    """
    Full add-single-account-via-phone+OTP flow.
    Runs inside an existing bot.conversation(...) block.
    Loops until the admin chooses "Done".
    """

    async def get_reply(txt: str):
        """Send a prompt and wait for admin reply. /cancel aborts."""
        await conv.send_message(txt + "\n\n<i>(Type /cancel to abort)</i>")
        resp = await conv.get_response()
        if resp.text.strip().lower() == "/cancel":
            raise ValueError("Cancelled")
        return resp

    async def select_account_year():
        import datetime
        current_year = datetime.datetime.now().year
        year_options = list(range(current_year, current_year - 6, -1))
        year_buttons = []
        row = []
        for i, yr in enumerate(year_options):
            row.append(Button.inline(str(yr), f"_yearpick_{uid}_{yr}"))
            if len(row) == 3 or i == len(year_options) - 1:
                year_buttons.append(row)
                row = []
        year_buttons.append([Button.inline("✏️ Other Year", f"_yearpick_{uid}_custom")])

        year_msg = await conv.send_message(
            f"{P_CAL} <b>Select Account Year:</b>",
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

    while True:  # loop so admin can add multiple numbers in one session
        # ── Step 1: phone number ─────────────────────────────────────────────
        phone = (
            (await get_reply(f"{P_PHONE} <b>Enter Phone Number (+919999...):</b>"))
            .text.replace(" ", "").replace("+", "")
        )

        sp = f"sessions/{phone}"

        # ── Step 1b: duplicate check ─────────────────────────────────────────
        pool = get_pool()
        async with pool.acquire() as conn:
            existing = await conn.fetchrow("SELECT 1 FROM stock WHERE phone=$1", phone)
        if existing:
            await conv.send_message(f"{P_NO} <b>+{phone}</b> already exists in stock. Process cancelled.")
            break

        os.makedirs("sessions", exist_ok=True)

        # Remove any stale leftover session files from a previous failed/cancelled
        # attempt for this number (since it's not in stock, any session here is junk).
        for ext in (".session", ".session-wal", ".session-shm", ".session-journal"):
            try:
                if os.path.exists(sp + ext):
                    os.remove(sp + ext)
            except Exception:
                pass

        client = TelegramClient(sp, API_ID, API_HASH, connection_retries=5, retry_delay=2, timeout=15)
        try:
            await client.connect()
        except Exception as exc:
            await conv.send_message(f"{P_NO} Failed to connect to Telegram: <code>{html.escape(str(exc))}</code>")
            break

        if not client.is_connected():
            await conv.send_message(f"{P_NO} Could not establish connection to Telegram. Please try again.")
            break

        # ── Step 2: send OTP request ─────────────────────────────────────────
        try:
            sreq = await client.send_code_request(phone)
        except Exception as exc:
            await client.disconnect()
            await conv.send_message(f"{P_NO} Failed to send OTP: <code>{html.escape(str(exc))}</code>")
            break

        # ── Step 3: sign in (handle 2FA) ─────────────────────────────────────
        twofa_pass = "None"
        try:
            if await client.is_user_authorized():
                # Session is already logged in (can happen with a reused/old
                # session file or if a previous attempt partially succeeded).
                # No need to send/verify OTP again.
                pass
            else:
                otp_text = (await get_reply(f"{P_OTP} <b>Enter OTP sent to +{phone}:</b>")).text.strip()
                try:
                    await client.sign_in(phone, otp_text, phone_code_hash=sreq.phone_code_hash)
                except SessionPasswordNeededError:
                    twofa_pass = html.escape(
                        (await get_reply(f"{P_2FA} <b>2FA required. Enter password:</b>")).text
                    )
                    await client.sign_in(password=twofa_pass)
        except Exception as exc:
            # Telethon can sometimes raise a spurious error (e.g. when the
            # session was already authorized mid-flow) even though sign-in
            # actually succeeded. Double-check before treating it as fatal.
            try:
                already_ok = await client.is_user_authorized()
            except Exception:
                already_ok = False
            if not already_ok:
                await client.disconnect()
                await conv.send_message(f"{P_NO} Sign-in failed: <code>{html.escape(str(exc))}</code>")
                break

        # ── Step 4: country detection ─────────────────────────────────────────
        c_name, c_icon = await get_country_info(phone)
        if c_name == "Unknown":
            await conv.send_message(f"{P_WARN} Country not recognized for +{phone}!")
            c_icon = html.escape(
                (await get_reply(f"{P_FLAG} <b>Enter Country Flag Emoji:</b>")).text.strip()
            )
            c_name = html.escape(
                (await get_reply(f"{P_GLOBE} <b>Enter Country Name:</b>")).text.strip()
            )

        # ── Step 5: year ──────────────────────────────────────────────────────
        # BUG FIX: client.disconnect() was called here (before year/price prompts).
        # It is now moved to AFTER all prompts are collected.
        try:
            year = await select_account_year()
        except asyncio.TimeoutError:
            await client.disconnect()
            await conv.send_message(f"{P_NO} Year selection timed out. Account not saved.")
            break
        except ValueError:
            await client.disconnect()
            await conv.send_message(f"{P_NO} Invalid year. Account not saved.")
            break
            break

        # ── Step 6: price (auto-detect or manual) ────────────────────────────
        pool = get_pool()
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
                await conv.send_message(f"⚡ Auto-Price applied: {P_INR}{price} for {c_name} ({year})")
            else:
                existing_price = await conn.fetchrow(
                    "SELECT price FROM stock WHERE country_name=$1 AND account_year=$2 LIMIT 1", c_name, int(year)
                )
                if existing_price:
                    price = existing_price["price"]
                    await conv.send_message(f"⚡ Auto-detected existing price: {P_INR}{price} for {c_name} ({year})")
                else:
                    price_raw = (await get_reply(f"{P_MONEY} <b>Enter Price (₹):</b>")).text.strip()
                    try:
                        price = int(price_raw)
                    except ValueError:
                        await client.disconnect()
                        await conv.send_message(f"{P_NO} Invalid price. Account not saved.")
                        break

        # ── Step 7: disconnect AFTER all prompts ──────────────────────────────
        # This is the fix — previously disconnect() was called before year/price
        # prompts, which closed the Telegram connection and caused
        # "Error: connection already closed" on the very next get_reply().
        try:
            await client.disconnect()
        except Exception:
            pass

        # ── Step 8: save to DB ────────────────────────────────────────────────
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO stock "
                "(phone, session_file, country_name, country_icon, account_year, category, price, available, twofa) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (phone) DO UPDATE SET "
                "session_file=$2, country_name=$3, country_icon=$4, account_year=$5, category=$6, price=$7, available=$8, twofa=$9",
                phone, sp + ".session", c_name, c_icon, year, "Good", price, 1, twofa_pass,
            )

        # ── Step 9: success message + "Add Another / Done" buttons ───────────
        success_msg = await conv.send_message(
            f"{P_YES} <b>Account Added Successfully!</b>\n\n"
            f"{P_PHONE} <b>Phone:</b> <code>+{phone}</code>\n"
            f"{P_FLAG} <b>Country:</b> {c_icon} {c_name}\n"
            f"{P_CAL} <b>Year:</b> {year}\n"
            f"{P_MONEY} <b>Price:</b> {P_INR}{price}\n"
            f"{P_2FA} <b>2FA:</b> {'Set ✅' if twofa_pass != 'None' else 'None'}\n\n"
            f"What would you like to do next?",
            buttons=[
                [Button.inline("➕ Add Another Number", b"addstock_again")],
                [Button.inline("✅ Done",               b"addstock_done")],
            ],
        )

        # Wait for the admin to click a button (timeout 60 s)
        try:
            click = await conv.wait_event(
                events.CallbackQuery(
                    chats=conv.chat_id,
                    func=lambda e: e.sender_id == uid and e.data in (b"addstock_again", b"addstock_done"),
                ),
                timeout=60,
            )
            await click.answer()
            if click.data == b"addstock_done":
                await conv.send_message("✅ All done! Returning to admin panel.")
                break
            # else: loop continues → add another number
            await conv.send_message("🔄 <b>Starting next number...</b>")
        except Exception:
            # Timeout or any error → just exit the loop silently
            break


# ─────────────────────────────────────────────────────────────────────────────
#  2.  MANAGE DEVICES  (list + per-session logout)
# ─────────────────────────────────────────────────────────────────────────────

def _device_label(auth) -> str:
    """Build a readable one-line label for a Telegram authorization object."""
    parts = []
    if getattr(auth, "app_name", None):
        parts.append(auth.app_name)
    if getattr(auth, "device_model", None):
        parts.append(auth.device_model)
    if getattr(auth, "platform", None):
        parts.append(auth.platform)
    label = " · ".join(p for p in parts if p) or "Unknown device"
    if getattr(auth, "country", None):
        label += f"  ({auth.country})"
    if getattr(auth, "current", False):
        label += "  ← THIS SESSION"
    return label


async def _get_client_from_orders(phone: str, active_orders: dict):
    """Return the TelegramClient for an active order, or None."""
    order = active_orders.get(phone)
    if order and order.get("client"):
        return order["client"]
    return None


def register_device_handlers(bot, active_orders: dict, delete_session_files_fn):
    """
    Register all device-management callback handlers on the bot.
    Call this once after the bot object is created, e.g.:

        from addstock_devices import register_device_handlers
        register_device_handlers(bot, active_orders, delete_session_files)
    """

    # ── List devices ─────────────────────────────────────────────────────────
    @bot.on(events.CallbackQuery(pattern=rb"^manage_devices\|(.+)$"))
    async def cb_manage_devices(e):
        phone = e.data.decode().split("|", 1)[1]
        client = await _get_client_from_orders(phone, active_orders)
        if client is None:
            return await e.answer("⚠️ Session no longer active.", alert=True)

        await e.answer("🔍 Fetching devices...")
        try:
            result = await client(GetAuthorizationsRequest())
            auths = result.authorizations
        except Exception as exc:
            return await e.answer(f"❌ Error: {exc}", alert=True)

        if not auths:
            return await e.answer("No active sessions found.", alert=True)

        # Check if user already "fake-logged-out" the bot session in this view
        bot_pending = bot_session_pending_logout.get(phone, False)

        lines = ["📱 <b>Active Sessions</b>\n"]
        btns = []
        for auth in auths:
            label = _device_label(auth)
            h = getattr(auth, "hash", 0)
            is_current = getattr(auth, "current", False)

            if is_current:
                # ── Bot's own session ──────────────────────────────────────
                if bot_pending:
                    # Already marked for logout — show as "logged out" (greyed label)
                    lines.append(f"• 🤖 {html.escape(label)} — <i>Logged out ✅</i>")
                    # No button; logout will happen on Finish & Logout
                else:
                    lines.append(f"• 🤖 {html.escape(label)}")
                    btns.append(
                        [Button.inline(
                            "🤖 Logout Bot Session",
                            f"dev_logout_bot_session|{phone}".encode(),
                        )]
                    )
            else:
                # ── Other sessions ─────────────────────────────────────────
                lines.append(f"• {html.escape(label)}")
                btns.append(
                    [Button.inline(
                        f"🚫 Logout: {html.escape(auth.app_name or 'Device')[:25]}",
                        f"dev_logout|{phone}|{h}".encode(),
                    )]
                )

        btns.append([Button.inline("🚫 Logout ALL Other Sessions", f"dev_logout_all|{phone}".encode())])
        btns.append([Button.inline("🔄 Refresh",                   f"manage_devices|{phone}".encode())])
        btns.append([Button.inline("« Back",                       f"get_otp_again|{phone}".encode())])

        if bot_pending:
            lines.append(
                "\n⚠️ <i>Bot session is marked for logout.\n"
                "Click <b>Finish &amp; Logout</b> to complete the logout and end the session.</i>"
            )

        msg = "\n".join(lines)
        try:
            await e.edit(msg, buttons=btns)
        except Exception:
            await bot.send_message(e.sender_id, msg, buttons=btns)

    # ── "Logout Bot Session" inside Manage Devices (fake logout — deferred) ──
    @bot.on(events.CallbackQuery(pattern=rb"^dev_logout_bot_session\|(.+)$"))
    async def cb_dev_logout_bot_session(e):
        """
        The user clicked "Logout Bot Session" inside Manage Devices.

        We do NOT disconnect or delete the session here.  Instead we mark it
        as pending so the device list shows it as "Logged out ✅".  The actual
        disconnect + session-file deletion happens only when the user goes back
        and clicks Finish & Logout (logout_bot|<phone> handler in bot.py).
        """
        phone = e.data.decode().split("|", 1)[1]
        client = await _get_client_from_orders(phone, active_orders)
        if client is None:
            return await e.answer("⚠️ Session no longer active.", alert=True)

        # Mark as pending — real logout deferred to Finish & Logout
        bot_session_pending_logout[phone] = True

        await e.answer(
            "✅ Bot session marked as logged out.\n"
            "Click 'Finish & Logout' to fully end the session.",
            alert=True,
        )

        # Refresh the device list to show updated state
        await cb_manage_devices(
            type("FakeEvent", (), {
                "data": f"manage_devices|{phone}".encode(),
                "answer": e.answer,
                "edit": e.edit,
                "sender_id": e.sender_id,
            })()
        )

    # ── Logout one session ───────────────────────────────────────────────────
    @bot.on(events.CallbackQuery(pattern=rb"^dev_logout\|([^|]+)\|(\d+)$"))
    async def cb_dev_logout_one(e):
        _, phone, hash_str = e.data.decode().split("|")
        h = int(hash_str)
        client = await _get_client_from_orders(phone, active_orders)
        if client is None:
            return await e.answer("⚠️ Session no longer active.", alert=True)
        try:
            await client(ResetAuthorizationRequest(hash=h))
            await e.answer("✅ Device logged out!", alert=False)
        except Exception as exc:
            return await e.answer(f"❌ {exc}", alert=True)
        # Refresh device list
        await cb_manage_devices(
            type("FakeEvent", (), {
                "data": f"manage_devices|{phone}".encode(),
                "answer": e.answer,
                "edit": e.edit,
                "sender_id": e.sender_id,
            })()
        )

    # ── Logout all other sessions ─────────────────────────────────────────────
    @bot.on(events.CallbackQuery(pattern=rb"^dev_logout_all\|(.+)$"))
    async def cb_dev_logout_all(e):
        phone = e.data.decode().split("|", 1)[1]
        client = await _get_client_from_orders(phone, active_orders)
        if client is None:
            return await e.answer("⚠️ Session no longer active.", alert=True)
        try:
            result = await client(GetAuthorizationsRequest())
            auths = result.authorizations
            count = 0
            for auth in auths:
                if not getattr(auth, "current", False):
                    h = getattr(auth, "hash", 0)
                    if h:
                        try:
                            await client(ResetAuthorizationRequest(hash=h))
                            count += 1
                        except Exception:
                            pass
            await e.answer(f"✅ Logged out {count} other session(s)!", alert=True)
        except Exception as exc:
            return await e.answer(f"❌ {exc}", alert=True)
        # Refresh
        await cb_manage_devices(
            type("FakeEvent", (), {
                "data": f"manage_devices|{phone}".encode(),
                "answer": e.answer,
                "edit": e.edit,
                "sender_id": e.sender_id,
            })()
        )
