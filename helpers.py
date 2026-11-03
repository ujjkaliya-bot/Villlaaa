"""
helpers.py - Utility functions: country lookup, session deletion, fsub check, admin checks
"""
import os
import logging

from telethon.errors import UserNotParticipantError
from telethon.tl.functions.channels import GetParticipantRequest

from config import CHECK_CHANNELS, ADMIN_ID
from database import get_pool, is_user_banned

logger = logging.getLogger(__name__)

# ─── Admin / Permission Helpers ──────────────────────────────────────────────
async def is_admin(uid: int) -> bool:
    if uid == ADMIN_ID:
        return True
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow("SELECT user_id FROM admins WHERE user_id=$1", uid)
    return bool(row)


async def has_perm(uid: int, perm: str) -> bool:
    if uid == ADMIN_ID:
        return True
    allowed = {"p_add_stock", "p_manage_stock", "p_stats", "p_bal", "p_settings"}
    if perm not in allowed:
        return False
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(f"SELECT {perm} FROM admins WHERE user_id=$1", uid)
    return bool(row and row[perm] == 1)


# ─── Masking helpers (for the public/"masked" log channel) ───────────────────
def mask_middle(value, keep_start: int, keep_end: int, mask_char: str = "*") -> str:
    """Hide the middle of `value`, keeping `keep_start` chars in front and
    `keep_end` at the back. Short values still get at least one char hidden."""
    s = str(value if value is not None else "").strip()
    if not s:
        return "N/A"
    n = len(s)
    if n <= keep_start + keep_end:
        keep_start = keep_end = max(0, n // 3)
        if n - keep_start - keep_end < 1:
            return mask_char * n
    return s[:keep_start] + mask_char * (n - keep_start - keep_end) + s[n - keep_end:]


def mask_phone(phone) -> str:
    """+919876543210 -> +9198******10  (country code + last 2 digits visible)."""
    if not phone:
        return "N/A"
    p = str(phone).strip().replace(" ", "")
    plus = "+" if p.startswith("+") or p[:1].isdigit() else ""
    p = p.lstrip("+")
    return plus + mask_middle(p, 4, 2)


def mask_id(uid) -> str:
    """8827362819234 -> 88273***19234 style: only the middle few digits hidden."""
    s = str(uid if uid is not None else "").strip()
    if not s:
        return "N/A"
    hide = max(3, len(s) // 4)
    start = max(0, (len(s) - hide) // 2)
    end = max(0, len(s) - hide - start)
    return mask_middle(s, start, end)


def mask_otp(otp) -> str:
    """483920 -> 4****0"""
    if not otp:
        return "N/A"
    o = str(otp).strip()
    if len(o) <= 2:
        return "*" * len(o)
    return o[0] + "*" * (len(o) - 2) + o[-1]


def mask_username(username) -> str:
    if not username:
        return "NoUsername"
    u = str(username)
    return u[:2] + "***" if len(u) > 2 else "***"


# ─── Force-Subscribe Check ───────────────────────────────────────────────────
async def check_channel_joined(bot, uid: int) -> bool:
    if await is_admin(uid):
        return True
    if not CHECK_CHANNELS:
        return True
    for ch in CHECK_CHANNELS:
        try:
            entity = await bot.get_entity(int(ch))
            await bot(GetParticipantRequest(channel=entity, participant=uid))
        except UserNotParticipantError:
            return False
        except ValueError:
            logger.warning(f"check_channel_joined: Could not resolve channel {ch}.")
            return False
        except Exception as exc:
            logger.error(f"check_channel_joined error for channel {ch}: {exc}")
            return False
    return True


# ─── Country Detection ───────────────────────────────────────────────────────
COUNTRY_CODES = {
    '1': ('USA/Canada', '🇺🇸'), '7': ('Russia', '🇷🇺'), '20': ('Egypt', '🇪🇬'),
    '27': ('South Africa', '🇿🇦'), '31': ('Netherlands', '🇳🇱'), '32': ('Belgium', '🇧🇪'),
    '33': ('France', '🇫🇷'), '34': ('Spain', '🇪🇸'), '39': ('Italy', '🇮🇹'),
    '44': ('UK', '🇬🇧'), '46': ('Sweden', '🇸🇪'), '48': ('Poland', '🇵🇱'),
    '49': ('Germany', '🇩🇪'), '51': ('Peru', '🇵🇪'), '52': ('Mexico', '🇲🇽'),
    '54': ('Argentina', '🇦🇷'), '55': ('Brazil', '🇧🇷'), '56': ('Chile', '🇨🇱'),
    '57': ('Colombia', '🇨🇴'), '58': ('Venezuela', '🇻🇪'), '60': ('Malaysia', '🇲🇾'),
    '61': ('Australia', '🇦🇺'), '62': ('Indonesia', '🇮🇩'), '63': ('Philippines', '🇵🇭'),
    '66': ('Thailand', '🇹🇭'), '84': ('Vietnam', '🇻🇳'), '86': ('China', '🇨🇳'),
    '90': ('Turkey', '🇹🇷'), '91': ('India', '🇮🇳'), '92': ('Pakistan', '🇵🇰'),
    '93': ('Afghanistan', '🇦🇫'), '94': ('Sri Lanka', '🇱🇰'), '95': ('Myanmar', '🇲🇲'),
    '98': ('Iran', '🇮🇷'), '212': ('Morocco', '🇲🇦'), '213': ('Algeria', '🇩🇿'),
    '234': ('Nigeria', '🇳🇬'), '254': ('Kenya', '🇰🇪'), '255': ('Tanzania', '🇹🇿'),
    '380': ('Ukraine', '🇺🇦'), '880': ('Bangladesh', '🇧🇩'), '964': ('Iraq', '🇮🇶'),
    '966': ('Saudi Arabia', '🇸🇦'), '971': ('UAE', '🇦🇪'), '998': ('Uzbekistan', '🇺🇿'),
}


async def get_flag_by_country_name(name: str) -> str:
    for _code, (c_name, c_flag) in COUNTRY_CODES.items():
        if c_name == name:
            return c_flag
    try:
        async with get_pool().acquire() as conn:
            row = await conn.fetchrow("SELECT flag FROM custom_countries WHERE name=$1", name)
        if row:
            return row["flag"]
    except Exception:
        pass
    return "🌍"


async def get_country_info(phone: str):
    phone = str(phone).replace(' ', '').replace('+', '')
    if not phone:
        return "Unknown", "🌍"

    # Check custom countries first (longer codes = more specific)
    try:
        async with get_pool().acquire() as conn:
            customs = await conn.fetch("SELECT code, name, flag FROM custom_countries")
        customs = sorted(customs, key=lambda x: len(x["code"]), reverse=True)
        for row in customs:
            if phone.startswith(row["code"]):
                return row["name"], row["flag"]
    except Exception:
        pass

    # Check built-in codes (longest prefix wins)
    for length in (3, 2, 1):
        prefix = phone[:length]
        if prefix in COUNTRY_CODES:
            return COUNTRY_CODES[prefix]

    return "Unknown", "🌍"


# ─── Session File Cleanup ─────────────────────────────────────────────────────
def delete_session_files(session_path: str):
    base = session_path if not session_path.endswith('.session') else session_path[:-8]
    for ext in ('.session', '.session-wal', '.session-shm', '.session-journal'):
        try:
            fpath = base + ext
            if os.path.exists(fpath):
                os.remove(fpath)
        except Exception:
            pass

