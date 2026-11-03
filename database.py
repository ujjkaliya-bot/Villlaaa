"""
database.py - MongoDB (Motor) database for OTP Bot.

The bot's queries go through mongo_compat.Pool, which offers the same
pool.acquire() / conn.fetch / fetchrow / execute API as before but stores
everything in MongoDB. No PostgreSQL / asyncpg needed anymore.
"""

import asyncio
import logging

from config import MONGO_URI, MONGO_DB_NAME
from mongo_compat import Pool, ensure_schema

logger = logging.getLogger(__name__)

if not MONGO_URI:
    raise RuntimeError("[CONFIG ERROR] MONGO_URI is not set (env var MONGO_URI or config.py)!")

# ─── Connection Pool ──────────────────────────────────────────────────────────

_pool: Pool = None


async def init_db_pool():
    """Connect to MongoDB. Call once at startup in main()."""
    global _pool
    if _pool is None:
        from motor.motor_asyncio import AsyncIOMotorClient
        client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=15000)
        await client.admin.command("ping")  # fail fast if URI/network is wrong
        _pool = Pool(client[MONGO_DB_NAME], client)
        logger.info("✅ MongoDB connected (db=%s).", MONGO_DB_NAME)


def get_pool() -> Pool:
    """Return the pool. Raises if init_db_pool() was not called first."""
    if _pool is None:
        raise RuntimeError("DB pool not initialised. Call await init_db_pool() first.")
    return _pool


# ─── Per-user async locks ─────────────────────────────────────────────────────

user_locks: dict = {}


def get_user_lock(uid: int) -> asyncio.Lock:
    if uid not in user_locks:
        user_locks[uid] = asyncio.Lock()
    return user_locks[uid]


# ─── Schema ───────────────────────────────────────────────────────────────────

async def setup_db():
    """Create indexes / counters if missing. Safe to call on every startup."""
    await ensure_schema(get_pool().db)
    logger.info("✅ MongoDB indexes ready.")


# ─── Keepalive loop (cheap ping so dropped connections are noticed early) ─────

async def keepalive_loop():
    """Ping MongoDB every 4 minutes."""
    while True:
        await asyncio.sleep(240)
        try:
            await get_pool().fetchval("SELECT 1")
            await get_pool().db.command("ping")
        except Exception as exc:
            logger.warning(f"Keepalive ping failed: {exc}")


# ─── Core helpers ─────────────────────────────────────────────────────────────

async def ensure_user(uid: int):
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", uid
        )


async def is_user_banned(uid: int) -> bool:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT banned FROM users WHERE user_id=$1", uid)
    return bool(row and row["banned"] == 1)


async def update_balance(uid: int, amount: int):
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, uid
        )


async def is_bot_online() -> bool:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM settings WHERE key='bot_status'")
    return row["value"] == "on" if row else True


async def get_usdt_rate() -> float:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM settings WHERE key='usdt_rate'")
    try:
        return float(row["value"]) if row else 94.0
    except Exception:
        return 94.0


async def get_support_url() -> str:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM settings WHERE key='support_url'")
    url = row["value"] if row and row["value"] else "https://t.me/vaishustocks"
    if not url.startswith("http"):
        url = "https://" + url.replace("@", "t.me/")
    return url


async def get_maint_msg() -> str:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM settings WHERE key='maint_msg'")
    return row["value"] if row and row["value"] else "🔧 Bot is under maintenance. Please try again later."


async def to_usd(inr) -> float:
    rate = await get_usdt_rate()
    return round(float(inr) / float(rate), 2) if rate else 0.0


async def is_referral_enabled() -> bool:
    """Return True if the referral system is ON (default: True)."""
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM settings WHERE key='referral_enabled'")
    return (row["value"] == "on") if row else True


async def is_images_enabled() -> bool:
    """Return True if banner images are enabled (default: True)."""
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM settings WHERE key='images_enabled'")
    return (row["value"] == "on") if row else True


async def is_autopay_enabled() -> bool:
    """Return True if the Auto Payment (QR + UTR + email auto-verify) method is enabled."""
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM settings WHERE key='autopay_enabled'")
    return (row["value"] == "on") if row else False


async def get_autopay_config() -> dict:
    """Return {'qr_file_id': str, 'caption': str} for the Auto Payment QR method."""
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT key, value FROM settings WHERE key IN ('autopay_qr_file_id','autopay_caption')"
        )
    data = {r["key"]: r["value"] for r in rows}
    return {
        "qr_file_id": data.get("autopay_qr_file_id", ""),
        "caption": data.get("autopay_caption", "🤖 <b>Auto Payment</b>\n\nScan the QR & pay the exact amount."),
    }


async def get_stock_layout() -> str:
    """Return 'side_by_side' or 'one_by_one' (default: side_by_side)."""
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM settings WHERE key='stock_layout'")
    return row["value"] if row and row["value"] in ("side_by_side", "one_by_one") else "side_by_side"


async def get_welcome_msg(first_name: str = "there") -> str:
    """Return the welcome message, with {name} replaced by the user's first name.
    Falls back to the default message if none is set in the DB."""
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM settings WHERE key='welcome_msg'")
    if row and row["value"]:
        return row["value"].replace("{name}", first_name)
    return (
        f"👋 <b>Hey {first_name}!</b> Welcome to <b>Fresh Tg Store</b> 🏪\n\n"
        f"<blockquote>📱 Fresh TG Accounts  |  📁 Sessions\n"
        f"⚡ Instant Delivery    |  🔒 Verified Stock\n\n</blockquote>\n"
        f"<blockquote> Best Prices  •  24/7 Support  •  Trusted Seller</blockquote>\n\n"
        f"👇 <b>Use the menu to get started!</b>"
    )


async def get_join_channels() -> list:
    """Return list of channel IDs (int) for force-join check, from DB or empty list."""
    from config import CHECK_CHANNELS
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT key, value FROM settings WHERE key LIKE 'channel_%_id' ORDER BY key"
        )
    if rows:
        ids = []
        for r in rows:
            try:
                ids.append(int(r["value"]))
            except Exception:
                pass
        return ids if ids else CHECK_CHANNELS
    return CHECK_CHANNELS


async def get_join_urls() -> list:
    """Return list of channel invite URLs, from DB or config fallback."""
    from config import JOIN_URLS
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT key, value FROM settings WHERE key LIKE 'channel_%_url' ORDER BY key"
        )
    if rows:
        urls = [r["value"] for r in rows if r["value"]]
        return urls if urls else JOIN_URLS
    return JOIN_URLS
