"""
config.py - Centralized configuration for OTP Bot (hardcoded values)
"""
import os
# ─── Telegram API ───────────────────────────────────────────────────────────
API_ID: int     = int(os.getenv("API_ID", 0))
API_HASH: str   = os.getenv("API_HASH", "")
BOT_TOKEN: str  = os.getenv("BOT_TOKEN", "")
ADMIN_ID: int   = int(os.getenv("ADMIN_ID", 8369781808))

# ─── Log Channels (payment / purchase logs) ──────────────────────────────────
# Har payment (deposit) aur purchase ke liye DO log channel:
#   FULL   -> private admin channel: sab kuch poora dikhta hai
#             (Num, ID, Time, Country, Date ... + Accept/Reject buttons & screenshots)
#   MASKED -> public/proof channel: Num aur ID beech se hide (827*****828),
#             Time / Date / Country visible. Sirf successful deposit/purchase yahan aata hai.
# Bot dono channels me admin hona chahiye. MASKED = 0 rakho to masked log band rahega.
PAYMENT_LOG_FULL_ID: int   = int(os.getenv("PAYMENT_LOG_FULL_ID", -1003877848867))
PAYMENT_LOG_MASKED_ID: int = int(os.getenv("PAYMENT_LOG_MASKED_ID", -1003986470399))   # <- yahan masked channel ki ID daalo

# Backward-compat aliases (bot.py / demo.py inhe use karte hain)
LOG_CHANNEL_ID: int          = PAYMENT_LOG_FULL_ID
PAYMENT_LOG_CHANNEL_ID: int  = PAYMENT_LOG_FULL_ID
PURCHASE_LOG_CHANNEL_ID: int = PAYMENT_LOG_FULL_ID

# ─── Force-Join Channels (sirf 2) ────────────────────────────────────────────
CHECK_CHANNELS: list = [-1003536717784, -1003986470399]   # channel IDs (bot admin ho)
JOIN_URLS: list = [                                       # inhi ke invite links, same order me
    "https://t.me/Proof_sold_ids",
    "https://t.me/+SV_SxFag-FQ1OTQ1",
]

# ─── Payment Gateways ────────────────────────────────────────────────────────
CWALLET_QR: str = ""
CWALLET_ID: str = ""
UPI_MID: str = ""
UPI_ID: str  = ""

# ─── Bank Transfer (Auto UTR Verify) ─────────────────────────────────────────
# These are shown on the payment screen — leave blank if your QR image
# already contains the account details.
BANK_NAME: str        = ""
BANK_HOLDER: str      = ""
BANK_ACCOUNT_NO: str  = ""
BANK_IFSC: str        = ""

# ─── Gmail IMAP (for Auto Payment email verification) ────────────────────────
# Use a Gmail App Password — NOT your normal Gmail login password.
# Generate one at: myaccount.google.com → Security → App Passwords
EMAIL_HOST: str   = "imap.gmail.com"
EMAIL_PORT: int   = 993
EMAIL_USER: str   = ""   # e.g. "yourbank.alerts@gmail.com"
EMAIL_PASS: str   = ""   # 16-character Gmail App Password
EMAIL_FOLDER: str = "INBOX"

# ─── Database (MongoDB) ──────────────────────────────────────────────────────
# Example: mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority
MONGO_URI: str     = os.getenv("MONGO_URI", "")
MONGO_DB_NAME: str = os.getenv("MONGO_DB_NAME", "vikrambot")

# ─── Links & Bot Settings ────────────────────────────────────────────────────
TERMS_URL: str           = "https://tgtele.onrender.com/"
AUTO_CANCEL_SECONDS: int = 600

# ─── Misc ─────────────────────────────────────────────────────────────────────
OTP_REGEX: str = r"\b\d{4,8}\b"

# ─── Proxy Pool ───────────────────────────────────────────────────────────────
PROXIES: list = []

# ─── Banner Images ────────────────────────────────────────────────────────────
START_IMAGE   = "images/start.jpg"
DEPOSIT_IMAGE = "images/deposit.jpg"
PROFILE_IMAGE = "images/profile.jpg"
STATS_IMAGE   = "images/stats.jpg"
SUPPORT_IMAGE = "images/support.jpg"
BUY_IMAGE     = "images/buy.jpg"

# ─── HD Emoji constants ───────────────────────────────────────────────────────
P_YES   = '✅'
P_NO    = '❌'
P_PKG   = '📦'
P_MONEY = '💰'
P_USDT  = '💲'
P_INR   = '₹'
P_TG    = '✈️'
P_GIFT  = '🎁'
P_STATS = '📊'
P_CARD  = '💳'
P_USERS = '👥'
P_CAL   = '📅'
P_PC    = '💻'
P_EYE   = '👁️'
P_UPI   = '🏦'
P_CW    = '👛'
P_ON    = '🟢'
P_OFF   = '🔴'
P_ID    = '🆔'
P_KEY   = '⌨️'
P_GLOBE = '🌎'
P_CART  = '🛒'
P_STORE = '🏬'
P_OTP   = '🔢'
P_2FA   = '🔐'
P_FLAG  = '🏳️'
P_PHONE = '📱'
P_WAIT  = '⏳'
P_TIME  = '⏰'
P_WARN  = '⚠️'
P_DOC   = '📃'
P_SOS   = '🆘'
P_ASST  = '🤖'
P_ACC   = '👤'
