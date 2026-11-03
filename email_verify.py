"""
email_verify.py - Reads bank-credit alert emails via IMAP (Gmail) and extracts
the credited amount + UPI/UTR reference number for auto deposit verification.

Built against Canara Bank alert format:
    An amount of INR 100.00 has been CREDITED on 30/06/26 to your account
    XXX055 from Mr RITESH RA with UPI Ref No.:654779910250.

Adjust AMOUNT_RE / UTR_RE if your bank uses different wording.
"""

import re
import email
import imaplib
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from config import EMAIL_HOST, EMAIL_PORT, EMAIL_USER, EMAIL_PASS, EMAIL_FOLDER

logger = logging.getLogger(__name__)

# ─── Regex patterns (case-insensitive) ────────────────────────────────────────
# Amount: "INR 100.00 has been CREDITED"
AMOUNT_RE = re.compile(r"INR\s*([\d,]+\.?\d*)\s*has\s*been\s*CREDITED", re.IGNORECASE)

# UTR/Ref variants:
#   "UPI Ref No.:654779910250"
#   "UPI Ref No:654779910250"
#   "Ref No. 654779910250"
#   "Transaction ID: 654779910250"
#   "UTR: 654779910250"
UTR_RE = re.compile(
    r"(?:UPI\s*Ref\s*No\.?:?\s*|Ref\s*No\.?\s*:?\s*|Transaction\s*ID\s*:?\s*|UTR\s*:?\s*)([A-Z0-9]{6,})",
    re.IGNORECASE,
)

# How many recent emails to scan per call (keeps IMAP fast)
MAX_EMAILS_TO_SCAN = 100


def _get_body(msg: email.message.Message) -> str:
    """Extract plain-text body from an email.message.Message, falling back to HTML."""
    text_body = ""
    html_body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp  = str(part.get("Content-Disposition") or "")
            if "attachment" in disp:
                continue
            try:
                payload = part.get_payload(decode=True)
                if payload is None:
                    continue
                decoded = payload.decode(errors="ignore")
                if ctype == "text/plain" and not text_body:
                    text_body = decoded
                elif ctype == "text/html" and not html_body:
                    html_body = decoded
            except Exception:
                continue
        return text_body or html_body
    try:
        return (msg.get_payload(decode=True) or b"").decode(errors="ignore")
    except Exception:
        return ""


def _fetch_credits_blocking(hours: int = 3) -> list:
    """
    Blocking IMAP scan — run via asyncio.to_thread().
    Returns list of {"amount": float, "utr": str} for every CREDITED email.
    Scans emails received within the last `hours` hours.
    """
    results = []
    if not EMAIL_USER or not EMAIL_PASS:
        logger.warning("email_verify: EMAIL_USER/EMAIL_PASS not set — skipping.")
        return results

    imap = None
    try:
        imap = imaplib.IMAP4_SSL(EMAIL_HOST, EMAIL_PORT)
        imap.login(EMAIL_USER, EMAIL_PASS)
        imap.select(EMAIL_FOLDER, readonly=True)

        # IMAP SINCE filter uses day granularity, so fetch at least one full day
        # when the requested window is under 24h, and more days for larger windows.
        lookback_days = 1 if hours <= 24 else int((hours + 23) // 24)
        since_date = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%d-%b-%Y")

        # Use server-side text search to avoid fetching all recent inbox emails.
        status, msg_ids = imap.search(None, f'(SINCE "{since_date}" TEXT "CREDITED" TEXT "INR")')
        if status != "OK" or not msg_ids or not msg_ids[0]:
            return results

        # Take the most recent MAX_EMAILS_TO_SCAN
        all_ids = msg_ids[0].split()
        ids_to_check = all_ids[-MAX_EMAILS_TO_SCAN:]

        for num in reversed(ids_to_check):
            try:
                # Fetch only the body text (not attachments) — much faster than RFC822
                status, data = imap.fetch(num, "(BODY.PEEK[])")
                if status != "OK" or not data or not data[0]:
                    continue

                raw = data[0][1]
                if isinstance(raw, (bytes, bytearray)):
                    msg = email.message_from_bytes(raw)
                else:
                    continue

                # Only keep emails whose Date header is within the requested window.
                date_hdr = msg.get("Date")
                if date_hdr:
                    try:
                        msg_dt = email.utils.parsedate_to_datetime(date_hdr)
                        if msg_dt is None:
                            continue
                        if msg_dt.tzinfo is None:
                            msg_dt = msg_dt.replace(tzinfo=timezone.utc)
                        msg_dt = msg_dt.astimezone(timezone.utc)
                        age = datetime.utcnow().replace(tzinfo=timezone.utc) - msg_dt
                        if age < timedelta(0) or age > timedelta(hours=hours):
                            continue
                    except Exception:
                        continue

                body = _get_body(msg)
                if not body:
                    continue

                # Quick pre-filter before running regex
                body_upper = body.upper()
                if "CREDITED" not in body_upper:
                    continue

                amt_match = AMOUNT_RE.search(body)
                utr_match = UTR_RE.search(body)
                if not amt_match or not utr_match:
                    logger.debug(f"email_verify: CREDITED email found but regex didn't match. Body snippet: {body[:300]}")
                    continue

                try:
                    amount = float(amt_match.group(1).replace(",", ""))
                except ValueError:
                    continue

                utr = re.sub(r'[^A-Z0-9]', '', utr_match.group(1).upper())
                if not utr:
                    continue

                results.append({"amount": amount, "utr": utr})
                logger.info(f"email_verify: Found credit — ₹{amount}, UTR={utr}")

            except Exception as exc:
                logger.debug(f"email_verify: Error parsing email #{num}: {exc}")
                continue

    except imaplib.IMAP4.error as exc:
        logger.error(f"email_verify IMAP error: {exc}")
    except Exception as exc:
        logger.error(f"email_verify unexpected error: {exc}")
    finally:
        if imap is not None:
            try:
                imap.logout()
            except Exception:
                pass

    return results


async def fetch_recent_credits(hours: int = 3) -> list:
    """Async wrapper — runs blocking IMAP scan in a thread pool."""
    return await asyncio.to_thread(_fetch_credits_blocking, hours)
