"""Telegram bot — /report runs GE checkpoint and replies with the DQ summary.

Long-polling mode (no webhook, no public URL). Bot stays connected to Telegram
and pulls updates on a loop. Survives NAT / restarts via `restart: unless-stopped`
in docker-compose.
"""
import asyncio
import logging

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

from app import config
from app.ge import run_report

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("bot")


def _is_authorised(chat_id: int) -> bool:
    """Empty allowlist = anyone allowed. Non-empty = strict whitelist."""
    return not config.TELEGRAM_ALLOWED_CHAT_IDS or chat_id in config.TELEGRAM_ALLOWED_CHAT_IDS


async def start_command(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    log.info(f"/start from chat_id={chat_id}")
    await update.message.reply_text(
        "NYC Analytics DQ bot.\n\n"
        "Send /report to run data quality checks against Silver + Gold "
        "and get a summary."
    )


async def report_command(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    log.info(f"/report from chat_id={chat_id}")

    if not _is_authorised(chat_id):
        log.warning(f"unauthorised chat_id={chat_id} — denying")
        await update.message.reply_text("Access denied.")
        return

    placeholder = await update.message.reply_text("Running DQ checks, please wait...")

    try:
        report = await asyncio.to_thread(run_report)
    except Exception as exc:
        log.exception("ge.run_report failed")
        await placeholder.edit_text(f"Error while running DQ checks:\n{type(exc).__name__}: {exc}")
        return

    # Telegram message limit is 4096 chars. Our reports run ~1.5kB — safe.
    # Wrap in a fenced code block so the monospaced table layout survives.
    body = f"<pre>{_escape_html(report)}</pre>"
    await placeholder.edit_text(body, parse_mode=ParseMode.HTML)


def _escape_html(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def run() -> None:
    if not config.TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set — refusing to start bot")

    log.info("starting Telegram bot in long-polling mode")
    if config.TELEGRAM_ALLOWED_CHAT_IDS:
        log.info(f"allowlist active — {len(config.TELEGRAM_ALLOWED_CHAT_IDS)} chat ID(s)")
    else:
        log.info("no allowlist — bot will respond to anyone")

    app = Application.builder().token(config.TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("report", report_command))
    app.run_polling(allowed_updates=Update.ALL_TYPES)
