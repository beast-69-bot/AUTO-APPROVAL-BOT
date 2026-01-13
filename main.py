
import asyncio
import logging
import random
import secrets
import time
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramNetworkError
from aiogram.filters import Command
from aiogram.types import CallbackQuery, ChatJoinRequest, ChatMemberUpdated, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import ClientTimeout

from config import Config
from db import Database
from texts import (
    ATTEMPTS_LEFT_TEXT,
    EXPIRED_TEXT,
    FAIL_TEXT,
    LANGUAGE_LABELS,
    SUCCESS_TEXT,
    VERIFY_BUTTONS,
    VERIFY_TEXT,
    WELCOME_TEXT,
)

router = Router()


def now_ts() -> int:
    return int(time.time())


def is_admin(cfg: Config, user_id: int) -> bool:
    return user_id in cfg.admin_ids


def safe_lang(value: Optional[str]) -> str:
    return value if value in LANGUAGE_LABELS else "en"


def build_language_keyboard(token: str) -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    for lang_key, label in LANGUAGE_LABELS.items():
        builder.button(
            text=label,
            callback_data=f"lang:{token}:{lang_key}",
        )
    builder.adjust(1)
    return builder


def build_verify_keyboard(token: str) -> InlineKeyboardBuilder:
    choices = list(VERIFY_BUTTONS.items())
    random.shuffle(choices)
    builder = InlineKeyboardBuilder()
    for key, label in choices:
        builder.button(text=label, callback_data=f"verify:{token}:{key}")
    builder.adjust(2)
    return builder


def build_approval_message(bot_username: str, chat_title: str) -> str:
    link = f"https://t.me/{bot_username}?start=join"
    return (
        "Thanks for adding me as an admin.\n"
        f"Chat: {chat_title}\n"
        "Share this link with users so they can start the bot before requesting to join:\n"
        f"{link}"
    )


def build_scoped_approval_message(bot_username: str, chat_title: str, chat_id: int) -> str:
    link = f"https://t.me/{bot_username}?start=join_{chat_id}"
    return (
        "Thanks for adding me as an admin.\n"
        f"Chat: {chat_title}\n"
        "Share this link with users so they can start the bot before requesting to join:\n"
        f"{link}"
    )


async def apply_failure_action(
    bot: Bot,
    db: Database,
    cfg: Config,
    request_id: int,
    chat_id: int,
    user_id: int,
    status: str,
    message_text: str,
) -> None:
    await db.mark_failed(request_id, now_ts(), status)
    if cfg.failure_action == "reject":
        try:
            await bot.decline_chat_join_request(chat_id=chat_id, user_id=user_id)
        except Exception:
            logging.exception("Failed to decline join request")
    try:
        await bot.send_message(chat_id=user_id, text=message_text)
    except Exception:
        logging.warning("Failed to notify user about failure")


@router.chat_join_request()
async def on_join_request(event: ChatJoinRequest, bot: Bot, cfg: Config, db: Database) -> None:
    user_id = event.from_user.id
    chat_id = event.chat.id
    if await db.is_blacklisted(user_id):
        await db.upsert_join_request(
            user_id=user_id,
            chat_id=chat_id,
            status="blocked",
            now=now_ts(),
            language_token="",
            language_expires_at=0,
        )
        try:
            await bot.decline_chat_join_request(chat_id=chat_id, user_id=user_id)
        except Exception:
            logging.exception("Failed to decline blacklisted join request")
        return
    existing = await db.get_latest_request_for_user_chat(user_id, chat_id)
    if existing and existing["status"] == "verified_pending":
        await db.mark_verified(existing["id"], now_ts())
        try:
            await bot.approve_chat_join_request(chat_id=chat_id, user_id=user_id)
            try:
                await bot.send_message(
                    chat_id=user_id, text=SUCCESS_TEXT[safe_lang(existing["language"])]
                )
            except Exception:
                logging.warning("Failed to notify user after approval")
        except Exception:
            logging.exception("Failed to approve pre-verified join request")
        return

    if existing and existing["status"] in {"awaiting_language", "awaiting_verification"}:
        try:
            await bot.send_message(
                chat_id=user_id,
                text="Join request received. Please complete verification in this chat.",
            )
        except Exception:
            logging.warning("Could not notify user about pending verification")
        return

    language_token = secrets.token_hex(8)
    expires_at = now_ts() + cfg.language_timeout_seconds
    await db.upsert_join_request(
        user_id=user_id,
        chat_id=chat_id,
        status="awaiting_language",
        now=now_ts(),
        language_token=language_token,
        language_expires_at=expires_at,
    )

    try:
        keyboard = build_language_keyboard(language_token)
        await bot.send_message(
            chat_id=user_id,
            text=WELCOME_TEXT["en"],
            reply_markup=keyboard.as_markup(),
        )
    except Exception:
        request_id = await db.get_latest_request_id(user_id, chat_id)
        await db.mark_failed(request_id=request_id, now=now_ts(), status="dm_failed")
        logging.warning("Could not DM user, leaving join request pending")


@router.callback_query(F.data.startswith("lang:"))
async def on_language_select(query: CallbackQuery, bot: Bot, cfg: Config, db: Database) -> None:
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer("Invalid selection.", show_alert=True)
        return
    _, token, lang = parts
    record = await db.get_join_request_by_lang_token(token)
    if not record:
        await query.answer("Expired.", show_alert=True)
        return
    if query.from_user.id != record["user_id"]:
        await query.answer("Not for you.", show_alert=True)
        return
    if record["status"] != "awaiting_language":
        await query.answer("Already handled.", show_alert=True)
        return
    if record["language_expires_at"] and now_ts() > record["language_expires_at"]:
        await apply_failure_action(
            bot,
            db,
            cfg,
            request_id=record["id"],
            chat_id=record["chat_id"],
            user_id=record["user_id"],
            status="expired",
            message_text=EXPIRED_TEXT["en"],
        )
        await query.answer("Expired.", show_alert=True)
        return

    lang = safe_lang(lang)
    verification_token = secrets.token_hex(8)
    timeout_value = int(
        await db.get_setting("verify_timeout", str(cfg.verification_timeout_seconds))
    )
    verification_expires_at = now_ts() + timeout_value
    await db.set_language_and_verification(
        request_id=record["id"],
        language=lang,
        verification_token=verification_token,
        verification_expires_at=verification_expires_at,
        now=now_ts(),
    )

    await query.answer("Language saved.")
    keyboard = build_verify_keyboard(verification_token)
    try:
        await query.message.edit_text(
            text=VERIFY_TEXT[lang],
            reply_markup=keyboard.as_markup(),
        )
    except Exception:
        logging.exception("Failed to update message with verification buttons")
        try:
            await bot.send_message(
                chat_id=record["user_id"],
                text=VERIFY_TEXT[lang],
                reply_markup=keyboard.as_markup(),
            )
        except Exception:
            logging.exception("Failed to send verification message")
            try:
                await query.message.edit_text(
                    "Could not send verification. Please /start and try again."
                )
            except Exception:
                logging.warning("Failed to update message after verification send error")


@router.callback_query(F.data.startswith("verify:"))
async def on_verify(query: CallbackQuery, bot: Bot, cfg: Config, db: Database) -> None:
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer("Invalid selection.", show_alert=True)
        return
    _, token, choice = parts
    record = await db.get_join_request_by_ver_token(token)
    if not record:
        await query.answer("Expired.", show_alert=True)
        return
    if query.from_user.id != record["user_id"]:
        await query.answer("Not for you.", show_alert=True)
        return
    if record["status"] != "awaiting_verification":
        await query.answer("Already handled.", show_alert=True)
        return
    if record["verification_expires_at"] and now_ts() > record["verification_expires_at"]:
        await apply_failure_action(
            bot,
            db,
            cfg,
            request_id=record["id"],
            chat_id=record["chat_id"],
            user_id=record["user_id"],
            status="expired",
            message_text=EXPIRED_TEXT[safe_lang(record["language"])],
        )
        await query.answer("Expired.", show_alert=True)
        return

    lang = safe_lang(record["language"])
    if choice != "human":
        await db.increment_attempts(record["id"], now_ts())
        attempts = record["attempts"] + 1
        max_attempts = int(await db.get_setting("max_attempts", str(cfg.max_attempts)))
        remaining = max_attempts - attempts
        if remaining <= 0:
            await apply_failure_action(
                bot,
                db,
                cfg,
                request_id=record["id"],
                chat_id=record["chat_id"],
                user_id=record["user_id"],
                status="failed",
                message_text=FAIL_TEXT[lang],
            )
            await query.answer("Failed.", show_alert=True)
            return
        await query.answer(
            ATTEMPTS_LEFT_TEXT[lang].format(remaining=remaining), show_alert=True
        )
        return

    await db.mark_verified(record["id"], now_ts())
    await query.answer("Verified.")
    try:
        await query.message.edit_text(SUCCESS_TEXT[lang])
    except Exception:
        logging.warning("Failed to edit success message")
        try:
            await bot.send_message(
                chat_id=record["user_id"],
                text=SUCCESS_TEXT[lang],
            )
        except Exception:
            logging.warning("Failed to send success message")

    try:
        await bot.approve_chat_join_request(
            chat_id=record["chat_id"], user_id=record["user_id"]
        )
    except Exception:
        await db.mark_status_for_user_chat(
            record["user_id"], record["chat_id"], "verified_pending", now_ts()
        )
        logging.exception("Failed to approve join request")
        try:
            await query.message.edit_text(
                "You are verified. Please request to join the chat now."
            )
        except Exception:
            logging.warning("Failed to update message after approval failure")


@router.my_chat_member()
async def on_bot_promoted(event: ChatMemberUpdated, bot: Bot) -> None:
    if event.new_chat_member.status not in {"administrator", "creator"}:
        return
    if event.old_chat_member.status in {"administrator", "creator"}:
        return
    if not event.from_user:
        return
    try:
        bot_username = (await bot.get_me()).username or "your_bot"
    except Exception:
        logging.exception("Failed to fetch bot username")
        bot_username = "your_bot"
    chat_title = event.chat.title or "this chat"
    text = build_scoped_approval_message(bot_username, chat_title, event.chat.id)
    try:
        await bot.send_message(chat_id=event.from_user.id, text=text)
    except Exception:
        logging.exception("Failed to DM approval link to admin")


@router.message(Command("status"))
async def on_status(message: Message, cfg: Config, db: Database) -> None:
    if not is_admin(cfg, message.from_user.id):
        return
    chat_id = None
    if message.chat.type != ChatType.PRIVATE:
        chat_id = message.chat.id
    counts = await db.count_statuses(chat_id)
    lines = ["Status counts:"]
    for key in sorted(counts.keys()):
        lines.append(f"{key}: {counts[key]}")
    await message.answer("\n".join(lines))


@router.message(Command("start"))
async def on_start(message: Message, cfg: Config, db: Database, bot: Bot) -> None:
    user_id = message.from_user.id
    await db.record_user_start(user_id, now_ts())
    payload = ""
    parts = message.text.split(maxsplit=1)
    if len(parts) == 2:
        payload = parts[1].strip()
    if payload.startswith("join_"):
        try:
            chat_id = int(payload.replace("join_", "", 1))
        except ValueError:
            await message.answer("Invalid link payload.")
            return
        existing = await db.get_latest_request_for_user_chat(user_id, chat_id)
        if not existing or existing["status"] in {
            "failed",
            "expired",
            "blocked",
            "dm_failed",
            "rejected",
        }:
            token = secrets.token_hex(8)
            expires_at = now_ts() + cfg.language_timeout_seconds
            await db.upsert_join_request(
                user_id=user_id,
                chat_id=chat_id,
                status="awaiting_language",
                now=now_ts(),
                language_token=token,
                language_expires_at=expires_at,
            )
            keyboard = build_language_keyboard(token)
            await message.answer(WELCOME_TEXT["en"], reply_markup=keyboard.as_markup())
            return
        if existing["status"] == "awaiting_language":
            token = existing["language_token"]
            expires_at = existing["language_expires_at"] or 0
            if not token or now_ts() > expires_at:
                token = secrets.token_hex(8)
                expires_at = now_ts() + cfg.language_timeout_seconds
                await db.update_language_token(
                    request_id=existing["id"],
                    token=token,
                    expires_at=expires_at,
                    now=now_ts(),
                )
            keyboard = build_language_keyboard(token)
            await message.answer(WELCOME_TEXT["en"], reply_markup=keyboard.as_markup())
            return
        if existing["status"] == "awaiting_verification":
            lang = safe_lang(existing["language"])
            token = existing["verification_token"]
            expires_at = existing["verification_expires_at"] or 0
            timeout_value = int(
                await db.get_setting(
                    "verify_timeout", str(cfg.verification_timeout_seconds)
                )
            )
            if not token or now_ts() > expires_at:
                token = secrets.token_hex(8)
                expires_at = now_ts() + timeout_value
                await db.update_verification_token(
                    request_id=existing["id"],
                    token=token,
                    expires_at=expires_at,
                    now=now_ts(),
                )
            keyboard = build_verify_keyboard(token)
            await message.answer(VERIFY_TEXT[lang], reply_markup=keyboard.as_markup())
            return
        if existing["status"] == "verified_pending":
            await message.answer(
                "You are verified. Please request to join the chat now."
            )
            return
        if existing["status"] == "verified":
            await message.answer("You are already verified.")
            return
    pending = await db.get_pending_requests_for_user(user_id)
    if not pending:
        await message.answer("No pending join requests found.")
        return
    sent_any = False
    for record in pending:
        if record["status"] == "awaiting_language":
            token = record["language_token"]
            expires_at = record["language_expires_at"] or 0
            if not token or now_ts() > expires_at:
                token = secrets.token_hex(8)
                expires_at = now_ts() + cfg.language_timeout_seconds
                await db.update_language_token(
                    request_id=record["id"],
                    token=token,
                    expires_at=expires_at,
                    now=now_ts(),
                )
            keyboard = build_language_keyboard(token)
            await bot.send_message(
                chat_id=user_id,
                text=WELCOME_TEXT["en"],
                reply_markup=keyboard.as_markup(),
            )
            sent_any = True
        elif record["status"] == "awaiting_verification":
            lang = safe_lang(record["language"])
            token = record["verification_token"]
            expires_at = record["verification_expires_at"] or 0
            timeout_value = int(
                await db.get_setting(
                    "verify_timeout", str(cfg.verification_timeout_seconds)
                )
            )
            if not token or now_ts() > expires_at:
                token = secrets.token_hex(8)
                expires_at = now_ts() + timeout_value
                await db.update_verification_token(
                    request_id=record["id"],
                    token=token,
                    expires_at=expires_at,
                    now=now_ts(),
                )
            keyboard = build_verify_keyboard(token)
            await bot.send_message(
                chat_id=user_id,
                text=VERIFY_TEXT[lang],
                reply_markup=keyboard.as_markup(),
            )
            sent_any = True
    if not sent_any:
        await message.answer("No pending join requests found.")


@router.message(Command("broadcast"))
async def on_broadcast(message: Message, cfg: Config, db: Database, bot: Bot) -> None:
    if not is_admin(cfg, message.from_user.id):
        return
    if message.chat.type != ChatType.PRIVATE:
        await message.answer("Please use /broadcast in private chat with the bot.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2 or not parts[1].strip():
        await message.answer("Usage: /broadcast <message>")
        return
    text = parts[1].strip()
    user_ids = await db.list_started_users()
    if not user_ids:
        await message.answer("No users to broadcast to.")
        return
    sent = 0
    failed = 0
    for user_id in user_ids:
        try:
            await bot.send_message(chat_id=user_id, text=text)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await message.answer(f"Broadcast done. Sent: {sent}, Failed: {failed}.")


@router.message(Command("setattempts"))
async def on_set_attempts(message: Message, cfg: Config, db: Database) -> None:
    if not is_admin(cfg, message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Usage: /setattempts <number>")
        return
    value = max(1, int(parts[1]))
    await db.set_setting("max_attempts", str(value))
    await message.answer(f"Max attempts set to {value}.")


@router.message(Command("settimeout"))
async def on_set_timeout(message: Message, cfg: Config, db: Database) -> None:
    if not is_admin(cfg, message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Usage: /settimeout <seconds>")
        return
    value = max(30, int(parts[1]))
    await db.set_setting("verify_timeout", str(value))
    await message.answer(f"Verification timeout set to {value} seconds.")


@router.message(Command("approve"))
async def on_approve(message: Message, cfg: Config, db: Database, bot: Bot) -> None:
    if not is_admin(cfg, message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: /approve <user_id> [chat_id]")
        return
    try:
        user_id = int(parts[1])
        chat_id = int(parts[2]) if len(parts) > 2 else message.chat.id
    except ValueError:
        await message.answer("Invalid IDs.")
        return
    if not await db.is_whitelisted(user_id):
        await message.answer("User is not whitelisted for manual approval.")
        return
    try:
        await bot.approve_chat_join_request(chat_id=chat_id, user_id=user_id)
        await db.mark_status_for_user_chat(user_id, chat_id, "verified", now_ts())
        await message.answer("Approved.")
    except Exception:
        logging.exception("Manual approve failed")
        await message.answer("Failed to approve.")


@router.message(Command("reject"))
async def on_reject(message: Message, cfg: Config, db: Database, bot: Bot) -> None:
    if not is_admin(cfg, message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: /reject <user_id> [chat_id]")
        return
    try:
        user_id = int(parts[1])
        chat_id = int(parts[2]) if len(parts) > 2 else message.chat.id
    except ValueError:
        await message.answer("Invalid IDs.")
        return
    try:
        await bot.decline_chat_join_request(chat_id=chat_id, user_id=user_id)
        await db.mark_status_for_user_chat(user_id, chat_id, "rejected", now_ts())
        await message.answer("Rejected.")
    except Exception:
        logging.exception("Manual reject failed")
        await message.answer("Failed to reject.")


@router.message(Command("whitelist"))
async def on_whitelist(message: Message, cfg: Config, db: Database) -> None:
    if not is_admin(cfg, message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 3 or parts[1] != "add" or not parts[2].isdigit():
        await message.answer("Usage: /whitelist add <user_id>")
        return
    user_id = int(parts[2])
    await db.add_whitelist(user_id, now_ts())
    await message.answer("Whitelisted.")


@router.message(Command("blacklist"))
async def on_blacklist(message: Message, cfg: Config, db: Database) -> None:
    if not is_admin(cfg, message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 3 or parts[1] != "add" or not parts[2].isdigit():
        await message.answer("Usage: /blacklist add <user_id>")
        return
    user_id = int(parts[2])
    await db.add_blacklist(user_id, now_ts())
    await message.answer("Blacklisted.")


async def expiry_worker(bot: Bot, cfg: Config, db: Database) -> None:
    while True:
        await asyncio.sleep(10)
        current = now_ts()
        expired_language = await db.list_expired_language(current)
        for record in expired_language:
            await apply_failure_action(
                bot,
                db,
                cfg,
                request_id=record["id"],
                chat_id=record["chat_id"],
                user_id=record["user_id"],
                status="expired",
                message_text=EXPIRED_TEXT["en"],
            )
        expired_ver = await db.list_expired_verification(current)
        for record in expired_ver:
            lang = safe_lang(record.get("language"))
            await apply_failure_action(
                bot,
                db,
                cfg,
                request_id=record["id"],
                chat_id=record["chat_id"],
                user_id=record["user_id"],
                status="expired",
                message_text=EXPIRED_TEXT[lang],
            )


async def main() -> None:
    cfg = Config()
    if not cfg.bot_token:
        raise RuntimeError("BOT_TOKEN is required")

    logging.basicConfig(level=cfg.log_level)
    timeout = ClientTimeout(total=60)
    session = AiohttpSession(timeout=timeout)
    bot = Bot(token=cfg.bot_token, session=session)
    dp = Dispatcher()
    db = Database(cfg.db_path)
    await db.init()

    dp.include_router(router)
    dp["cfg"] = cfg
    dp["db"] = db

    asyncio.create_task(expiry_worker(bot, cfg, db))
    for attempt in range(5):
        try:
            await bot.get_me()
            break
        except TelegramNetworkError:
            await asyncio.sleep(2**attempt)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
