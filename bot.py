import asyncio
import logging
import os
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, ReactionTypeEmoji
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DB_PATH = BASE_DIR / "bridge.db"
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TARGET_GROUP_ID_RAW = os.getenv("TARGET_GROUP_ID", "")
ALLOWED_USER_IDS_RAW = os.getenv("ALLOWED_USER_IDS", "")
CLAIM_REACTION = os.getenv("CLAIM_REACTION", "👍")
DONE_REACTION = os.getenv("DONE_REACTION", "👍")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN topilmadi. .env faylga BOT_TOKEN kiriting.")

try:
    TARGET_GROUP_ID = int(TARGET_GROUP_ID_RAW)
except ValueError as exc:
    raise RuntimeError("TARGET_GROUP_ID noto'g'ri. U son bo'lishi kerak.") from exc

if not TARGET_GROUP_ID:
    raise RuntimeError("TARGET_GROUP_ID topilmadi.")


def parse_allowed_user_ids(raw_value: str) -> set[int]:
    allowed_ids: set[int] = set()
    for item in raw_value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            allowed_ids.add(int(item))
        except ValueError as exc:
            raise RuntimeError(
                "ALLOWED_USER_IDS noto'g'ri. Masalan: 123456789,987654321"
            ) from exc
    return allowed_ids


ALLOWED_USER_IDS = parse_allowed_user_ids(ALLOWED_USER_IDS_RAW)

if not ALLOWED_USER_IDS:
    raise RuntimeError("ALLOWED_USER_IDS topilmadi.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("telegram-bridge-bot")

router = Router(name="main")


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS message_links (
                group_chat_id INTEGER NOT NULL,
                group_message_id INTEGER NOT NULL,
                ticket_message_id INTEGER NOT NULL,
                user_chat_id INTEGER NOT NULL,
                user_message_id INTEGER,
                username TEXT,
                full_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (group_chat_id, group_message_id)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ticket_claims (
                group_chat_id INTEGER NOT NULL,
                ticket_message_id INTEGER NOT NULL,
                admin_user_id INTEGER NOT NULL,
                admin_full_name TEXT NOT NULL,
                claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (group_chat_id, ticket_message_id)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_open_tickets (
                user_chat_id INTEGER PRIMARY KEY,
                group_chat_id INTEGER NOT NULL,
                ticket_message_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ticket_text_cache (
                group_chat_id INTEGER NOT NULL,
                ticket_message_id INTEGER NOT NULL,
                rendered_text TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (group_chat_id, ticket_message_id)
            )
            """
        )

        conn.commit()


def save_link(
    group_chat_id: int,
    group_message_id: int,
    ticket_message_id: int,
    user_chat_id: int,
    user_message_id: Optional[int],
    username: Optional[str],
    full_name: str,
) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO message_links (
                group_chat_id,
                group_message_id,
                ticket_message_id,
                user_chat_id,
                user_message_id,
                username,
                full_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                group_chat_id,
                group_message_id,
                ticket_message_id,
                user_chat_id,
                user_message_id,
                username,
                full_name,
            ),
        )
        conn.commit()


def get_user_by_group_message(group_chat_id: int, group_message_id: int) -> Optional[dict]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT ticket_message_id, user_chat_id, user_message_id, username, full_name
            FROM message_links
            WHERE group_chat_id = ? AND group_message_id = ?
            """,
            (group_chat_id, group_message_id),
        ).fetchone()
        return dict(row) if row else None


def get_ticket_messages(group_chat_id: int, ticket_message_id: int) -> list[int]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        rows = conn.execute(
            """
            SELECT group_message_id
            FROM message_links
            WHERE group_chat_id = ? AND ticket_message_id = ?
            ORDER BY group_message_id ASC
            """,
            (group_chat_id, ticket_message_id),
        ).fetchall()
        return [row[0] for row in rows]


def get_ticket_claim(group_chat_id: int, ticket_message_id: int) -> Optional[dict]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT admin_user_id, admin_full_name, claimed_at
            FROM ticket_claims
            WHERE group_chat_id = ? AND ticket_message_id = ?
            """,
            (group_chat_id, ticket_message_id),
        ).fetchone()
        return dict(row) if row else None


def claim_ticket(
    group_chat_id: int,
    ticket_message_id: int,
    admin_user_id: int,
    admin_full_name: str,
) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO ticket_claims (
                group_chat_id,
                ticket_message_id,
                admin_user_id,
                admin_full_name
            ) VALUES (?, ?, ?, ?)
            """,
            (group_chat_id, ticket_message_id, admin_user_id, admin_full_name),
        )
        conn.commit()
        return cursor.rowcount > 0


def get_open_ticket_for_user(user_chat_id: int) -> Optional[dict]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT user_chat_id, group_chat_id, ticket_message_id, status, created_at, updated_at
            FROM user_open_tickets
            WHERE user_chat_id = ? AND status = 'open'
            """,
            (user_chat_id,),
        ).fetchone()
        return dict(row) if row else None


def open_or_update_user_ticket(user_chat_id: int, group_chat_id: int, ticket_message_id: int) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO user_open_tickets (
                user_chat_id, group_chat_id, ticket_message_id, status, updated_at
            )
            VALUES (?, ?, ?, 'open', CURRENT_TIMESTAMP)
            ON CONFLICT(user_chat_id) DO UPDATE SET
                group_chat_id = excluded.group_chat_id,
                ticket_message_id = excluded.ticket_message_id,
                status = 'open',
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_chat_id, group_chat_id, ticket_message_id),
        )
        conn.commit()


def close_user_ticket(user_chat_id: int) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            UPDATE user_open_tickets
            SET status = 'closed',
                updated_at = CURRENT_TIMESTAMP
            WHERE user_chat_id = ? AND status = 'open'
            """,
            (user_chat_id,),
        )
        conn.commit()


def save_ticket_text(group_chat_id: int, ticket_message_id: int, rendered_text: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO ticket_text_cache (
                group_chat_id, ticket_message_id, rendered_text, updated_at
            )
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(group_chat_id, ticket_message_id) DO UPDATE SET
                rendered_text = excluded.rendered_text,
                updated_at = CURRENT_TIMESTAMP
            """,
            (group_chat_id, ticket_message_id, rendered_text),
        )
        conn.commit()


def get_ticket_text(group_chat_id: int, ticket_message_id: int) -> Optional[str]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        row = conn.execute(
            """
            SELECT rendered_text
            FROM ticket_text_cache
            WHERE group_chat_id = ? AND ticket_message_id = ?
            """,
            (group_chat_id, ticket_message_id),
        ).fetchone()
        return row[0] if row else None


def append_text_to_ticket(existing_text: str, message: Message) -> str:
    added_text = (message.text or "").strip()
    if not added_text:
        return existing_text

    return (
        existing_text
        + "\n\n"
        + "📨 <b>Qo'shimcha xabar:</b>\n"
        + added_text
    )


def get_last_unanswered_message(group_chat_id: int) -> Optional[dict]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                uot.ticket_message_id AS group_message_id,
                uot.ticket_message_id,
                ml.full_name,
                ml.username,
                ml.user_chat_id,
                uot.updated_at
            FROM user_open_tickets uot
            JOIN message_links ml
              ON ml.group_chat_id = uot.group_chat_id
             AND ml.group_message_id = uot.ticket_message_id
            LEFT JOIN ticket_claims tc
              ON tc.group_chat_id = uot.group_chat_id
             AND tc.ticket_message_id = uot.ticket_message_id
            WHERE uot.group_chat_id = ?
              AND uot.status = 'open'
              AND tc.ticket_message_id IS NULL
            ORDER BY uot.updated_at DESC, uot.ticket_message_id DESC
            LIMIT 1
            """,
            (group_chat_id,),
        ).fetchone()
        return dict(row) if row else None


def is_user_allowed(message: Message) -> bool:
    return bool(message.from_user and message.from_user.id in ALLOWED_USER_IDS)


def is_allowed_user_content(message: Message) -> bool:
    return bool(message.text or message.voice or message.document)


def build_sender_card(message: Message) -> str:
    user = message.from_user
    full_name = user.full_name if user else "Noma'lum foydalanuvchi"
    user_id = user.id if user else 0
    username = f"@{user.username}" if user and user.username else "yo'q"
    return (
        "📩 <b>Yangi murojaat</b>\n"
        f"👤 <b>F.I.Sh.:</b> {full_name}\n"
        f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
        f"🔗 <b>Username:</b> {username}\n\n"
        "Quyidagi xabarga <b>reply</b> qilib javob bering. "
        "Birinchi bo'lib javob yozgan operator murojaatni band qiladi."
    )


def build_full_text_message(message: Message) -> str:
    user = message.from_user
    full_name = user.full_name if user else "Noma'lum foydalanuvchi"
    user_id = user.id if user else 0
    username = f"@{user.username}" if user and user.username else "yo'q"
    user_text = (message.text or "").strip()

    return (
        "📩 <b>Yangi murojaat</b>\n"
        f"👤 <b>F.I.Sh.:</b> {full_name}\n"
        f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
        f"🔗 <b>Username:</b> {username}\n\n"
        f"💬 <b>Xabar:</b>\n{user_text}\n\n"
        "Quyidagi xabarga <b>reply</b> qilib javob bering. "
        "Birinchi bo'lib javob yozgan operator murojaatni band qiladi."
    )


async def safe_set_reaction(bot: Bot, chat_id: int, message_id: int, emoji: str) -> None:
    try:
        await bot.set_message_reaction(
            chat_id=chat_id,
            message_id=message_id,
            reaction=[ReactionTypeEmoji(emoji=emoji)],
            is_big=False,
        )
    except Exception as exc:
        logger.warning(
            "Reaction qo'yib bo'lmadi | chat_id=%s | message_id=%s | error=%s",
            chat_id,
            message_id,
            exc,
        )


async def mark_ticket(bot: Bot, chat_id: int, ticket_message_id: int, emoji: str) -> None:
    for message_id in get_ticket_messages(chat_id, ticket_message_id):
        await safe_set_reaction(bot, chat_id, message_id, emoji)


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    if not is_user_allowed(message):
        await message.answer("⛔ Sizda ushbu botdan foydalanish huquqi yo'q.")
        return

    await message.answer(
        "Assalomu alaykum.\n\n"
        "Quyidagi formatlargina qabul qilinadi:\n"
        "• text xabar\n"
        "• ovozli xabar\n"
        "• fayl\n\n"
        "Agar sizga hali javob berilmagan bo'lsa, keyingi text xabarlaringiz shu murojaatga qo'shib boriladi."
    )


@router.message(F.chat.type == ChatType.PRIVATE)
async def from_user_to_group(message: Message, bot: Bot) -> None:
    if not is_user_allowed(message):
        await message.answer("⛔ Sizda ushbu botdan foydalanish huquqi yo'q.")
        return

    if not is_allowed_user_content(message):
        await message.answer(
            "❌ Faqat quyidagilar yuborish mumkin:\n"
            "• text xabar\n"
            "• ovozli xabar\n"
            "• fayl"
        )
        return

    try:
        full_name = message.from_user.full_name if message.from_user else "Noma'lum foydalanuvchi"
        username = message.from_user.username if message.from_user else None
        user_chat_id = message.chat.id

        open_ticket = get_open_ticket_for_user(user_chat_id)

        # Ochiq ticket bo'lsa
        if open_ticket:
            ticket_message_id = open_ticket["ticket_message_id"]

            # TEXT bo'lsa — ticket matnini edit qilamiz
            if message.text:
                current_text = get_ticket_text(TARGET_GROUP_ID, ticket_message_id)

                if not current_text:
                    current_text = build_sender_card(message)

                updated_text = append_text_to_ticket(current_text, message)

                await bot.edit_message_text(
                    chat_id=TARGET_GROUP_ID,
                    message_id=ticket_message_id,
                    text=updated_text,
                    parse_mode=ParseMode.HTML,
                )

                save_ticket_text(
                    group_chat_id=TARGET_GROUP_ID,
                    ticket_message_id=ticket_message_id,
                    rendered_text=updated_text,
                )

                open_or_update_user_ticket(
                    user_chat_id=user_chat_id,
                    group_chat_id=TARGET_GROUP_ID,
                    ticket_message_id=ticket_message_id,
                )

                await message.answer("✅ Xabaringiz avvalgi murojaatga qo'shildi.")
                return

            # VOICE yoki DOCUMENT bo'lsa — reply bo'lib tushadi
            if message.voice or message.document:
                group_msg = await bot.copy_message(
                    chat_id=TARGET_GROUP_ID,
                    from_chat_id=message.chat.id,
                    message_id=message.message_id,
                    reply_to_message_id=ticket_message_id,
                )

                save_link(
                    group_chat_id=TARGET_GROUP_ID,
                    group_message_id=group_msg.message_id,
                    ticket_message_id=ticket_message_id,
                    user_chat_id=user_chat_id,
                    user_message_id=message.message_id,
                    username=username,
                    full_name=full_name,
                )

                open_or_update_user_ticket(
                    user_chat_id=user_chat_id,
                    group_chat_id=TARGET_GROUP_ID,
                    ticket_message_id=ticket_message_id,
                )

                await message.answer("✅ Xabaringiz avvalgi murojaatga qo'shib yuborildi.")
                return

        # Yangi ticket ochish
        if message.text:
            initial_text = build_full_text_message(message)

            group_msg = await bot.send_message(
                chat_id=TARGET_GROUP_ID,
                text=initial_text,
                parse_mode=ParseMode.HTML,
            )

            save_link(
                group_chat_id=TARGET_GROUP_ID,
                group_message_id=group_msg.message_id,
                ticket_message_id=group_msg.message_id,
                user_chat_id=user_chat_id,
                user_message_id=message.message_id,
                username=username,
                full_name=full_name,
            )

            save_ticket_text(
                group_chat_id=TARGET_GROUP_ID,
                ticket_message_id=group_msg.message_id,
                rendered_text=initial_text,
            )

            open_or_update_user_ticket(
                user_chat_id=user_chat_id,
                group_chat_id=TARGET_GROUP_ID,
                ticket_message_id=group_msg.message_id,
            )

        elif message.voice or message.document:
            initial_text = build_sender_card(message)

            sender_card = await bot.send_message(
                chat_id=TARGET_GROUP_ID,
                text=initial_text,
                parse_mode=ParseMode.HTML,
            )

            save_link(
                group_chat_id=TARGET_GROUP_ID,
                group_message_id=sender_card.message_id,
                ticket_message_id=sender_card.message_id,
                user_chat_id=user_chat_id,
                user_message_id=message.message_id,
                username=username,
                full_name=full_name,
            )

            save_ticket_text(
                group_chat_id=TARGET_GROUP_ID,
                ticket_message_id=sender_card.message_id,
                rendered_text=initial_text,
            )

            forwarded = await bot.copy_message(
                chat_id=TARGET_GROUP_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                reply_to_message_id=sender_card.message_id,
            )

            save_link(
                group_chat_id=TARGET_GROUP_ID,
                group_message_id=forwarded.message_id,
                ticket_message_id=sender_card.message_id,
                user_chat_id=user_chat_id,
                user_message_id=message.message_id,
                username=username,
                full_name=full_name,
            )

            open_or_update_user_ticket(
                user_chat_id=user_chat_id,
                group_chat_id=TARGET_GROUP_ID,
                ticket_message_id=sender_card.message_id,
            )

        await message.answer(
            "✅ Murojaatingiz qabul qilindi. Javob tayyor bo'lgach shu bot orqali sizga yuboriladi."
        )

    except Exception as exc:
        logger.exception("Foydalanuvchi xabarini guruhga yuborishda xatolik: %s", exc)
        await message.answer(
            "❌ Xabarni guruhga yuborishda xatolik yuz berdi. Guruh ID va bot huquqlarini tekshiring."
        )


@router.message(F.chat.id == TARGET_GROUP_ID, Command("last"))
async def cmd_last_unanswered(message: Message, bot: Bot) -> None:
    last_msg = get_last_unanswered_message(message.chat.id)

    if not last_msg:
        await message.reply("✅ Javob berilmagan xabar topilmadi.")
        return

    target_message_id = last_msg["group_message_id"]
    ticket_message_id = last_msg["ticket_message_id"]
    full_name = last_msg.get("full_name") or "Noma'lum foydalanuvchi"
    username = last_msg.get("username")
    user_chat_id = last_msg["user_chat_id"]
    username_text = f"@{username}" if username else "yo'q"

    try:
        service_msg = await bot.send_message(
            chat_id=message.chat.id,
            text=(
                "📌 <b>Oxirgi javob berilmagan murojaat</b>\n"
                f"👤 <b>F.I.Sh.:</b> {full_name}\n"
                f"🔗 <b>Username:</b> {username_text}\n\n"
                "Shu thread ichida <b>reply</b> qilib javob bering."
            ),
            parse_mode=ParseMode.HTML,
            reply_to_message_id=target_message_id,
        )

        # Shu xizmat xabariga ham reply qilinsa ishlashi uchun bazaga bog'laymiz
        save_link(
            group_chat_id=message.chat.id,
            group_message_id=service_msg.message_id,
            ticket_message_id=ticket_message_id,
            user_chat_id=user_chat_id,
            user_message_id=None,
            username=username,
            full_name=full_name,
        )

    except Exception as exc:
        logger.exception("Oxirgi javob berilmagan xabarga yo'naltirishda xatolik: %s", exc)
        await message.reply("❌ Oxirgi javob berilmagan xabarga reply qilib bo'lmadi.")


@router.message(F.chat.id == TARGET_GROUP_ID, F.reply_to_message.as_("reply_to"))
async def from_group_to_user(message: Message, bot: Bot, reply_to: Message) -> None:
    if message.text and message.text.strip().lower() == "/last":
        return

    link = get_user_by_group_message(message.chat.id, reply_to.message_id)
    if not link:
        await message.reply("❌ Bu reply qaysi foydalanuvchiga tegishli ekanini topa olmadim.")
        return

    admin = message.from_user
    if not admin:
        await message.reply("❌ Operator ma'lumotini aniqlab bo'lmadi.")
        return

    ticket_message_id = link["ticket_message_id"]
    existing_claim = get_ticket_claim(message.chat.id, ticket_message_id)

    if existing_claim is None:
        claim_ticket(
            group_chat_id=message.chat.id,
            ticket_message_id=ticket_message_id,
            admin_user_id=admin.id,
            admin_full_name=admin.full_name,
        )
        await mark_ticket(bot, message.chat.id, ticket_message_id, CLAIM_REACTION)
        existing_claim = get_ticket_claim(message.chat.id, ticket_message_id)

    if existing_claim and existing_claim["admin_user_id"] != admin.id:
        await message.reply(
            "⛔ Bu murojaat allaqachon boshqa operator tomonidan olindi. "
            f"Band qilgan operator: {existing_claim['admin_full_name']}"
        )
        return

    try:
        await bot.copy_message(
            chat_id=link["user_chat_id"],
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        )

        close_user_ticket(link["user_chat_id"])
        await mark_ticket(bot, message.chat.id, ticket_message_id, DONE_REACTION)

        await message.reply("✅ Javob foydalanuvchiga yuborildi va ticket yopildi.")
    except Exception as exc:
        logger.exception("Javobni foydalanuvchiga yuborishda xatolik: %s", exc)
        await message.reply(
            "❌ Javobni yuborib bo'lmadi. Foydalanuvchi botni bloklagan bo'lishi mumkin."
        )


@router.message(F.chat.id == TARGET_GROUP_ID)
async def ignore_non_replies(message: Message) -> None:
    return


async def main() -> None:
    init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    await bot.delete_webhook(drop_pending_updates=True)

    dp = Dispatcher()
    dp.include_router(router)

    me = await bot.get_me()
    logger.info("Bot ishga tushdi: @%s", me.username)
    logger.info("Target group id: %s", TARGET_GROUP_ID)
    logger.info("Allowed user ids count: %s", len(ALLOWED_USER_IDS))

    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

#taskkill /F /IM python.exe

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot to'xtatildi")