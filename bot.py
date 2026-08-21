#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ربات حرفه‌ای مدیریت گروه تلگرام
================================
قابلیت‌ها:
  - ضداسپم (فلود) و ضدلینک/فوروارد/منشن/استیکر/گیف/... با قفل‌های جداگانه
  - سیستم اخطار با سقف قابل‌تنظیم و مجازات خودکار (میوت/کیک/بن)
  - دستورهای mute/unmute/tmute/ban/unban/tban/kick با پشتیبانی از مدت‌زمان
  - پاکسازی پیام (purge/del)
  - مدیریت ادمین‌ها (promote/demote/adminlist)
  - خوشامدگویی و قوانین قابل‌شخصی‌سازی
  - مدیریت ربات‌ها و اکانت‌های حذف‌شده هنگام ورود به گروه
  - لیست سیاه کاربران و کلمات ممنوعه
  - قفل زمان‌دار کل گروه (lockall/unlockall)
  - آمار گروه
  - پنل تنظیمات با دکمه‌های اینلاین + پنل مدیریتی Mini App (وب‌اپ تلگرام)

معماری: async کامل (aiogram 3 + SQLAlchemy async + aiosqlite)، ماژولار در قالب
بخش‌های مجزا داخل همین فایل، قابل توسعه و بدون وابستگی به سرویس خارجی.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import html
import json
import logging
import os
import re
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qsl

from aiohttp import web
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import BaseFilter, Command, CommandStart
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    ChatMemberUpdated,
    ChatPermissions,
    Message,
    TelegramObject,
    User as TgUser,
    WebAppInfo,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# =====================================================================
# لاگ‌گیری
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("groupguard")


# =====================================================================
# تنظیمات (config.py)
# =====================================================================
# 🔑 اگه نمی‌خوای رو Railway متغیرهای محیطی ست کنی، می‌تونی همینجا مستقیم
# توکن و آیدی‌هات رو بنویسی. اگه اینجا پر باشه، دیگه لازم نیست تو تب
# Variables چیزی ست کنی (اولویت با متغیر محیطی‌ست، ولی اگه خالی بود میاد سراغ این‌ها).
#
# ⚠️ فقط اگه ریپوی گیت‌هابت PRIVATE هست از این روش استفاده کن، و مطمئن شو
# هیچ‌وقت public/فورک نمی‌شه — چون یه بار push شدن توکن یعنی برای همیشه تو
# تاریخچه‌ی گیت می‌مونه، حتی اگه تو کامیت بعدی حذفش کنی.
HARDCODED_BOT_TOKEN = ""  # مثل: "123456789:AAExxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
HARDCODED_OWNER_IDS = ""  # مثل: "123456789,987654321"


@dataclass
class Config:
    bot_token: str
    owner_ids: Set[int] = field(default_factory=set)
    database_url: str = "sqlite+aiosqlite:///groupguard.db"
    miniapp_enabled: bool = True
    miniapp_host: str = "0.0.0.0"
    miniapp_port: int = 8080
    miniapp_public_url: str = ""


def load_config() -> Config:
    load_dotenv()
    token = (os.getenv("BOT_TOKEN", "").strip()) or HARDCODED_BOT_TOKEN.strip()
    if not token:
        raise RuntimeError(
            "❌ توکن ربات تنظیم نشده. یا متغیر محیطی BOT_TOKEN رو ست کن، یا مقدار "
            "HARDCODED_BOT_TOKEN رو بالای همین فایل پر کن."
        )
    owners_raw = os.getenv("OWNER_IDS", "").strip() or HARDCODED_OWNER_IDS
    owners = {
        int(x) for x in owners_raw.replace(" ", "").split(",") if x.strip().lstrip("-").isdigit()
    }
    return Config(
        bot_token=token,
        owner_ids=owners,
        database_url=os.getenv("DATABASE_URL", "sqlite+aiosqlite:///groupguard.db"),
        miniapp_enabled=os.getenv("MINIAPP_ENABLED", "true").strip().lower() == "true",
        miniapp_host=os.getenv("MINIAPP_HOST", "0.0.0.0"),
        # Railway (و اغلب PaaSها) پورت رو خودکار با متغیر PORT تزریق می‌کنن؛
        # اگه PORT ست بود همون اولویت داره، وگرنه MINIAPP_PORT یا 8080 پیش‌فرضه.
        miniapp_port=int(os.getenv("PORT", os.getenv("MINIAPP_PORT", "8080"))),
        miniapp_public_url=os.getenv("MINIAPP_PUBLIC_URL", "").rstrip("/"),
    )


config = load_config()


# =====================================================================
# دیتابیس: database.py
# =====================================================================
class Base(DeclarativeBase):
    pass


DEFAULT_WELCOME = "👋 به {chat} خوش اومدی {mention}!\nلطفاً قوانین گروه رو با دستور /rules بخون."


class Chat(Base):
    __tablename__ = "chats"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)  # آیدی چت تلگرام
    title: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # --- قفل‌ها ---
    lock_link: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_forward: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_mention: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_sticker: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_gif: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_contact: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_location: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_poll: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_photo: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_video: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_voice: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_document: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_bot_join: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_deleted_accounts: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_all_until: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # --- فیلتر کلمات ---
    filter_words_enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    # --- ضداسپم ---
    flood_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    flood_limit: Mapped[int] = mapped_column(Integer, default=6)
    flood_window: Mapped[int] = mapped_column(Integer, default=8)  # ثانیه
    flood_action: Mapped[str] = mapped_column(String(20), default="mute")  # mute/kick/ban

    # --- اخطار ---
    warn_limit: Mapped[int] = mapped_column(Integer, default=3)
    warn_action: Mapped[str] = mapped_column(String(20), default="mute")

    # --- خوشامد و قوانین ---
    welcome_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    welcome_text: Mapped[str] = mapped_column(Text, default=DEFAULT_WELCOME)
    delete_join_leave: Mapped[bool] = mapped_column(Boolean, default=False)
    rules_text: Mapped[str] = mapped_column(Text, default="")


class Warn(Base):
    __tablename__ = "warns"
    __table_args__ = (UniqueConstraint("chat_id", "user_id", name="uq_warn_chat_user"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger)
    user_id: Mapped[int] = mapped_column(BigInteger)
    count: Mapped[int] = mapped_column(Integer, default=0)
    last_reason: Mapped[str] = mapped_column(String(255), default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class BlacklistWord(Base):
    __tablename__ = "blacklist_words"
    __table_args__ = (UniqueConstraint("chat_id", "word", name="uq_word_chat"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger)
    word: Mapped[str] = mapped_column(String(255))


class BlacklistUser(Base):
    __tablename__ = "blacklist_users"
    __table_args__ = (UniqueConstraint("chat_id", "user_id", name="uq_black_chat_user"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger)
    user_id: Mapped[int] = mapped_column(BigInteger)
    reason: Mapped[str] = mapped_column(String(255), default="")


class TimedAction(Base):
    __tablename__ = "timed_actions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger)
    user_id: Mapped[int] = mapped_column(BigInteger, default=0)
    action: Mapped[str] = mapped_column(String(20))  # unmute / unban / unlockall
    expires_at: Mapped[datetime] = mapped_column(DateTime)
    applied: Mapped[bool] = mapped_column(Boolean, default=False)


class MessageStat(Base):
    __tablename__ = "message_stats"
    __table_args__ = (
        UniqueConstraint("chat_id", "user_id", "day", name="uq_stat_chat_user_day"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger)
    user_id: Mapped[int] = mapped_column(BigInteger)
    day: Mapped[date] = mapped_column(Date)
    count: Mapped[int] = mapped_column(Integer, default=0)


engine = create_async_engine(config.database_url, echo=False)
async_session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# =====================================================================
# ابزارهای عمومی: utils.py
# =====================================================================
DURATION_RE = re.compile(r"^(\d+)([smhdw])$", re.IGNORECASE)
DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_duration(text: str) -> Optional[timedelta]:
    """رشته‌هایی مثل 30s ، 10m ، 2h ، 1d ، 1w رو به timedelta تبدیل می‌کنه."""
    match = DURATION_RE.match((text or "").strip())
    if not match:
        return None
    value, unit = int(match.group(1)), match.group(2).lower()
    seconds = value * DURATION_UNITS[unit]
    if seconds <= 0 or seconds > 366 * 86400:
        return None
    return timedelta(seconds=seconds)


FA_DURATION_UNITS = {
    "دقیقه": "m", "دقيقه": "m", "دیقه": "m",
    "ساعت": "h",
    "روز": "d",
    "هفته": "w",
    "ثانیه": "s",
}


def parse_duration_fa(text: str) -> Optional[timedelta]:
    """
    هم فرمت کوتاه (30m، 2h، 1d) و هم فرمت فارسی («2 ساعت»، «۳۰دقیقه») رو می‌فهمه.
    """
    text = (text or "").strip()
    if not text:
        return None

    # اول فرمت کوتاه رو امتحان کن
    direct = parse_duration(text.split()[0])
    if direct is not None:
        return direct

    # تبدیل ارقام فارسی/عربی به لاتین
    digits_map = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")
    normalized = text.translate(digits_map)

    match = re.match(r"^(\d+)\s*([آ-ی]+)$", normalized)
    if not match:
        return None
    value, unit_word = match.group(1), match.group(2)
    unit = FA_DURATION_UNITS.get(unit_word)
    if unit is None:
        return None
    return parse_duration(f"{value}{unit}")


def mention_html(user: TgUser) -> str:
    name = html.escape(user.full_name or str(user.id))
    return f'<a href="tg://user?id={user.id}">{name}</a>'


def looks_like_deleted_account(user: TgUser) -> bool:
    """اکانت‌های حذف‌شده معمولاً بدون نام و یوزرنیم نمایش داده می‌شن."""
    if user.is_bot:
        return False
    first_name = (user.first_name or "").strip()
    return (not first_name or first_name == "Deleted Account") and not user.username


ACTION_FA = {"mute": "سایلنت", "kick": "اخراج", "ban": "بن"}

# دسترسی کامل (پیش‌فرض بعد از رفع سکوت)
DEFAULT_PERMISSIONS = ChatPermissions(
    can_send_messages=True,
    can_send_audios=True,
    can_send_documents=True,
    can_send_photos=True,
    can_send_videos=True,
    can_send_video_notes=True,
    can_send_voice_notes=True,
    can_send_polls=True,
    can_send_other_messages=True,
    can_add_web_page_previews=True,
    can_change_info=False,
    can_invite_users=True,
    can_pin_messages=False,
)

# سکوت کامل (نه ارسال پیام، نه هیچ نوع رسانه‌ای)
MUTE_PERMISSIONS = ChatPermissions(
    can_send_messages=False,
    can_send_audios=False,
    can_send_documents=False,
    can_send_photos=False,
    can_send_videos=False,
    can_send_video_notes=False,
    can_send_voice_notes=False,
    can_send_polls=False,
    can_send_other_messages=False,
    can_add_web_page_previews=False,
    can_change_info=False,
    can_invite_users=False,
    can_pin_messages=False,
)


# --- کش ادمین‌های هر گروه (برای کاهش تعداد فراخوانی API) ---
_admin_cache: Dict[int, Tuple[float, Set[int]]] = {}
ADMIN_CACHE_TTL = 60.0


async def get_chat_admin_ids(bot: Bot, chat_id: int) -> Set[int]:
    now = time.time()
    cached = _admin_cache.get(chat_id)
    if cached and now - cached[0] < ADMIN_CACHE_TTL:
        return cached[1]
    try:
        admins = await bot.get_chat_administrators(chat_id)
    except (TelegramBadRequest, TelegramForbiddenError):
        return set()
    ids = {a.user.id for a in admins}
    _admin_cache[chat_id] = (now, ids)
    return ids


async def is_chat_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    if user_id in config.owner_ids:
        return True
    ids = await get_chat_admin_ids(bot, chat_id)
    return user_id in ids


def invalidate_admin_cache(chat_id: int) -> None:
    _admin_cache.pop(chat_id, None)


# --- ردیاب فلود در حافظه ---
_flood_tracker: Dict[Tuple[int, int], Deque[float]] = defaultdict(deque)

# --- ردیاب کاربرانی که الان سکوت‌شون فعاله (برای پاکسازی خودکار پیام‌های احتمالی) ---
_muted_users: Dict[int, Set[int]] = defaultdict(set)


def mark_muted(chat_id: int, user_id: int) -> None:
    _muted_users[chat_id].add(user_id)


def unmark_muted(chat_id: int, user_id: int) -> None:
    _muted_users[chat_id].discard(user_id)


def is_marked_muted(chat_id: int, user_id: int) -> bool:
    return user_id in _muted_users[chat_id]


# --- ست‌اپ ساده‌ی خوش‌آمدگویی/قوانین با یک پیام (بدون دستور) ---
# کلید: (chat_id, user_id) -> "welcome" یا "rules"
_pending_text_setup: Dict[Tuple[int, int], str] = {}


def check_flood(chat_id: int, user_id: int, limit: int, window: int) -> bool:
    key = (chat_id, user_id)
    dq = _flood_tracker[key]
    now = time.time()
    dq.append(now)
    while dq and now - dq[0] > window:
        dq.popleft()
    return len(dq) > limit


URL_RE = re.compile(
    r"(https?://|www\.|t\.me/|telegram\.me/|[a-z0-9-]+\.(ir|com|net|org|io|xyz|info))",
    re.IGNORECASE,
)
# آیدی/یوزرنیم به شکل @channel هم زیرمجموعه‌ی «لینک» حساب می‌شه (چون همون لینک عضویته)
AT_USERNAME_RE = re.compile(r"@[a-zA-Z0-9_]{4,32}")


def _render_welcome(template: str, member: TgUser, chat_title: str) -> str:
    return (
        (template or DEFAULT_WELCOME)
        .replace("{name}", html.escape(member.full_name or ""))
        .replace("{mention}", mention_html(member))
        .replace("{id}", str(member.id))
        .replace("{chat}", html.escape(chat_title or ""))
    )


# =====================================================================
# سرویس‌ها: services/chat_service.py
# =====================================================================
async def get_or_create_chat(session: AsyncSession, tg_chat) -> Chat:
    result = await session.execute(select(Chat).where(Chat.id == tg_chat.id))
    chat = result.scalar_one_or_none()
    if chat is None:
        chat = Chat(id=tg_chat.id, title=tg_chat.title or "")
        session.add(chat)
        await session.commit()
        await session.refresh(chat)
    elif tg_chat.title and chat.title != tg_chat.title:
        chat.title = tg_chat.title
        session.add(chat)
        await session.commit()
    return chat


async def get_chat_by_id(session: AsyncSession, chat_id: int) -> Optional[Chat]:
    result = await session.execute(select(Chat).where(Chat.id == chat_id))
    return result.scalar_one_or_none()


# =====================================================================
# سرویس‌ها: services/warn_service.py
# =====================================================================
async def add_warn(session: AsyncSession, chat_id: int, user_id: int, reason: str) -> Warn:
    result = await session.execute(
        select(Warn).where(Warn.chat_id == chat_id, Warn.user_id == user_id)
    )
    warn = result.scalar_one_or_none()
    if warn is None:
        warn = Warn(chat_id=chat_id, user_id=user_id, count=1, last_reason=reason)
    else:
        warn.count += 1
        warn.last_reason = reason
    warn.updated_at = datetime.utcnow()
    session.add(warn)
    await session.commit()
    await session.refresh(warn)
    return warn


async def get_warn_count(session: AsyncSession, chat_id: int, user_id: int) -> int:
    result = await session.execute(
        select(Warn.count).where(Warn.chat_id == chat_id, Warn.user_id == user_id)
    )
    row = result.scalar_one_or_none()
    return row or 0


async def reset_warns(session: AsyncSession, chat_id: int, user_id: int) -> None:
    result = await session.execute(
        select(Warn).where(Warn.chat_id == chat_id, Warn.user_id == user_id)
    )
    warn = result.scalar_one_or_none()
    if warn is not None:
        await session.delete(warn)
        await session.commit()


# =====================================================================
# سرویس‌ها: services/blacklist_service.py
# =====================================================================
async def add_blacklist_word(session: AsyncSession, chat_id: int, word: str) -> bool:
    word = (word or "").strip().lower()
    if not word:
        return False
    existing = await session.execute(
        select(BlacklistWord).where(BlacklistWord.chat_id == chat_id, BlacklistWord.word == word)
    )
    if existing.scalar_one_or_none() is not None:
        return False
    session.add(BlacklistWord(chat_id=chat_id, word=word))
    await session.commit()
    return True


async def remove_blacklist_word(session: AsyncSession, chat_id: int, word: str) -> bool:
    word = (word or "").strip().lower()
    result = await session.execute(
        select(BlacklistWord).where(BlacklistWord.chat_id == chat_id, BlacklistWord.word == word)
    )
    row = result.scalar_one_or_none()
    if row is None:
        return False
    await session.delete(row)
    await session.commit()
    return True


async def list_blacklist_words(session: AsyncSession, chat_id: int) -> List[str]:
    result = await session.execute(select(BlacklistWord.word).where(BlacklistWord.chat_id == chat_id))
    return [row[0] for row in result.all()]


async def find_blacklisted_word(session: AsyncSession, chat_id: int, text: str) -> Optional[str]:
    words = await list_blacklist_words(session, chat_id)
    if not words:
        return None
    low = text.lower()
    for word in words:
        if word in low:
            return word
    return None


async def add_blacklist_user(session: AsyncSession, chat_id: int, user_id: int, reason: str = "") -> bool:
    existing = await session.execute(
        select(BlacklistUser).where(BlacklistUser.chat_id == chat_id, BlacklistUser.user_id == user_id)
    )
    if existing.scalar_one_or_none() is not None:
        return False
    session.add(BlacklistUser(chat_id=chat_id, user_id=user_id, reason=reason))
    await session.commit()
    return True


async def remove_blacklist_user(session: AsyncSession, chat_id: int, user_id: int) -> bool:
    result = await session.execute(
        select(BlacklistUser).where(BlacklistUser.chat_id == chat_id, BlacklistUser.user_id == user_id)
    )
    row = result.scalar_one_or_none()
    if row is None:
        return False
    await session.delete(row)
    await session.commit()
    return True


async def is_user_blacklisted(session: AsyncSession, chat_id: int, user_id: int) -> bool:
    result = await session.execute(
        select(BlacklistUser.id).where(BlacklistUser.chat_id == chat_id, BlacklistUser.user_id == user_id)
    )
    return result.scalar_one_or_none() is not None


async def list_blacklist_users(session: AsyncSession, chat_id: int) -> List[int]:
    result = await session.execute(select(BlacklistUser.user_id).where(BlacklistUser.chat_id == chat_id))
    return [row[0] for row in result.all()]


# =====================================================================
# سرویس‌ها: services/timed_action_service.py
# =====================================================================
async def add_timed_action(
    session: AsyncSession, chat_id: int, user_id: int, action: str, expires_at: datetime
) -> None:
    session.add(TimedAction(chat_id=chat_id, user_id=user_id, action=action, expires_at=expires_at))
    await session.commit()


# =====================================================================
# سرویس‌ها: services/stats_service.py
# =====================================================================
async def record_message_stat(session: AsyncSession, chat_id: int, user_id: int) -> None:
    today = date.today()
    result = await session.execute(
        select(MessageStat).where(
            MessageStat.chat_id == chat_id, MessageStat.user_id == user_id, MessageStat.day == today
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        session.add(MessageStat(chat_id=chat_id, user_id=user_id, day=today, count=1))
    else:
        row.count += 1
        session.add(row)
    await session.commit()


async def build_stats_text(session: AsyncSession, chat_id: int) -> str:
    today = date.today()
    week_ago = today - timedelta(days=7)

    today_result = await session.execute(
        select(func.sum(MessageStat.count)).where(MessageStat.chat_id == chat_id, MessageStat.day == today)
    )
    today_count = today_result.scalar() or 0

    week_result = await session.execute(
        select(func.sum(MessageStat.count), func.count(func.distinct(MessageStat.user_id))).where(
            MessageStat.chat_id == chat_id, MessageStat.day >= week_ago
        )
    )
    week_count, week_users = week_result.one()

    total_result = await session.execute(
        select(func.sum(MessageStat.count), func.count(func.distinct(MessageStat.user_id))).where(
            MessageStat.chat_id == chat_id
        )
    )
    total_count, total_users = total_result.one()

    return (
        "📊 <b>آمار گروه</b>\n\n"
        f"📅 امروز: {today_count} پیام\n"
        f"🗓 هفت روز اخیر: {week_count or 0} پیام از {week_users or 0} کاربر\n"
        f"📈 مجموع ثبت‌شده: {total_count or 0} پیام از {total_users or 0} کاربر\n\n"
        "ℹ️ آمار از زمان فعال شدن ربات در این گروه ثبت می‌شود."
    )


# =====================================================================
# سرویس‌ها: services/punishment_service.py
# =====================================================================
async def apply_punishment(
    bot: Bot,
    session: AsyncSession,
    chat_id: int,
    user_id: int,
    action: str,
    duration: Optional[timedelta] = None,
) -> None:
    until = (datetime.utcnow() + duration) if duration else None
    if action == "mute":
        await bot.restrict_chat_member(chat_id, user_id, permissions=MUTE_PERMISSIONS, until_date=until)
        mark_muted(chat_id, user_id)
        if duration:
            await add_timed_action(session, chat_id, user_id, "unmute", datetime.utcnow() + duration)
    elif action == "kick":
        await bot.ban_chat_member(chat_id, user_id)
        await bot.unban_chat_member(chat_id, user_id, only_if_banned=True)
        unmark_muted(chat_id, user_id)
    elif action == "ban":
        await bot.ban_chat_member(chat_id, user_id, until_date=until)
        unmark_muted(chat_id, user_id)
        if duration:
            await add_timed_action(session, chat_id, user_id, "unban", datetime.utcnow() + duration)
    else:
        raise ValueError(f"اکشن نامعتبر: {action}")


async def issue_warn(
    bot: Bot, session: AsyncSession, chat: Chat, chat_id: int, user: TgUser, reason: str
) -> None:
    warn = await add_warn(session, chat_id, user.id, reason)
    if warn.count >= chat.warn_limit:
        try:
            await apply_punishment(bot, session, chat_id, user.id, chat.warn_action)
        except (TelegramBadRequest, TelegramForbiddenError):
            logger.exception("اعمال مجازات اخطار ناموفق بود")
        await reset_warns(session, chat_id, user.id)
        try:
            await bot.send_message(
                chat_id,
                f"⚠️ {mention_html(user)} به‌دلیل «{html.escape(reason)}» و رسیدن به سقف اخطار "
                f"({chat.warn_limit}) {ACTION_FA[chat.warn_action]} شد.",
            )
        except (TelegramBadRequest, TelegramForbiddenError):
            pass
    else:
        try:
            await bot.send_message(
                chat_id,
                f"⚠️ {mention_html(user)} اخطار گرفت ({warn.count}/{chat.warn_limit})\nدلیل: {html.escape(reason)}",
            )
        except (TelegramBadRequest, TelegramForbiddenError):
            pass


async def apply_flood_action(bot: Bot, session: AsyncSession, chat: Chat, message: Message) -> None:
    user = message.from_user
    action = chat.flood_action
    try:
        duration = timedelta(minutes=10) if action == "mute" else None
        await apply_punishment(bot, session, message.chat.id, user.id, action, duration)
        await bot.send_message(
            message.chat.id,
            f"🌊 {mention_html(user)} به‌دلیل ارسال پیام زیاد (فلود) {ACTION_FA[action]} شد.",
        )
    except (TelegramBadRequest, TelegramForbiddenError):
        logger.exception("اعمال مجازات فلود ناموفق بود")


# =====================================================================
# میان‌افزارها: middlewares/db.py
# =====================================================================
class DbSessionMiddleware:
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        async with async_session_maker() as session:
            data["session"] = session
            return await handler(event, data)


# =====================================================================
# میان‌افزارها: middlewares/moderation.py
# =====================================================================
LOCK_FIELDS: List[Tuple[str, str]] = [
    ("lock_link", "🔗 لینک"),
    ("lock_forward", "↪️ فوروارد"),
    ("lock_mention", "👤 منشن"),
    ("lock_sticker", "🎭 استیکر"),
    ("lock_gif", "🎬 گیف"),
    ("lock_contact", "📇 مخاطب"),
    ("lock_location", "📍 لوکیشن"),
    ("lock_poll", "📊 نظرسنجی"),
    ("lock_photo", "🖼 عکس"),
    ("lock_video", "🎥 ویدیو"),
    ("lock_voice", "🎙 ویس"),
    ("lock_document", "📎 فایل"),
    ("lock_bot_join", "🤖 ورود ربات"),
    ("lock_deleted_accounts", "👻 اکانت حذف‌شده"),
]


def detect_lock_violation(message: Message, chat: Chat) -> Optional[str]:
    if chat.lock_forward and (
        message.forward_from or message.forward_from_chat or message.forward_sender_name
    ):
        return "فوروارد"

    if chat.lock_link:
        text = message.text or message.caption or ""
        if URL_RE.search(text) or AT_USERNAME_RE.search(text):
            return "لینک"
        entities = (message.entities or []) + (message.caption_entities or [])
        for ent in entities:
            if ent.type in ("url", "text_link", "mention", "text_mention"):
                return "لینک"

    if chat.lock_mention:
        entities = (message.entities or []) + (message.caption_entities or [])
        for ent in entities:
            if ent.type in ("mention", "text_mention"):
                return "منشن"

    if chat.lock_sticker and message.sticker:
        return "استیکر"
    if chat.lock_gif and message.animation:
        return "گیف"
    if chat.lock_contact and message.contact:
        return "مخاطب"
    if chat.lock_location and (message.location or message.venue):
        return "لوکیشن"
    if chat.lock_poll and message.poll:
        return "نظرسنجی"
    if chat.lock_photo and message.photo:
        return "عکس"
    if chat.lock_video and message.video:
        return "ویدیو"
    if chat.lock_voice and (message.voice or message.video_note):
        return "ویس"
    if chat.lock_document and message.document:
        return "فایل"
    return None


async def _safe_delete(message: Message) -> None:
    try:
        await message.delete()
    except (TelegramBadRequest, TelegramForbiddenError):
        pass


class ModerationMiddleware:
    """
    میان‌افزار اصلی نگهبانی گروه: به ترتیب فلود، قفل‌های محتوا و فیلتر کلمات رو
    چک می‌کنه. اگر پیامی نقض بشه، حذف می‌شه و اجرای هندلرهای بعدی متوقف می‌شه.
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        if not isinstance(event, Message) or event.chat.type not in ("group", "supergroup"):
            return await handler(event, data)
        if event.from_user is None or event.from_user.is_bot:
            return await handler(event, data)

        session: AsyncSession = data["session"]
        bot: Bot = data["bot"]

        chat = await get_or_create_chat(session, event.chat)

        # خط دفاعی اضافه: اگه کاربر الان سکوته ولی به هر دلیلی پیامش رد شده، پاکش کن
        if is_marked_muted(event.chat.id, event.from_user.id):
            await _safe_delete(event)
            return None

        if await is_chat_admin(bot, event.chat.id, event.from_user.id):
            await record_message_stat(session, event.chat.id, event.from_user.id)
            return await handler(event, data)

        # ۱) فلود / ضداسپم
        if chat.flood_enabled and check_flood(
            event.chat.id, event.from_user.id, chat.flood_limit, chat.flood_window
        ):
            await _safe_delete(event)
            await apply_flood_action(bot, session, chat, event)
            return None

        # ۲) قفل‌های محتوا
        violation = detect_lock_violation(event, chat)
        if violation:
            await _safe_delete(event)
            await issue_warn(bot, session, chat, event.chat.id, event.from_user, f"نقض قفل «{violation}»")
            return None

        # ۳) فیلتر کلمات ممنوعه
        if chat.filter_words_enabled and (event.text or event.caption):
            bad_word = await find_blacklisted_word(session, event.chat.id, event.text or event.caption or "")
            if bad_word:
                await _safe_delete(event)
                await issue_warn(bot, session, chat, event.chat.id, event.from_user, "استفاده از کلمه‌ی ممنوعه")
                return None

        await record_message_stat(session, event.chat.id, event.from_user.id)
        return await handler(event, data)


# =====================================================================
# فیلترها: filters/admin.py
# =====================================================================
class GroupAdminFilter(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        if message.chat.type not in ("group", "supergroup"):
            await message.reply("این دستور فقط داخل گروه قابل استفاده‌ست.")
            return False
        if message.from_user is None:
            return False
        if not await is_chat_admin(message.bot, message.chat.id, message.from_user.id):
            await message.reply("❌ این دستور فقط برای ادمین‌های گروهه.")
            return False
        return True


# =====================================================================
# کیبوردها: keyboards/settings.py
# =====================================================================
class SettingsMenuCallback(CallbackData, prefix="setmenu"):
    chat_id: int
    section: str


class ToggleCallback(CallbackData, prefix="toggle"):
    chat_id: int
    field: str


class BackCallback(CallbackData, prefix="setback"):
    chat_id: int


class SelectGroupCallback(CallbackData, prefix="selgrp"):
    chat_id: int


def settings_main_keyboard(chat_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="🔒 قفل‌ها و فیلترها", callback_data=SettingsMenuCallback(chat_id=chat_id, section="locks").pack())
    builder.button(text="⚠️ اخطار و مجازات", callback_data=SettingsMenuCallback(chat_id=chat_id, section="warns").pack())
    builder.button(text="🌊 ضداسپم", callback_data=SettingsMenuCallback(chat_id=chat_id, section="flood").pack())
    builder.button(text="👋 خوش‌آمدگویی", callback_data=SettingsMenuCallback(chat_id=chat_id, section="welcome").pack())
    builder.button(text="📊 آمار گروه", callback_data=SettingsMenuCallback(chat_id=chat_id, section="stats").pack())
    if config.miniapp_enabled and config.miniapp_public_url:
        builder.button(
            text="🖥 پنل مدیریتی Mini App",
            web_app=WebAppInfo(url=f"{config.miniapp_public_url}/?chat_id={chat_id}"),
        )
    builder.adjust(2, 2, 1, 1)
    return builder.as_markup()


def back_keyboard(chat_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ بازگشت", callback_data=BackCallback(chat_id=chat_id).pack())
    return builder.as_markup()


def locks_keyboard(chat: Chat):
    builder = InlineKeyboardBuilder()
    for field_name, label in LOCK_FIELDS:
        state = getattr(chat, field_name)
        emoji = "✅" if state else "❌"
        builder.button(text=f"{emoji} {label}", callback_data=ToggleCallback(chat_id=chat.id, field=field_name).pack())
    filter_state = "✅" if chat.filter_words_enabled else "❌"
    builder.button(
        text=f"{filter_state} 🧹 فیلتر کلمات",
        callback_data=ToggleCallback(chat_id=chat.id, field="filter_words_enabled").pack(),
    )
    builder.button(text="⬅️ بازگشت", callback_data=BackCallback(chat_id=chat.id).pack())
    builder.adjust(2)
    return builder.as_markup()


def flood_keyboard(chat: Chat):
    builder = InlineKeyboardBuilder()
    state = "✅ فعال" if chat.flood_enabled else "❌ غیرفعال"
    builder.button(text=f"وضعیت: {state}", callback_data=ToggleCallback(chat_id=chat.id, field="flood_enabled").pack())
    builder.button(text="⬅️ بازگشت", callback_data=BackCallback(chat_id=chat.id).pack())
    builder.adjust(1)
    return builder.as_markup()


def welcome_keyboard(chat: Chat):
    builder = InlineKeyboardBuilder()
    state = "✅ فعال" if chat.welcome_enabled else "❌ غیرفعال"
    builder.button(text=f"وضعیت: {state}", callback_data=ToggleCallback(chat_id=chat.id, field="welcome_enabled").pack())
    dj_state = "✅ فعال" if chat.delete_join_leave else "❌ غیرفعال"
    builder.button(
        text=f"حذف پیام ورود/خروج: {dj_state}",
        callback_data=ToggleCallback(chat_id=chat.id, field="delete_join_leave").pack(),
    )
    builder.button(text="⬅️ بازگشت", callback_data=BackCallback(chat_id=chat.id).pack())
    builder.adjust(1)
    return builder.as_markup()


# =====================================================================
# هندلرها: handlers (همه روی یک روتر مرکزی)
# =====================================================================
router = Router(name="core")


async def _resolve_target(message: Message) -> Optional[TgUser]:
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user
    return None


HELP_TEXT = (
    "🤖 <b>راهنمای ربات مدیریت گروه</b>\n\n"
    "<b>⚡️ فرمان سریع بدون اسلش (فقط ادمین، با ریپلای روی پیام فرد):</b>\n"
    "«کیک» یا «اخراج» — اخراج از گروه\n"
    "«بن» — مسدود کردن\n"
    "«رفع بن» — رفع مسدودیت\n"
    "«لال» یا «سکوت» — سکوت دائم؛ «سکوت 1h» یا «سکوت 2 ساعت» — سکوت زمان‌دار\n"
    "«رفع سکوت» — باز کردن سکوت\n"
    "«اخطار» یا «اخطار دلیلش» — ثبت اخطار\n"
    "«ادمین» — ارتقا به ادمین\n"
    "«برکناری» یا «عزل» — عزل از ادمینی\n"
    "«لیست ادمین» — لیست ادمین‌ها\n"
    "«پاکسازی» — پاک کردن از پیام ریپلای‌شده تا الان\n\n"
    "<b>مدیریت اعضا (دستوری):</b>\n"
    "/mute /tmute [مدت] — سکوت (ریپلای)\n"
    "/unmute — رفع سکوت (ریپلای)\n"
    "/ban /tban [مدت] — بن (ریپلای)\n"
    "/unban — رفع بن (ریپلای یا آیدی)\n"
    "/kick — اخراج (ریپلای)\n"
    "/warn [دلیل] — اخطار (ریپلای)\n"
    "/warns — تعداد اخطار\n"
    "/resetwarns — صفر کردن اخطار (ریپلای)\n\n"
    "<b>پاکسازی:</b>\n"
    "/purge — پاک کردن از پیام ریپلای‌شده تا الان\n"
    "/del — پاک کردن پیام ریپلای‌شده\n\n"
    "<b>ادمین‌ها:</b>\n"
    "/promote [عنوان] — ارتقا به ادمین (ریپلای)\n"
    "/demote — عزل ادمین (ریپلای)\n"
    "/adminlist — لیست ادمین‌ها\n\n"
    "<b>قفل‌ها (همه پیش‌فرض خاموش، از /settings روشن کن):</b>\n"
    "/locks — وضعیت قفل‌ها\n"
    "/lockall [مدت] — قفل کامل گروه\n"
    "/unlockall — باز کردن قفل کامل\n\n"
    "<b>فیلتر و لیست سیاه:</b>\n"
    "/addword /delword /words — مدیریت کلمات ممنوعه\n"
    "/blacklist add|del|list — مدیریت لیست سیاه کاربران\n\n"
    "<b>تنظیمات متنی:</b>\n"
    "/setwelcome /resetwelcome — متن خوش‌آمد؛ اگه بدون متن بفرستی، پیام بعدیت خودکار ذخیره می‌شه\n"
    "متغیرها: {name} {mention} {id} {chat}\n"
    "/setrules — تنظیم قوانین (همون‌طور، بدون متن هم کار می‌کنه)\n"
    "/rules — نمایش قوانین\n"
    "/setwarnlimit /setwarnaction — تنظیمات اخطار\n"
    "/setflood /setfloodaction — تنظیمات ضداسپم\n\n"
    "<b>سایر:</b>\n"
    "/settings — پنل تنظیمات با دکمه (+ Mini App)\n"
    "/stats — آمار گروه\n"
    "/id — نمایش آیدی چت/کاربر"
)


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    if message.chat.type == "private":
        me = await message.bot.get_me()
        builder = InlineKeyboardBuilder()
        builder.button(text="➕ افزودن به گروه", url=f"https://t.me/{me.username}?startgroup=true")
        builder.button(text="⚙️ مدیریت گروه‌هام", callback_data="show_my_groups")
        builder.button(text="📚 راهنمای کامل", callback_data="show_help")
        builder.adjust(1)
        await message.reply(
            "👋 <b>سلام، خوش اومدی!</b>\n\n"
            "من یه ربات حرفه‌ای مدیریت گروه‌م 🛡\n\n"
            "🌊 ضداسپم و ضدفلود\n"
            "🔒 قفل لینک/فوروارد/منشن/استیکر/گیف/عکس/ویدیو/...\n"
            "⚠️ سیستم اخطار با مجازات خودکار (میوت/اخراج/بن)\n"
            "🧹 فیلتر کلمات و لیست سیاه\n"
            "👋 خوش‌آمدگویی و قوانین اختصاصی\n"
            "📊 آمار گروه + پنل تنظیمات کامل (حتی از همین‌جا تو پیوی!)\n\n"
            "برای شروع، با دکمه‌ی زیر من رو به گروهت اضافه کن و "
            "<b>ادمین کامل</b> بده 👇",
            reply_markup=builder.as_markup(),
        )
    else:
        await message.reply("👋 سلام! برای مدیریت گروه از /settings یا /help استفاده کن.")


async def _get_admin_group_chats(bot: Bot, session: AsyncSession, user_id: int) -> List[Chat]:
    result = await session.execute(select(Chat))
    all_chats = result.scalars().all()
    admin_chats: List[Chat] = []
    for c in all_chats:
        if await is_chat_admin(bot, c.id, user_id):
            admin_chats.append(c)
    return admin_chats


def _groups_list_keyboard(chats: List[Chat]):
    builder = InlineKeyboardBuilder()
    for c in chats:
        builder.button(text=c.title or str(c.id), callback_data=SelectGroupCallback(chat_id=c.id).pack())
    builder.adjust(1)
    return builder.as_markup()


@router.callback_query(F.data == "show_help")
async def on_show_help(callback: CallbackQuery) -> None:
    await callback.message.answer(HELP_TEXT)
    await callback.answer()


@router.callback_query(F.data == "show_my_groups")
async def on_show_my_groups(callback: CallbackQuery, session: AsyncSession) -> None:
    admin_chats = await _get_admin_group_chats(callback.bot, session, callback.from_user.id)
    if not admin_chats:
        await callback.answer(
            "گروهی پیدا نکردم که هم من توش باشم هم تو ادمینش باشی. اول من رو به گروهت اضافه و ادمین کن.",
            show_alert=True,
        )
        return
    await callback.message.answer("کدوم گروه رو می‌خوای مدیریت کنی؟", reply_markup=_groups_list_keyboard(admin_chats))
    await callback.answer()


# --- مدیریت گروه از داخل پیوی (بدون نیاز به داخل گروه بودن) ---
@router.message(Command("settings", "تنظیمات"), F.chat.type == "private")
async def cmd_settings_private(message: Message, session: AsyncSession) -> None:
    admin_chats = await _get_admin_group_chats(message.bot, session, message.from_user.id)
    if not admin_chats:
        await message.reply(
            "گروهی پیدا نکردم که هم من توش باشم هم تو ادمینش باشی.\n"
            "اول من رو با /start به گروهت اضافه کن و ادمین کامل بده."
        )
        return
    await message.reply("کدوم گروه رو می‌خوای مدیریت کنی؟", reply_markup=_groups_list_keyboard(admin_chats))


@router.callback_query(SelectGroupCallback.filter())
async def on_select_group(callback: CallbackQuery, callback_data: SelectGroupCallback, session: AsyncSession) -> None:
    if not await is_chat_admin(callback.bot, callback_data.chat_id, callback.from_user.id):
        await callback.answer("❌ دیگه ادمین این گروه نیستی.", show_alert=True)
        return
    chat = await get_chat_by_id(session, callback_data.chat_id)
    if chat is None:
        await callback.answer("❌ این گروه پیدا نشد.", show_alert=True)
        return
    await callback.message.edit_text(
        f"⚙️ <b>پنل تنظیمات گروه «{html.escape(chat.title or str(chat.id))}»</b>\nیکی از بخش‌ها رو انتخاب کن:",
        reply_markup=settings_main_keyboard(chat.id),
    )
    await callback.answer()


@router.message(Command("help", "راهنما"))
async def cmd_help(message: Message) -> None:
    await message.answer(HELP_TEXT)


@router.message(Command("id"))
async def cmd_id(message: Message) -> None:
    target = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    await message.reply(
        f"🆔 آیدی چت: <code>{message.chat.id}</code>\n👤 آیدی کاربر: <code>{target.id if target else '-'}</code>"
    )


# --- مجازات‌ها ---
async def _apply_punish_to_target(
    message: Message,
    session: AsyncSession,
    target: TgUser,
    action: str,
    duration: Optional[timedelta] = None,
    duration_label: str = "",
    reason: str = "",
) -> bool:
    """اعتبارسنجی و اعمال مجازات روی یک هدف. اگه موفق بود True برمی‌گردونه."""
    if target.id == message.from_user.id:
        await message.reply("❌ نمی‌تونی این کارو رو خودت انجام بدی.")
        return False
    if target.id == message.bot.id:
        await message.reply("❌ نمی‌تونم رو خودم این کارو انجام بدم 🙂")
        return False
    if await is_chat_admin(message.bot, message.chat.id, target.id):
        await message.reply("❌ این فرد ادمین گروهه و نمی‌تونم این کارو روش انجام بدم.")
        return False

    try:
        await apply_punishment(message.bot, session, message.chat.id, target.id, action, duration)
    except (TelegramBadRequest, TelegramForbiddenError) as exc:
        await message.reply(f"❌ خطا: {exc}")
        return False

    dur_text = f" برای مدت {duration_label}" if duration_label else ""
    reason_text = f"\nدلیل: {html.escape(reason)}" if reason else ""
    await message.reply(f"✅ {mention_html(target)} {ACTION_FA[action]} شد{dur_text}.{reason_text}")
    return True


async def _handle_punish_command(
    message: Message, session: AsyncSession, action: str, require_duration: bool
) -> None:
    target = await _resolve_target(message)
    if target is None:
        await message.reply("❗️ باید روی پیام فرد موردنظر ریپلای کنی.")
        return

    args = (message.text or "").split(maxsplit=1)
    rest = args[1].strip() if len(args) > 1 else ""
    duration: Optional[timedelta] = None
    duration_label = ""
    reason = rest
    if rest:
        parts = rest.split(maxsplit=1)
        parsed = parse_duration(parts[0])
        if parsed is not None:
            duration = parsed
            duration_label = parts[0]
            reason = parts[1] if len(parts) > 1 else ""

    if require_duration and duration is None:
        await message.reply("❗️ باید مدت زمان رو مشخص کنی. مثال: /tmute 1h")
        return

    await _apply_punish_to_target(message, session, target, action, duration, duration_label, reason)


@router.message(Command("mute"), GroupAdminFilter())
async def cmd_mute(message: Message, session: AsyncSession) -> None:
    await _handle_punish_command(message, session, "mute", require_duration=False)


@router.message(Command("tmute"), GroupAdminFilter())
async def cmd_tmute(message: Message, session: AsyncSession) -> None:
    await _handle_punish_command(message, session, "mute", require_duration=True)


@router.message(Command("ban"), GroupAdminFilter())
async def cmd_ban(message: Message, session: AsyncSession) -> None:
    await _handle_punish_command(message, session, "ban", require_duration=False)


@router.message(Command("tban"), GroupAdminFilter())
async def cmd_tban(message: Message, session: AsyncSession) -> None:
    await _handle_punish_command(message, session, "ban", require_duration=True)


@router.message(Command("kick"), GroupAdminFilter())
async def cmd_kick(message: Message, session: AsyncSession) -> None:
    await _handle_punish_command(message, session, "kick", require_duration=False)


async def _unmute_target(message: Message, target: TgUser) -> None:
    try:
        await message.bot.restrict_chat_member(message.chat.id, target.id, permissions=DEFAULT_PERMISSIONS)
    except (TelegramBadRequest, TelegramForbiddenError) as exc:
        await message.reply(f"❌ خطا: {exc}")
        return
    unmark_muted(message.chat.id, target.id)
    await message.reply(f"🔊 سکوت {mention_html(target)} برداشته شد.")


@router.message(Command("unmute"), GroupAdminFilter())
async def cmd_unmute(message: Message) -> None:
    target = await _resolve_target(message)
    if target is None:
        await message.reply("❗️ روی پیام فرد موردنظر ریپلای کن.")
        return
    await _unmute_target(message, target)


UNBAN_NOTE = (
    "\nℹ️ توجه: تلگرام به هیچ ربات‌ای اجازه نمی‌ده کاربر رو زوری به گروه برگردونه؛ "
    "این محدودیت خود پلتفرم تلگرامه. کاربر فقط با لینک دعوت گروه می‌تونه خودش دوباره عضو بشه."
)


async def _unban_target_id(message: Message, target_id: int) -> None:
    try:
        await message.bot.unban_chat_member(message.chat.id, target_id, only_if_banned=True)
    except (TelegramBadRequest, TelegramForbiddenError) as exc:
        await message.reply(f"❌ خطا: {exc}")
        return
    await message.reply(f"✅ رفع مسدودیت کاربر <code>{target_id}</code> انجام شد.{UNBAN_NOTE}")


@router.message(Command("unban"), GroupAdminFilter())
async def cmd_unban(message: Message) -> None:
    target = await _resolve_target(message)
    args = (message.text or "").split(maxsplit=1)
    target_id = target.id if target else None
    if target_id is None and len(args) > 1 and args[1].strip().lstrip("-").isdigit():
        target_id = int(args[1].strip())
    if target_id is None:
        await message.reply("❗️ روی پیام فرد ریپلای کن یا آیدی عددی بده.")
        return
    await _unban_target_id(message, target_id)


@router.message(Command("warn"), GroupAdminFilter())
async def cmd_warn(message: Message, session: AsyncSession) -> None:
    target = await _resolve_target(message)
    if target is None:
        await message.reply("❗️ روی پیام فرد موردنظر ریپلای کن.")
        return
    if await is_chat_admin(message.bot, message.chat.id, target.id):
        await message.reply("❌ نمی‌تونم به ادمین اخطار بدم.")
        return
    args = (message.text or "").split(maxsplit=1)
    reason = args[1] if len(args) > 1 else "بدون دلیل"
    chat = await get_or_create_chat(session, message.chat)
    await issue_warn(message.bot, session, chat, message.chat.id, target, reason)


@router.message(Command("warns"))
async def cmd_warns(message: Message, session: AsyncSession) -> None:
    target = await _resolve_target(message) or message.from_user
    count = await get_warn_count(session, message.chat.id, target.id)
    chat = await get_or_create_chat(session, message.chat)
    await message.reply(f"⚠️ {mention_html(target)}: {count}/{chat.warn_limit} اخطار")


@router.message(Command("resetwarns"), GroupAdminFilter())
async def cmd_resetwarns(message: Message, session: AsyncSession) -> None:
    target = await _resolve_target(message)
    if target is None:
        await message.reply("❗️ روی پیام فرد موردنظر ریپلای کن.")
        return
    await reset_warns(session, message.chat.id, target.id)
    await message.reply(f"✅ اخطارهای {mention_html(target)} صفر شد.")


# --- پاکسازی ---
async def _purge_range(message: Message, start_id: int, end_id: int) -> int:
    """حذف دسته‌جمعی پیام‌ها با API بولک تلگرام (سریع و بدون محدودیت نرخ سنگین)."""
    all_ids = list(range(start_id, end_id + 1))
    deleted = 0
    for i in range(0, len(all_ids), 100):
        chunk = all_ids[i : i + 100]
        try:
            await message.bot.delete_messages(chat_id=message.chat.id, message_ids=chunk)
            deleted += len(chunk)
        except (TelegramBadRequest, TelegramForbiddenError):
            # اگه حذف دسته‌ای شکست خورد، تک‌تک امتحان کن
            for mid in chunk:
                try:
                    await message.bot.delete_message(message.chat.id, mid)
                    deleted += 1
                except (TelegramBadRequest, TelegramForbiddenError):
                    continue
    return deleted


@router.message(Command("purge"), GroupAdminFilter())
async def cmd_purge(message: Message) -> None:
    if not message.reply_to_message:
        await message.reply("❗️ روی پیامی که می‌خوای پاکسازی از اونجا شروع بشه ریپلای کن.")
        return
    start_id = message.reply_to_message.message_id
    end_id = message.message_id
    if end_id - start_id > 2000:
        await message.reply("❗️ بازه‌ی پاکسازی خیلی بزرگه (حداکثر ۲۰۰۰ پیام).")
        return
    deleted = await _purge_range(message, start_id, end_id)
    notice = await message.answer(f"🧹 {deleted} پیام پاک شد.")
    await asyncio.sleep(4)
    try:
        await notice.delete()
    except (TelegramBadRequest, TelegramForbiddenError):
        pass


@router.message(Command("del"), GroupAdminFilter())
async def cmd_del(message: Message) -> None:
    if not message.reply_to_message:
        await message.reply("❗️ روی پیامی که می‌خوای حذف بشه ریپلای کن.")
        return
    try:
        await message.reply_to_message.delete()
        await message.delete()
    except (TelegramBadRequest, TelegramForbiddenError):
        await message.reply("❌ نتونستم پیام رو حذف کنم.")


# =====================================================================
# دستورهای طبیعی فارسی (بدون اسلش) — فقط با ریپلای روی پیام کاربر
# =====================================================================
NL_EXACT_TRIGGERS: Dict[str, str] = {
    "کیک": "kick", "سیک": "kick", "اخراج": "kick",
    "بن": "ban",
    "رفع بن": "unban", "آنبن": "unban", "رفع مسدودیت": "unban",
    "رفع سکوت": "unmute", "رفع لال": "unmute", "باز کردن سکوت": "unmute",
    "ادمین": "promote", "ادمین کن": "promote",
    "برداشتن ادمین": "demote", "برکناری": "demote", "عزل ادمین": "demote", "عزل": "demote",
    "لیست ادمین": "adminlist", "لیست ادمینا": "adminlist", "لیست ادمین ها": "adminlist",
    "پاکسازی": "purge",
    "لال": "mute", "سکوت": "mute",
    "اخطار": "warn",
}
NL_PREFIX_TRIGGERS: Dict[str, str] = {"لال": "mute", "سکوت": "mute", "اخطار": "warn"}


def _normalize_fa(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


class NaturalTriggerFilter(BaseFilter):
    """فقط پیام‌های ریپلای‌شده که دقیقاً یکی از کلمات فرمان فارسی‌ان رو می‌گیره."""

    async def __call__(self, message: Message) -> bool:
        if not message.reply_to_message or not message.text:
            return False
        norm = _normalize_fa(message.text)
        if not norm or norm.startswith("/"):
            return False
        if norm in NL_EXACT_TRIGGERS:
            return True
        first_word = norm.split(maxsplit=1)[0]
        return first_word in NL_PREFIX_TRIGGERS


@router.message(NaturalTriggerFilter())
async def cmd_natural_language_actions(message: Message, session: AsyncSession) -> None:
    # فقط ادمین‌های گروه می‌تونن این کلمات رو فرمان حساب کنن؛ برای بقیه بی‌سروصدا نادیده می‌گیریم
    if not await is_chat_admin(message.bot, message.chat.id, message.from_user.id):
        return

    norm = _normalize_fa(message.text or "")
    action = NL_EXACT_TRIGGERS.get(norm)
    rest = ""
    if action is None:
        parts = norm.split(maxsplit=1)
        first = parts[0]
        rest = parts[1] if len(parts) > 1 else ""
        action = NL_PREFIX_TRIGGERS.get(first)
    if action is None:
        return

    if action == "adminlist":
        await cmd_adminlist(message)
        return
    if action == "purge":
        await cmd_purge(message)
        return

    target = await _resolve_target(message)
    if target is None:
        await message.reply("❗️ باید روی پیام فرد موردنظر ریپلای کنی.")
        return

    if action in ("kick", "ban"):
        await _apply_punish_to_target(message, session, target, action)
        return

    if action == "mute":
        duration = parse_duration_fa(rest) if rest else None
        duration_label = rest if duration else ""
        await _apply_punish_to_target(message, session, target, "mute", duration, duration_label)
        return

    if action == "unmute":
        await _unmute_target(message, target)
        return

    if action == "unban":
        await _unban_target_id(message, target.id)
        return

    if action == "promote":
        await _promote_target(message, target)
        return

    if action == "demote":
        await _demote_target(message, target)
        return

    if action == "warn":
        if await is_chat_admin(message.bot, message.chat.id, target.id):
            await message.reply("❌ نمی‌تونم به ادمین اخطار بدم.")
            return
        reason = rest or "بدون دلیل"
        chat = await get_or_create_chat(session, message.chat)
        await issue_warn(message.bot, session, chat, message.chat.id, target, reason)
        return


# --- ادمین‌ها ---
async def _promote_target(message: Message, target: TgUser, custom_title: Optional[str] = None) -> None:
    try:
        await message.bot.promote_chat_member(
            message.chat.id,
            target.id,
            can_change_info=True,
            can_delete_messages=True,
            can_invite_users=True,
            can_restrict_members=True,
            can_pin_messages=True,
            can_manage_chat=True,
            can_manage_video_chats=True,
        )
        if custom_title:
            await message.bot.set_chat_administrator_custom_title(message.chat.id, target.id, custom_title[:16])
    except (TelegramBadRequest, TelegramForbiddenError) as exc:
        await message.reply(f"❌ خطا: {exc}")
        return
    invalidate_admin_cache(message.chat.id)
    await message.reply(f"⭐️ {mention_html(target)} ادمین شد.")


async def _demote_target(message: Message, target: TgUser) -> None:
    try:
        await message.bot.promote_chat_member(
            message.chat.id,
            target.id,
            can_change_info=False,
            can_delete_messages=False,
            can_invite_users=False,
            can_restrict_members=False,
            can_pin_messages=False,
            can_manage_chat=False,
            can_manage_video_chats=False,
        )
    except (TelegramBadRequest, TelegramForbiddenError) as exc:
        await message.reply(f"❌ خطا: {exc}")
        return
    invalidate_admin_cache(message.chat.id)
    await message.reply(f"⬇️ {mention_html(target)} از ادمینی عزل شد.")


@router.message(Command("promote"), GroupAdminFilter())
async def cmd_promote(message: Message) -> None:
    target = await _resolve_target(message)
    if target is None:
        await message.reply("❗️ روی پیام فرد موردنظر ریپلای کن.")
        return
    args = (message.text or "").split(maxsplit=1)
    custom_title = args[1] if len(args) > 1 else None
    await _promote_target(message, target, custom_title)


@router.message(Command("demote"), GroupAdminFilter())
async def cmd_demote(message: Message) -> None:
    target = await _resolve_target(message)
    if target is None:
        await message.reply("❗️ روی پیام فرد موردنظر ریپلای کن.")
        return
    await _demote_target(message, target)


@router.message(Command("adminlist", "admins"))
async def cmd_adminlist(message: Message) -> None:
    if message.chat.type not in ("group", "supergroup"):
        return
    try:
        admins = await message.bot.get_chat_administrators(message.chat.id)
    except (TelegramBadRequest, TelegramForbiddenError):
        await message.reply("❌ نتونستم لیست ادمین‌ها رو بگیرم.")
        return
    lines = ["👮 <b>لیست ادمین‌های گروه:</b>\n"]
    for admin in admins:
        role = "👑 مالک" if admin.status == "creator" else "🛡 ادمین"
        lines.append(f"{role} — {mention_html(admin.user)}")
    await message.answer("\n".join(lines))


# --- قفل‌ها ---
@router.message(Command("locks"), GroupAdminFilter())
async def cmd_locks(message: Message, session: AsyncSession) -> None:
    chat = await get_or_create_chat(session, message.chat)
    lines = ["🔒 <b>وضعیت قفل‌ها:</b>\n"]
    for field_name, label in LOCK_FIELDS:
        state = "✅ فعال" if getattr(chat, field_name) else "❌ غیرفعال"
        lines.append(f"{label}: {state}")
    words_state = "✅ فعال" if chat.filter_words_enabled else "❌ غیرفعال"
    lines.append(f"\n🧹 فیلتر کلمات: {words_state}")
    if chat.lock_all_until:
        lines.append(f"\n🔒 گروه در حال حاضر کاملاً قفله تا: {chat.lock_all_until.strftime('%Y-%m-%d %H:%M UTC')}")
    await message.reply("\n".join(lines))


@router.message(Command("lockall"), GroupAdminFilter())
async def cmd_lockall(message: Message, session: AsyncSession) -> None:
    args = (message.text or "").split(maxsplit=1)
    duration = parse_duration(args[1]) if len(args) > 1 else None
    try:
        await message.bot.set_chat_permissions(message.chat.id, permissions=MUTE_PERMISSIONS)
    except (TelegramBadRequest, TelegramForbiddenError) as exc:
        await message.reply(f"❌ خطا: {exc}")
        return

    chat = await get_or_create_chat(session, message.chat)
    chat.lock_all_until = (datetime.utcnow() + duration) if duration else None
    session.add(chat)
    await session.commit()

    if duration:
        await add_timed_action(session, message.chat.id, 0, "unlockall", datetime.utcnow() + duration)
        await message.answer(f"🔒 گروه برای {args[1]} کاملاً قفل شد. فقط ادمین‌ها می‌تونن پیام بدن.")
    else:
        await message.answer("🔒 گروه کاملاً قفل شد. فقط ادمین‌ها می‌تونن پیام بدن.\nبرای باز کردن: /unlockall")


@router.message(Command("unlockall"), GroupAdminFilter())
async def cmd_unlockall(message: Message, session: AsyncSession) -> None:
    try:
        await message.bot.set_chat_permissions(message.chat.id, permissions=DEFAULT_PERMISSIONS)
    except (TelegramBadRequest, TelegramForbiddenError) as exc:
        await message.reply(f"❌ خطا: {exc}")
        return
    chat = await get_or_create_chat(session, message.chat)
    chat.lock_all_until = None
    session.add(chat)
    await session.commit()
    await message.answer("🔓 قفل کامل گروه برداشته شد.")


# --- فیلتر کلمات ---
@router.message(Command("addword"), GroupAdminFilter())
async def cmd_addword(message: Message, session: AsyncSession) -> None:
    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2:
        await message.reply("استفاده: /addword کلمه")
        return
    ok = await add_blacklist_word(session, message.chat.id, args[1])
    await message.reply("✅ کلمه اضافه شد." if ok else "این کلمه از قبل تو لیست بود یا نامعتبره.")


@router.message(Command("delword"), GroupAdminFilter())
async def cmd_delword(message: Message, session: AsyncSession) -> None:
    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2:
        await message.reply("استفاده: /delword کلمه")
        return
    ok = await remove_blacklist_word(session, message.chat.id, args[1])
    await message.reply("✅ حذف شد." if ok else "این کلمه تو لیست نبود.")


@router.message(Command("words"))
async def cmd_words(message: Message, session: AsyncSession) -> None:
    words = await list_blacklist_words(session, message.chat.id)
    if not words:
        await message.reply("لیست کلمات ممنوعه خالیه.")
        return
    await message.reply("🚫 <b>کلمات ممنوعه:</b>\n" + "، ".join(html.escape(w) for w in words))


# --- لیست سیاه کاربران ---
@router.message(Command("blacklist"), GroupAdminFilter())
async def cmd_blacklist(message: Message, session: AsyncSession) -> None:
    args = (message.text or "").split(maxsplit=2)
    if len(args) < 2:
        await message.reply(
            "استفاده:\n/blacklist add (ریپلای یا آیدی عددی) [دلیل]\n"
            "/blacklist del آیدی_عددی\n/blacklist list"
        )
        return
    sub = args[1].lower()

    if sub == "list":
        ids = await list_blacklist_users(session, message.chat.id)
        if not ids:
            await message.reply("لیست سیاه خالیه.")
            return
        await message.reply("🚫 <b>لیست سیاه:</b>\n" + "\n".join(f"• <code>{uid}</code>" for uid in ids))
        return

    target = await _resolve_target(message)
    target_id = target.id if target else None
    extra = args[2].strip() if len(args) > 2 else ""
    if target_id is None and extra:
        parts = extra.split(maxsplit=1)
        if parts and parts[0].isdigit():
            target_id = int(parts[0])
            extra = parts[1] if len(parts) > 1 else ""

    if target_id is None:
        await message.reply("❗️ باید ریپلای کنی یا آیدی عددی کاربر رو بدی.")
        return

    if sub == "add":
        await add_blacklist_user(session, message.chat.id, target_id, extra)
        try:
            await message.bot.ban_chat_member(message.chat.id, target_id)
        except (TelegramBadRequest, TelegramForbiddenError):
            pass
        await message.reply(f"🚫 کاربر <code>{target_id}</code> به لیست سیاه اضافه و بن شد.")
    elif sub == "del":
        removed = await remove_blacklist_user(session, message.chat.id, target_id)
        await message.reply("✅ از لیست سیاه حذف شد." if removed else "این کاربر تو لیست سیاه نبود.")
    else:
        await message.reply("دستور نامعتبره. از add / del / list استفاده کن.")


# --- خوشامد و قوانین ---
@router.message(Command("setwelcome"), GroupAdminFilter())
async def cmd_setwelcome(message: Message, session: AsyncSession) -> None:
    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2:
        _pending_text_setup[(message.chat.id, message.from_user.id)] = "welcome"
        await message.reply(
            "✍️ باشه، حالا فقط متن خوش‌آمدگویی جدید رو همین‌جا بفرست (بدون هیچ دستوری) "
            "تا خودکار ذخیره‌ش کنم.\nمتغیرهای قابل‌استفاده: {name} {mention} {id} {chat}"
        )
        return
    chat = await get_or_create_chat(session, message.chat)
    chat.welcome_text = args[1]
    session.add(chat)
    await session.commit()
    await message.reply("✅ متن خوش‌آمدگویی بروزرسانی شد.")


@router.message(Command("resetwelcome"), GroupAdminFilter())
async def cmd_resetwelcome(message: Message, session: AsyncSession) -> None:
    chat = await get_or_create_chat(session, message.chat)
    chat.welcome_text = DEFAULT_WELCOME
    session.add(chat)
    await session.commit()
    await message.reply("✅ متن خوش‌آمدگویی به حالت پیش‌فرض برگشت.")


@router.message(Command("rules"))
async def cmd_rules(message: Message, session: AsyncSession) -> None:
    chat = await get_or_create_chat(session, message.chat)
    if not chat.rules_text:
        await message.reply("📜 قوانینی برای این گروه تنظیم نشده.")
        return
    await message.reply(f"📜 <b>قوانین گروه:</b>\n\n{chat.rules_text}")


@router.message(Command("setrules"), GroupAdminFilter())
async def cmd_setrules(message: Message, session: AsyncSession) -> None:
    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2:
        _pending_text_setup[(message.chat.id, message.from_user.id)] = "rules"
        await message.reply("✍️ باشه، حالا فقط متن قوانین جدید رو همین‌جا بفرست تا خودکار ذخیره‌ش کنم.")
        return
    chat = await get_or_create_chat(session, message.chat)
    chat.rules_text = args[1]
    session.add(chat)
    await session.commit()
    await message.reply("✅ قوانین بروزرسانی شد.")


# --- گرفتن متن خوش‌آمدگویی/قوانین وقتی ادمین منتظرشه (بدون نیاز به تکرار دستور) ---
class PendingTextSetupFilter(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        if not message.text or message.text.startswith("/"):
            return False
        if message.chat.type not in ("group", "supergroup"):
            return False
        return (message.chat.id, message.from_user.id) in _pending_text_setup


@router.message(PendingTextSetupFilter())
async def catch_pending_text_setup(message: Message, session: AsyncSession) -> None:
    key = (message.chat.id, message.from_user.id)
    kind = _pending_text_setup.get(key)
    if kind is None:
        return
    if not await is_chat_admin(message.bot, message.chat.id, message.from_user.id):
        return

    chat = await get_or_create_chat(session, message.chat)
    if kind == "welcome":
        chat.welcome_text = message.text
        session.add(chat)
        await session.commit()
        await message.reply("✅ متن خوش‌آمدگویی ذخیره شد.")
    elif kind == "rules":
        chat.rules_text = message.text
        session.add(chat)
        await session.commit()
        await message.reply("✅ قوانین ذخیره شد.")
    del _pending_text_setup[key]


# --- اخطار و ضداسپم (تنظیمات متنی) ---
@router.message(Command("setwarnlimit"), GroupAdminFilter())
async def cmd_setwarnlimit(message: Message, session: AsyncSession) -> None:
    args = (message.text or "").split()
    if len(args) < 2 or not args[1].isdigit() or not (1 <= int(args[1]) <= 50):
        await message.reply("استفاده: /setwarnlimit عدد (بین ۱ تا ۵۰، مثلاً 3)")
        return
    chat = await get_or_create_chat(session, message.chat)
    chat.warn_limit = int(args[1])
    session.add(chat)
    await session.commit()
    await message.reply(f"✅ سقف اخطار روی {chat.warn_limit} تنظیم شد.")


@router.message(Command("setwarnaction"), GroupAdminFilter())
async def cmd_setwarnaction(message: Message, session: AsyncSession) -> None:
    args = (message.text or "").split()
    if len(args) < 2 or args[1].lower() not in ("mute", "kick", "ban"):
        await message.reply("استفاده: /setwarnaction mute یا kick یا ban")
        return
    chat = await get_or_create_chat(session, message.chat)
    chat.warn_action = args[1].lower()
    session.add(chat)
    await session.commit()
    await message.reply(f"✅ مجازات رسیدن به سقف اخطار: {ACTION_FA[chat.warn_action]}")


@router.message(Command("setflood"), GroupAdminFilter())
async def cmd_setflood(message: Message, session: AsyncSession) -> None:
    args = (message.text or "").split()
    if len(args) < 3 or not args[1].isdigit() or not args[2].isdigit():
        await message.reply("استفاده: /setflood تعداد_پیام ثانیه (مثلاً /setflood 6 8)")
        return
    chat = await get_or_create_chat(session, message.chat)
    chat.flood_limit = max(2, min(50, int(args[1])))
    chat.flood_window = max(2, min(120, int(args[2])))
    session.add(chat)
    await session.commit()
    await message.reply(f"✅ ضداسپم روی {chat.flood_limit} پیام در {chat.flood_window} ثانیه تنظیم شد.")


@router.message(Command("setfloodaction"), GroupAdminFilter())
async def cmd_setfloodaction(message: Message, session: AsyncSession) -> None:
    args = (message.text or "").split()
    if len(args) < 2 or args[1].lower() not in ("mute", "kick", "ban"):
        await message.reply("استفاده: /setfloodaction mute یا kick یا ban")
        return
    chat = await get_or_create_chat(session, message.chat)
    chat.flood_action = args[1].lower()
    session.add(chat)
    await session.commit()
    await message.reply(f"✅ مجازات فلود: {ACTION_FA[chat.flood_action]}")


# --- آمار ---
@router.message(Command("stats"))
async def cmd_stats(message: Message, session: AsyncSession) -> None:
    if message.chat.type not in ("group", "supergroup"):
        return
    await message.answer(await build_stats_text(session, message.chat.id))


# --- پنل تنظیمات ---
@router.message(Command("settings", "تنظیمات"), GroupAdminFilter())
async def cmd_settings(message: Message, session: AsyncSession) -> None:
    await get_or_create_chat(session, message.chat)
    await message.answer(
        "⚙️ <b>پنل تنظیمات گروه</b>\nیکی از بخش‌ها رو انتخاب کن:",
        reply_markup=settings_main_keyboard(message.chat.id),
    )


@router.callback_query(SettingsMenuCallback.filter())
async def on_settings_section(
    callback: CallbackQuery, callback_data: SettingsMenuCallback, session: AsyncSession
) -> None:
    if not await is_chat_admin(callback.bot, callback_data.chat_id, callback.from_user.id):
        await callback.answer("❌ فقط ادمین‌های گروه.", show_alert=True)
        return
    chat = await get_chat_by_id(session, callback_data.chat_id)
    if chat is None:
        await callback.answer("❌ خطا در یافتن تنظیمات گروه.", show_alert=True)
        return

    if callback_data.section == "locks":
        await callback.message.edit_text(
            "🔒 <b>مدیریت قفل‌ها و فیلترها</b>\nروی هرکدوم بزن تا فعال/غیرفعال بشه:",
            reply_markup=locks_keyboard(chat),
        )
    elif callback_data.section == "warns":
        await callback.message.edit_text(
            f"⚠️ <b>اخطار و مجازات</b>\n\nسقف اخطار: <b>{chat.warn_limit}</b>\n"
            f"مجازات: <b>{ACTION_FA[chat.warn_action]}</b>\n\n"
            "برای تغییر سقف: <code>/setwarnlimit عدد</code>\n"
            "برای تغییر مجازات: <code>/setwarnaction mute|kick|ban</code>",
            reply_markup=back_keyboard(chat.id),
        )
    elif callback_data.section == "flood":
        await callback.message.edit_text(
            f"🌊 <b>ضداسپم</b>\n\nوضعیت: {'فعال ✅' if chat.flood_enabled else 'غیرفعال ❌'}\n"
            f"سقف: <b>{chat.flood_limit}</b> پیام در <b>{chat.flood_window}</b> ثانیه\n"
            f"مجازات: <b>{ACTION_FA[chat.flood_action]}</b>\n\n"
            "برای تغییر سقف: <code>/setflood تعداد ثانیه</code>\n"
            "برای تغییر مجازات: <code>/setfloodaction mute|kick|ban</code>",
            reply_markup=flood_keyboard(chat),
        )
    elif callback_data.section == "welcome":
        await callback.message.edit_text(
            f"👋 <b>خوش‌آمدگویی</b>\n\nوضعیت: {'فعال ✅' if chat.welcome_enabled else 'غیرفعال ❌'}\n\n"
            f"متن فعلی:\n{html.escape(chat.welcome_text)}\n\n"
            "برای تغییر متن: <code>/setwelcome متن_جدید</code>",
            reply_markup=welcome_keyboard(chat),
        )
    elif callback_data.section == "stats":
        await callback.message.edit_text(
            await build_stats_text(session, callback_data.chat_id), reply_markup=back_keyboard(chat.id)
        )
    await callback.answer()


@router.callback_query(ToggleCallback.filter())
async def on_toggle(callback: CallbackQuery, callback_data: ToggleCallback, session: AsyncSession) -> None:
    if not await is_chat_admin(callback.bot, callback_data.chat_id, callback.from_user.id):
        await callback.answer("❌ فقط ادمین‌های گروه به این دسترسی دارن.", show_alert=True)
        return
    chat = await get_chat_by_id(session, callback_data.chat_id)
    valid_fields = {f for f, _ in LOCK_FIELDS} | {
        "filter_words_enabled",
        "flood_enabled",
        "welcome_enabled",
        "delete_join_leave",
    }
    if chat is None or callback_data.field not in valid_fields:
        await callback.answer("❌ خطا", show_alert=True)
        return

    setattr(chat, callback_data.field, not getattr(chat, callback_data.field))
    session.add(chat)
    await session.commit()
    await callback.answer("✅ بروزرسانی شد.")

    if callback_data.field.startswith("lock_") or callback_data.field == "filter_words_enabled":
        await callback.message.edit_reply_markup(reply_markup=locks_keyboard(chat))
    elif callback_data.field == "flood_enabled":
        await callback.message.edit_reply_markup(reply_markup=flood_keyboard(chat))
    elif callback_data.field in ("welcome_enabled", "delete_join_leave"):
        await callback.message.edit_reply_markup(reply_markup=welcome_keyboard(chat))


@router.callback_query(BackCallback.filter())
async def on_back(callback: CallbackQuery, callback_data: BackCallback) -> None:
    await callback.message.edit_text(
        "⚙️ <b>پنل تنظیمات گروه</b>\nیکی از بخش‌ها رو انتخاب کن:",
        reply_markup=settings_main_keyboard(callback_data.chat_id),
    )
    await callback.answer()


# --- رویدادهای عضویت ---
@router.message(F.new_chat_members)
async def on_new_members(message: Message, session: AsyncSession) -> None:
    chat = await get_or_create_chat(session, message.chat)
    inviter_is_admin = False
    if message.from_user is not None:
        inviter_is_admin = await is_chat_admin(message.bot, message.chat.id, message.from_user.id)

    for member in message.new_chat_members:
        if member.id == message.bot.id:
            continue

        if member.is_bot:
            if chat.lock_bot_join and not inviter_is_admin:
                try:
                    await message.bot.ban_chat_member(message.chat.id, member.id)
                    await message.bot.unban_chat_member(message.chat.id, member.id, only_if_banned=True)
                except (TelegramBadRequest, TelegramForbiddenError):
                    pass
                continue

        if await is_user_blacklisted(session, message.chat.id, member.id):
            try:
                await message.bot.ban_chat_member(message.chat.id, member.id)
            except (TelegramBadRequest, TelegramForbiddenError):
                pass
            continue

        if chat.lock_deleted_accounts and looks_like_deleted_account(member):
            try:
                await message.bot.ban_chat_member(message.chat.id, member.id)
                await message.bot.unban_chat_member(message.chat.id, member.id, only_if_banned=True)
            except (TelegramBadRequest, TelegramForbiddenError):
                pass
            continue

        if chat.welcome_enabled and not member.is_bot:
            text = _render_welcome(chat.welcome_text, member, message.chat.title or "")
            try:
                await message.answer(text)
            except (TelegramBadRequest, TelegramForbiddenError):
                pass

    if chat.delete_join_leave:
        try:
            await message.delete()
        except (TelegramBadRequest, TelegramForbiddenError):
            pass


@router.message(F.left_chat_member)
async def on_left_member(message: Message, session: AsyncSession) -> None:
    chat = await get_or_create_chat(session, message.chat)
    if chat.delete_join_leave:
        try:
            await message.delete()
        except (TelegramBadRequest, TelegramForbiddenError):
            pass


@router.my_chat_member()
async def on_bot_membership_change(update: ChatMemberUpdated, session: AsyncSession) -> None:
    invalidate_admin_cache(update.chat.id)
    old_status = update.old_chat_member.status
    new_status = update.new_chat_member.status
    if new_status in ("member", "administrator") and old_status in ("left", "kicked"):
        await get_or_create_chat(session, update.chat)
        try:
            await update.bot.send_message(
                update.chat.id,
                "✅ سلام! فعال شدم.\nبرای مدیریت کامل گروه از دستور /settings استفاده کن (فقط ادمین‌ها).\n"
                "⚠️ حتماً من رو ادمین کامل کن (حداقل با اختیار حذف پیام، محدود کردن اعضا و بن) "
                "تا بتونم قفل‌ها، اخطار، میوت و بن رو مدیریت کنم.",
            )
        except (TelegramBadRequest, TelegramForbiddenError):
            pass


@router.chat_member()
async def on_chat_member_change(update: ChatMemberUpdated) -> None:
    # هر تغییری در وضعیت اعضا (مثل ارتقا/عزل ادمین توسط شخص دیگه) کش رو باطل می‌کنه
    invalidate_admin_cache(update.chat.id)


# =====================================================================
# زمان‌بند: scheduler.py (اجرای خودکار پایان میوت/بن/قفل موقت)
# =====================================================================
async def scheduler_loop(bot: Bot) -> None:
    while True:
        try:
            async with async_session_maker() as session:
                now = datetime.utcnow()
                result = await session.execute(
                    select(TimedAction).where(TimedAction.applied.is_(False), TimedAction.expires_at <= now)
                )
                for action in result.scalars().all():
                    try:
                        if action.action == "unmute":
                            await bot.restrict_chat_member(action.chat_id, action.user_id, permissions=DEFAULT_PERMISSIONS)
                            unmark_muted(action.chat_id, action.user_id)
                        elif action.action == "unban":
                            await bot.unban_chat_member(action.chat_id, action.user_id, only_if_banned=True)
                        elif action.action == "unlockall":
                            await bot.set_chat_permissions(action.chat_id, permissions=DEFAULT_PERMISSIONS)
                            chat = await get_chat_by_id(session, action.chat_id)
                            if chat is not None:
                                chat.lock_all_until = None
                                session.add(chat)
                            try:
                                await bot.send_message(action.chat_id, "🔓 قفل کامل گروه به‌صورت خودکار برداشته شد.")
                            except (TelegramBadRequest, TelegramForbiddenError):
                                pass
                    except (TelegramBadRequest, TelegramForbiddenError):
                        logger.exception("خطا در اجرای اکشن زمان‌دار #%s", action.id)
                    action.applied = True
                    session.add(action)
                await session.commit()
        except Exception:  # noqa: BLE001
            logger.exception("خطای غیرمنتظره در حلقه‌ی زمان‌بند")
        await asyncio.sleep(15)


# =====================================================================
# پنل مدیریتی Mini App: miniapp.py
# =====================================================================
def validate_webapp_init_data(init_data: str, bot_token: str) -> Optional[Dict[str, str]]:
    if not init_data:
        return None
    try:
        parsed = dict(parse_qsl(init_data, strict_parsing=True))
    except ValueError:
        return None
    received_hash = parsed.pop("hash", None)
    if not received_hash:
        return None
    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
    secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(computed_hash, received_hash):
        return None
    return parsed


MINIAPP_BOOL_FIELDS = {f for f, _ in LOCK_FIELDS} | {
    "filter_words_enabled",
    "flood_enabled",
    "welcome_enabled",
    "delete_join_leave",
}

MINIAPP_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>پنل مدیریت گروه</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
  body { font-family: Tahoma, Arial, sans-serif; background: var(--tg-theme-bg-color,#111); color: var(--tg-theme-text-color,#eee); margin:0; padding:16px; }
  h2 { font-size: 18px; margin-bottom: 12px; }
  .row { display:flex; justify-content:space-between; align-items:center; padding:12px; margin-bottom:8px; background: var(--tg-theme-secondary-bg-color,#1c1c1c); border-radius:10px; }
  .switch { position: relative; width:44px; height:24px; flex-shrink:0; }
  .switch input { display:none; }
  .slider { position:absolute; inset:0; background:#555; border-radius:24px; cursor:pointer; transition:.2s; }
  .slider:before { content:""; position:absolute; width:18px; height:18px; left:3px; top:3px; background:white; border-radius:50%; transition:.2s; }
  input:checked + .slider { background:#34c759; }
  input:checked + .slider:before { transform: translateX(20px); }
  .msg { text-align:center; padding:24px; opacity:.7; }
  .section-title { margin-top:20px; margin-bottom:8px; font-size:13px; opacity:.6; }
</style>
</head>
<body>
<div id="app"><div class="msg">در حال بارگذاری...</div></div>
<script>
const tg = window.Telegram.WebApp;
tg.ready();
tg.expand();

const params = new URLSearchParams(window.location.search);
const chatId = params.get("chat_id");
const initData = tg.initData;

const LOCK_LABELS = {
  lock_link: "🔗 لینک", lock_forward: "↪️ فوروارد", lock_mention: "👤 منشن",
  lock_sticker: "🎭 استیکر", lock_gif: "🎬 گیف", lock_contact: "📇 مخاطب",
  lock_location: "📍 لوکیشن", lock_poll: "📊 نظرسنجی", lock_photo: "🖼 عکس",
  lock_video: "🎥 ویدیو", lock_voice: "🎙 ویس", lock_document: "📎 فایل",
  lock_bot_join: "🤖 ورود ربات", lock_deleted_accounts: "👻 اکانت حذف‌شده"
};
const OTHER_LABELS = {
  filter_words_enabled: "🧹 فیلتر کلمات ممنوعه",
  flood_enabled: "🌊 ضداسپم",
  welcome_enabled: "👋 خوش‌آمدگویی",
  delete_join_leave: "🗑 حذف پیام ورود/خروج"
};

async function loadSettings() {
  if (!chatId || !initData) {
    document.getElementById("app").innerHTML = '<div class="msg">این صفحه فقط از داخل تلگرام قابل استفاده‌ست.</div>';
    return;
  }
  try {
    const res = await fetch("/api/settings?chat_id=" + encodeURIComponent(chatId) + "&initData=" + encodeURIComponent(initData));
    if (!res.ok) {
      document.getElementById("app").innerHTML = '<div class="msg">دسترسی نداری یا خطایی رخ داد.</div>';
      return;
    }
    const data = await res.json();
    render(data);
  } catch (e) {
    document.getElementById("app").innerHTML = '<div class="msg">خطا در ارتباط با سرور.</div>';
  }
}

function rowHtml(field, label, checked) {
  return '<div class="row"><span>' + label + '</span>' +
    '<label class="switch"><input type="checkbox" data-field="' + field + '" ' + (checked ? "checked" : "") + '><span class="slider"></span></label></div>';
}

function render(data) {
  let out = '<h2>⚙️ تنظیمات ' + (data.title || "گروه") + '</h2>';
  out += '<div class="section-title">قفل‌ها و فیلترها</div>';
  for (const key in LOCK_LABELS) out += rowHtml(key, LOCK_LABELS[key], data[key]);
  out += '<div class="section-title">سایر تنظیمات</div>';
  for (const key in OTHER_LABELS) out += rowHtml(key, OTHER_LABELS[key], data[key]);
  document.getElementById("app").innerHTML = out;

  document.querySelectorAll("input[type=checkbox]").forEach(function (cb) {
    cb.addEventListener("change", async function (e) {
      const fieldName = e.target.getAttribute("data-field");
      const value = e.target.checked;
      const body = { chat_id: chatId, initData: initData, settings: {} };
      body.settings[fieldName] = value;
      await fetch("/api/settings", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
      if (tg.HapticFeedback) tg.HapticFeedback.impactOccurred("light");
    });
  });
}

loadSettings();
</script>
</body>
</html>
"""


async def miniapp_index(request: web.Request) -> web.Response:
    return web.Response(text=MINIAPP_HTML, content_type="text/html")


async def api_get_settings(request: web.Request) -> web.Response:
    init_data = request.query.get("initData", "")
    chat_id_raw = request.query.get("chat_id", "")
    parsed = validate_webapp_init_data(init_data, config.bot_token)
    if parsed is None:
        return web.json_response({"error": "invalid_init_data"}, status=401)
    if not chat_id_raw.lstrip("-").isdigit():
        return web.json_response({"error": "invalid_chat_id"}, status=400)
    chat_id = int(chat_id_raw)

    try:
        user_data = json.loads(parsed.get("user", "{}"))
        user_id = int(user_data.get("id"))
    except (ValueError, TypeError, json.JSONDecodeError):
        return web.json_response({"error": "invalid_user"}, status=400)

    bot: Bot = request.app["bot"]
    if not await is_chat_admin(bot, chat_id, user_id):
        return web.json_response({"error": "forbidden"}, status=403)

    async with async_session_maker() as session:
        chat = await get_chat_by_id(session, chat_id)
        if chat is None:
            return web.json_response({"error": "not_found"}, status=404)
        payload: Dict[str, Any] = {f: getattr(chat, f) for f in MINIAPP_BOOL_FIELDS}
        payload.update(
            {
                "title": chat.title,
                "flood_limit": chat.flood_limit,
                "flood_window": chat.flood_window,
                "warn_limit": chat.warn_limit,
                "warn_action": chat.warn_action,
                "flood_action": chat.flood_action,
            }
        )
    return web.json_response(payload)


async def api_post_settings(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "invalid_json"}, status=400)

    init_data = body.get("initData", "")
    chat_id_raw = str(body.get("chat_id", ""))
    parsed = validate_webapp_init_data(init_data, config.bot_token)
    if parsed is None:
        return web.json_response({"error": "invalid_init_data"}, status=401)
    if not chat_id_raw.lstrip("-").isdigit():
        return web.json_response({"error": "invalid_chat_id"}, status=400)
    chat_id = int(chat_id_raw)

    try:
        user_data = json.loads(parsed.get("user", "{}"))
        user_id = int(user_data.get("id"))
    except (ValueError, TypeError, json.JSONDecodeError):
        return web.json_response({"error": "invalid_user"}, status=400)

    bot: Bot = request.app["bot"]
    if not await is_chat_admin(bot, chat_id, user_id):
        return web.json_response({"error": "forbidden"}, status=403)

    updates = body.get("settings", {})
    if not isinstance(updates, dict):
        return web.json_response({"error": "invalid_settings"}, status=400)

    async with async_session_maker() as session:
        chat = await get_chat_by_id(session, chat_id)
        if chat is None:
            return web.json_response({"error": "not_found"}, status=404)
        for key, value in updates.items():
            if key in MINIAPP_BOOL_FIELDS and isinstance(value, bool):
                setattr(chat, key, value)
            elif key == "warn_limit" and isinstance(value, int) and 1 <= value <= 50:
                chat.warn_limit = value
            elif key == "warn_action" and value in ("mute", "kick", "ban"):
                chat.warn_action = value
            elif key == "flood_limit" and isinstance(value, int) and 2 <= value <= 50:
                chat.flood_limit = value
            elif key == "flood_window" and isinstance(value, int) and 2 <= value <= 120:
                chat.flood_window = value
            elif key == "flood_action" and value in ("mute", "kick", "ban"):
                chat.flood_action = value
        session.add(chat)
        await session.commit()
    return web.json_response({"ok": True})


def build_miniapp_app(bot: Bot) -> web.Application:
    app = web.Application()
    app["bot"] = bot
    app.router.add_get("/", miniapp_index)
    app.router.add_get("/api/settings", api_get_settings)
    app.router.add_post("/api/settings", api_post_settings)
    return app


# =====================================================================
# اجرای ربات: main.py
# =====================================================================
BOT_COMMANDS = [
    BotCommand(command="start", description="شروع"),
    BotCommand(command="help", description="راهنمای کامل"),
    BotCommand(command="settings", description="پنل تنظیمات گروه"),
    BotCommand(command="rules", description="نمایش قوانین گروه"),
    BotCommand(command="stats", description="آمار گروه"),
    BotCommand(command="adminlist", description="لیست ادمین‌ها"),
]


async def on_dispatcher_error(event, exception: Exception) -> bool:  # noqa: ANN001
    logger.exception("خطای مدیریت‌نشده در پردازش آپدیت: %s", exception)
    return True


async def main() -> None:
    await init_db()

    bot = Bot(token=config.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())

    dp.update.middleware(DbSessionMiddleware())
    dp.message.middleware(ModerationMiddleware())

    dp.include_router(router)
    dp.error.register(on_dispatcher_error)

    try:
        await bot.set_my_commands(BOT_COMMANDS)
    except (TelegramBadRequest, TelegramForbiddenError):
        logger.warning("تنظیم لیست دستورها ناموفق بود (اهمیتی نداره).")

    asyncio.create_task(scheduler_loop(bot))

    runner: Optional[web.AppRunner] = None
    if config.miniapp_enabled:
        app = build_miniapp_app(bot)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, config.miniapp_host, config.miniapp_port)
        await site.start()
        logger.info("Mini App روی %s:%s در حال اجراست.", config.miniapp_host, config.miniapp_port)
        if not config.miniapp_public_url:
            logger.warning(
                "MINIAPP_PUBLIC_URL تنظیم نشده؛ دکمه‌ی Mini App در پنل تنظیمات نمایش داده نمی‌شه. "
                "این آدرس باید HTTPS و از بیرون قابل‌دسترسی باشه."
            )

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("ربات مدیریت گروه در حال اجراست...")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        if runner is not None:
            await runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("ربات متوقف شد.")
