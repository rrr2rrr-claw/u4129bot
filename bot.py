import os
import re
import logging
import uuid
import sqlite3
from datetime import datetime, timezone
import boto3
import httpx
from io import BytesIO
from openai import OpenAI
from telegram import Update, ChatMemberUpdated
from telegram.ext import (
    Application, MessageHandler, ChatMemberHandler,
    CommandHandler, filters, ContextTypes,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"]
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "https://fsn1.your-objectstorage.com")
S3_BUCKET = os.environ.get("S3_BUCKET", "4129")
S3_PREFIX = "telegram-files/"
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
LOCAL_API_URL = os.environ.get("LOCAL_API_URL", "http://localhost:8081/bot")
DUB_API_KEY = os.environ["DUB_API_KEY"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
BOT_PASSWORD = os.environ.get("BOT_PASSWORD", "")

# Seed users who don't need a password (comma-separated IDs)
SEED_USERS = set(map(int, filter(None, os.environ.get("SEED_USERS", "").split(","))))

DB_PATH = os.environ.get("DB_PATH", "/data/bot.db")

openai_client = OpenAI(api_key=OPENAI_API_KEY)

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

URL_RE = re.compile(r'https?://[^\s<>\"\']+')


# ─── Database ───

def init_db():
    """Initialize SQLite database."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            language_code TEXT,
            is_premium INTEGER DEFAULT 0,
            is_bot INTEGER DEFAULT 0,
            authorized INTEGER DEFAULT 0,
            authorized_at TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
    """)
    conn.commit()
    # Seed users
    now = datetime.now(timezone.utc).isoformat()
    for uid in SEED_USERS:
        conn.execute("""
            INSERT OR IGNORE INTO users (user_id, authorized, authorized_at, first_seen_at, last_seen_at)
            VALUES (?, 1, ?, ?, ?)
        """, (uid, now, now, now))
    conn.commit()
    conn.close()
    logger.info(f"Database initialized at {DB_PATH}, seed users: {SEED_USERS}")


def upsert_user(user) -> bool:
    """Update or insert user info. Returns True if authorized."""
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT authorized FROM users WHERE user_id = ?", (user.id,))
    row = cur.fetchone()

    if row is None:
        # New user
        cur.execute("""
            INSERT INTO users (user_id, username, first_name, last_name, language_code, is_premium, is_bot, authorized, first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
        """, (
            user.id,
            user.username,
            user.first_name,
            user.last_name,
            user.language_code,
            1 if getattr(user, 'is_premium', False) else 0,
            1 if user.is_bot else 0,
            now, now,
        ))
        conn.commit()
        conn.close()
        return False
    else:
        # Update existing user info
        cur.execute("""
            UPDATE users SET
                username = ?,
                first_name = ?,
                last_name = ?,
                language_code = ?,
                is_premium = ?,
                is_bot = ?,
                last_seen_at = ?
            WHERE user_id = ?
        """, (
            user.username,
            user.first_name,
            user.last_name,
            user.language_code,
            1 if getattr(user, 'is_premium', False) else 0,
            1 if user.is_bot else 0,
            now,
            user.id,
        ))
        conn.commit()
        authorized = row[0] == 1
        conn.close()
        return authorized


def authorize_user(user_id: int):
    """Mark user as authorized."""
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE users SET authorized = 1, authorized_at = ? WHERE user_id = ?", (now, user_id))
    conn.commit()
    conn.close()


def is_user_authorized(user_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT authorized FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row is not None and row[0] == 1


# ─── Access control ───

def check_access(update: Update) -> str:
    """Check user access. Returns 'ok', 'need_password', or 'new'."""
    user = update.effective_user
    if not user:
        return "new"

    authorized = upsert_user(user)
    if authorized:
        return "ok"
    return "need_password"


# ─── Helpers ───

def has_formatting(entities):
    """Check if there's at least one meaningful formatting entity."""
    if not entities:
        return False
    skip = {"bot_command", "url", "email", "phone_number", "mention", "hashtag", "cashtag"}
    return any(e.type not in skip for e in entities)


def entities_to_html(text: str, entities) -> str:
    """Convert Telegram entities to HTML."""
    if not entities:
        return _escape_html(text)

    utf16 = text.encode("utf-16-le")
    pieces = []
    last = 0

    sorted_ents = sorted(entities, key=lambda e: e.offset)
    for ent in sorted_ents:
        start = ent.offset * 2
        end = (ent.offset + ent.length) * 2
        pieces.append(_escape_html(utf16[last:start].decode("utf-16-le")))
        inner = _escape_html(utf16[start:end].decode("utf-16-le"))

        tag = _html_tag(ent, inner)
        pieces.append(tag)
        last = end

    pieces.append(_escape_html(utf16[last:].decode("utf-16-le")))
    return "".join(pieces)


def _escape_html(t):
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _html_tag(ent, inner):
    t = ent.type
    if t == "bold":
        return f"<b>{inner}</b>"
    if t == "italic":
        return f"<i>{inner}</i>"
    if t == "underline":
        return f"<u>{inner}</u>"
    if t == "strikethrough":
        return f"<s>{inner}</s>"
    if t == "code":
        return f"<code>{inner}</code>"
    if t == "pre":
        lang = ent.language or ""
        if lang:
            return f'<pre><code class="language-{lang}">{inner}</code></pre>'
        return f"<pre>{inner}</pre>"
    if t == "spoiler":
        return f'<tg-spoiler>{inner}</tg-spoiler>'
    if t == "text_link":
        return f'<a href="{_escape_html(ent.url)}">{inner}</a>'
    if t == "text_mention":
        uid = ent.user.id if ent.user else 0
        return f'<a href="tg://user?id={uid}">{inner}</a>'
    if t == "custom_emoji":
        return f'<tg-emoji emoji-id="{ent.custom_emoji_id}">{inner}</tg-emoji>'
    if t == "blockquote":
        return f"<blockquote>{inner}</blockquote>"
    if t == "expandable_blockquote":
        return f"<blockquote expandable>{inner}</blockquote>"
    return inner


def entities_to_mdv2(text: str, entities) -> str:
    """Convert Telegram entities to MarkdownV2."""
    if not entities:
        return _escape_mdv2(text)

    utf16 = text.encode("utf-16-le")
    pieces = []
    last = 0
    sorted_ents = sorted(entities, key=lambda e: e.offset)

    for ent in sorted_ents:
        start = ent.offset * 2
        end = (ent.offset + ent.length) * 2
        pieces.append(_escape_mdv2(utf16[last:start].decode("utf-16-le")))
        inner_raw = utf16[start:end].decode("utf-16-le")

        t = ent.type
        if t == "bold":
            pieces.append(f"*{_escape_mdv2(inner_raw)}*")
        elif t == "italic":
            pieces.append(f"_{_escape_mdv2(inner_raw)}_")
        elif t == "underline":
            pieces.append(f"__{_escape_mdv2(inner_raw)}__")
        elif t == "strikethrough":
            pieces.append(f"~{_escape_mdv2(inner_raw)}~")
        elif t == "code":
            pieces.append(f"`{inner_raw}`")
        elif t == "pre":
            lang = ent.language or ""
            pieces.append(f"```{lang}\n{inner_raw}\n```")
        elif t == "spoiler":
            pieces.append(f"||{_escape_mdv2(inner_raw)}||")
        elif t == "text_link":
            pieces.append(f"[{_escape_mdv2(inner_raw)}]({_escape_mdv2_url(ent.url)})")
        elif t == "text_mention":
            uid = ent.user.id if ent.user else 0
            pieces.append(f"[{_escape_mdv2(inner_raw)}](tg://user?id={uid})")
        elif t == "custom_emoji":
            pieces.append(f"![{_escape_mdv2(inner_raw)}](tg://emoji?id={ent.custom_emoji_id})")
        elif t == "blockquote" or t == "expandable_blockquote":
            lines = inner_raw.split("\n")
            pieces.append("\n".join(f">{_escape_mdv2(l)}" for l in lines))
        else:
            pieces.append(_escape_mdv2(inner_raw))
        last = end

    pieces.append(_escape_mdv2(utf16[last:].decode("utf-16-le")))
    return "".join(pieces)


def _escape_mdv2(t):
    special = r'_*[]()~`>#+-=|{}.!\\'
    return re.sub(f'([{re.escape(special)}])', r'\\\1', t)


def _escape_mdv2_url(t):
    return t.replace("\\", "\\\\").replace(")", "\\)")


def entities_to_markdown(text: str, entities) -> str:
    """Convert Telegram entities to GitHub-style Markdown."""
    if not entities:
        return text

    utf16 = text.encode("utf-16-le")
    pieces = []
    last = 0
    sorted_ents = sorted(entities, key=lambda e: e.offset)

    for ent in sorted_ents:
        start = ent.offset * 2
        end = (ent.offset + ent.length) * 2
        pieces.append(utf16[last:start].decode("utf-16-le"))
        inner = utf16[start:end].decode("utf-16-le")

        t = ent.type
        if t == "bold":
            pieces.append(f"**{inner}**")
        elif t == "italic":
            pieces.append(f"*{inner}*")
        elif t == "underline":
            pieces.append(f"<u>{inner}</u>")
        elif t == "strikethrough":
            pieces.append(f"~~{inner}~~")
        elif t == "code":
            pieces.append(f"`{inner}`")
        elif t == "pre":
            lang = ent.language or ""
            pieces.append(f"```{lang}\n{inner}\n```")
        elif t == "spoiler":
            pieces.append(f"<details><summary>spoiler</summary>{inner}</details>")
        elif t == "text_link":
            pieces.append(f"[{inner}]({ent.url})")
        elif t == "text_mention":
            uid = ent.user.id if ent.user else 0
            pieces.append(f"[{inner}](tg://user?id={uid})")
        elif t == "custom_emoji":
            pieces.append(inner)
        elif t == "blockquote" or t == "expandable_blockquote":
            lines = inner.split("\n")
            pieces.append("\n".join(f"> {l}" for l in lines))
        else:
            pieces.append(inner)
        last = end

    pieces.append(utf16[last:].decode("utf-16-le"))
    return "".join(pieces)


# ─── Whisper transcription ───

async def transcribe_voice(file_data: bytes, file_name: str = "voice.ogg") -> str:
    """Transcribe audio using OpenAI Whisper API."""
    transcript = openai_client.audio.transcriptions.create(
        model="whisper-1",
        file=(file_name, BytesIO(file_data)),
    )
    return transcript.text


# ─── Handlers ───

async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access = check_access(update)
    if access == "need_password":
        await update.message.reply_text(
            "🔐 Для доступа к боту введите пароль:",
        )
        return

    await update.message.reply_text(
        "👋 <b>u4129bot</b>\n\n"
        "📎 Отправь файл → загрузка в S3\n"
        "✨ Форматированный текст → 3 файла (HTML, MD, TG MD)\n"
        "🔍 @username или t.me/link → ID lookup\n"
        "🔗 Ссылка → сокращение через dub.sh\n"
        "🎤 Голосовое → расшифровка через Whisper",
        parse_mode="HTML",
    )


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle voice messages — transcribe with Whisper."""
    access = check_access(update)
    if access == "need_password":
        await update.message.reply_text("🔐 Для доступа к боту введите пароль:")
        return

    message = update.message
    if not message or not message.voice:
        return

    try:
        voice = message.voice
        tg_file = await voice.get_file(read_timeout=300, write_timeout=300, connect_timeout=60)
        file_path = tg_file.file_path

        if file_path.startswith("/"):
            with open(file_path, "rb") as f:
                data = f.read()
        else:
            data = bytes(await tg_file.download_as_bytearray(
                read_timeout=300, write_timeout=300, connect_timeout=60
            ))

        duration = voice.duration or 0
        size_kb = len(data) / 1024

        await message.reply_text("🎤 Расшифровываю...")

        text = await transcribe_voice(data)

        duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "?"
        await message.reply_text(
            f"🎤 <b>Расшифровка</b> ({duration_str}, {size_kb:.0f} KB)\n\n{text}",
            parse_mode="HTML",
        )
        logger.info(f"Transcribed voice ({duration}s, {len(data)} bytes)")

    except Exception as e:
        logger.error(f"Voice transcription error: {e}")
        await message.reply_text(f"❌ Ошибка расшифровки: {e}")


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access = check_access(update)
    if access == "need_password":
        await update.message.reply_text("🔐 Для доступа к боту введите пароль:")
        return

    message = update.message
    if not message:
        return

    # Voice messages are handled separately by handle_voice
    if message.voice:
        return

    file_obj = None
    file_name = None

    if message.document:
        file_obj = message.document
        file_name = file_obj.file_name or f"document_{file_obj.file_id}"
    elif message.photo:
        file_obj = message.photo[-1]
        file_name = f"photo_{file_obj.file_unique_id}.jpg"
    elif message.video:
        file_obj = message.video
        file_name = file_obj.file_name or f"video_{file_obj.file_unique_id}.mp4"
    elif message.audio:
        file_obj = message.audio
        file_name = file_obj.file_name or f"audio_{file_obj.file_unique_id}.mp3"
    elif message.video_note:
        file_obj = message.video_note
        file_name = f"videonote_{file_obj.file_unique_id}.mp4"
    elif message.sticker:
        file_obj = message.sticker
        ext = ".webp" if not file_obj.is_animated else ".tgs"
        file_name = f"sticker_{file_obj.file_unique_id}{ext}"
    elif message.animation:
        file_obj = message.animation
        file_name = file_obj.file_name or f"animation_{file_obj.file_unique_id}.mp4"
    else:
        return

    file_size = getattr(file_obj, "file_size", None) or 0
    size_mb = file_size / (1024 * 1024) if file_size else 0

    try:
        tg_file = await file_obj.get_file(read_timeout=300, write_timeout=300, connect_timeout=60)
        file_path = tg_file.file_path
        if file_path.startswith("/"):
            with open(file_path, "rb") as f:
                data = f.read()
        else:
            data = bytes(await tg_file.download_as_bytearray(
                read_timeout=300, write_timeout=300, connect_timeout=60
            ))

        unique_name = f"{uuid.uuid4()}_{file_name}"
        s3_key = f"{S3_PREFIX}{unique_name}"
        s3.upload_fileobj(BytesIO(data), S3_BUCKET, s3_key)

        public_url = f"{S3_ENDPOINT}/{S3_BUCKET}/{s3_key}"
        size_str = f" ({size_mb:.1f} MB)" if size_mb > 1 else ""
        await message.reply_text(
            f"✅ Сохранено!{size_str}\n\n📎 <code>{s3_key}</code>\n🔗 {public_url}",
            parse_mode="HTML",
        )
        logger.info(f"Uploaded {file_name} ({len(data)} bytes) -> {s3_key}")

        # If caption has formatting, also send 3 format files
        caption_text = message.caption
        caption_entities = message.caption_entities
        if caption_text and has_formatting(caption_entities):
            html = entities_to_html(caption_text, caption_entities)
            mdv2 = entities_to_mdv2(caption_text, caption_entities)
            md = entities_to_markdown(caption_text, caption_entities)
            from telegram import InputFile as IF2
            for fname, content in [("formatted_html.txt", html), ("formatted_tg_md.txt", mdv2), ("formatted_md.txt", md)]:
                await message.reply_document(
                    document=IF2(BytesIO(content.encode("utf-8")), filename=fname),
                )

    except Exception as e:
        logger.error(f"Upload error: {e}")
        await message.reply_text(f"❌ Ошибка: {e}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message or not message.text:
        return

    text = message.text.strip()
    user = update.effective_user

    # Check if this is a password attempt from unauthorized user
    if user and not is_user_authorized(user.id):
        # Record the user visit
        upsert_user(user)

        if BOT_PASSWORD and text == BOT_PASSWORD:
            authorize_user(user.id)
            await message.reply_text(
                "✅ Пароль принят! Добро пожаловать.\n\n"
                "Нажми /start чтобы увидеть возможности бота.",
            )
            logger.info(f"User {user.id} (@{user.username}) authorized with password")
            return
        elif BOT_PASSWORD:
            await message.reply_text("🔐 Для доступа к боту введите пароль:")
            return
        # If no password set, allow all
        if not BOT_PASSWORD:
            pass
        else:
            return

    entities = message.entities or []

    # Skip commands
    if text.startswith("/"):
        return

    # (b) Detect HTML or Markdown in plain text → render preview (BEFORE link checks)
    html_tag_re = re.compile(r'<(b|i|u|s|code|pre|a |tg-spoiler|tg-emoji|blockquote)[>\s/]', re.IGNORECASE)
    md_pattern_re = re.compile(r'(\*\*.+?\*\*|\*[^*]+\*|`.+?`|~~.+?~~|\[.+?\]\(.+?\))')

    if html_tag_re.search(text):
        try:
            await message.reply_text(text, parse_mode="HTML")
        except Exception as e:
            await message.reply_text(f"❌ HTML parse error: {e}")
        return

    if md_pattern_re.search(text):
        try:
            await message.reply_text(text, parse_mode="Markdown")
        except Exception as e:
            await message.reply_text(f"❌ Markdown parse error: {e}")
        return

    # (d) Telegram username/link → ID lookup (only short messages like "@username")
    if text.startswith("@") or (text.startswith("http") and "t.me/" in text and len(text) < 200):
        target = text
        m = re.search(r't\.me/([A-Za-z0-9_]+)', target)
        if m:
            username = "@" + m.group(1)
        elif target.startswith("@"):
            username = target.split()[0]
        else:
            username = target

        try:
            chat = await context.bot.get_chat(username)
            lines = [f"🔍 <b>Chat Info</b>"]
            lines.append(f"ID: <code>{chat.id}</code>")
            lines.append(f"Type: {chat.type}")
            if chat.title:
                lines.append(f"Title: {chat.title}")
            if getattr(chat, "first_name", None):
                lines.append(f"Name: {chat.first_name} {chat.last_name or ''}".strip())
            if chat.username:
                lines.append(f"Username: @{chat.username}")
            if getattr(chat, "bio", None):
                lines.append(f"Bio: {chat.bio}")
            await message.reply_text("\n".join(lines), parse_mode="HTML")
        except Exception as e:
            await message.reply_text(f"❌ Не удалось получить info: {e}")
        return

    # (b) Formatted text → 3 files
    html_tag_check = re.compile(r'<(b|i|u|s|code|pre|a |tg-spoiler|tg-emoji|blockquote)[>\s/]', re.IGNORECASE)
    if has_formatting(entities) and not html_tag_check.search(text):
        html = entities_to_html(text, entities)
        mdv2 = entities_to_mdv2(text, entities)
        md = entities_to_markdown(text, entities)

        files = [
            ("formatted_html.txt", html),
            ("formatted_tg_md.txt", mdv2),
            ("formatted_md.txt", md),
        ]
        from telegram import InputFile
        for fname, content in files:
            await message.reply_document(
                document=InputFile(BytesIO(content.encode("utf-8")), filename=fname),
            )
        return

    # (e) URLs → shorten via dub.co
    urls = URL_RE.findall(text)
    tme_urls = [u for u in urls if "t.me/" in u]
    other_urls = [u for u in urls if "t.me/" not in u]

    if other_urls:
        results = []
        async with httpx.AsyncClient(timeout=15) as client:
            for url in other_urls:
                try:
                    resp = await client.post(
                        "https://api.dub.co/links",
                        headers={"Authorization": f"Bearer {DUB_API_KEY}"},
                        json={"url": url, "domain": "dub.sh"},
                    )
                    data = resp.json()
                    short = data.get("shortLink", data.get("short_link", "?"))
                    results.append(f"🔗 {short}")
                except Exception as e:
                    results.append(f"❌ {url}: {e}")
        await message.reply_text("\n".join(results))
        return

    # Plain text → help
    await message.reply_text(
        "💡 Отправь файл, форматированный текст, @username, ссылку t.me/ или любую ссылку.",
    )


async def handle_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """When bot is added to a group/channel, send info."""
    result = update.my_chat_member
    if not result:
        return

    new = result.new_chat_member
    if new.user.id != context.bot.id:
        return

    if new.status in ("member", "administrator"):
        chat = result.chat
        lines = [
            "👋 <b>Привет! Вот инфо о чате:</b>",
            f"ID: <code>{chat.id}</code>",
            f"Title: {chat.title or 'N/A'}",
            f"Type: {chat.type}",
        ]
        if chat.username:
            lines.append(f"Username: @{chat.username}")
        try:
            await context.bot.send_message(chat.id, "\n".join(lines), parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed to send group info: {e}")


def main():
    init_db()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .base_url(LOCAL_API_URL)
        .base_file_url(LOCAL_API_URL)
        .local_mode(True)
        .read_timeout(300)
        .write_timeout(300)
        .connect_timeout(60)
        .build()
    )

    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(CommandHandler("help", handle_start))
    app.add_handler(ChatMemberHandler(handle_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    # Voice handler BEFORE generic file handler
    app.add_handler(MessageHandler(filters.VOICE, handle_voice))
    app.add_handler(MessageHandler(
        filters.Document.ALL | filters.PHOTO | filters.VIDEO |
        filters.AUDIO | filters.VIDEO_NOTE |
        filters.Sticker.ALL | filters.ANIMATION,
        handle_file,
    ))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info("u4129bot started (seed users: %s)", SEED_USERS or "none")
    app.run_polling(
        allowed_updates=["message", "my_chat_member"],
        read_timeout=300, write_timeout=300, connect_timeout=60,
    )


if __name__ == "__main__":
    main()
