import os
import re
import logging
import uuid
import boto3
import httpx
from io import BytesIO
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

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

URL_RE = re.compile(r'https?://[^\s<>\"\']+')

# ─── Helpers ───

def has_formatting(entities):
    """Check if there's at least one non-bot_command entity."""
    if not entities:
        return False
    return any(e.type != "bot_command" for e in entities)


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
        # text before this entity
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


# ─── Handlers ───

async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 <b>u4129bot</b>\n\n"
        "📎 Отправь файл → загрузка в S3\n"
        "✨ Форматированный текст → 3 файла (HTML, MD, TG MD)\n"
        "🔍 @username или t.me/link → ID lookup\n"
        "🔗 Ссылка → сокращение через dub.sh",
        parse_mode="HTML",
    )


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
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
    elif message.voice:
        file_obj = message.voice
        file_name = f"voice_{file_obj.file_unique_id}.ogg"
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
        # Extract username
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

    # (b) Formatted text → 3 files (but NOT if text contains literal HTML/MD tags — that's preview mode)
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

    # Bot was added (member/admin) vs removed
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
    app.add_handler(MessageHandler(
        filters.Document.ALL | filters.PHOTO | filters.VIDEO |
        filters.AUDIO | filters.VOICE | filters.VIDEO_NOTE |
        filters.Sticker.ALL | filters.ANIMATION,
        handle_file,
    ))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info("u4129bot started")
    app.run_polling(
        allowed_updates=["message", "my_chat_member"],
        read_timeout=300, write_timeout=300, connect_timeout=60,
    )


if __name__ == "__main__":
    main()
