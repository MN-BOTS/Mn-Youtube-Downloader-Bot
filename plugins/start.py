import os
import re
import time
import asyncio
import logging
import aiohttp
import aiofiles
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from pyrogram.errors import FloodWait, MessageNotModified
from config import Config

logger = logging.getLogger("mnbots.ytdl")

YT_API = Config.YT_API
DOWNLOAD_DIR = Path(Config.DOWNLOAD_DIR)
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

YT_REGEX = re.compile(
    r"(https?://)?(www\.)?"
    r"(youtube\.com/(watch\?v=|shorts/|embed/)|youtu\.be/)"
    r"[\w\-]{11}"
)

# ─── helpers ────────────────────────────────────────────────────────────────

def extract_url(text: str) -> str | None:
    m = YT_REGEX.search(text)
    return m.group(0) if m else None



# Per-endpoint timeouts (seconds). mp4/mp3 can take 2–3 min for muxing.
_TIMEOUTS = {
    "info":   aiohttp.ClientTimeout(total=30),
    "mp4":    aiohttp.ClientTimeout(total=300),   # muxing is slow
    "mp3":    aiohttp.ClientTimeout(total=180),
    "search": aiohttp.ClientTimeout(total=20),
}
_DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=60)

_MAX_RETRIES = 3
_RETRY_DELAY = 5   # seconds between retries


async def safe_edit(msg: Message, text: str, **kwargs) -> None:
    """Edit a message, silently ignoring MessageNotModified errors."""
    try:
        await msg.edit(text, **kwargs)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(e.value)
    except Exception:
        pass


async def api_get(
    session: aiohttp.ClientSession,
    path: str,
    **params,
) -> dict | list:
    url = f"{YT_API}/{path}"
    timeout = _TIMEOUTS.get(path, _DEFAULT_TIMEOUT)
    last_exc: Exception | None = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            async with session.get(
                url, params=params, timeout=timeout
            ) as r:
                r.raise_for_status()
                return await r.json()
        except (asyncio.TimeoutError, aiohttp.ServerDisconnectedError) as e:
            last_exc = e
            if attempt < _MAX_RETRIES:
                logger.warning(
                    "api_get /%s timeout/disconnect (attempt %d/%d), retrying in %ds",
                    path, attempt, _MAX_RETRIES, _RETRY_DELAY,
                )
                await asyncio.sleep(_RETRY_DELAY)
            continue
        except aiohttp.ClientResponseError as e:
            # 502/503/504 = gateway/worker overloaded — worth retrying
            if e.status in (502, 503, 504) and attempt < _MAX_RETRIES:
                last_exc = e
                logger.warning(
                    "api_get /%s HTTP %d (attempt %d/%d), retrying in %ds",
                    path, e.status, attempt, _MAX_RETRIES, _RETRY_DELAY,
                )
                await asyncio.sleep(_RETRY_DELAY)
                continue
            raise  # 4xx or unrecoverable 5xx — don't retry

    raise asyncio.TimeoutError(
        f"/{path} did not respond after {_MAX_RETRIES} attempts"
    ) from last_exc


async def fetch_info(session: aiohttp.ClientSession, yt_url: str) -> dict:
    return await api_get(session, "info", url=yt_url)


async def fetch_mp4(session: aiohttp.ClientSession, yt_url: str, quality: int | None = None) -> dict:
    params = {"url": yt_url}
    if quality is not None:
        params["quality"] = quality
    return await api_get(session, "mp4", **params)


async def fetch_mp3(session: aiohttp.ClientSession, yt_url: str, quality: int | None = None) -> dict:
    params = {"url": yt_url}
    if quality is not None:
        params["quality"] = quality
    return await api_get(session, "mp3", **params)


async def search_yt(session: aiohttp.ClientSession, query: str) -> list:
    result = await api_get(session, "search", s=query)
    logger.debug("search_yt raw response type=%s value=%r", type(result).__name__, result)
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        # try common wrapper keys
        for key in ("results", "videos", "items", "data"):
            if isinstance(result.get(key), list):
                return result[key]
        # dict may itself be a single video — unlikely but guard
        logger.warning("search_yt unexpected dict shape: %r", list(result.keys()))
    return []


def progress_bar(done: int, total: int, width: int = 16) -> str:
    pct = done / total if total else 0
    filled = int(width * pct)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {pct*100:.1f}%"


def _fmt_size(b: int) -> str:
    """Return a human-readable size string."""
    if b >= 1_073_741_824:
        return f"{b/1_073_741_824:.2f} GB"
    if b >= 1_048_576:
        return f"{b/1_048_576:.1f} MB"
    return f"{b/1024:.1f} KB"


def _fmt_speed(bps: float) -> str:
    """Return a human-readable speed string."""
    if bps >= 1_048_576:
        return f"{bps/1_048_576:.2f} MB/s"
    return f"{bps/1024:.1f} KB/s"


async def download_file(url: str, dest: Path, msg: Message) -> Path:
    start = time.time()
    last_edit = 0.0
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=3600)) as r:
            r.raise_for_status()
            total = int(r.headers.get("Content-Length", 0))
            done = 0
            async with aiofiles.open(dest, "wb") as f:
                async for chunk in r.content.iter_chunked(1024 * 256):
                    await f.write(chunk)
                    done += len(chunk)
                    now = time.time()
                    if now - last_edit >= 3:
                        last_edit = now
                        elapsed = now - start
                        speed = done / elapsed if elapsed else 0
                        eta = (total - done) / speed if speed and total else 0
                        bar = progress_bar(done, total)
                        done_str  = _fmt_size(done)
                        total_str = _fmt_size(total) if total else "?"
                        speed_str = _fmt_speed(speed)
                        eta_str   = f"{int(eta//60)}m {int(eta%60)}s" if eta >= 60 else f"{int(eta)}s"
                        try:
                            await msg.edit(
                                f"⬇️ **Downloading...**\n"
                                f"{bar}\n"
                                f"`{done_str}` / `{total_str}`\n"
                                f"🚀 `{speed_str}`   ⏱ ETA `{eta_str}`"
                            )
                        except FloodWait as e:
                            await asyncio.sleep(e.value)
                        except Exception:
                            pass
    return dest


def quality_buttons(qualities: list, vid_id: str, kind: str) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for q in qualities:
        label = f"{q}p" if kind == "mp4" else f"{q}kbps"
        cb = f"dl:{kind}:{vid_id}:{q}"
        row.append(InlineKeyboardButton(label, callback_data=cb))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="dl:cancel")])
    return InlineKeyboardMarkup(buttons)


def search_result_buttons(results: list) -> InlineKeyboardMarkup:
    buttons = []
    videos = [r for r in results if r.get("type") == "video"][:8]
    for v in videos:
        title = v["title"][:40]
        vid_id = v["videoId"]
        buttons.append([InlineKeyboardButton(f"▶ {title}", callback_data=f"sr:{vid_id}")])
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="dl:cancel")])
    return InlineKeyboardMarkup(buttons)


def vid_id_from_url(url: str) -> str:
    m = re.search(r"(?:v=|youtu\.be/|shorts/)([\w\-]{11})", url)
    return m.group(1) if m else url


def clean_audio_title(filename: str) -> str:
    # BUG FIX 3: original rsplit("(", 1)[0] crashes cleanly but gives ugly result
    # on filenames without "(" — strip extension and trailing junk instead
    name = Path(filename).stem  # drop .mp3
    # Remove trailing bracketed quality tags like "(128kbps)" or "[HQ]"
    name = re.sub(r"[\(\[][^\)\]]*[\)\]]$", "", name).strip()
    return name or filename


# ─── /start ─────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, msg: Message):
    await msg.reply(
        "👋 **Welcome to MN YT Downloader!**\n\n"
        "**What I can do:**\n"
        "• Send a YouTube link → choose MP4 or MP3 quality\n"
        "• `/search <query>` → search YouTube and pick a video\n"
        "• `/help` → detailed usage\n\n"
        "Just paste a YouTube URL to get started. 🚀",
        quote=True
    )


# ─── /help ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("help") & filters.private)
async def cmd_help(client: Client, msg: Message):
    await msg.reply(
        "**📖 MN YT Downloader — Help**\n\n"
        "**Direct download:**\n"
        "  Send any YouTube URL. I'll show video info and ask format.\n\n"
        "**Quality selection:**\n"
        "  After choosing MP4/MP3 you get quality buttons.\n"
        "  Supports: `144p 360p 480p 720p 1080p` for video\n"
        "  Supports: `92 128 256 320 kbps` for audio\n\n"
        "**Search:**\n"
        "  `/search lofi hip hop` — search YouTube, pick from results\n\n"
        "**Notes:**\n"
        "  • Files > 2 GB sent as link\n"
        "  • Shorts, full videos, embeds all supported",
        quote=True
    )


# ─── /search ────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("search"))
async def cmd_search(client: Client, msg: Message):
    query = msg.text.split(None, 1)[1].strip() if len(msg.text.split()) > 1 else ""
    if not query:
        return await msg.reply("Usage: `/search <query>`", quote=True)

    status = await msg.reply("🔍 Searching YouTube...", quote=True)
    try:
        async with aiohttp.ClientSession() as session:
            results = await search_yt(session, query)
    except Exception as e:
        return await safe_edit(status, f"❌ Search failed: `{e}`")

    videos = [r for r in results if r.get("type") == "video"]
    if not videos:
        logger.warning("cmd_search: no videos in results=%r", results)
        return await safe_edit(status, f"❌ No video results found for `{query}`.")

    markup = search_result_buttons(results)
    await safe_edit(
        status,
        f"🔎 **Results for:** `{query}`\nPick a video:",
        reply_markup=markup
    )


# ─── YouTube URL handler ─────────────────────────────────────────────────────

@Client.on_message(filters.text & ~filters.command(["start", "help", "search"]))
async def handle_url(client: Client, msg: Message):
    # BUG FIX 4: guard against None msg.text (e.g. captions, edited messages)
    if not msg.text:
        return

    yt_url = extract_url(msg.text)
    if not yt_url:
        return

    status = await msg.reply("🔄 Fetching info...", quote=True)
    try:
        async with aiohttp.ClientSession() as session:
            info = await fetch_info(session, yt_url)
    except Exception as e:
        return await safe_edit(status, f"❌ Failed to fetch info: `{e}`")

    title = info.get("title", "Unknown")
    thumb = info.get("thumbnail", "")
    vid_id = vid_id_from_url(yt_url)

    markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎬 MP4 (Video)", callback_data=f"fmt:mp4:{vid_id}"),
            InlineKeyboardButton("🎵 MP3 (Audio)", callback_data=f"fmt:mp3:{vid_id}"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="dl:cancel")],
    ])

    caption = f"**{title}**\n\nChoose format:"
    try:
        if thumb:
            await status.delete()
            await msg.reply_photo(thumb, caption=caption, reply_markup=markup)
        else:
            await safe_edit(status, caption, reply_markup=markup)
    except Exception:
        await safe_edit(status, caption, reply_markup=markup)


# ─── Callback: cancel ─────────────────────────────────────────────────────────
# BUG FIX 5: cancel handler MUST be registered before the dl download handler.
# Pyrogram matches callbacks in registration order. Since dl:cancel matches the
# broader dl:(mp4|mp3) pattern only if placed after, but the download regex
# ^dl:(mp4|mp3):(.+):(\d+)$ won't match "dl:cancel" anyway — however placing
# cancel first is safer and avoids any future regex overlap issues.

@Client.on_callback_query(filters.regex(r"^dl:cancel$"))
async def cb_cancel(client: Client, cq: CallbackQuery):
    await cq.answer("Cancelled.")
    try:
        await cq.message.edit_reply_markup(None)
        await cq.message.edit_text("❌ Cancelled.")
    except Exception:
        pass


# ─── Callback: format selection ──────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^fmt:(mp4|mp3):(.+)$"))
async def cb_format(client: Client, cq: CallbackQuery):
    parts = cq.data.split(":", 2)
    kind = parts[1]
    vid_id = parts[2]
    yt_url = f"https://youtu.be/{vid_id}"

    # BUG FIX 6: answer() must be called within 5s or Telegram shows "loading"
    # indefinitely. Answer before any async API work.
    await cq.answer("Fetching qualities...")

    try:
        await cq.message.edit_reply_markup(None)
    except Exception:
        pass

    status = await cq.message.reply("⏳ Fetching available qualities...")
    try:
        async with aiohttp.ClientSession() as session:
            if kind == "mp4":
                data = await fetch_mp4(session, yt_url)
            else:
                data = await fetch_mp3(session, yt_url)
    except Exception as e:
        return await safe_edit(status, f"❌ Error: `{e}`")

    qualities = data.get("availableQuality", [])
    if not qualities:
        return await safe_edit(status, "❌ No qualities available.")

    markup = quality_buttons(qualities, vid_id, kind)
    icon = "🎬" if kind == "mp4" else "🎵"
    await safe_edit(status, 
        f"{icon} Select quality for **{'Video' if kind == 'mp4' else 'Audio'}**:",
        reply_markup=markup
    )


# ─── Callback: search result selection ───────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^sr:(.+)$"))
async def cb_search_result(client: Client, cq: CallbackQuery):
    vid_id = cq.data.split(":", 1)[1]
    yt_url = f"https://youtu.be/{vid_id}"

    await cq.answer()

    try:
        await cq.message.edit_reply_markup(None)
    except Exception:
        pass

    status = await cq.message.reply("🔄 Fetching info...")
    try:
        async with aiohttp.ClientSession() as session:
            info = await fetch_info(session, yt_url)
    except Exception as e:
        return await safe_edit(status, f"❌ Failed: `{e}`")

    title = info.get("title", "Unknown")
    thumb = info.get("thumbnail", "")

    markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎬 MP4", callback_data=f"fmt:mp4:{vid_id}"),
            InlineKeyboardButton("🎵 MP3", callback_data=f"fmt:mp3:{vid_id}"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="dl:cancel")],
    ])

    caption = f"**{title}**\n\nChoose format:"
    try:
        if thumb:
            await status.delete()
            await cq.message.reply_photo(thumb, caption=caption, reply_markup=markup)
        else:
            await safe_edit(status, caption, reply_markup=markup)
    except Exception:
        await safe_edit(status, caption, reply_markup=markup)


# ─── Callback: quality → download ────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^dl:(mp4|mp3):(.+):(\d+)$"))
async def cb_download(client: Client, cq: CallbackQuery):
    parts = cq.data.split(":", 3)
    kind = parts[1]
    vid_id = parts[2]
    quality = int(parts[3])
    yt_url = f"https://youtu.be/{vid_id}"

    await cq.answer("Starting download...")

    try:
        await cq.message.edit_reply_markup(None)
    except Exception:
        pass

    status = await cq.message.reply("⚙️ **Processing...** `0s elapsed`")

    # dest must be None-initialised before try so finally can always reference it
    dest: Path | None = None

    async def _processing_ticker(msg: Message, stop: asyncio.Event) -> None:
        """Edit the status message every 5 s with elapsed time while API works."""
        start = time.time()
        spinner = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        i = 0
        while not stop.is_set():
            await asyncio.sleep(5)
            if stop.is_set():
                break
            elapsed = int(time.time() - start)
            mins, secs = divmod(elapsed, 60)
            elapsed_str = f"{mins}m {secs}s" if mins else f"{secs}s"
            try:
                await msg.edit(
                    f"{spinner[i % len(spinner)]} **Processing...** `{elapsed_str} elapsed`\n"
                    f"`Muxing & encoding — please wait`"
                )
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception:
                pass
            i += 1

    _stop_ticker = asyncio.Event()
    _ticker_task = asyncio.create_task(_processing_ticker(status, _stop_ticker))

    try:
        async with aiohttp.ClientSession() as session:
            if kind == "mp4":
                data = await fetch_mp4(session, yt_url, quality)
            else:
                data = await fetch_mp3(session, yt_url, quality)
    except Exception:
        _stop_ticker.set()
        await _ticker_task
        raise
    finally:
        _stop_ticker.set()

    try:
        if not data.get("status"):
            await safe_edit(status, "❌ API returned failure status.")
            return

        dl_url = data.get("url")
        if not dl_url:
            await safe_edit(status, "❌ API did not return a download URL.")
            return

        filename = data.get("filename", f"{vid_id}.{kind}")
        q_label = data.get("quality", str(quality))

        dest = DOWNLOAD_DIR / filename

        # Download
        await safe_edit(status, "⬇️ **Downloading...**\n`Starting...`")
        await download_file(dl_url, dest, status)

        file_size = dest.stat().st_size
        size_mb = file_size / 1e6

        # Upload to Telegram
        await safe_edit(status, f"📤 **Uploading** `{filename}`...")
        caption = (
            f"{'🎬' if kind == 'mp4' else '🎵'} **{filename}**\n"
            f"Quality: `{q_label}`  •  Size: `{size_mb:.1f} MB`\n"
            f"via @MNBotsYTDL"
        )

        async def _upload_progress(current: int, total: int) -> None:
            """Pyrogram upload progress callback — fires every ~512 KB."""
            now = time.time()
            # throttle edits to every 3 s to avoid flood
            if now - _upload_progress.last_edit < 3 and current < total:
                return
            _upload_progress.last_edit = now
            bar = progress_bar(current, total)
            done_str  = _fmt_size(current)
            total_str = _fmt_size(total)
            elapsed   = now - _upload_progress.start
            speed     = current / elapsed if elapsed else 0
            speed_str = _fmt_speed(speed)
            eta       = (total - current) / speed if speed and current < total else 0
            eta_str   = f"{int(eta//60)}m {int(eta%60)}s" if eta >= 60 else f"{int(eta)}s"
            await safe_edit(
                status,
                f"📤 **Uploading...**\n"
                f"{bar}\n"
                f"`{done_str}` / `{total_str}`\n"
                f"🚀 `{speed_str}`   ⏱ ETA `{eta_str}`",
            )
        _upload_progress.last_edit = time.time()
        _upload_progress.start     = time.time()

        if size_mb > Config.MAX_FILE_SIZE:
            await safe_edit(status,
                f"⚠️ File is `{size_mb:.0f} MB`, too large to send via Telegram.\n"
                f"[Direct Download Link]({dl_url})"
            )
        elif kind == "mp4":
            await cq.message.reply_video(
                str(dest),
                caption=caption,
                supports_streaming=True,
                progress=_upload_progress,
            )
            await status.delete()
        else:
            await cq.message.reply_audio(
                str(dest),
                caption=caption,
                title=clean_audio_title(filename),
                progress=_upload_progress,
            )
            await status.delete()

    except FloodWait as e:
        await asyncio.sleep(e.value)
        await safe_edit(status, "⚠️ Hit flood limit, please retry.")
    except Exception as e:
        logger.exception("Download/upload failed for %s", vid_id)
        try:
            await safe_edit(status, f"❌ Failed: `{e}`")
        except Exception:
            pass
    finally:
        # BUG FIX 9: dest may be None if exception occurred before assignment
        if dest is not None:
            try:
                if dest.exists():
                    dest.unlink()
            except Exception:
                pass
