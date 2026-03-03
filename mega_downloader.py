import os
import re
import subprocess
import asyncio
import time
import shutil
import logging
import mimetypes
import signal
from logging.handlers import RotatingFileHandler
from pyrogram import Client, filters
from pyrogram.types import Message
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════════════

os.makedirs("logs", exist_ok=True)
_fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

_fh = RotatingFileHandler("logs/mega_bot.log", maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
_fh.setFormatter(_fmt)
_ch = logging.StreamHandler()
_ch.setFormatter(_fmt)

logger = logging.getLogger("MegaBot")
logger.setLevel(logging.INFO)
logger.addHandler(_fh)
logger.addHandler(_ch)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════

API_ID          = int(os.getenv("API_ID"))
API_HASH        = os.getenv("API_HASH")
BOT_TOKEN       = os.getenv("BOT_TOKEN")
STORAGE_CHANNEL = int(os.getenv("STORAGE_CHANNEL"))
HEALTH_PORT     = int(os.getenv("PORT", "8000"))

MAX_SIZE_BYTES   = 4 * 1024 * 1024 * 1024   # 4 GB
QUOTA_RETRY_SECS = 6 * 60 * 60              # 6 hours
PREFIX           = "[TG - @Mid_Night_Hub]"
MEGA_LINK_RE     = re.compile(r'https?://mega\.nz/(?:file|folder)/[A-Za-z0-9_\-]+#[A-Za-z0-9_\-]+')

# ── MEGA Accounts ─────────────────────────────────────────────────────────────
# .env: MEGA_ACCOUNTS=user1@gmail.com,user2@gmail.com,...
def _load_accounts() -> list:
    raw = os.getenv("MEGA_ACCOUNTS", "").strip()
    return [u.strip() for u in raw.split(",") if u.strip()] if raw else []

MEGA_ACCOUNTS  = _load_accounts()
_acc_idx       = [0]

def current_account() -> str | None:
    if not MEGA_ACCOUNTS:
        return None
    _acc_idx[0] = _acc_idx[0] % len(MEGA_ACCOUNTS)
    return MEGA_ACCOUNTS[_acc_idx[0]]

def rotate_account() -> tuple[str | None, bool]:
    """Returns (next_account, full_cycle_done)."""
    if not MEGA_ACCOUNTS:
        return None, True
    _acc_idx[0] += 1
    wrapped = _acc_idx[0] >= len(MEGA_ACCOUNTS)
    if wrapped:
        _acc_idx[0] = 0
    acc = MEGA_ACCOUNTS[_acc_idx[0]]
    logger.info(f"[ACCOUNT] → {acc} ({_acc_idx[0]+1}/{len(MEGA_ACCOUNTS)}) wrapped={wrapped}")
    return acc, wrapped

def reset_accounts():
    _acc_idx[0] = 0

# ── File type → Telegram send method ─────────────────────────────────────────
def get_file_type(path: str) -> str:
    """Returns: video | audio | photo | document"""
    ext  = os.path.splitext(path)[1].lower()
    mime = mimetypes.guess_type(path)[0] or ""

    if ext in {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm", ".m4v", ".ts", ".3gp"}:
        return "video"
    if ext in {".mp3", ".flac", ".ogg", ".wav", ".aac", ".m4a", ".opus", ".wma"}:
        return "audio"
    if ext in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}:
        return "photo"
    if mime.startswith("video/"):
        return "video"
    if mime.startswith("audio/"):
        return "audio"
    if mime.startswith("image/"):
        return "photo"
    return "document"

def file_type_icon(ftype: str) -> str:
    return {"video": "🎬", "audio": "🎵", "photo": "🖼️", "document": "📄"}.get(ftype, "📄")

# ══════════════════════════════════════════════════════════════════════════════
#  PYROGRAM CLIENT
# ══════════════════════════════════════════════════════════════════════════════

app = Client("mega_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Per-user queue
user_queues:  dict[int, asyncio.Queue] = {}
user_workers: dict[int, bool]          = {}

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def fmt_size(b: float) -> str:
    for u in ["B", "KB", "MB", "GB"]:
        if b < 1024: return f"{b:.2f} {u}"
        b /= 1024
    return f"{b:.2f} TB"

def fmt_time(s: float) -> str:
    m, sec = divmod(int(s), 60)
    h, m   = divmod(m, 60)
    return f"{h}h {m}m {sec}s" if h else (f"{m}m {sec}s" if m else f"{sec}s")

def make_bar(cur: int, total: int, n: int = 18) -> tuple:
    p = cur / total if total else 0
    return "█" * int(n*p) + "░" * (n - int(n*p)), p * 100

async def safe_edit(msg: Message, text: str):
    try:
        await msg.edit_text(text)
    except Exception:
        pass

def get_files(folder: str, include_tmp: bool = False) -> list:
    result = []
    for root, _, files in os.walk(folder):
        for f in sorted(files):
            if not include_tmp and f.startswith(".megatmp"):
                continue
            result.append(os.path.join(root, f))
    return result

def is_folder_link(url: str) -> bool:
    return "/folder/" in url

# ══════════════════════════════════════════════════════════════════════════════
#  MEGA DOWNLOAD
# ══════════════════════════════════════════════════════════════════════════════

class QuotaExceededError(Exception):
    pass

def _megatools_cmd(base: list) -> list:
    acc = current_account()
    return base + [f"--username={acc}"] if acc else base

def _run_megatools(cmd: list) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        err = result.stderr + result.stdout
        if any(k in err.lower() for k in ["quota", "exceeded", "overquota", "509"]):
            raise QuotaExceededError(err)
        raise subprocess.CalledProcessError(result.returncode, cmd[0], err)

def download_url(url: str, dest: str):
    acc = current_account() or "anonymous"
    logger.info(f"[DL] {url} | acc={acc}")
    _run_megatools(_megatools_cmd(["megatools", "dl", url, "--path", dest]))
    logger.info(f"[DL] Done: {url}")

# ══════════════════════════════════════════════════════════════════════════════
#  DOWNLOAD PROGRESS TRACKER
# ══════════════════════════════════════════════════════════════════════════════

async def track_download(dest: str, msg: Message, header: str):
    start = time.time()
    last_edit = last_size = 0
    while True:
        await asyncio.sleep(3)
        now     = time.time()
        elapsed = now - start
        try:
            files   = get_files(dest, include_tmp=True)
            current = sum(os.path.getsize(f) for f in files) if files else 0
        except Exception:
            break
        if now - last_edit >= 3:
            speed = current / elapsed if elapsed > 0 and current > 0 else 0
            if current > 0:
                await safe_edit(msg, f"{header}\n\n"
                    f"📦 **Downloaded:** {fmt_size(current)}\n"
                    f"⚡ **Speed:** {fmt_size(speed)}/s\n"
                    f"⏱️ **Elapsed:** {fmt_time(elapsed)}")
            else:
                await safe_edit(msg, f"{header}\n\n"
                    f"🔄 **Connecting to MEGA...**\n"
                    f"⏱️ **Elapsed:** {fmt_time(elapsed)}")
            last_edit = now
            last_size = current

# ══════════════════════════════════════════════════════════════════════════════
#  UPLOAD ONE FILE  (any type)
# ══════════════════════════════════════════════════════════════════════════════

async def upload_file(
    client: Client,
    message: Message,
    file_path: str,
    idx: int,
    total: int,
    status_msg: Message,
) -> bool:
    if not os.path.exists(file_path):
        await safe_edit(status_msg, f"❌ File not found: `{os.path.basename(file_path)}`")
        return False

    orig_name  = os.path.basename(file_path)
    actual_sz  = os.path.getsize(file_path)
    ftype      = get_file_type(file_path)
    icon       = file_type_icon(ftype)

    if actual_sz > MAX_SIZE_BYTES:
        await safe_edit(status_msg,
            f"⏭️ **Skipping — File {idx}/{total}**\n"
            f"📄 `{orig_name}`\n"
            f"❌ Size `{fmt_size(actual_sz)}` exceeds 4 GB limit.")
        return False

    # Rename with prefix
    new_name = f"{PREFIX}{orig_name}"
    new_path = os.path.join(os.path.dirname(file_path), new_name)
    os.rename(file_path, new_path)

    caption = f"**File Name :** `{new_name}`\n**File Size :** `{fmt_size(actual_sz)}`"

    await safe_edit(status_msg,
        f"📤 **Uploading {icon} — File {idx}/{total}**\n"
        f"📄 `{new_name}`\n\n`Initializing...`")

    upload_start = time.time()
    last_up = [0.0]

    async def progress(cur, tot):
        now = time.time()
        if now - last_up[0] < 3: return
        last_up[0] = now
        elapsed = now - upload_start
        speed   = cur / elapsed if elapsed > 0 else 0
        eta     = (tot - cur) / speed if speed > 0 else 0
        bar, pct = make_bar(cur, tot)
        await safe_edit(status_msg,
            f"📤 **Uploading {icon} — File {idx}/{total}**\n"
            f"📄 `{new_name}`\n\n"
            f"`[{bar}]` **{pct:.1f}%**\n\n"
            f"📦 **Size:** {fmt_size(cur)} / {fmt_size(tot)}\n"
            f"⚡ **Speed:** {fmt_size(speed)}/s\n"
            f"⏳ **ETA:** {fmt_time(eta)}")

    logger.info(f"[UP] {new_name} ({fmt_size(actual_sz)}) type={ftype} user={message.chat.id}")
    try:
        kwargs = dict(caption=caption, progress=progress)

        if ftype == "video":
            sent = await client.send_video(STORAGE_CHANNEL, new_path,
                file_name=new_name, supports_streaming=True, **kwargs)
        elif ftype == "audio":
            sent = await client.send_audio(STORAGE_CHANNEL, new_path,
                file_name=new_name, **kwargs)
        elif ftype == "photo":
            sent = await client.send_photo(STORAGE_CHANNEL, new_path, **kwargs)
        else:
            sent = await client.send_document(STORAGE_CHANNEL, new_path,
                file_name=new_name, **kwargs)

        await sent.copy(message.chat.id, reply_to_message_id=message.id, caption=caption)
        logger.info(f"[UP] Done: {new_name}")
        return True

    except Exception as e:
        logger.error(f"[UP] Failed: {new_name} | {e}")
        await safe_edit(status_msg,
            f"❌ **Upload Failed — File {idx}/{total}**\n📄 `{new_name}`\n`{e}`")
        return False
    finally:
        try:
            if os.path.exists(new_path): os.remove(new_path)
        except Exception:
            pass

# ══════════════════════════════════════════════════════════════════════════════
#  PROCESS ONE MEGA LINK
# ══════════════════════════════════════════════════════════════════════════════

async def process_one_link(client: Client, message: Message, url: str, status_msg: Message) -> bool:
    is_folder = is_folder_link(url)
    header    = f"📂 **Downloading Folder...**\n🔗 `{url}`" if is_folder else f"📥 **Downloading...**\n🔗 `{url}`"
    dest      = f"downloads/{message.chat.id}_{int(time.time())}"
    os.makedirs(dest, exist_ok=True)

    await safe_edit(status_msg, f"{header}\n\n🔄 Connecting to MEGA...")

    loop      = asyncio.get_event_loop()
    prog_task = asyncio.create_task(track_download(dest, status_msg, header))
    try:
        await loop.run_in_executor(None, download_url, url, dest)
    except QuotaExceededError:
        prog_task.cancel()
        shutil.rmtree(dest, ignore_errors=True)
        raise
    except subprocess.CalledProcessError as e:
        prog_task.cancel()
        shutil.rmtree(dest, ignore_errors=True)
        await safe_edit(status_msg, f"❌ **Download Failed!**\n🔗 `{url}`\n\n`{e.stderr or e}`")
        return False
    finally:
        prog_task.cancel()

    all_files = get_files(dest)
    if not all_files:
        shutil.rmtree(dest, ignore_errors=True)
        await safe_edit(status_msg, f"❌ **No files found!**\n🔗 `{url}`")
        return False

    total = len(all_files)
    if is_folder:
        await safe_edit(status_msg,
            f"📂 **Folder downloaded — {total} file{'s' if total>1 else ''} found**\n"
            f"⏳ Uploading one by one...")
        await asyncio.sleep(1)

    success = 0
    for idx in range(1, total + 1):
        cur_files = get_files(dest)
        if not cur_files: break
        fpath = cur_files[0]
        fname = os.path.basename(fpath)
        icon  = file_type_icon(get_file_type(fpath))
        await safe_edit(status_msg,
            f"⏳ **{'📂 ' if is_folder else ''}{icon} File {idx}/{total}**\n📄 `{fname}`")
        ok = await upload_file(client, message, fpath, idx, total, status_msg)
        if ok: success += 1

    shutil.rmtree(dest, ignore_errors=True)
    return success > 0

# ══════════════════════════════════════════════════════════════════════════════
#  QUOTA RETRY  (account rotation → 6hr wait → repeat)
# ══════════════════════════════════════════════════════════════════════════════

async def quota_retry_loop(client: Client, message: Message, url: str, notify_msg: Message):
    total = len(MEGA_ACCOUNTS) if MEGA_ACCOUNTS else 1
    next_acc, wrapped = rotate_account()

    if not wrapped:
        # Switch to next account — retry immediately
        logger.info(f"[QUOTA] Switching → {next_acc} for {url}")
        await safe_edit(notify_msg,
            f"⚠️ **Quota Exceeded!**\n\n"
            f"🔄 Switching account → `{next_acc}`\n"
            f"({_acc_idx[0]+1}/{total})")
        await asyncio.sleep(1)
        try:
            ok = await process_one_link(client, message, url, notify_msg)
            if ok:
                try: await notify_msg.delete()
                except Exception: pass
        except QuotaExceededError:
            await quota_retry_loop(client, message, url, notify_msg)

    else:
        # All accounts exhausted — wait 6hr then retry from acc1
        acc1 = MEGA_ACCOUNTS[0] if MEGA_ACCOUNTS else "anonymous"
        logger.warning(f"[QUOTA] All {total} accounts exhausted — waiting {fmt_time(QUOTA_RETRY_SECS)}")
        await safe_edit(notify_msg,
            f"⚠️ **All {total} MEGA account(s) quota exceeded!**\n\n"
            f"⏳ Waiting **{fmt_time(QUOTA_RETRY_SECS)}** for reset...\n"
            f"🔁 Will retry from `{acc1}`\n"
            f"📩 You'll be notified.")
        await asyncio.sleep(QUOTA_RETRY_SECS)
        reset_accounts()
        logger.info(f"[QUOTA] Wait done — retrying from: {acc1}")
        await safe_edit(notify_msg, f"🔁 **Retrying after quota reset**\n👤 `{acc1}`\n🔗 `{url}`")
        try:
            ok = await process_one_link(client, message, url, notify_msg)
            if ok:
                try: await notify_msg.delete()
                except Exception: pass
        except QuotaExceededError:
            await quota_retry_loop(client, message, url, notify_msg)

# ══════════════════════════════════════════════════════════════════════════════
#  PER-USER QUEUE WORKER
# ══════════════════════════════════════════════════════════════════════════════

async def user_worker(client: Client, user_id: int):
    queue = user_queues[user_id]
    while True:
        try:
            url, message, status_msg = await asyncio.wait_for(queue.get(), timeout=30)
        except asyncio.TimeoutError:
            logger.info(f"[WORKER] Idle — stopping for user {user_id}")
            user_workers.pop(user_id, None)
            user_queues.pop(user_id, None)
            break

        logger.info(f"[WORKER] Processing: {url} | user={user_id} | queue={queue.qsize()}")
        try:
            ok = await process_one_link(client, message, url, status_msg)
            if ok:
                try: await status_msg.delete()
                except Exception: pass
            else:
                logger.warning(f"[WORKER] No files uploaded: {url}")
        except QuotaExceededError:
            logger.warning(f"[WORKER] Quota exceeded: {url} — scheduling rotation")
            asyncio.create_task(quota_retry_loop(client, message, url, status_msg))
        except Exception as e:
            logger.exception(f"[WORKER] Error: {url} | {e}")
            await safe_edit(status_msg, f"❌ Unexpected error:\n`{e}`")
        finally:
            queue.task_done()

# ══════════════════════════════════════════════════════════════════════════════
#  HEALTH CHECK SERVER  (Koyeb ke liye — plain threading, no deps)
# ══════════════════════════════════════════════════════════════════════════════

from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass  # suppress access logs

def start_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info(f"[HEALTH] Server listening on 0.0.0.0:{HEALTH_PORT}")

# ══════════════════════════════════════════════════════════════════════════════
#  HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

@app.on_message(filters.private & filters.text)
async def handle(client: Client, message: Message):
    text = message.text.strip()

    mega_links = MEGA_LINK_RE.findall(text)
    if not mega_links:
        return await message.reply(
            "❌ No valid MEGA links found.\n"
            "Supported:\n"
            "• `https://mega.nz/file/XXXX#YYYY`\n"
            "• `https://mega.nz/folder/XXXX#YYYY`",
            quote=True,
        )

    seen, unique_links = set(), []
    for link in mega_links:
        if link not in seen:
            seen.add(link)
            unique_links.append(link)

    user_id   = message.chat.id
    user_name = getattr(message.from_user, "username", None) or getattr(message.from_user, "first_name", str(user_id))
    logger.info(f"[REQUEST] {user_name} ({user_id}) | {len(unique_links)} link(s)")
    for u in unique_links:
        logger.info(f"[REQUEST]   → {u}")

    if user_id not in user_queues:
        user_queues[user_id] = asyncio.Queue()

    queue = user_queues[user_id]
    for url in unique_links:
        label      = "📂 Folder" if is_folder_link(url) else "📄 File"
        status_msg = await message.reply(f"⏳ **Queued** {label}\n🔗 `{url}`", quote=True)
        await queue.put((url, message, status_msg))
        logger.info(f"[QUEUE] Added | size={queue.qsize()} | user={user_id}")

    if not user_workers.get(user_id):
        user_workers[user_id] = True
        logger.info(f"[WORKER] Starting for user {user_id}")
        asyncio.create_task(user_worker(client, user_id))


@app.on_message(filters.private & filters.command("start"))
async def cmd_start(client: Client, message: Message):
    await message.reply(
        "👋 **MEGA Downloader Bot**\n\n"
        "Send me any MEGA link and I'll download and upload it here.\n\n"
        "**Supported:**\n"
        "• `mega.nz/file/...` — single file\n"
        "• `mega.nz/folder/...` — entire folder\n\n"
        "**File types:** 🎬 Video • 🎵 Audio • 🖼️ Photo • 📄 Documents • 📦 Archives\n"
        "**Max size:** 4 GB per file\n\n"
        "Just send the link(s) and I'll handle the rest!",
        quote=True,
    )


@app.on_message(filters.private & filters.command("status"))
async def cmd_status(client: Client, message: Message):
    user_id = message.chat.id
    q_size  = user_queues[user_id].qsize() if user_id in user_queues else 0
    acc     = current_account() or "anonymous"
    await message.reply(
        f"📊 **Bot Status**\n\n"
        f"👤 **Active Account:** `{acc}`\n"
        f"📋 **Your Queue:** `{q_size}` item(s) pending\n"
        f"🏦 **Total Accounts:** `{len(MEGA_ACCOUNTS)}`",
        quote=True,
    )

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    # 1. Health server FIRST — Koyeb checks it immediately on startup
    start_health_server()
    logger.info(f"[HEALTH] Ready on port {HEALTH_PORT}")

    # 2. Startup info
    if MEGA_ACCOUNTS:
        logger.info(f"[STARTUP] {len(MEGA_ACCOUNTS)} MEGA account(s) loaded:")
        for i, acc in enumerate(MEGA_ACCOUNTS, 1):
            logger.info(f"[STARTUP]   {i}. {acc}")
        reset_accounts()
    else:
        logger.info("[STARTUP] No MEGA accounts — anonymous mode")

    # 3. Start Pyrogram bot
    logger.info("[STARTUP] Connecting to Telegram...")
    await app.start()
    logger.info("[STARTUP] Bot online ✅")

    # 4. Keep alive — handle signals gracefully
    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("[SHUTDOWN] Signal received")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass  # Windows

    await stop_event.wait()

    logger.info("[SHUTDOWN] Stopping bot...")
    try:
        await app.stop()
    except Exception as e:
        logger.warning(f"[SHUTDOWN] Stop error (ignored): {e}")
    logger.info("[SHUTDOWN] Done.")

if __name__ == "__main__":
    asyncio.run(main())
