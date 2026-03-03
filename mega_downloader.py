import os
import re
import subprocess
import asyncio
import time
import shutil
from pyrogram import Client, filters
from pyrogram.types import Message
from dotenv import load_dotenv

load_dotenv()

API_ID          = int(os.getenv("API_ID"))
API_HASH        = os.getenv("API_HASH")
BOT_TOKEN       = os.getenv("BOT_TOKEN")
STORAGE_CHANNEL = int(os.getenv("STORAGE_CHANNEL"))

app = Client("mega_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

MAX_SIZE_BYTES   = 4 * 1024 * 1024 * 1024
QUOTA_RETRY_SECS = 6 * 60 * 60
PREFIX           = "[TG - @Mid_Night_Hub]"
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm", ".m4v", ".ts", ".3gp"}
MEGA_LINK_RE     = re.compile(r'https?://mega\.nz/(?:file|folder)/[A-Za-z0-9_\-]+#[A-Za-z0-9_\-]+')

# Per-user queue: user_id -> asyncio.Queue of (url, message, status_msg)
user_queues:  dict[int, asyncio.Queue] = {}
user_workers: dict[int, bool]          = {}


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def fmt_size(b: float) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} TB"


def fmt_time(s: float) -> str:
    m, sec = divmod(int(s), 60)
    h, m   = divmod(m, 60)
    if h:
        return f"{h}h {m}m {sec}s"
    return f"{m}m {sec}s" if m else f"{sec}s"


def make_bar(current: int, total: int, length: int = 18):
    pct    = current / total if total else 0
    filled = int(length * pct)
    return "█" * filled + "░" * (length - filled), pct * 100


async def safe_edit(msg: Message, text: str):
    try:
        await msg.edit_text(text)
    except Exception:
        pass


def get_all_files(folder: str) -> list:
    result = []
    for root, dirs, files in os.walk(folder):
        for f in sorted(files):
            if not f.startswith(".megatmp"):
                result.append(os.path.join(root, f))
    return result


def is_folder_link(url: str) -> bool:
    return "/folder/" in url


# ══════════════════════════════════════════════════════════════════════════════
#  MEGA
# ══════════════════════════════════════════════════════════════════════════════

class QuotaExceededError(Exception):
    pass


def download_mega(url: str, dest_folder: str):
    result = subprocess.run(
        ["megatools", "dl", url, "--path", dest_folder],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        err = result.stderr + result.stdout
        if any(k in err.lower() for k in ["quota", "exceeded", "overquota", "509"]):
            raise QuotaExceededError(err)
        raise subprocess.CalledProcessError(result.returncode, "megatools", err)


def megals_list_files(url: str) -> list:
    """
    Returns list of full remote paths for files in a MEGA folder.
    e.g. ['/Root/FolderName/file1.mp4', '/Root/FolderName/file2.mp4']
    Returns [] if megals fails.
    """
    try:
        result = subprocess.run(
            ["megals", "--reload", url],
            capture_output=True, text=True, timeout=60
        )
        files = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if line and os.path.splitext(line)[1]:  # has extension = file not dir
                files.append(line)
        return sorted(files)
    except Exception:
        return []


def download_mega_path(remote_path: str, dest_folder: str):
    """Download a single file from MEGA using its remote path."""
    result = subprocess.run(
        ["megatools", "dl", "--path", dest_folder, remote_path],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        err = result.stderr + result.stdout
        if any(k in err.lower() for k in ["quota", "exceeded", "overquota", "509"]):
            raise QuotaExceededError(err)
        raise subprocess.CalledProcessError(result.returncode, "megatools", err)


# ══════════════════════════════════════════════════════════════════════════════
#  DOWNLOAD PROGRESS TRACKER
# ══════════════════════════════════════════════════════════════════════════════

async def track_download(dest_folder: str, status_msg: Message, header: str):
    start     = time.time()
    last_edit = 0
    last_size = 0

    while True:
        await asyncio.sleep(3)
        try:
            files = get_all_files(dest_folder)
        except Exception:
            break
        if not files:
            continue

        current = sum(os.path.getsize(f) for f in files)
        now     = time.time()

        if now - last_edit >= 3 and current != last_size:
            elapsed = now - start
            speed   = current / elapsed if elapsed > 0 else 0
            await safe_edit(
                status_msg,
                f"{header}\n\n"
                f"📦 **Downloaded:** {fmt_size(current)}\n"
                f"⚡ **Speed:** {fmt_size(speed)}/s\n"
                f"⏱️ **Elapsed:** {fmt_time(elapsed)}"
            )
            last_edit = now
            last_size = current


# ══════════════════════════════════════════════════════════════════════════════
#  UPLOAD ONE FILE
# ══════════════════════════════════════════════════════════════════════════════

async def upload_file(
    client: Client,
    message: Message,
    file_path: str,
    file_index: int,
    total_files: int,
    status_msg: Message,
) -> bool:
    if not os.path.exists(file_path):
        await safe_edit(status_msg, f"❌ File not found: `{os.path.basename(file_path)}`")
        return False

    actual_name = os.path.basename(file_path)
    actual_ext  = os.path.splitext(actual_name)[1].lower()
    actual_sz   = os.path.getsize(file_path)

    if actual_ext not in VIDEO_EXTENSIONS:
        await safe_edit(
            status_msg,
            f"⏭️ **Skipping — File {file_index}/{total_files}**\n"
            f"📄 `{actual_name}`\n"
            f"❌ Not a video file (`{actual_ext or 'unknown'}`)"
        )
        return False

    if actual_sz > MAX_SIZE_BYTES:
        await safe_edit(
            status_msg,
            f"⏭️ **Skipping — File {file_index}/{total_files}**\n"
            f"📄 `{actual_name}`\n"
            f"❌ Size `{fmt_size(actual_sz)}` exceeds 4 GB limit."
        )
        return False

    new_name = f"{PREFIX}{actual_name}"
    new_path = os.path.join(os.path.dirname(file_path), new_name)
    os.rename(file_path, new_path)

    caption = (
        f"**File Name :** `{new_name}`\n"
        f"**File Size :** `{fmt_size(actual_sz)}`"
    )

    await safe_edit(
        status_msg,
        f"📤 **Uploading — File {file_index}/{total_files}**\n"
        f"📄 `{new_name}`\n\n`Initializing...`"
    )
    upload_start  = time.time()
    last_up_edit  = [0.0]

    async def up_progress(current, total):
        now = time.time()
        if now - last_up_edit[0] < 3:
            return
        last_up_edit[0] = now
        elapsed  = now - upload_start
        speed    = current / elapsed if elapsed > 0 else 0
        eta      = (total - current) / speed if speed > 0 else 0
        bar, pct = make_bar(current, total)
        await safe_edit(
            status_msg,
            f"📤 **Uploading — File {file_index}/{total_files}**\n"
            f"📄 `{new_name}`\n\n"
            f"`[{bar}]` **{pct:.1f}%**\n\n"
            f"📦 **Size:** {fmt_size(current)} / {fmt_size(total)}\n"
            f"⚡ **Speed:** {fmt_size(speed)}/s\n"
            f"⏳ **ETA:** {fmt_time(eta)}"
        )

    try:
        sent = await client.send_video(
            STORAGE_CHANNEL,
            new_path,
            caption=caption,
            file_name=new_name,
            supports_streaming=True,
            progress=up_progress,
        )
        await sent.copy(
            message.chat.id,
            reply_to_message_id=message.id,
            caption=caption,
        )
        return True
    except Exception as e:
        await safe_edit(
            status_msg,
            f"❌ **Upload Failed — File {file_index}/{total_files}**\n"
            f"📄 `{new_name}`\n`{e}`"
        )
        return False
    finally:
        try:
            if os.path.exists(new_path):
                os.remove(new_path)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  PROCESS ONE MEGA LINK  (file or folder)
# ══════════════════════════════════════════════════════════════════════════════

async def process_one_link(
    client: Client,
    message: Message,
    url: str,
    status_msg: Message,
) -> bool:

    is_folder = is_folder_link(url)

    # ── Show what we're working with ─────────────────────────────────────────
    if is_folder:
        await safe_edit(
            status_msg,
            f"📂 **Folder link detected**\n🔗 `{url}`\n\n🔍 Counting files..."
        )
        file_count = megals_count(url)
        count_str  = f"{file_count} file{'s' if file_count != 1 else ''}" if file_count else "unknown files"
        dl_header  = f"📂 **Downloading Folder** ({count_str})\n🔗 `{url}`"
    else:
        dl_header  = f"📥 **Downloading...**\n🔗 `{url}`"

    # ── Download ──────────────────────────────────────────────────────────────
    dest = f"downloads/{message.chat.id}_{int(time.time())}"
    os.makedirs(dest, exist_ok=True)

    await safe_edit(status_msg, f"{dl_header}\n\n`Initializing...`")

    loop      = asyncio.get_event_loop()
    prog_task = asyncio.create_task(track_download(dest, status_msg, dl_header))

    try:
        await loop.run_in_executor(None, download_mega, url, dest)
    except QuotaExceededError:
        prog_task.cancel()
        shutil.rmtree(dest, ignore_errors=True)
        raise
    except subprocess.CalledProcessError as e:
        prog_task.cancel()
        shutil.rmtree(dest, ignore_errors=True)
        await safe_edit(
            status_msg,
            f"❌ **Download Failed!**\n🔗 `{url}`\n\n`{e.stderr or e}`"
        )
        return False
    finally:
        prog_task.cancel()

    # ── Find downloaded files ─────────────────────────────────────────────────
    all_files = get_all_files(dest)
    if not all_files:
        shutil.rmtree(dest, ignore_errors=True)
        await safe_edit(status_msg, f"❌ **No files found!**\n🔗 `{url}`")
        return False

    total_files = len(all_files)

    if is_folder:
        await safe_edit(
            status_msg,
            f"📂 **Folder downloaded**\n"
            f"📦 {total_files} file{'s' if total_files > 1 else ''} found\n"
            f"⏳ Uploading one by one..."
        )
        await asyncio.sleep(1)

    # ── Upload each file sequentially ─────────────────────────────────────────
    success = 0
    for idx in range(1, total_files + 1):
        current_files = get_all_files(dest)
        if not current_files:
            break
        fpath = current_files[0]
        fname = os.path.basename(fpath)

        await safe_edit(
            status_msg,
            f"⏳ **{'📂 Folder — ' if is_folder else ''}File {idx}/{total_files}**\n"
            f"📄 `{fname}`"
        )
        ok = await upload_file(client, message, fpath, idx, total_files, status_msg)
        if ok:
            success += 1

    shutil.rmtree(dest, ignore_errors=True)
    return success > 0


# ══════════════════════════════════════════════════════════════════════════════
#  QUOTA RETRY
# ══════════════════════════════════════════════════════════════════════════════

async def quota_retry_loop(
    client: Client,
    message: Message,
    url: str,
    notify_msg: Message,
    attempt: int = 1,
    max_attempts: int = 3,
):
    if attempt > max_attempts:
        await safe_edit(
            notify_msg,
            f"❌ **Quota exceeded {max_attempts} times — giving up.**\n"
            f"Please try again after 24 hours."
        )
        return

    wait = QUOTA_RETRY_SECS
    await safe_edit(
        notify_msg,
        f"⚠️ **MEGA Quota Exceeded!**\n\n"
        f"🔁 Auto-retry **#{attempt}/{max_attempts}** in **{fmt_time(wait)}**\n"
        f"📩 You'll be notified when it resumes."
    )
    await asyncio.sleep(wait)

    retry_msg = await message.reply(
        f"🔁 **Retrying (attempt {attempt}/{max_attempts})**\n🔗 `{url}`",
        quote=True,
    )
    try:
        ok = await process_one_link(client, message, url, retry_msg)
        if ok:
            try:
                await retry_msg.delete()
            except Exception:
                pass
    except QuotaExceededError:
        await quota_retry_loop(client, message, url, retry_msg, attempt + 1, max_attempts)


# ══════════════════════════════════════════════════════════════════════════════
#  PER-USER QUEUE WORKER
# ══════════════════════════════════════════════════════════════════════════════

async def user_worker(client: Client, user_id: int):
    queue = user_queues[user_id]

    while True:
        try:
            url, message, status_msg = await asyncio.wait_for(queue.get(), timeout=30)
        except asyncio.TimeoutError:
            user_workers.pop(user_id, None)
            user_queues.pop(user_id, None)
            break

        try:
            ok = await process_one_link(client, message, url, status_msg)
            if ok:
                try:
                    await status_msg.delete()
                except Exception:
                    pass
        except QuotaExceededError:
            asyncio.create_task(
                quota_retry_loop(client, message, url, status_msg)
            )
        except Exception as e:
            await safe_edit(status_msg, f"❌ Unexpected error:\n`{e}`")
        finally:
            queue.task_done()


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN HANDLER
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

    # Deduplicate preserving order
    seen, unique_links = set(), []
    for link in mega_links:
        if link not in seen:
            seen.add(link)
            unique_links.append(link)

    user_id = message.chat.id
    if user_id not in user_queues:
        user_queues[user_id] = asyncio.Queue()

    queue = user_queues[user_id]

    for url in unique_links:
        label      = "📂 Folder" if is_folder_link(url) else "📄 File"
        status_msg = await message.reply(
            f"⏳ **Queued** {label}\n🔗 `{url}`",
            quote=True,
        )
        await queue.put((url, message, status_msg))

    if not user_workers.get(user_id):
        user_workers[user_id] = True
        asyncio.create_task(user_worker(client, user_id))


# ══════════════════════════════════════════════════════════════════════════════
#  /retry COMMAND
# ══════════════════════════════════════════════════════════════════════════════

@app.on_message(filters.private & filters.command("retry"))
async def cmd_retry(client: Client, message: Message):
    await message.reply(
        "ℹ️ **Auto Retry Info**\n\n"
        "If MEGA quota is exceeded, bot automatically retries **up to 3 times**, "
        "waiting **6 hours** between each attempt.\n\n"
        "To force a fresh download, just send the MEGA link again.",
        quote=True,
    )


app.run()
