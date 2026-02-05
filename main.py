import os
import logging
import asyncio
import time
import base64
import random
import sys
import json 
import traceback

import httpx
import aiofiles
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from telegram.error import TimedOut, NetworkError, RetryAfter, TelegramError, BadRequest
from telegram.request import HTTPXRequest 
from telegram.ext import (
    ApplicationBuilder, ContextTypes, CommandHandler,
    MessageHandler, CallbackQueryHandler, filters, ConversationHandler
)

import database as db

# =========================================================
# CONFIG
# =========================================================
load_dotenv(dotenv_path=".env")

TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    print("❌ ERROR: Token 'TELEGRAM_TOKEN' tidak ditemukan di file .env!")
    raise SystemExit(1)

ADMIN_ID = int(os.getenv("ADMIN_ID")) if os.getenv("ADMIN_ID") else None

# =========================================================
# LOGGING SETUP
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler("bot.log", maxBytes=5*1024*1024, backupCount=2),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.ext._application").setLevel(logging.INFO)

# =========================================================
# GLOBAL LIMITS
# =========================================================
CURRENT_RUNNING_TASKS = 0
MAX_SAFE_CONCURRENT = 10 
MAX_TASKS_PER_USER = 3 
USER_TASK_COUNTS: dict[int, int] = {}

TASK_SEMAPHORE = asyncio.Semaphore(MAX_SAFE_CONCURRENT)
UPLOAD_SEMAPHORE = asyncio.Semaphore(1)
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(1)

# States
SET_APIKEY_STATE, WAITING_IMAGE, WAITING_REF_VIDEO, WAITING_PROMPT, WAITING_ORIENTATION, WAITING_DURATION = range(6)
ADMIN_SELECT, ADMIN_INPUT = range(2)

# =========================================================
# HELPERS
# =========================================================
def is_admin(user_id: int) -> bool:
    return bool(ADMIN_ID) and user_id == ADMIN_ID

def sanitize_html(text):
    if not text: return "None"
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def tg_retry(func, *args, **kwargs):
    for attempt in range(1, 6):
        try:
            return await func(*args, **kwargs)
        except BadRequest as e:
            if "Message is not modified" in str(e): return
            raise e
        except RetryAfter as e:
            await asyncio.sleep(int(getattr(e, "retry_after", 2)) + 1)
        except (TimedOut, NetworkError):
            await asyncio.sleep(min(2 * attempt, 8))
        except TelegramError as e: raise e
        except Exception: await asyncio.sleep(min(2 * attempt, 8))
    raise NetworkError("Max retries exceeded")

async def check_auth(update: Update) -> bool:
    user = update.effective_user
    if not user: return False
    if is_admin(user.id): return True
    return db.check_access(user.id)

def safe_user_label(user) -> str:
    first = (user.first_name or "").strip() or "User"
    last = (user.last_name or "").strip()
    return (first + (" " + last if last else "")).strip()

def format_task_slot(user_id: int) -> str:
    used = USER_TASK_COUNTS.get(user_id, 0)
    return f"{min(used, MAX_TASKS_PER_USER)}/{MAX_TASKS_PER_USER}"

def build_progress_text(stage: str, extra: str = "") -> str:
    base = f"☕ 🚬 <b>{stage}</b>"
    if extra: base += f"\n{extra}"
    return base

async def check_freepik_key(api_key: str) -> bool:
    url = "https://api.freepik.com/v1/ai/image-to-video/pixverse-v5"
    headers = {"x-freepik-api-key": api_key, "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json={}, headers=headers)
            return resp.status_code not in [401, 403]
    except:
        return False

async def notify_admin(bot, user, model, prompt, status="STARTED", task_id="N/A", error_msg="", vid_url=None):
    if not ADMIN_ID: return
    
    # Handle user object being a dict (from DB resumption) or Telegram User object
    if isinstance(user, dict):
        user_id = user.get("user_id")
        username = f"@{user.get('username')}" if user.get("username") else "No Username"
        full_name = user.get("full_name") or "Restored User"
    else:
        user_id = user.id
        username = f"@{user.username}" if user.username else "No Username"
        full_name = sanitize_html(safe_user_label(user))

    emoji = "🔔" if status == "STARTED" else "✅" if status == "COMPLETED" else "❌"
    
    msg = (
        f"{emoji} <b>TASK {status}</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"🤖 <b>Model:</b> <code>{model}</code>\n"
        f"🆔 <b>Task ID:</b> <code>{task_id}</code>\n"
        f"👤 <b>User:</b> {full_name} ({username})\n"
        f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
    )

    if status == "STARTED":
        msg += f"📝 <b>Prompt:</b> <i>{sanitize_html(prompt[:300])}</i>"
    elif status == "COMPLETED":
        if vid_url: msg += f"🔗 <b>URL:</b> <a href='{vid_url}'>Lihat Hasil</a>"
    else:
        msg += f"⚠️ <b>Error:</b> <code>{sanitize_html(error_msg)}</code>"

    try:
        await bot.send_message(chat_id=ADMIN_ID, text=msg, parse_mode="HTML", disable_web_page_preview=True)
    except: pass

async def smart_upload_file(file_path: str) -> str | None:
    filename = os.path.basename(file_path)
    ext = os.path.splitext(file_path)[1] or ".mp4"
    try:
        file_size = os.path.getsize(file_path) / 1024 / 1024
        logger.info(f"📂 [UPLOAD] Baca file: {filename} ({file_size:.2f} MB)")
        async with aiofiles.open(file_path, "rb") as f: content = await f.read()
    except Exception as e:
        logger.error(f"❌ Gagal baca file: {e}")
        return None

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    # --- 1. CATBOX ---
    try:
        logger.info(f"🚀 Upload Catbox (Timeout 120s)...")
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post("https://catbox.moe/user/api.php", 
                data={"reqtype": "fileupload"}, files={"fileToUpload": (f"media{ext}", content)}, headers=headers)
        
        if resp.status_code == 200:
            if resp.text.startswith("http"):
                url = resp.text.strip().replace("http://", "https://")
                logger.info(f"✅ Catbox OK: {url}")
                return url
            else:
                logger.warning(f"⚠️ Catbox 200 tapi response aneh: {resp.text[:50]}")
    except Exception as e:
        logger.warning(f"⚠️ Catbox Error: {e}")

    # --- 2. TMPFILES (Fallback) ---
    try:
        logger.info(f"🚀 Fallback Tmpfiles (Timeout 120s)...")
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post("https://tmpfiles.org/api/v1/upload", 
                files={"file": (filename, content)}, headers=headers)
            
        if resp.status_code == 200:
            raw_url = resp.json().get("data", {}).get("url", "")
            if raw_url:
                direct_url = raw_url.replace("tmpfiles.org/", "tmpfiles.org/dl/").replace("http://", "https://")
                logger.info(f"✅ Tmpfiles OK: {direct_url}")
                return direct_url
    except Exception as e:
        logger.error(f"❌ Tmpfiles Error: {e}")

    return None

# =========================================================
# WORKER ENGINE
# =========================================================
async def run_kling_task(app, user_obj, chat_id, prompt, image_path, video_path, api_key, model_version, duration="5", orientation="video"):
    global CURRENT_RUNNING_TASKS, USER_TASK_COUNTS
    bot = app.bot
    t0 = time.time()
    task_id = "PENDING"
    
    if "motion" in model_version: model_name = "🕺 Kling Motion"
    elif model_version == "pixverse_v5": model_name = "🌟 PixVerse V5"
    elif model_version == "seedance": model_name = "💃 Seedance"
    else: model_name = f"🎬 Kling {model_version}"

    current = USER_TASK_COUNTS.get(chat_id, 0)
    if current >= MAX_TASKS_PER_USER:
        await tg_retry(bot.send_message, chat_id, "⛔ <b>Limit 3 task aktif!</b>", parse_mode="HTML")
        return

    USER_TASK_COUNTS[chat_id] = current + 1
    status_msg = None

    try:
        if CURRENT_RUNNING_TASKS >= MAX_SAFE_CONCURRENT:
            await tg_retry(bot.send_message, chat_id, f"⏳ <b>Antrian Penuh ({CURRENT_RUNNING_TASKS})...</b>", parse_mode="HTML")

        async with TASK_SEMAPHORE:
            CURRENT_RUNNING_TASKS += 1
            status_msg = await tg_retry(bot.send_message, chat_id, build_progress_text("Memulai...", f"Slot: {format_task_slot(chat_id)}"), parse_mode="HTML")
            
            BASE_URL = "https://api.freepik.com"
            headers = {"x-freepik-api-key": api_key, "Content-Type": "application/json"}
            payload = {}
            URL_CREATE = ""
            URL_STATUS_TEMPLATE = ""

            if "motion" in model_version:
                await tg_retry(bot.edit_message_text, chat_id=chat_id, message_id=status_msg.message_id, text=build_progress_text("Upload asset..."), parse_mode="HTML")
                async with UPLOAD_SEMAPHORE:
                    pub_img = await smart_upload_file(image_path)
                    pub_vid = await smart_upload_file(video_path) if video_path else None
                
                if not pub_img: raise Exception("Gagal Upload Gambar")
                if video_path and not pub_vid: raise Exception("Gagal Upload Video")
                
                ver = "pro" if "pro" in model_version else "std"
                URL_CREATE = f"{BASE_URL}/v1/ai/video/kling-v2-6-motion-control-{ver}"
                URL_STATUS_TEMPLATE = f"{BASE_URL}/v1/ai/image-to-video/kling-v2-6/{{}}"
                payload = {"image_url": pub_img, "video_url": pub_vid, "prompt": prompt or "Moving", "cfg_scale": 0.5, "character_orientation": orientation}

            elif model_version == "pixverse_v5":
                URL_CREATE = f"{BASE_URL}/v1/ai/image-to-video/pixverse-v5"
                URL_STATUS_TEMPLATE = f"{BASE_URL}/v1/ai/image-to-video/pixverse-v5/{{}}"
                async with UPLOAD_SEMAPHORE: pub_img = await smart_upload_file(image_path)
                if not pub_img: raise Exception("Gagal Upload Gambar")
                payload = {"prompt": prompt, "resolution": "1080p", "duration": int(duration), "image_url": pub_img, "seed": random.randint(1, 999999)}
            
            elif model_version == "seedance":
                await tg_retry(bot.edit_message_text, chat_id=chat_id, message_id=status_msg.message_id, text=build_progress_text("Encoding..."), parse_mode="HTML")
                async with aiofiles.open(image_path, "rb") as f: b64 = base64.b64encode(await f.read()).decode("utf-8")
                URL_CREATE = f"{BASE_URL}/v1/ai/video/seedance-1-5-pro-1080p"
                URL_STATUS_TEMPLATE = f"{BASE_URL}/v1/ai/video/seedance-1-5-pro-1080p/{{}}"
                payload = {"image": f"data:image/jpeg;base64,{b64}", "prompt": prompt, "duration": int(duration), "generate_audio": True, "aspect_ratio": "widescreen_16_9"}

            else: 
                await tg_retry(bot.edit_message_text, chat_id=chat_id, message_id=status_msg.message_id, text=build_progress_text("Encoding..."), parse_mode="HTML")
                async with aiofiles.open(image_path, "rb") as f: b64 = base64.b64encode(await f.read()).decode("utf-8")
                ver = "v2-6" if "2.6" in model_version else "v2-5" if "2.5" in model_version else "v2-1"
                URL_CREATE = f"{BASE_URL}/v1/ai/image-to-video/kling-{ver}-pro"
                URL_STATUS_TEMPLATE = f"{BASE_URL}/v1/ai/image-to-video/kling-{ver}/{{}}"
                payload = {"image": f"data:image/jpeg;base64,{b64}", "prompt": prompt, "duration": duration, "cfg_scale": 0.5}

            await tg_retry(bot.edit_message_text, chat_id=chat_id, message_id=status_msg.message_id, text=build_progress_text("Mengirim ke AI..."), parse_mode="HTML")
            
            async with httpx.AsyncClient(timeout=300.0) as client:
                res = await client.post(URL_CREATE, json=payload, headers=headers)
                
                if res.status_code != 200:
                    raw_err = res.text.lower()
                    if "daily limit" in raw_err or "payment" in raw_err or "quota" in raw_err:
                        err_msg = "⛔ <b>Kuota API Habis!</b>\nSilakan ganti API Key di menu utama."
                        admin_err = "Kuota Habis / Daily Limit"
                    else:
                        try:
                            err_msg = f"❌ <b>API Error:</b>\n{res.json().get('message', 'Unknown')}"
                            admin_err = res.text
                        except:
                            err_msg = f"❌ <b>API Error:</b>\n{res.text[:200]}"
                            admin_err = res.text
                    
                    logger.error(f"❌ API FAILED (Initial): {res.text}") 
                    await notify_admin(bot, user_obj, model_name, prompt, "FAILED", task_id="N/A", error_msg=admin_err)
                    await tg_retry(bot.send_message, chat_id, err_msg, parse_mode="HTML")
                    return
                
                task_id = res.json()["data"]["task_id"]
                
                # --- PERSISTENCE START ---
                db.add_task(task_id, user_obj.id, chat_id, model_name, prompt, time.time())
                # --- PERSISTENCE END ---

                await notify_admin(bot, user_obj, model_name, prompt, "STARTED", task_id=task_id)
                await tg_retry(bot.edit_message_text, chat_id=chat_id, message_id=status_msg.message_id, text=build_progress_text("Rendering...", f"ID: <code>{task_id}</code>"), parse_mode="HTML")
                
                # Handover to polling function (AWAIT to keep semaphore/limits active)
                await poll_and_finish_task(bot, chat_id, user_obj, model_name, prompt, task_id, api_key, model_version, duration)

    except Exception as e:
        logger.error(f"❌ System Error: {traceback.format_exc()}")
        await tg_retry(bot.send_message, chat_id, f"❌ Terjadi kesalahan sistem: {str(e)[:100]}")
    finally:
        CURRENT_RUNNING_TASKS = max(0, CURRENT_RUNNING_TASKS - 1)
        USER_TASK_COUNTS[chat_id] = max(0, USER_TASK_COUNTS.get(chat_id, 1) - 1)
        for p in [image_path, video_path]:
            try: os.remove(p) if p else None
            except: pass

async def poll_and_finish_task(bot, chat_id, user_obj, model_name, prompt, task_id, api_key, model_version, duration, start_time=None):
    if not start_time: start_time = time.time()
    
    headers = {"x-freepik-api-key": api_key, "Content-Type": "application/json"}
    
    # Reconstruct URL template based on model_version (Logic copied from run_kling_task)
    BASE_URL = "https://api.freepik.com"
    if "motion" in model_version:
        URL_STATUS_TEMPLATE = f"{BASE_URL}/v1/ai/image-to-video/kling-v2-6/{{}}"
    elif model_version == "pixverse_v5":
        URL_STATUS_TEMPLATE = f"{BASE_URL}/v1/ai/image-to-video/pixverse-v5/{{}}"
    elif model_version == "seedance":
        URL_STATUS_TEMPLATE = f"{BASE_URL}/v1/ai/video/seedance-1-5-pro-1080p/{{}}"
    else:
        ver = "v2-6" if "2.6" in model_version else "v2-5" if "2.5" in model_version else "v2-1"
        URL_STATUS_TEMPLATE = f"{BASE_URL}/v1/ai/image-to-video/kling-{ver}/{{}}"

    async with httpx.AsyncClient(timeout=60.0) as client:
        # Loop for 60 minutes
        for _ in range(360):
            await asyncio.sleep(10)
            try:
                check = await client.get(URL_STATUS_TEMPLATE.format(task_id), headers=headers)
                if check.status_code != 200: continue
                data = check.json().get("data", {})
                status = data.get("status")

                if status == "COMPLETED":
                    vid_url = (data.get("generated") or [{}])[0] if isinstance(data.get("generated"), list) else None
                    if not vid_url: vid_url = (data.get("videos") or [{}])[0].get("url")
                    
                    v_file = f"vid_{task_id}.mp4"
                    async with DOWNLOAD_SEMAPHORE:
                        vid_req = await client.get(vid_url, timeout=180.0, follow_redirects=True)
                        async with aiofiles.open(v_file, "wb") as f: await f.write(vid_req.content)

                    f_size = os.path.getsize(v_file)
                    caption = f"✨ <b>{model_name} SUKSES!</b>\n⏱ {duration}s | ⏳ {int(time.time()-start_time)}s"
                    
                    if f_size > 50 * 1024 * 1024:
                        await tg_retry(bot.send_message, chat_id, f"{caption}\n\n📦 <b>File > 50MB.</b>\n📥 <a href='{vid_url}'>Klik Download Disini</a>", parse_mode="HTML")
                    else:
                        with open(v_file, "rb") as vf: 
                            await tg_retry(bot.send_video, chat_id, vf, caption=caption, parse_mode="HTML")
                    
                    await notify_admin(bot, user_obj, model_name, prompt, "COMPLETED", task_id=task_id, vid_url=vid_url)
                    try: os.remove(v_file)
                    except: pass
                    
                    db.increment_usage(chat_id)
                    db.remove_task(task_id) # CLEANUP DB
                    return
                
                if status == "FAILED":
                    err_det = data.get("message") or data.get("error") or "Unknown API failure"
                    await notify_admin(bot, user_obj, model_name, prompt, "FAILED", task_id=task_id, error_msg=err_det)
                    await tg_retry(bot.send_message, chat_id, f"❌ <b>GAGAL:</b> {sanitize_html(err_det)}", parse_mode="HTML")
                    db.remove_task(task_id) # CLEANUP DB
                    return

            except Exception as e:
                logger.error(f"Polling error task {task_id}: {e}")
                continue
        
        # Timeout
        await tg_retry(bot.send_message, chat_id, "⚠️ Timeout Rendering (> 60 Menit).")
        db.remove_task(task_id)

async def resume_pending_tasks(app):
    tasks = db.get_active_tasks()
    if not tasks: return
    
    logger.info(f"🔄 Resuming {len(tasks)} pending tasks...")
    for row in tasks:
        # Reconstruct user object dict for notify_admin
        user_dict = {
            "user_id": row["user_id"],
            "username": "ResumedUser", # We could fetch real username if we wanted
            "full_name": "Resumed User"
        }
        # Attempt to determine model_version from model_name (A bit tricky, but we can store it or guess)
        # For simplicity, we stored 'model' which is "model_name". We need to guess 'model_version' for URL template.
        # Actually, let's just make sure we passed the right string in 'model' when saving.
        # In run_kling_task, we passed 'model_name' (e.g. "🎬 Kling 2.6"). This is display name.
        # ISSUE: poll_and_finish_task needs 'model_version' (e.g. 'kling-v2-6-pro') to build URL.
        # FIX: We should save 'model_version' in DB ideally. Or map it back.
        # Let's simple MAP back for now to minimize DB changes if possible, OR just save raw version in DB in previous step.
        # Wait, I saved 'model_name' in previous chunk logic: `db.add_task(..., model_name, ...)`
        # model_name is "🎬 Kling 2.6".
        # I should change that to save 'model_version' or have a mapper.
        
        # Mapping back
        m_name = row["model"]
        m_ver = "kling-2.6" # default
        if "Motion" in m_name: m_ver = "kling-motion" 
        elif "PixVerse" in m_name: m_ver = "pixverse_v5"
        elif "Seedance" in m_name: m_ver = "seedance"
        elif "2.5" in m_name: m_ver = "kling-2.5"
        elif "2.1" in m_name: m_ver = "kling-2.1"
        elif "2.6" in m_name: m_ver = "kling-2.6" # pro/std handled inside poll based on string

        api_key = db.get_apikey(row["user_id"])
        if not api_key: 
            db.remove_task(row["task_id"])
            continue

        asyncio.create_task(poll_and_finish_task(
            app.bot, row["chat_id"], user_dict, row["model"], row["prompt"], 
            row["task_id"], api_key, m_ver, "5", row["start_time"]
        ))


# =========================================================
# HANDLERS
# =========================================================
async def admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return ConversationHandler.END
    kb = [[InlineKeyboardButton("➕ Add User", callback_data="admin_add"), InlineKeyboardButton("➖ Del User", callback_data="admin_del")],
          [InlineKeyboardButton("📊 List Users", callback_data="admin_list")],
          [InlineKeyboardButton("❌ Close", callback_data="admin_close")]]
    await tg_retry(update.effective_message.reply_text, "👮‍♂️ <b>ADMIN</b>", reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
    return ADMIN_SELECT

async def admin_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "admin_close":
        try: await q.message.delete()
        except: pass
        return ConversationHandler.END
    if q.data == "admin_list":
        users = db.get_all_users()
        msg = "📊 <b>TOP USERS:</b>\n" + "\n".join([f"• <code>{u['user_id']}</code> | {u['usage_count']}" for u in users[-50:]])
        await tg_retry(q.edit_message_text, msg, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data="admin_home")]]), parse_mode="HTML")
        return ADMIN_SELECT
    if q.data in ["admin_add", "admin_del"]:
        context.user_data["mode"] = q.data
        await tg_retry(q.edit_message_text, "Kirim ID User:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data="admin_home")]]))
        return ADMIN_INPUT
    return await admin_start(update, context)

async def admin_process_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        tid = int(update.message.text.strip())
        if context.user_data["mode"] == "admin_add": db.grant_access(tid)
        else: db.delete_user(tid)
        await update.message.reply_text("✅ Done.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data="admin_home")]]))
        return ADMIN_SELECT
    except: return ADMIN_INPUT

async def set_apikey_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    current_key = db.get_apikey(q.from_user.id)
    txt = "🔑 <b>Setting API Key</b>\n\n"
    if current_key:
        masked = f"....{current_key[-8:]}" if len(current_key) > 8 else current_key
        txt += f"Key saat ini: <code>{masked}</code>\n\nKirim key baru atau klik Batal."
    else:
        txt += "Silakan kirimkan API Key Freepik Anda sekarang."
    kb = [[InlineKeyboardButton("🔙 Batal", callback_data="back_home")]]
    await tg_retry(q.edit_message_text, txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
    return SET_APIKEY_STATE

async def save_apikey(update: Update, context: ContextTypes.DEFAULT_TYPE):
    api_key = update.message.text.strip()
    if len(api_key) < 5:
        await update.message.reply_text("❌ API Key tidak valid.")
        return SET_APIKEY_STATE
    wait = await update.message.reply_text("⏳ <i>Memeriksa key...</i>", parse_mode="HTML")
    if await check_freepik_key(api_key):
        db.set_apikey(update.effective_user.id, api_key)
        await tg_retry(wait.edit_text, "✅ <b>Key Tersimpan!</b>\nSilakan coba generate.", parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Menu Utama", callback_data="back_home")]]))
        return ConversationHandler.END
    else:
        await tg_retry(wait.edit_text, "❌ <b>Key INVALID.</b> Coba lagi.", parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Batal", callback_data="back_home")]]))
        return SET_APIKEY_STATE

def build_home_keyboard(user_id: int, has_key: bool = False):
    key_label = "🔄 Ganti API Key" if has_key else "➕ Input API Key"
    kb = [[InlineKeyboardButton("🌟 PixVerse V5", callback_data="gen_pixverse_v5")],
          [InlineKeyboardButton("💃 Seedance 1.5", callback_data="gen_seedance")],
          [InlineKeyboardButton("🕺 Motion Std", callback_data="gen_motion_std"), InlineKeyboardButton("💃 Motion Pro", callback_data="gen_motion_pro")],
          [InlineKeyboardButton("🚀 Kling 2.6 Pro", callback_data="gen_2.6"), InlineKeyboardButton("⚡ Kling 2.5", callback_data="gen_2.5")],
          [InlineKeyboardButton("🎬 Kling 2.1", callback_data="gen_2.1")],
          [InlineKeyboardButton(key_label, callback_data="menu_apikey"), InlineKeyboardButton("🆔 ID", callback_data="show_user_id")]]
    if is_admin(user_id): kb.append([InlineKeyboardButton("👮‍♂️ Admin", callback_data="open_admin")])
    return InlineKeyboardMarkup(kb)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear() # Reset state
    u = update.effective_user
    db.register_user(u.id, safe_user_label(u), u.username)
    has_key = bool(db.get_apikey(u.id))
    current_key = db.get_apikey(u.id) or ""
    masked = f".......{current_key[-8:]}" if len(current_key) > 8 else "❌ Belum diset"
    
    txt = f"🤖 <b>Bot Ready</b>\nSlot: {format_task_slot(u.id)}\n🔑 Key: <code>{masked}</code>"
    func = update.callback_query.edit_message_text if update.callback_query else update.message.reply_text
    await tg_retry(func, txt, reply_markup=build_home_keyboard(u.id, has_key), parse_mode="HTML")
    return ConversationHandler.END

async def pre_gen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not await check_auth(update) or not db.get_apikey(q.from_user.id):
        await q.answer("⛔ Akses Ditolak / API Key Kosong", show_alert=True)
        return ConversationHandler.END
    context.user_data.clear()
    context.user_data["model"] = q.data.split("gen_", 1)[1]
    await tg_retry(q.edit_message_text, "🖼 <b>Kirim Gambar:</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data="back_home")]]))
    return WAITING_IMAGE

async def get_img(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document or (update.message.photo[-1] if update.message.photo else None)
    if doc.file_size > 20 * 1024 * 1024:
        await update.message.reply_text("❌ File > 20MB.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data="back_home")]]))
        return WAITING_IMAGE
    f = await tg_retry(doc.get_file)
    path = f"img_{update.effective_user.id}_{int(time.time())}.jpg"
    await f.download_to_drive(path)
    context.user_data["img"] = path
    if "motion" in context.user_data["model"]:
        await update.message.reply_text("🎥 <b>Kirim Video Referensi:</b>", parse_mode="HTML")
        return WAITING_REF_VIDEO
    await update.message.reply_text("📝 <b>Prompt:</b>", parse_mode="HTML")
    return WAITING_PROMPT

async def get_vid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.video or update.message.document
    if doc.file_size > 20 * 1024 * 1024:
        await update.message.reply_text("❌ File > 20MB.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data="back_home")]]))
        return WAITING_REF_VIDEO
    f = await tg_retry(doc.get_file)
    path = f"vid_{update.effective_user.id}_{int(time.time())}.mp4"
    await f.download_to_drive(path)
    context.user_data["vid"] = path
    await update.message.reply_text("📝 <b>Prompt Gerakan:</b>", parse_mode="HTML")
    return WAITING_PROMPT

async def get_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["prompt"] = update.message.text.strip()
    if "motion" in context.user_data["model"]:
        kb = [[InlineKeyboardButton("🕺 Video Focus", callback_data="or_video")], [InlineKeyboardButton("📷 Image Focus", callback_data="or_image")]]
        await update.message.reply_text("🎯 <b>Fokus Gerakan:</b>", reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
        return WAITING_ORIENTATION
    return await show_dur(update, context)

async def get_orient(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data["orient"] = q.data.split("_")[1]
    await tg_retry(q.edit_message_text, "🚀 <b>Task Diterima!</b>", parse_mode="HTML")
    asyncio.create_task(run_kling_task(context.application, q.from_user, q.from_user.id, context.user_data["prompt"], context.user_data["img"], context.user_data.get("vid"), db.get_apikey(q.from_user.id), context.user_data["model"], "5", context.user_data.get("orient")))
    return ConversationHandler.END

async def show_dur(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [[InlineKeyboardButton("⏱ 5 Detik", callback_data="dur_5"), InlineKeyboardButton("⏱ 10 Detik", callback_data="dur_10")]]
    await update.message.reply_text("⏱ <b>Pilih Durasi:</b>", reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
    return WAITING_DURATION

async def run_gen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    dur = q.data.split("_")[1]
    await tg_retry(q.edit_message_text, "🚀 <b>Task Diterima!</b>", parse_mode="HTML")
    asyncio.create_task(run_kling_task(context.application, q.from_user, q.from_user.id, context.user_data["prompt"], context.user_data["img"], None, db.get_apikey(q.from_user.id), context.user_data["model"], dur))
    return ConversationHandler.END

# =========================================================
# MAIN
# =========================================================
def run_bot():
    t_request = HTTPXRequest(
        connection_pool_size=8,
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
        http_version="1.1"
    )

    app = ApplicationBuilder().token(TOKEN).request(t_request).build()
    
    admin_h = ConversationHandler(
        entry_points=[CommandHandler("admin", admin_start), CallbackQueryHandler(admin_start, pattern="^open_admin$")],
        states={
            ADMIN_SELECT: [CallbackQueryHandler(admin_button_handler)],
            ADMIN_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_process_input)]
        },
        fallbacks=[CallbackQueryHandler(admin_start, pattern="^admin_home$")],
        per_message=False,
        allow_reentry=True
    )
    
    user_h = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CallbackQueryHandler(pre_gen, pattern="^gen_"),
            CallbackQueryHandler(set_apikey_start, pattern="^menu_apikey$")
        ],
        states={
            SET_APIKEY_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_apikey)],
            WAITING_IMAGE: [MessageHandler((filters.PHOTO | filters.Document.IMAGE) & ~filters.COMMAND, get_img)],
            WAITING_REF_VIDEO: [MessageHandler((filters.VIDEO | filters.Document.VIDEO) & ~filters.COMMAND, get_vid)],
            WAITING_PROMPT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_prompt)],
            WAITING_ORIENTATION: [CallbackQueryHandler(get_orient, pattern="^or_")],
            WAITING_DURATION: [CallbackQueryHandler(run_gen, pattern="^dur_")]
        },
        fallbacks=[CallbackQueryHandler(start, pattern="^back_home$")],
        per_message=False,
        allow_reentry=True
    )
    
    app.add_handler(admin_h)
    app.add_handler(user_h)
    app.add_handler(CommandHandler("reset", lambda u, c: asyncio.create_task(reset_bot(u, c))))
    app.add_handler(CallbackQueryHandler(start, pattern="^back_home$"))
    app.add_handler(CallbackQueryHandler(lambda u,c: u.callback_query.edit_message_text(f"ID: <code>{u.effective_user.id}</code>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data="back_home")]])), pattern="^show_user_id$"))
    
    
    logger.info("🤖 Bot is running... (Press Ctrl+C to stop)")
    
    # Resume tasks
    asyncio.get_event_loop().create_task(resume_pending_tasks(app))
    
    app.run_polling()

async def reset_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    global USER_TASK_COUNTS, CURRENT_RUNNING_TASKS
    USER_TASK_COUNTS = {}
    CURRENT_RUNNING_TASKS = 0
    if update.message:
        await tg_retry(update.message.reply_text, "🔄 Bot Reset.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

if __name__ == "__main__":
    try:
        print("🚀 Memulai Bot... (Tunggu sebentar)")
        run_bot()
    except KeyboardInterrupt:
        print("\n🛑 Bot Stopped by User!")
    except Exception as e:
        print(f"❌ Fatal Error: {e}")
