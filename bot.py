import os
import logging
import asyncio
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from archive_scraper import parse_archive_url, fetch_metadata, list_files_from_metadata
from uploader import rclone_copy, rclone_list_remotes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_ID = int(os.environ.get('API_ID'))
API_HASH = os.environ.get('API_HASH')
BOT_TOKEN = os.environ.get('BOT_TOKEN')
TEMP_DIR = os.environ.get('TEMP_DOWNLOAD_DIR', '/downloads')
RCLONE_CONFIG_PATH = os.environ.get('RCLONE_CONFIG_PATH', '/config/rclone.conf')

os.makedirs(TEMP_DIR, exist_ok=True)

app = Client("archive_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

JOBS = {}

@app.on_message(filters.command("start"))
async def start_cmd(client, message):
    await message.reply_text("Hello! Send /download <archive.org link> to begin.")

@app.on_message(filters.command("download"))
async def download_cmd(client, message):
    if len(message.command) < 2:
        await message.reply_text("Usage: /download https://archive.org/details/<identifier>")
        return
    url = message.command[1]
    ident = parse_archive_url(url)
    if not ident:
        await message.reply_text("Could not parse identifier.")
        return
    msg = await message.reply_text(f"Fetching metadata for: {ident} ...")
    try:
        meta = fetch_metadata(ident)
        files = list_files_from_metadata(meta)
        if not files:
            await msg.edit("No downloadable files found.")
            return
        jobid = f"{message.chat.id}:{message.message_id}"
        JOBS[jobid] = {'identifier': ident, 'files': files, 'meta': meta}
        buttons = []
        for f in files[:10]:
            label = f"{f['name']} ({f['format']})"
            buttons.append([InlineKeyboardButton(label, callback_data=f"pickfile|{jobid}|{f['name']}")])
        await msg.edit("Choose a file to download:", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.exception(e)
        await msg.edit(f"Error: {e}")

@app.on_callback_query(filters.regex(r"^pickfile\|"))
async def pickfile(client, cq):
    _, jobid, filename = cq.data.split('|', 2)
    await cq.answer()
    job = JOBS.get(jobid)
    if not job:
        await cq.message.edit("Job not found.")
        return
    remotes = rclone_list_remotes(RCLONE_CONFIG_PATH)
    if not remotes:
        await cq.message.edit("No remotes in rclone.conf. Upload one with /set_rclone_conf.")
        return
    buttons = [[InlineKeyboardButton(r, callback_data=f"upload|{jobid}|{filename}|{r}")] for r in remotes]
    await cq.message.edit(f"Selected {filename}\nChoose destination:", reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex(r"^upload\|"))
async def upload(client, cq):
    _, jobid, filename, remote = cq.data.split('|', 3)
    await cq.answer()
    job = JOBS.get(jobid)
    if not job:
        await cq.message.edit("Job not found.")
        return
    ident = job['identifier']
    target_dir = os.path.join(TEMP_DIR, ident)
    os.makedirs(target_dir, exist_ok=True)
    local_path = os.path.join(target_dir, filename)
    url = f"https://archive.org/download/{ident}/{filename}"
    m = await cq.message.reply_text(f"Downloading {filename} ...")
    try:
        import requests
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as fh:
                for chunk in r.iter_content(1024*1024):
                    if chunk:
                        fh.write(chunk)
        await m.edit("Download complete, uploading...")
        remote_path = f"{remote}:Archive/{ident}"
        out = await asyncio.get_event_loop().run_in_executor(None, rclone_copy, local_path, remote_path, RCLONE_CONFIG_PATH, [])
        await m.edit(f"Upload successful!\n{out[:500]}")
        try:
            os.remove(local_path)
        except:
            pass
    except Exception as e:
        logger.exception(e)
        await m.edit(f"Error: {e}")

@app.on_message(filters.command("set_rclone_conf"))
async def set_rclone_conf(client, message):
    await message.reply_text("Please reply with your rclone.conf file.")

@app.on_message(filters.document)
async def on_document(client, message):
    doc = message.document
    if doc and 'rclone.conf' in doc.file_name.lower():
        target = RCLONE_CONFIG_PATH
        os.makedirs(os.path.dirname(target), exist_ok=True)
        await message.download(file_name=target)
        await message.reply_text(f"Saved rclone config to {target}")
    else:
        await message.reply_text("Upload must be named rclone.conf")

if __name__ == "__main__":
    app.run()
