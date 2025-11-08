import os
import logging
import asyncio
import requests
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from archive_scraper import parse_archive_url, fetch_metadata, list_files_from_metadata
from uploader import rclone_copy, rclone_list_remotes, RcloneAuthError # RcloneAuthError ကို ထည့်သွင်းလိုက်ပါပြီ

# Progress Bar အတွက် tqdm ကို သုံးပါမယ်
from tqdm import tqdm 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_ID = int(os.environ.get('API_ID'))
API_HASH = os.environ.get('API_HASH')
BOT_TOKEN = os.environ.get('BOT_TOKEN')
TEMP_DIR = os.environ.get('TEMP_DOWNLOAD_DIR', '/downloads')
RCLONE_CONFIG_PATH = os.environ.get('RCLONE_CONFIG_PATH', '/config/rclone.conf')

# Download လုပ်နေစဉ် message ကို update လုပ်မယ့် interval (စက္ကန့်)
PROGRESS_UPDATE_INTERVAL = 5

os.makedirs(TEMP_DIR, exist_ok=True)

app = Client("archive_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

JOBS = {}

# Download Progress ပြသပေးမယ့် Utility Function
def get_progress_string(current, total):
    if total is None or total == 0:
        return ""
    percent = (current / total) * 100
    bar_length = 20
    filled = int(bar_length * current / total)
    bar = '█' * filled + '░' * (bar_length - filled)
    return f"`{bar}` {percent:.1f}% ({current / (1024*1024):.2f}MB / {total / (1024*1024):.2f}MB)"


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
        
        # files ကို format အလိုက် စုစည်းခြင်း
        jobid = f"{message.chat.id}:{message.message_id}"
        JOBS[jobid] = {'identifier': ident, 'files': files, 'meta': meta}
        
        # Inline Button များ တည်ဆောက်ခြင်း
        buttons = []
        # format တစ်မျိုးကို တစ်ခါသာ ပြသဖို့ set ကို သုံးခြင်း
        available_formats = sorted(list(set(f.get('format', 'Other') for f in files)))
        
        for f in available_formats:
            buttons.append([InlineKeyboardButton(f, callback_data=f"pickformat|{jobid}|{f}")])
            
        await msg.edit(
            f"Found **{len(files)}** files in archive **`{ident}`**.\nChoose a format to proceed:", 
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    except Exception as e:
        logger.exception(e)
        await msg.edit(f"Error: {e}")

@app.on_callback_query(filters.regex(r"^pickformat\|"))
async def pickformat(client, cq):
    _, jobid, file_format = cq.data.split('|', 2)
    await cq.answer()
    job = JOBS.get(jobid)
    if not job:
        await cq.message.edit("Job not found.")
        return

    # ရွေးချယ်ထားသော format နဲ့ ကိုက်ညီတဲ့ files တွေကို ရှာခြင်း
    selected_files = [f for f in job['files'] if f.get('format') == file_format]
    
    if not selected_files:
        await cq.message.edit("No files found for this format.")
        return
        
    # ရွေးချယ်ထားသော files များကို job ထဲမှာ သိမ်းဆည်းခြင်း
    job['selected_files'] = selected_files
    
    remotes = rclone_list_remotes(RCLONE_CONFIG_PATH)
    if not remotes:
        # Inline Keyboard ကို ဖယ်ရှားပြီး Text Message ပြန်ပေးခြင်း
        await cq.message.edit_text(
            "No remotes in rclone.conf. Upload one with /set_rclone_conf.",
            reply_markup=None # Keyboard ဖယ်ရှားခြင်း
        )
        return
    
    # destination ရွေးဖို့ button များ ပြန်တည်ဆောက်ခြင်း
    buttons = [[InlineKeyboardButton(r, callback_data=f"upload|{jobid}|{file_format}|{r}")] for r in remotes]
    
    # Inline Keyboard ကို update လုပ်ခြင်း (ရွေးစရာများပြသရန်)
    await cq.message.edit_text(
        f"Selected format: **{file_format}** ({len(selected_files)} files).\nChoose destination:", 
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@app.on_callback_query(filters.regex(r"^upload\|"))
async def upload(client, cq):
    _, jobid, file_format, remote = cq.data.split('|', 3)
    await cq.answer("Starting upload process...", show_alert=False)
    job = JOBS.get(jobid)
    
    if not job or 'selected_files' not in job:
        await cq.message.edit_text("Job not found or file list missing.", reply_markup=None)
        return
        
    ident = job['identifier']
    selected_files = job['selected_files']
    total_files_to_process = len(selected_files)
    
    # Inline Keyboard ကို ချက်ချင်း ဖယ်ရှားခြင်း
    await cq.message.edit_text(
        f"✅ **Process Started**\nArchive: `{ident}`\nFormat: `{file_format}`\nDestination: `{remote}:Archive/{ident}`\nFiles: {total_files_to_process} files", 
        reply_markup=None
    )
    
    current_m = await cq.message.reply_text("Starting file processing...")
    
    for idx, file_info in enumerate(selected_files):
        filename = file_info['name']
        filesize = int(file_info.get('size', 0))
        target_dir = os.path.join(TEMP_DIR, ident)
        os.makedirs(target_dir, exist_ok=True)
        local_path = os.path.join(target_dir, filename)
        url = f"https://archive.org/download/{ident}/{filename}"

        try:
            # 1. DOWNLOAD PHASE (Progress Bar ဖြင့် ပြသခြင်း)
            await current_m.edit_text(f"📥 **({idx+1}/{total_files_to_process})** Downloading: `{filename}`")
            
            downloaded_bytes = 0
            last_edit_time = 0
            
            with requests.get(url, stream=True, timeout=3600) as r: # Timeout ကို ဖိုင်ကြီးတွေအတွက် တိုးလိုက်ပါပြီ
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0)) or filesize # content-length မရရင် metadata က size ကို သုံး
                
                with open(local_path, 'wb') as fh:
                    # tqdm ကို progress bar အတွက် သုံးပါတယ်
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"DL {filename}") as t:
                        for chunk in r.iter_content(chunk_size=1024*1024):
                            if chunk:
                                fh.write(chunk)
                                chunk_size = len(chunk)
                                downloaded_bytes += chunk_size
                                t.update(chunk_size)
                                
                                # Telegram Message Update လုပ်ခြင်း
                                current_time = asyncio.get_event_loop().time()
                                if current_time - last_edit_time > PROGRESS_UPDATE_INTERVAL:
                                    progress_str = get_progress_string(downloaded_bytes, total_size)
                                    await current_m.edit_text(
                                        f"📥 **({idx+1}/{total_files_to_process})** Downloading: `{filename}`\n{progress_str}"
                                    )
                                    last_edit_time = current_time

            # 2. UPLOAD PHASE
            await current_m.edit_text(f"📤 **({idx+1}/{total_files_to_process})** Download complete, uploading: `{filename}`...")
            remote_path = f"{remote}:Archive/{ident}"
            
            # rclone_copy ကို run ခြင်း
            out = await asyncio.get_event_loop().run_in_executor(None, rclone_copy, local_path, remote_path, RCLONE_CONFIG_PATH, [])
            
            # 3. CLEANUP
            try:
                os.remove(local_path)
            except Exception as e:
                logger.warning(f"Failed to remove local file {local_path}: {e}")
                
            # ဖိုင်တစ်ခု ပြီးဆုံးကြောင်း ပြသခြင်း
            await current_m.edit_text(
                f"✅ **({idx+1}/{total_files_to_process})** Uploaded: `{filename}`\n`{remote}:Archive/{ident}`"
            )

        except RcloneAuthError as e:
            # Token Error ကို အထူး ကိုင်တွယ်ခြင်း
            remote_name = remote.split(':')[0]
            await current_m.edit_text(
                f"🛑 **Authentication Error**\n{e}\n\nPlease run the following command **manually** to refresh the token for **`{remote_name}`**:\n\n`rclone config reconnect {remote_name}:`",
            )
            break # Token error ဖြစ်ရင် ကျန်တဲ့ file တွေဆက်မလုပ်တော့ပါဘူး
            
        except requests.HTTPError as e:
            if r.status_code == 404:
                await current_m.edit_text(f"⚠️ **({idx+1}/{total_files_to_process})** File not found on Archive.org: `{filename}`")
            else:
                await current_m.edit_text(f"❌ **({idx+1}/{total_files_to_process})** Download Error for `{filename}`: {e}")
        except Exception as e:
            logger.exception(e)
            await current_m.edit_text(f"❌ **({idx+1}/{total_files_to_process})** General Error for `{filename}`: {e}")
            break # အခြားပြဿနာဖြစ်ရင်လည်း ရပ်လိုက်ပါမယ်
            
    # အားလုံးပြီးဆုံးကြောင်း နောက်ဆုံး အသိပေးခြင်း
    if idx + 1 == total_files_to_process and not current_m.text.startswith("🛑"):
        await current_m.edit_text(
            f"🎉 **Finished!**\nAll **{total_files_to_process}** files uploaded to `{remote}:Archive/{ident}`"
        )


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
        await message.reply_text(f"Saved rclone config to `{target}`")
    else:
        await message.reply_text("Upload must be named rclone.conf")

if __name__ == "__main__":
    app.run()
