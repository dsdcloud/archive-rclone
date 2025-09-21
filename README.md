# Archive.org → OneDrive (rclone) Telegram Bot (Bot API Version)

This bot uses **Telegram Bot API** (Bot Token), not user session.  
It can be safely deployed with just a Bot Token, API ID, and API Hash.

## Features
- Accepts `/download <archive.org link>` commands.
- Fetches metadata from archive.org and lists available files (formats).
- User can pick file via inline buttons.
- Downloads file to server, uploads to OneDrive Business using `rclone`.
- Uploads your `rclone.conf` file via `/set_rclone_conf`.
- Cleans up temporary files after upload.

## Environment Variables
- `BOT_TOKEN` — Telegram bot token (from @BotFather)
- `API_ID` — Telegram API ID (from my.telegram.org)
- `API_HASH` — Telegram API Hash
- `RCLONE_CONFIG_PATH` — path to rclone config file (default `/config/rclone.conf`)
- `TEMP_DOWNLOAD_DIR` — path to temp downloads (default `/downloads`)

## Deployment (Railway)
1. Create a new Railway project.
2. Add your secrets (`BOT_TOKEN`, `API_ID`, `API_HASH`, ...).
3. Deploy directly from this repo (Dockerfile included).
4. Use `/set_rclone_conf` to upload your rclone.conf file, or mount one into `/config/rclone.conf`.

