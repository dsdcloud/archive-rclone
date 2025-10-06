# Base image for your bot
FROM python:3.11-slim

# 1. Update and install necessary tools (curl, unzip, ffmpeg, git, ca-certificates)
# We need curl and unzip for the rclone installation.
RUN apt-get update && \
    apt-get install -y curl unzip ca-certificates git ffmpeg && \
    # Cleanup to reduce image size
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Install the LATEST Rclone (Ensures Linkbox support is included)
# This uses the current AMD64 link, which will give a version >= 1.73 (with Linkbox support).
RUN curl -fsSLo /tmp/rclone.zip https://downloads.rclone.org/rclone-current-linux-amd64.zip && \
    unzip /tmp/rclone.zip -d /tmp && \
    cp /tmp/rclone-*-linux-amd64/rclone /usr/bin/rclone && \
    chown root:root /usr/bin/rclone && chmod 755 /usr/bin/rclone && \
    rm -rf /tmp/rclone* # 3. Copy files and install Python dependencies
WORKDIR /app
COPY . /app

# === Permission Fix: Add this line ===
# This solves the "permission denied" error for the entrypoint script.
RUN chmod +x /app/entrypoint.sh
# =====================================

RUN pip install --no-cache-dir -r requirements.txt

# 4. Define volumes and environment variables
VOLUME ["/config", "/downloads"]

ENV RCLONE_CONFIG_PATH=/config/rclone.conf
ENV TEMP_DOWNLOAD_DIR=/downloads
ENV PYTHONUNBUFFERED=1

# 5. Entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
