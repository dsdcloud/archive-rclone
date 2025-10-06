# Base image for your bot
FROM python:3.11-slim

# 1. Update and install necessary tools (curl, unzip, ffmpeg, git)
RUN apt-get update && \
    apt-get install -y curl unzip ca-certificates git ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Install the LATEST Rclone (Force AMD64 as expected by Railway/Zeabur)
# We use the official static download link and move the binary to /usr/bin/
RUN curl -fsSLo /tmp/rclone.zip https://downloads.rclone.org/rclone-current-linux-amd64.zip && \
    unzip /tmp/rclone.zip -d /tmp && \
    cp /tmp/rclone-*-linux-amd64/rclone /usr/bin/rclone && \
    chown root:root /usr/bin/rclone && chmod 755 /usr/bin/rclone && \
    rm -rf /tmp/rclone* # 3. Copy files and install Python dependencies
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt

# 4. Define volumes and environment variables
VOLUME ["/config", "/downloads"]

ENV RCLONE_CONFIG_PATH=/config/rclone.conf
ENV TEMP_DOWNLOAD_DIR=/downloads
ENV PYTHONUNBUFFERED=1

# 5. Entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
