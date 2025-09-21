FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl unzip ca-certificates &&     curl -fsSLo /tmp/rclone.zip https://downloads.rclone.org/rclone-current-linux-amd64.zip &&     unzip /tmp/rclone.zip -d /tmp &&     cp /tmp/rclone-*-linux-amd64/rclone /usr/bin/rclone &&     chown root:root /usr/bin/rclone && chmod 755 /usr/bin/rclone &&     rm -rf /tmp/rclone* && apt-get remove -y unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt

VOLUME ["/config", "/downloads"]

ENV RCLONE_CONFIG_PATH=/config/rclone.conf
ENV TEMP_DOWNLOAD_DIR=/downloads
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint.sh"]
