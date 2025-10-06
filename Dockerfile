FROM python:3.10-slim

WORKDIR /app

# Install deps including rclone
RUN apt-get update && apt-get install -y rclone && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Make entrypoint executable
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
