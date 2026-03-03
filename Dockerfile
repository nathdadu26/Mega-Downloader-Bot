FROM python:3.11-slim

# Install megatools + curl (for healthcheck)
RUN apt-get update && apt-get install -y \
    megatools \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mega_downloader.py .

# Koyeb health check port
EXPOSE 8080

# Docker-level healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["python", "mega_downloader.py"]
