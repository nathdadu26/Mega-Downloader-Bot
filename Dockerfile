FROM python:3.11-slim

# Install megatools
RUN apt-get update && apt-get install -y \
    megatools \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mega_downloader.py .

# Koyeb health check port
EXPOSE 8080

CMD ["python", "mega_downloader.py"]
