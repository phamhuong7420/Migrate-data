# Dockerfile — Oracle Sync Web App
# python-oracledb chạy Thin Mode, không cần Oracle Instant Client
FROM python:3.11-slim

# Cài thư viện hệ thống cần thiết
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libc-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cài dependencies trước (tận dụng cache layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ source code
COPY config/        ./config/
COPY src/           ./src/
COPY web/           ./web/
COPY entrypoint.sh  ./entrypoint.sh

# Convert CRLF → LF (file tạo trên Windows) rồi cấp quyền thực thi
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Thư mục scripts chứa data files (sync_config.xml, job_sync.csv, job_his.xml)
# → mount volume từ ngoài vào để data persistent
RUN mkdir -p scripts

EXPOSE 5000

# PYTHONPATH=. để import config.* và src.* hoạt động đúng
ENV PYTHONPATH=/app

# entrypoint: reset job_his.xml → start Flask
CMD ["/app/entrypoint.sh"]
