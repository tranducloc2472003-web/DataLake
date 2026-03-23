# Sử dụng bản python nhẹ để tiết kiệm dung lượng
FROM python:3.9-slim

# Cài đặt các công cụ hệ thống cần thiết (nếu có)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Tạo thư mục làm việc
WORKDIR /app

# Copy file requirement và cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code webhook vào
COPY main.py .

# Mở port 9100 như trong docker-compose của ông
EXPOSE 9100

# Lệnh khởi chạy Webhook
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9100"]