# Use Python 3.11 Slim to keep image small
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# 1. Install System Dependencies required for TgCrypto & Pyrogram
# gcc and python3-dev are needed to compile the C extensions
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Copy Requirements and Install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy the rest of the application code
COPY . .

# 4. Expose the port Koyeb expects (8000)
EXPOSE 8000

# 5. Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
