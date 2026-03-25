FROM python:3.11-slim

# --- timezone ---
ENV TZ=Asia/Jerusalem

RUN apt-get update && apt-get install -y \
    build-essential \
    tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

# --- workdir ---
WORKDIR /app

# --- install deps ---
COPY back-end/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# --- copy project ---
COPY back-end ./back-end
COPY front-end ./front-end
COPY bots ./bots
COPY summary ./summary

# --- expose port ---
EXPOSE 8003

# --- run ---
CMD ["uvicorn", "back-end.main:app", "--host", "0.0.0.0", "--port", "8003"]
