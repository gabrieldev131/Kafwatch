FROM python:3.10-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    ffmpeg \
    v4l-utils \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN pip install --no-cache-dir .
CMD ["kafwatch-run"]