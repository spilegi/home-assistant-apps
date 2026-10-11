FROM python:3.12-slim AS ffmpeg-build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates curl nasm pkg-config xz-utils && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://ffmpeg.org/releases/ffmpeg-8.1.3.tar.xz -o /tmp/ffmpeg.tar.xz \
    && echo "7138d28c96d9d3e3af4ee3d8cad72741f8ffb40da90c1112235dea3ecd3178a3  /tmp/ffmpeg.tar.xz" | sha256sum -c - \
    && mkdir /tmp/ffmpeg-src \
    && tar -xJf /tmp/ffmpeg.tar.xz --strip-components=1 -C /tmp/ffmpeg-src \
    && cd /tmp/ffmpeg-src \
    && ./configure --prefix=/usr/local --disable-doc --disable-debug --disable-ffplay --disable-ffprobe --disable-autodetect --disable-everything \
        --enable-ffmpeg --enable-network --enable-protocol=file,pipe,tcp,udp --enable-demuxer=rtp,rtsp --enable-decoder=h264 \
        --enable-encoder=mjpeg --enable-parser=h264,mjpeg --enable-filter=fps,scale --enable-muxer=image2,image2pipe \
    && make -j2 ffmpeg
RUN install -D /tmp/ffmpeg-src/ffmpeg /usr/local/bin/ffmpeg

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends tesseract-ocr libglib2.0-0 libgomp1 && rm -rf /var/lib/apt/lists/*
COPY --from=ffmpeg-build /usr/local/bin/ffmpeg /usr/local/bin/ffmpeg
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py ./
COPY static ./static
RUN mkdir -p /data && chmod 700 /data
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
