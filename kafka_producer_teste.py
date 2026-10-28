import os
import signal
import sys
import threading
import time
from urllib.parse import quote

os.environ.setdefault(
    "OPENCV_FFMPEG_CAPTURE_OPTIONS",
    "rtsp_transport;tcp|fflags;nobuffer|flags;low_delay|stimeout;3000000"
)

import cv2
from confluent_kafka import Producer
from dotenv import load_dotenv


# --- Classe para Leitura de Câmera em Thread Separada ---
# Isso impede que o envio ao Kafka bloqueie a leitura da câmera
class CameraBufferCleaner:
    def __init__(self, rtsp_url, reconnect_delay=2.0):
        self.rtsp_url = rtsp_url
        self.reconnect_delay = reconnect_delay
        self.stream = None

        self.last_frame = None
        self.stopped = False
        self.lock = threading.Lock()
        self.new_frame_event = threading.Event()

        # Inicia a thread de leitura
        self.t = threading.Thread(target=self.update, args=(), daemon=True)
        self.t.start()

    def _open_stream(self):
        if self.stream is not None:
            try:
                self.stream.release()
            except Exception:
                pass
        self.stream = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
        if self.stream.isOpened():
            self.stream.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            return True
        return False

    def update(self):
        while not self.stopped:
            if self.stream is None or not self.stream.isOpened():
                if not self._open_stream():
                    time.sleep(self.reconnect_delay)
                    continue

            ret, frame = self.stream.read()
            if not ret or frame is None:
                try:
                    self.stream.release()
                except Exception:
                    pass
                self.stream = None
                time.sleep(0.5)
                continue

            # Mantém apenas o frame mais recente na memória e sinaliza chegada de novo frame
            with self.lock:
                self.last_frame = frame
                self.new_frame_event.set()

    def read(self, timeout=0.5):
        if not self.new_frame_event.wait(timeout=timeout):
            return False, None
        with self.lock:
            self.new_frame_event.clear()
            return self.last_frame is not None, self.last_frame

    def stop(self):
        self.stopped = True
        self.new_frame_event.set()
        if self.t.is_alive():
            self.t.join(timeout=2.0)
        if self.stream is not None:
            try:
                self.stream.release()
            except Exception:
                pass


def stream_camera_worker(
    cam_id: str,
    rtsp_url: str,
    produtor: Producer,
    topic: str,
    novo_tamanho: tuple[int, int],
    qualidade_jpeg: int,
    max_fps: float,
    stop_event: threading.Event,
):
    encode_params = [int(cv2.IMWRITE_JPEG_QUALITY), qualidade_jpeg]
    camera_key = cam_id.encode("utf-8")
    min_interval = 1.0 / max_fps if max_fps > 0 else 0.0

    print(f"[{cam_id}] Conectando a {rtsp_url} -> tópico '{topic}'...", flush=True)
    cam_cleaner = CameraBufferCleaner(rtsp_url)

    contador_frames = 0
    contador_bytes = 0
    tempo_inicio = time.monotonic()
    last_sent = 0.0

    try:
        while not stop_event.is_set():
            sucess, frame = cam_cleaner.read(timeout=0.5)
            if not sucess or frame is None:
                produtor.poll(0)
                continue

            agora = time.monotonic()
            if min_interval > 0 and (agora - last_sent) < min_interval:
                continue

            if frame.shape[1] != novo_tamanho[0] or frame.shape[0] != novo_tamanho[1]:
                frame_redimensionado = cv2.resize(frame, novo_tamanho, interpolation=cv2.INTER_LINEAR)
            else:
                frame_redimensionado = frame

            ret, buffer = cv2.imencode(".jpg", frame_redimensionado, encode_params)
            if ret:
                dados_msg = buffer.tobytes()
                try:
                    produtor.produce(topic, key=camera_key, value=dados_msg)
                    produtor.poll(0)
                    last_sent = agora
                    contador_frames += 1
                    contador_bytes += len(dados_msg)
                except BufferError:
                    produtor.poll(0.05)

            tempo_passado = agora - tempo_inicio
            if tempo_passado >= 5.0:
                fps = contador_frames / tempo_passado
                mb_s = (contador_bytes / (1024 * 1024)) / tempo_passado
                print(f"[{cam_id}] FPS: {fps:.1f} | Rede: {mb_s:.2f} MiB/s", flush=True)
                contador_frames = 0
                contador_bytes = 0
                tempo_inicio = agora
    finally:
        cam_cleaner.stop()


def main():
    load_dotenv()
    RTSP_URL = os.environ.get("RTSP_URL")
    CAMERA_ID = os.environ.get("CAMERA_ID", "cam1")
    KAFKA_HOST = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "meu-topico-de-video")
    USERNAME = os.environ.get("RTSP_USERNAME", "admin")
    PASSWORD = quote(os.environ.get("RTSP_PASSWORD", "Aluno@00"))

    NOVO_TAMANHO = (640, 480)
    QUALIDADE_JPEG = int(os.environ.get("JPEG_QUALITY", "75"))
    MAX_FPS = float(os.environ.get("MAX_FPS", "30"))

    # Se RTSP_URL não estiver definida, descobre automaticamente as câmeras na rede local
    sources = {}
    if RTSP_URL:
        sources[CAMERA_ID] = RTSP_URL
    else:
        from network.teste_scapy import discover
        print("RTSP_URL não definida; descobrindo câmeras RTSP na rede...")
        cameras = discover()
        for idx, ip in enumerate(cameras, start=1):
            sources[f"cam{idx}"] = f"rtsp://{USERNAME}:{PASSWORD}@{ip}:554"

    if not sources:
        print("Nenhuma câmera encontrada ou configurada em RTSP_URL.")
        sys.exit(1)

    producer_conf = {
        "bootstrap.servers": KAFKA_HOST,
        "linger.ms": 2,
        "batch.num.messages": 16,
        "queue.buffering.max.kbytes": 32768,
        "message.max.bytes": 5242880,
        "acks": 1,
        "socket.keepalive.enable": True,
    }
    produtor = Producer(producer_conf)

    stop_event = threading.Event()

    def _sig_handler(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    threads = []
    for cid, url in sources.items():
        t = threading.Thread(
            target=stream_camera_worker,
            args=(cid, url, produtor, TOPIC_NAME, NOVO_TAMANHO, QUALIDADE_JPEG, MAX_FPS, stop_event),
            daemon=True,
        )
        t.start()
        threads.append(t)

    print(f"Produtor Kafka iniciado em {KAFKA_HOST} para câmeras: {list(sources.keys())}")
    try:
        while not stop_event.is_set():
            time.sleep(0.2)
    except KeyboardInterrupt:
        print("Parando...")
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=2.0)
        produtor.flush(timeout=2.0)


if __name__ == "__main__":
    main()