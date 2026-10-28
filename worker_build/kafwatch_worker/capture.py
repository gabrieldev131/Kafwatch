import os
import signal
import sys
import threading
import time

# Força transporte RTSP via TCP, desativa buffers do FFmpeg e define timeout de socket (3s)
# para menor latência e reconexão imediata no Docker/Kubernetes
os.environ.setdefault(
    "OPENCV_FFMPEG_CAPTURE_OPTIONS",
    "rtsp_transport;tcp|fflags;nobuffer|flags;low_delay|stimeout;3000000",
)

import cv2
from confluent_kafka import Producer
from dotenv import load_dotenv


# --- Classe para Leitura de Câmera em Thread Separada ---
# Evita que o envio ao Kafka bloqueie a câmera, descarta frames acumulados
# e reconecta automaticamente caso o stream RTSP oscile.
class CameraBufferCleaner:
    def __init__(self, rtsp_url: str, reconnect_delay: float = 2.0):
        self.rtsp_url = rtsp_url
        self.reconnect_delay = reconnect_delay
        self.stream = None

        self.last_frame = None
        self.stopped = False
        self.lock = threading.Lock()
        # Evento sinalizado apenas quando chega um NOVO frame da câmera
        # (evita reprocessar e reenviar o mesmo frame centenas de vezes por segundo!)
        self.new_frame_event = threading.Event()

        self.t = threading.Thread(target=self.update, args=(), daemon=True)
        self.t.start()

    def _open_stream(self) -> bool:
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
                    print(f"[Worker] Aguardando stream RTSP ficar disponível: {self.rtsp_url}", flush=True)
                    time.sleep(self.reconnect_delay)
                    continue
                print(f"[Worker] Conectado ao stream RTSP: {self.rtsp_url}", flush=True)

            ret, frame = self.stream.read()
            if not ret or frame is None:
                # Stream caiu ou falhou temporariamente: reconecta sem derrubar o container
                try:
                    self.stream.release()
                except Exception:
                    pass
                self.stream = None
                time.sleep(0.5)
                continue

            # Mantém apenas o frame mais recente na memória e avisa o consumidor
            with self.lock:
                self.last_frame = frame
                self.new_frame_event.set()

    def read(self, timeout: float = 0.5):
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


def create_kafka_producer(bootstrap_servers: str) -> Producer:
    """Cria o produtor Confluent Kafka (librdkafka em C) otimizado para streaming de vídeo."""
    producer_conf = {
        "bootstrap.servers": bootstrap_servers,
        "linger.ms": 2,
        "batch.num.messages": 16,
        "queue.buffering.max.kbytes": 32768,
        "message.max.bytes": 5242880,
        "acks": 1,
        "socket.keepalive.enable": True,
        "reconnect.backoff.ms": 250,
        "reconnect.backoff.max.ms": 2000,
    }
    return Producer(producer_conf)


def main():
    load_dotenv()
    RTSP_URL = os.environ.get("RTSP_URL")
    CAMERA_ID = os.environ.get("CAMERA_ID", "cam1")
    KAFKA_HOST = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "meu-topico-de-video")

    if not RTSP_URL:
        print("Erro: Variável de ambiente RTSP_URL não definida.", flush=True)
        sys.exit(1)

    # Configurações de Qualidade e Taxa de Quadros
    WIDTH = int(os.environ.get("FRAME_WIDTH", "640"))
    HEIGHT = int(os.environ.get("FRAME_HEIGHT", "480"))
    NOVO_TAMANHO = (WIDTH, HEIGHT)
    QUALIDADE_JPEG = int(os.environ.get("JPEG_QUALITY", "75"))
    MAX_FPS = float(os.environ.get("MAX_FPS", "30"))
    min_frame_interval = 1.0 / MAX_FPS if MAX_FPS > 0 else 0.0

    encode_params = [int(cv2.IMWRITE_JPEG_QUALITY), QUALIDADE_JPEG]
    camera_key = CAMERA_ID.encode("utf-8")

    running = True

    def _handle_signal(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # 1. Configurar Produtor Confluent Kafka (C-based)
    produtor = create_kafka_producer(KAFKA_HOST)

    # 2. Iniciar captura em Thread
    print(
        f"[{CAMERA_ID}] Iniciando captura otimizada de {RTSP_URL} -> Kafka ({KAFKA_HOST}, tópico='{TOPIC_NAME}')...",
        flush=True,
    )
    cam_cleaner = CameraBufferCleaner(RTSP_URL)

    last_sent_time = 0.0
    sent_count = 0
    t_log = time.monotonic()

    try:
        while running:
            # Bloqueia com 0% de CPU até que um NOVO frame chegue da câmera
            sucess, frame = cam_cleaner.read(timeout=0.5)

            if not sucess or frame is None:
                produtor.poll(0)
                continue

            now = time.monotonic()
            if min_frame_interval > 0 and (now - last_sent_time) < min_frame_interval:
                continue

            # --- Processamento ---
            if frame.shape[1] != NOVO_TAMANHO[0] or frame.shape[0] != NOVO_TAMANHO[1]:
                frame_redimensionado = cv2.resize(frame, NOVO_TAMANHO, interpolation=cv2.INTER_LINEAR)
            else:
                frame_redimensionado = frame

            # --- Compressão JPEG com qualidade controlada ---
            ret, buffer = cv2.imencode(".jpg", frame_redimensionado, encode_params)

            if ret:
                try:
                    produtor.produce(
                        TOPIC_NAME,
                        key=camera_key,
                        value=buffer.tobytes(),
                    )
                    produtor.poll(0)
                    last_sent_time = now
                    sent_count += 1
                except BufferError:
                    produtor.poll(0.05)

            if (now - t_log) >= 5.0:
                actual_fps = sent_count / (now - t_log)
                print(f"[{CAMERA_ID}] Publicando no Kafka a {actual_fps:.1f} FPS", flush=True)
                sent_count = 0
                t_log = now

    except KeyboardInterrupt:
        print("Parando...", flush=True)
    finally:
        cam_cleaner.stop()
        try:
            produtor.flush(timeout=2.0)
        except Exception:
            pass


if __name__ == "__main__":
    main()