import os
import signal
import sys
import threading
import time

# Força transporte RTSP via TCP e desativa buffers do FFmpeg para menor latência e maior estabilidade no Docker
os.environ.setdefault(
    "OPENCV_FFMPEG_CAPTURE_OPTIONS",
    "rtsp_transport;tcp|fflags;nobuffer|flags;low_delay"
)

import cv2
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


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


def create_kafka_producer(bootstrap_servers: str, retry_delay: float = 2.0) -> KafkaProducer:
    """Conecta ao Kafka com tentativas automáticas até o broker estar pronto."""
    while True:
        try:
            # Nota: não usamos compression_type='gzip' pois imagens JPEG já são comprimidas;
            # comprimir JPEG com gzip consome muita CPU sem reduzir tamanho.
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                acks=1,
                linger_ms=5,
                batch_size=65536,
                buffer_memory=16777216,
                max_in_flight_requests_per_connection=5,
                request_timeout_ms=10000,
                max_block_ms=5000,
            )
        except NoBrokersAvailable:
            print(f"[Worker] Aguardando broker Kafka em {bootstrap_servers}...", flush=True)
            time.sleep(retry_delay)
        except Exception as e:
            print(f"[Worker] Erro ao conectar ao Kafka ({e}), tentando novamente...", flush=True)
            time.sleep(retry_delay)


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
    NOVO_TAMANHO = (640, 480)
    QUALIDADE_JPEG = int(os.environ.get("JPEG_QUALITY", "70"))  # 70 oferece ótimo equilíbrio tamanho/qualidade
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

    # 1. Configurar Kafka (com retry caso o broker ainda esteja iniciando)
    produtor = create_kafka_producer(KAFKA_HOST)

    # 2. Iniciar captura em Thread
    print(f"[{CAMERA_ID}] Iniciando captura otimizada de {RTSP_URL}...", flush=True)
    cam_cleaner = CameraBufferCleaner(RTSP_URL)

    last_sent_time = 0.0

    try:
        while running:
            # Bloqueia com 0% de CPU até que um NOVO frame chegue da câmera
            sucess, frame = cam_cleaner.read(timeout=0.5)

            if not sucess or frame is None:
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
                produtor.send(
                    TOPIC_NAME,
                    key=camera_key,
                    value=buffer.tobytes(),
                )
                last_sent_time = now

    except KeyboardInterrupt:
        print("Parando...", flush=True)
    finally:
        cam_cleaner.stop()
        try:
            produtor.flush(timeout=2.0)
            produtor.close(timeout=2.0)
        except Exception:
            pass


if __name__ == "__main__":
    main()