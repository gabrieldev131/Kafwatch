import os
import sys
import threading
import time

os.environ.setdefault(
    "OPENCV_FFMPEG_CAPTURE_OPTIONS",
    "rtsp_transport;tcp|fflags;nobuffer|flags;low_delay"
)

import cv2
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Classe para Leitura de Câmera em Thread Separada ---
# Isso impede que o 'send' do Kafka bloqueie a leitura da câmera
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

def main():
    load_dotenv()
    RTSP_URL = os.environ.get("RTSP_URL")
    CAMERA_ID = os.environ.get("CAMERA_ID", "cam1")
    KAFKA_HOST = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

    # Configurações de Qualidade
    NOVO_TAMANHO = (640, 480)
    QUALIDADE_JPEG = int(os.environ.get("JPEG_QUALITY", "70")) # 70 é um ótimo balanço entre tamanho e velocidade
    encode_params = [int(cv2.IMWRITE_JPEG_QUALITY), QUALIDADE_JPEG]
    camera_key = CAMERA_ID.encode('utf-8')
    
    # 1. Configurar Kafka
    while True:
        try:
            produtor = KafkaProducer(
                bootstrap_servers=KAFKA_HOST,
                acks=1,
                linger_ms=5, 
                batch_size=65536,
                buffer_memory=16777216,
                max_in_flight_requests_per_connection=5,
            )
            break
        except NoBrokersAvailable:
            print(f"Aguardando Kafka em {KAFKA_HOST}...")
            time.sleep(2.0)
    
    # 2. Iniciar captura em Thread
    cam_cleaner = CameraBufferCleaner(RTSP_URL)

    contador_frames = 0
    contador_bytes = 0
    tempo_inicio = time.time()
    
    try:
        while True:
            # Aguarda um NOVO frame da câmera sem desperdiçar CPU reenviando duplicatas
            sucess, frame = cam_cleaner.read(timeout=0.5)
            
            if not sucess or frame is None:
                continue
            
            # --- Processamento ---
            if frame.shape[1] != NOVO_TAMANHO[0] or frame.shape[0] != NOVO_TAMANHO[1]:
                frame_redimensionado = cv2.resize(frame, NOVO_TAMANHO, interpolation=cv2.INTER_LINEAR)
            else:
                frame_redimensionado = frame
            
            # --- Compressão (Essencial para FPS alto) ---
            ret, buffer = cv2.imencode('.jpg', frame_redimensionado, encode_params)
            
            if ret:
                dados_msg = buffer.tobytes()
                
                # Envio Assíncrono (Fire and Forget)
                produtor.send(
                    'meu-topico-de-video', 
                    key=camera_key, 
                    value=dados_msg
                )
                
                # Stats
                contador_frames += 1
                contador_bytes += len(dados_msg)
            
            # --- Monitoramento (A cada 1s) ---
            tempo_atual = time.time()
            tempo_passado = tempo_atual - tempo_inicio
            if tempo_passado >= 1.0:
                fps = contador_frames / tempo_passado
                mb_s = (contador_bytes / (1024*1024)) / tempo_passado
                
                #print(f"[{CAMERA_ID}] FPS: {fps:.2f} | Rede: {mb_s:.2f} MiB/s")
                
                contador_frames = 0
                contador_bytes = 0
                tempo_inicio = time.time()

    except KeyboardInterrupt:
        print("Parando...")
    finally:
        cam_cleaner.stop()
        produtor.close()

if __name__ == "__main__":
    main()