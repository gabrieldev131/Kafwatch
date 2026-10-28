import os
import cv2
import numpy as np
import threading
import time
from confluent_kafka import Consumer, TopicPartition, OFFSET_END
from dotenv import load_dotenv

# --- Classe Worker para Consumo em Background (Confluent Kafka C-Based) ---
class KafkaStreamReader:
    def __init__(self, bootstrap_servers, topic_name, group_id):
        self.lock = threading.Lock()
        self.running = False
        self.latest_frames = {} # Dicionário compartilhado: {'cam1': frame, 'cam2': frame}
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self._assigned = False
        self._last_assign_attempt = 0.0
        
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': f"{group_id}-{os.getpid()}-{int(time.time() * 1000)}",
            'auto.offset.reset': 'latest',
            'socket.receive.buffer.bytes': 10 * 1024 * 1024,
            'fetch.message.max.bytes': 5 * 1024 * 1024,
            'max.partition.fetch.bytes': 5 * 1024 * 1024,
            'fetch.wait.max.ms': 5,
            'enable.auto.commit': False,
            'allow.auto.create.topics': True,
            'topic.metadata.refresh.interval.ms': 2000,
        }
        self.consumer = Consumer(conf)
        self._ensure_assigned()
        
        self.thread = threading.Thread(target=self._update_loop)
        self.thread.daemon = True # Morre se o programa principal morrer

    def _ensure_assigned(self) -> bool:
        if self._assigned:
            return True

        now = time.perf_counter()
        if (now - self._last_assign_attempt) < 1.0:
            return False
        self._last_assign_attempt = now

        try:
            md = self.consumer.list_topics(self.topic_name, timeout=1.5)
            topic_meta = md.topics.get(self.topic_name)
            if topic_meta is None or topic_meta.error is not None or not topic_meta.partitions:
                return False

            tps = []
            for pid in sorted(topic_meta.partitions.keys()):
                try:
                    low, high = self.consumer.get_watermark_offsets(
                        TopicPartition(self.topic_name, pid), timeout=1.5
                    )
                    start_off = max(low, high - 16) if high > low else OFFSET_END
                except Exception:
                    start_off = OFFSET_END
                tps.append(TopicPartition(self.topic_name, pid, start_off))

            self.consumer.assign(tps)
            self._assigned = True
            return True
        except Exception:
            return False

    def start(self):
        self.running = True
        self.thread.start()
        print("[Thread] Leitor Kafka iniciado em background.")

    def stop(self):
        self.running = False
        if self.thread.is_alive():
            self.thread.join()
        self.consumer.close()

    def _update_loop(self):
        """Loop que roda em outra thread, focado apenas em REDE e DECODE."""
        while self.running:
            if not self._ensure_assigned():
                time.sleep(0.02)
                continue

            msgs = self.consumer.consume(num_messages=32, timeout=0.015)
            if not msgs:
                continue

            batch_updates = {}
            for msg in msgs:
                if msg.error() or msg.value() is None:
                    continue
                key = msg.key()
                cam_id = key.decode('utf-8', errors='replace') if key else 'cam1'
                batch_updates[cam_id] = msg.value()

            for cam_id, img_bytes in batch_updates.items():
                try:
                    nparr = np.frombuffer(img_bytes, np.uint8)
                    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                    if frame is not None:
                        with self.lock:
                            self.latest_frames[cam_id] = frame
                except Exception as e:
                    print(f"Erro decode: {e}")

    def get_frames(self):
        """Retorna uma cópia segura dos frames atuais para exibição."""
        with self.lock:
            return self.latest_frames.copy()

# --- Função Principal (Apenas GUI) ---
def main():
    load_dotenv()
    KAFKA_HOST = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    TOPIC = 'meu-topico-de-video'
    GROUP_ID = 'grupo-turbo-v1'

    # 1. Inicia o leitor em background
    reader = KafkaStreamReader(KAFKA_HOST, TOPIC, GROUP_ID)
    reader.start()

    print("[Main] Interface gráfica iniciada. Pressione 'q' para sair.")

    try:
        while True:
            # 2. Pega os frames já decodificados da thread
            frames = reader.get_frames()

            if not frames:
                # Se não tem frames, dorme um pouco para não fritar a CPU à toa
                time.sleep(0.01)
                continue

            # 3. Exibe as janelas
            for cam_id, frame in frames.items():
                cv2.imshow(f"Monitor: {cam_id}", frame)

            # 4. Controle da GUI
            # waitKey(1) é o mínimo para o OpenCV processar eventos de janela
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

    except KeyboardInterrupt:
        print("Ctrl+C detectado.")
    finally:
        reader.stop()
        cv2.destroyAllWindows()
        print("Encerrado.")

if __name__ == "__main__":
    main()