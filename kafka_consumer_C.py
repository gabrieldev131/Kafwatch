import os
import cv2
import numpy as np
import threading
import time
from confluent_kafka import Consumer, TopicPartition, OFFSET_END
from dotenv import load_dotenv

# --- Classe Worker de Alta Performance (C-Based) ---
class TurboKafkaReader:
    def __init__(self, bootstrap_servers, topic_name, group_id, target_camera_id=None):
        self.lock = threading.Lock()
        self.running = False
        self.latest_frames = {} 
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.target_camera_id = target_camera_id # Opcional: Filtra apenas uma câmera
        self._assigned = False
        self._last_assign_attempt = 0.0
        self._warned_no_topic = False
        
        # Configurações do librdkafka (C Driver) para Latência Zero
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': f"{group_id}-{os.getpid()}-{int(time.time() * 1000)}",
            'auto.offset.reset': 'latest',
            
            # OTIMIZAÇÕES DE REDE
            # Buffer gigante para não travar o TCP
            'socket.receive.buffer.bytes': 10 * 1024 * 1024, 
            # Tamanho máximo de fetch
            'fetch.message.max.bytes': 5 * 1024 * 1024,
            'max.partition.fetch.bytes': 5 * 1024 * 1024,
            
            # OTIMIZAÇÕES DE LATÊNCIA
            # Não espere o buffer encher, envie o que tiver
            'fetch.wait.max.ms': 5, 
            # Desliga o commit automático (ganha performance, não precisamos persistir offset de vídeo)
            'enable.auto.commit': False,
            'allow.auto.create.topics': True,
            'topic.metadata.refresh.interval.ms': 2000,
        }

        self.consumer = Consumer(conf)
        self._ensure_assigned()
        
        self.thread = threading.Thread(target=self._update_loop)
        self.thread.daemon = True 

    def _ensure_assigned(self) -> bool:
        """
        Atribui diretamente as partições do tópico usando os offsets finais (watermark)
        para eliminar o atraso de rebalanceamento de grupo (3s-45s) e ler imediatamente
        o frame compactado mais recente de cada câmera.
        """
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
                if not self._warned_no_topic:
                    print(
                        f"[Kafka] Tópico '{self.topic_name}' ainda sem partições em "
                        f"'{self.bootstrap_servers}'. Aguardando produtores..."
                    )
                    self._warned_no_topic = True
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
            print(
                f"[Kafka] Conectado a '{self.bootstrap_servers}' | Tópico '{self.topic_name}' "
                f"({len(tps)} partição/ões) atribuído com sucesso."
            )
            return True
        except Exception as e:
            print(f"[Kafka] Falha ao conectar ao broker '{self.bootstrap_servers}': {e}")
            return False

    def start(self):
        self.running = True
        self.thread.start()
        print(f"[Turbo] Leitor C-Based iniciado. Alvo: {self.target_camera_id if self.target_camera_id else 'TODOS'}")

    def stop(self):
        self.running = False
        if self.thread.is_alive():
            self.thread.join()
        self.consumer.close()

    def _update_loop(self):
        """Loop de alta velocidade em C."""
        while self.running:
            if not self._ensure_assigned():
                time.sleep(0.02)
                continue

            # 1. DRENAGEM DE FILA (Consume Batch)
            # Em vez de poll(), usamos consume() para pegar até 50 mensagens de uma vez
            # Isso limpa o buffer interno do driver rapidamente
            msgs = self.consumer.consume(num_messages=50, timeout=0.015)
            
            if not msgs:
                continue
            
            # Dicionário temporário para guardar apenas a ÚLTIMA msg de cada câmera neste lote
            batch_updates = {}
            
            for msg in msgs:
                if msg.error():
                    continue
                val = msg.value()
                if val is None:
                    continue
                
                # Leitura da Chave (Rápida)
                key_bytes = msg.key()
                cam_id = key_bytes.decode('utf-8', errors='replace') if key_bytes else (self.target_camera_id or 'cam1')
                
                # Filtro de Câmera (Economia de CPU)
                # Se definimos que queremos ver só a 'cam3', ignoramos o resto aqui.
                if self.target_camera_id and cam_id != self.target_camera_id:
                    continue

                # Sobrescreve: Se vieram 10 frames da 'cam3', só o último fica no dict
                # Não decodificamos ainda! Só guardamos os bytes.
                batch_updates[cam_id] = val

            # 2. DECODIFICAÇÃO (Gargalo de CPU)
            # Agora decodificamos APENAS os vencedores (1 frame por câmera)
            for cam_id, img_bytes in batch_updates.items():
                try:
                    # imdecode é pesado, fazemos apenas para o frame final
                    nparr = np.frombuffer(img_bytes, np.uint8)
                    
                    # Decodificação
                    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                    
                    if frame is not None:
                        with self.lock:
                            self.latest_frames[cam_id] = frame
                            
                except Exception:
                    pass

    def get_frames(self):
        with self.lock:
            return self.latest_frames.copy()

# --- Main (GUI) ---
def main():
    load_dotenv()
    
    # Configurações
    KAFKA_HOST = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    TOPIC = 'meu-topico-de-video'
    GROUP_ID = 'grupo-turbo-vFinal'
    
    # Se existir CAMERA_ID no .env (ex: "cam3"), filtraremos apenas ela
    # Isso melhora muito a performance se você só quer ver uma.
    TARGET_CAM = os.getenv('CAMERA_ID') 

    # Inicia o Leitor Turbo
    reader = TurboKafkaReader(KAFKA_HOST, TOPIC, GROUP_ID, target_camera_id=TARGET_CAM)
    reader.start()

    print("[Main] Interface gráfica iniciada.")
    print(f"[*] Conectado a: {KAFKA_HOST}")

    # Variáveis para cálculo de FPS de Renderização
    frame_count = 0
    start_time = time.time()

    try:
        while True:
            # Pega os frames decodificados da thread secundária
            frames = reader.get_frames()

            if not frames:
                time.sleep(0.001) # Sleep minúsculo para não fritar a CPU
                continue

            for cam_id, frame in frames.items():
                # Opcional: Resize para exibição menor acelera o imshow em telas grandes
                # frame = cv2.resize(frame, (640, 480)) 
                
                cv2.imshow(f"Monitor: {cam_id}", frame)

            # --- Monitor de FPS da GUI ---
            frame_count += 1
            if frame_count % 60 == 0:
                elapsed = time.time() - start_time
                fps = frame_count / elapsed
                print(f"FPS da Interface: {fps:.1f}") # Descomente para ver no terminal
                
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

    except KeyboardInterrupt:
        print("Parando...")
    finally:
        reader.stop()
        cv2.destroyAllWindows()

if __name__ == "__main__":
    main()