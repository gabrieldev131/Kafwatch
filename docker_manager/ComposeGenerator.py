import yaml
import os
from pathlib import Path
from urllib.parse import quote
from typing import Dict, Any, Tuple, List

class ComposeGenerator:
    
    """
    (S)RP: Tem a única responsabilidade de GERAR as configurações
    do Docker Compose e MediaMTX em memória.
    """
    
    # MODIFICAÇÃO: Aceita 'default_port' para saber qual porta usar
    def __init__(self, username: str, password: str, default_port: int = 554):
        self.username = username
        self.password_encoded = quote(password)
        self.default_port = default_port
        
        # (O)CP: A configuração base é "fechada para modificação",
        # mas "aberta para extensão" pelo método build_configs.
        self._base_compose_config = {
            'services': {
                'rtsp-proxy': {
                    'image': 'bluenviron/mediamtx:latest',
                    'container_name': 'rtsp-proxy-server',
                    'restart': 'unless-stopped',
                    'ports': ["8554:8554", "8443:8443", "8888:8888"],
                    'volumes': ['./mediamtx.yml:/mediamtx.yml'],
                    'networks': ['camera-net']
                },
                'kafka': {
                    'image': 'confluentinc/cp-kafka:latest',
                    'container_name': 'kafka',
                    'networks': ['camera-net'],
                    'restart': 'unless-stopped',
                    'ports': ["9092:9092"],
                    # MODIFICAÇÃO: o diretório de logs do Kafka passa a viver
                    # inteiramente em RAM (tmpfs). Nada é persistido em disco;
                    # se o container reiniciar, o histórico é perdido (o que é
                    # aceitável aqui, já que só nos importa o frame mais recente).
                    'tmpfs': ['/var/lib/kafka/data'],
                    'environment': {
                        'KAFKA_PROCESS_ROLES': 'broker,controller',
                        'KAFKA_NODE_ID': 1,
                        'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@kafka:9093',
                        'KAFKA_LISTENERS': 'INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093',
                        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT',
                        'KAFKA_ADVERTISED_LISTENERS': 'INTERNAL://kafka:29092,EXTERNAL://localhost:9092',
                        'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
                        'KAFKA_INTER_BROKER_LISTENER_NAME': 'INTERNAL',
                        'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': 1,
                        'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': 1,
                        'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': 1,
                        'CLUSTER_ID': 'MkU3OEVBNTcwNTJENDM2Qk',

                        # --- MODIFICAÇÃO: otimizações para "guardar só o último frame" ---
                        # Aponta explicitamente o log dir para o tmpfs montado acima.
                        'KAFKA_LOG_DIRS': '/var/lib/kafka/data',
                        # 'compact' faz o Kafka manter apenas a última mensagem
                        # por chave (key) em cada partição, descartando as antigas.
                        # Ex.: se o worker publicar com key=CAMERA_ID, cada tópico
                        # de câmera vai reter só o frame mais recente daquela câmera.
                        'KAFKA_LOG_CLEANUP_POLICY': 'compact',
                        'KAFKA_LOG_CLEANER_ENABLE': 'true',
                        # Sem lag mínimo: o compactor pode limpar assim que um
                        # frame novo com a mesma key chegar.
                        'KAFKA_LOG_CLEANER_MIN_COMPACTION_LAG_MS': 0,
                        # Segmentos pequenos e de vida curta = o compactor roda
                        # com muito mais frequência (por padrão o Kafka só
                        # compacta segmentos já "fechados").
                        'KAFKA_LOG_SEGMENT_BYTES': 1048576,       # 1 MB
                        'KAFKA_LOG_SEGMENT_MS': 30000,            # força "roll" a cada 30s
                        'KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS': 30000,
                        # Aumenta o limite de tamanho de mensagem/fetch para
                        # comportar um frame de imagem (ex.: JPEG) inteiro.
                        'KAFKA_MESSAGE_MAX_BYTES': 5242880,       # 5 MB
                        'KAFKA_REPLICA_FETCH_MAX_BYTES': 5242880,
                    }
                }
            },
            'networks': {
                'camera-net': {'driver': 'bridge'}
            }
        }
        
        self._base_mediamtx_config = {
            'paths': {}
        }

    def _build_mediamtx_path(self, ip: str, port: int) -> Dict[str, Any]:
        """Helper privado para gerar a configuração de um path do MediaMTX."""
        source_url = (
            f"rtsp://{self.username}:{self.password_encoded}"
            f"@{ip}:{port}"
        )
        return {'source': source_url}

    def _build_worker_service(self, path_name: str) -> Dict[str, Any]:
        """Helper privado para gerar a configuração de um serviço worker."""
        worker_rtsp_url = f"rtsp://rtsp-proxy:8554/{path_name}"
        
        return {
            'build': {'context': '../worker_build', 'dockerfile': 'Dockerfile'},
            'container_name': f"worker-{path_name}",
            'restart': 'on-failure',
            'depends_on': ['rtsp-proxy', 'kafka'],
            'environment': [
                f'RTSP_URL={worker_rtsp_url}',
                f'CAMERA_ID={path_name}',
                'KAFKA_BOOTSTRAP_SERVERS=kafka:29092'
            ],
            'volumes': ['./capturas:/app/capturas'],
            'networks': ['camera-net']
        }

    # MODIFICAÇÃO: Aceita 'camera_ips' como uma lista de strings
    def build_configs(self, camera_ips: List[str]) -> Tuple[Dict, Dict]:
        """
        Usa a lista de IPs de câmeras para gerar os arquivos de configuração.
        Assume que self.default_port será usado para todas as câmeras.
        """
        # Copiamos as bases para não modificar os templates originais
        compose_config = self._base_compose_config.copy()
        mediamtx_config = self._base_mediamtx_config.copy()
        
        cam_counter = 1
        # O loop agora é mais simples
        for ip in camera_ips:
            path_name = f"cam{cam_counter}"
            
            # 1. Configuração do MediaMTX (usa self.default_port)
            mediamtx_config['paths'][path_name] = self._build_mediamtx_path(ip, self.default_port)
            
            # 2. Configuração do Worker
            worker_service_name = f"worker-{path_name}"
            compose_config['services'][worker_service_name] = self._build_worker_service(path_name)
            
            cam_counter += 1
        
        return compose_config, mediamtx_config

    def print_report(self, mediamtx_config: Dict):
        """(S)RP: Responsabilidade separada para apenas imprimir o relatório."""
        print("\n--- Relatório de Streams ---")
        for path, config in mediamtx_config['paths'].items():
            print(f"  [Câmera Real] {config['source']}")
            print(f"    -> [Proxy] rtsp://rtsp-proxy:8554/{path} (acesso interno dos workers)")
            print(f"    -> [Host]  rtsp://localhost:8554/{path} (acesso do seu PC, ex: VLC)")