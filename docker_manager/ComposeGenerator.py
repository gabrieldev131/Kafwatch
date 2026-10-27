import copy
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
        
        self._default_logging = {
            'driver': 'json-file',
            'options': {
                'max-size': '10m',
                'max-file': '2'
            }
        }

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
                    'networks': ['camera-net'],
                    'logging': copy.deepcopy(self._default_logging)
                },
                'kafka': {
                    'image': 'confluentinc/cp-kafka:latest',
                    'container_name': 'kafka',
                    'networks': ['camera-net'],
                    'restart': 'unless-stopped',
                    'ports': ["9092:9092"],
                    # O diretório de logs do Kafka vive inteiramente em RAM (tmpfs).
                    # Nada é persistido no SSD; evita desgaste de disco e garante alta velocidade.
                    'tmpfs': ['/var/lib/kafka/data:uid=1000,gid=1000,mode=1770,size=1g'],
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

                        # Limita o heap da JVM para evitar consumo excessivo de RAM ou OOM
                        'KAFKA_HEAP_OPTS': '-Xmx512M -Xms512M',

                        # --- Otimizações para guardar só o último frame e reter por no máx 5 minutos ---
                        'KAFKA_LOG_DIRS': '/var/lib/kafka/data',
                        # 'compact,delete' mantém apenas a última imagem por chave (CAMERA_ID)
                        # e garante descarte por tempo (5 min) e tamanho (256 MB) para nunca encher o tmpfs.
                        'KAFKA_LOG_CLEANUP_POLICY': 'compact,delete',
                        'KAFKA_LOG_RETENTION_MS': 300000,          # 5 minutos (300.000 ms)
                        'KAFKA_LOG_RETENTION_BYTES': 268435456,    # Máximo 256 MB por partição
                        'KAFKA_LOG_CLEANER_ENABLE': 'true',
                        'KAFKA_LOG_CLEANER_MIN_COMPACTION_LAG_MS': 0,
                        'KAFKA_LOG_CLEANER_MAX_COMPACTION_LAG_MS': 30000,
                        'KAFKA_LOG_CLEANER_DELETE_RETENTION_MS': 10000,
                        'KAFKA_LOG_CLEANER_BACKOFF_MS': 2000,
                        # IMPORTANTE: SEGMENT_BYTES (16 MB) deve ser maior que MESSAGE_MAX_BYTES (5 MB)
                        # para que a thread LogCleaner do Kafka não falhe ao compactar segmentos!
                        'KAFKA_LOG_SEGMENT_BYTES': 16777216,       # 16 MB
                        'KAFKA_LOG_INDEX_SIZE_MAX_BYTES': 1048576, # 1 MB max por índice
                        'KAFKA_LOG_SEGMENT_MS': 30000,             # Rotaciona segmento a cada 30s
                        'KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS': 10000,
                        # Remove arquivos '.deleted' após 1s em vez do padrão de 60s (evita lotar tmpfs)
                        'KAFKA_LOG_SEGMENT_DELETE_DELAY_MS': 1000,
                        'KAFKA_OFFSETS_TOPIC_RETENTION_MINUTES': 5,
                        'KAFKA_MESSAGE_MAX_BYTES': 5242880,        # 5 MB
                        'KAFKA_REPLICA_FETCH_MAX_BYTES': 5242880,
                    },
                    'healthcheck': {
                        'test': ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:29092 > /dev/null 2>&1"],
                        'interval': '5s',
                        'timeout': '5s',
                        'retries': 12,
                        'start_period': '15s'
                    },
                    'logging': copy.deepcopy(self._default_logging)
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
        return {
            'source': source_url,
            'rtspTransport': 'tcp'
        }

    def _build_worker_service(self, path_name: str, include_build: bool = True) -> Dict[str, Any]:
        """Helper privado para gerar a configuração de um serviço worker."""
        worker_rtsp_url = f"rtsp://rtsp-proxy:8554/{path_name}"
        
        service_cfg: Dict[str, Any] = {
            'image': 'kafwatch-worker:latest',
        }
        if include_build:
            service_cfg['build'] = {'context': '../worker_build', 'dockerfile': 'Dockerfile'}

        service_cfg.update({
            'container_name': f"worker-{path_name}",
            'restart': 'unless-stopped',
            'depends_on': {
                'rtsp-proxy': {'condition': 'service_started'},
                'kafka': {'condition': 'service_healthy'}
            },
            'environment': [
                f'RTSP_URL={worker_rtsp_url}',
                f'CAMERA_ID={path_name}',
                'KAFKA_BOOTSTRAP_SERVERS=kafka:29092'
            ],
            'volumes': ['./capturas:/app/capturas'],
            'networks': ['camera-net'],
            'logging': copy.deepcopy(self._default_logging)
        })
        return service_cfg

    # MODIFICAÇÃO: Aceita 'camera_ips' como uma lista de strings
    def build_configs(self, camera_ips: List[str]) -> Tuple[Dict, Dict]:
        """
        Usa a lista de IPs de câmeras para gerar os arquivos de configuração.
        Assume que self.default_port será usado para todas as câmeras.
        """
        # Deepcopy para não modificar os templates originais em memória
        compose_config = copy.deepcopy(self._base_compose_config)
        mediamtx_config = copy.deepcopy(self._base_mediamtx_config)
        
        # Remove eventuais IPs duplicados mantendo a ordem
        unique_camera_ips = list(dict.fromkeys(camera_ips))

        cam_counter = 1
        for ip in unique_camera_ips:
            path_name = f"cam{cam_counter}"
            
            # 1. Configuração do MediaMTX (usa self.default_port)
            mediamtx_config['paths'][path_name] = self._build_mediamtx_path(ip, self.default_port)
            
            # 2. Configuração do Worker (compartilha a mesma imagem kafwatch-worker:latest;
            # apenas o primeiro worker precisa da diretiva 'build' para evitar N builds simultâneos)
            worker_service_name = f"worker-{path_name}"
            compose_config['services'][worker_service_name] = self._build_worker_service(
                path_name,
                include_build=(cam_counter == 1)
            )
            
            cam_counter += 1
        
        return compose_config, mediamtx_config

    def print_report(self, mediamtx_config: Dict):
        """(S)RP: Responsabilidade separada para apenas imprimir o relatório."""
        print("\n--- Relatório de Streams ---")
        for path, config in mediamtx_config['paths'].items():
            print(f"  [Câmera Real] {config['source']}")
            print(f"    -> [Proxy] rtsp://rtsp-proxy:8554/{path} (acesso interno dos workers)")
            print(f"    -> [Host]  rtsp://localhost:8554/{path} (acesso do seu PC, ex: VLC)")