import os
import yaml
from dotenv import load_dotenv
from docker_manager.ComposeGenerator import ComposeGenerator
from docker_manager.ComposeManager import ComposeManager
from docker_manager.FileSystemWriter import FileSystemWriter
from network.teste_scapy import discover

if __name__ == "__main__":
    load_dotenv()

    # -----------------------------------------------------------------------------------------------
    # 1. Scan na rede para descobrir câmeras (ou usa CAMERA_IPS do .env se definido)
    env_cameras = os.environ.get("CAMERA_IPS", "").strip()
    if env_cameras:
        cameras = [ip.strip() for ip in env_cameras.split(",") if ip.strip()]
        print(f"Usando câmeras configuradas em CAMERA_IPS: {cameras}")
    else:
        cameras = discover()
        print(f"Câmeras RTSP descobertas na rede ({len(cameras)}): {cameras}")

    # Se nenhuma câmera foi encontrada no scan, preserva as câmeras já salvas no mediamtx.yml (se houver)
    if not cameras and os.path.exists("docker_files/mediamtx.yml"):
        try:
            with open("docker_files/mediamtx.yml", "r") as f:
                existing_mtx = yaml.safe_load(f) or {}
            existing_paths = existing_mtx.get("paths") or {}
            if existing_paths:
                print(
                    "[Aviso] Nenhuma câmera respondeu ao scan agora; "
                    "mantendo configuración existente em docker_files/mediamtx.yml."
                )
        except Exception:
            pass

    USERNAME = os.environ.get("RTSP_USERNAME", "admin")
    PASSWORD = os.environ.get("RTSP_PASSWORD", "Aluno@00")
    KAFKA_EXTERNAL_HOST = os.environ.get("KAFKA_EXTERNAL_HOST")

    # --- 1. Carregar Templates Estáticos ---
    # (S)RP: A classe FileSystemWriter também pode carregar templates
    dockerfile_lines = FileSystemWriter.load_template("docker_manager/templates/Dockerfile.template")
    pyproject_lines = FileSystemWriter.load_template("docker_manager/templates/pyproject.toml.template")
    
    # --- 2. Gerar Configs Dinâmicas ---
    # (S)RP: O Generator só gera as configs em memória
    generator = ComposeGenerator(USERNAME, PASSWORD, kafka_external_host=KAFKA_EXTERNAL_HOST)
    compose_cfg, mtx_cfg = generator.build_configs(cameras)
    generator.print_report(mtx_cfg)
    
    # --- 3. Escrever Todos os Ficheiros ---
    # (S)RP: O Writer só escreve os ficheiros
    # (D)IP: Estamos a "injetar" a dependência do writer
    
    # Os ficheiros do Worker vão para a pasta 'worker_build'
    writer_worker = FileSystemWriter(base_path="worker_build")
    writer_worker.write_lines("Dockerfile", dockerfile_lines)
    writer_worker.write_lines("pyproject.toml", pyproject_lines)

    # Os ficheiros do Compose vão para a pasta 'docker_files'
    writer_compose = FileSystemWriter(base_path="docker_files")
    writer_compose.write_yaml("docker-compose.yml", compose_cfg)
    writer_compose.write_yaml("mediamtx.yml", mtx_cfg)

    # --- 4. Subir os serviços Docker via Docker Compose ---
    compose_manager = ComposeManager("docker_files/docker-compose.yml")
    compose_manager.up(build=True, detach=True, force_recreate=True)

    