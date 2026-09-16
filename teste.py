#from network.NetworkScanner import NetworkScanner
from docker_manager.ComposeGenerator import ComposeGenerator
from docker_manager.ComposeManager import ComposeManager
from docker_manager.FileSystemWriter import FileSystemWriter
from network.teste_scapy import discover

if __name__ == "__main__":
    
    # -----------------------------------------------------------------------------------------------
    # 1. Scan na rede para descobrir câmeras
    cameras = discover()
    print(cameras)
    '''
    # -----------------------------------------------------------------------------------------------
    # 2. Gerar arquivos docker-compose para as câmeras descobertas
    docker_compose_gen = ComposeGenerator("admin", "Aluno@00")
    scan_results = {ip: [554] for ip in cameras}  # Todas as câmeras na porta 554
    docker_compose_gen.generate_files(scan_results)
    # -----------------------------------------------------------------------------------------------


    # 3. Subir os serviços Docker via Docker Compose
    compose_manager = ComposeManager("docker_files/docker-compose.yml")
    compose_manager.up(build=True, detach=True)
    # -----------------------------------------------------------------------------------------------
    '''
    USERNAME = "admin"
    PASSWORD = "Aluno@00"

    # --- 1. Carregar Templates Estáticos ---
    # (S)RP: A classe FileSystemWriter também pode carregar templates
    dockerfile_lines = FileSystemWriter.load_template("docker_manager/templates/Dockerfile.template")
    pyproject_lines = FileSystemWriter.load_template("docker_manager/templates/pyproject.toml.template")
    
    # --- 2. Gerar Configs Dinâmicas ---
    # (S)RP: O Generator só gera as configs em memória
    generator = ComposeGenerator(USERNAME, PASSWORD)
    compose_cfg, mtx_cfg = generator.build_configs(cameras)
    
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
    compose_manager.up(build=True, detach=True)
    