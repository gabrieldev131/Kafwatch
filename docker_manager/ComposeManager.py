import subprocess
import os

class ComposeManager:
    """
    Gerencia um projeto Docker Compose usando comandos de subprocesso.
    
    Esta classe automatiza a execução de comandos 'docker compose'
    para um arquivo .yml específico.
    """
    def __init__(self, compose_file_path: str):
        """
        Inicializa o gerenciador com o caminho para o arquivo docker-compose.yml.
        
        Args:
            compose_file_path (str): O caminho relativo ou absoluto para o 
                                     arquivo docker-compose.yml.
        """
        if not os.path.exists(compose_file_path):
            raise FileNotFoundError(f"Arquivo compose não encontrado em: {compose_file_path}")
            
        # O argumento '-f' aponta para o arquivo de configuração
        self.base_command = ["docker", "compose", "-f", compose_file_path]
        print(f"ComposeManager inicializado para o arquivo: {compose_file_path}")

    def _run_command(self, command_args):
        """
        Função auxiliar interna para executar um comando de subprocesso
        e imprimir a saída em tempo real.
        """
        #command = self.base_command + command_args
        #print(f"\n Executando comando: {' '.join(command)}")
        aux = self.base_command + command_args
        command = ' '.join(aux)
        try:
            # Inicia o processo
            # stdout=subprocess.PIPE e stderr=subprocess.PIPE capturam a saída
            # text=True decodifica a saída como texto (UTF-8)
            # bufsize=1 significa "line-buffered"
            with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                                  text=True, bufsize=1, shell=True) as proc:
                
                # Lê e imprime stdout em tempo real
                print("--- Saída do Contêiner (stdout) ---")
                if proc.stdout:
                    for line in proc.stdout:
                        print(line, end='', flush=True)
                
                # Lê e imprime stderr após o stdout
                print("\n--- Saída de Erro (stderr) ---")
                if proc.stderr:
                    for line in proc.stderr:
                        print(line, end='', flush=True)
                
                proc.wait() # Espera o processo terminar
                
                if proc.returncode != 0:
                    print(f"\n Erro: Comando falhou com código de saída {proc.returncode}")
                    return False
                
            print("\n Comando executado com sucesso.")
            return True

        except FileNotFoundError:
            print("\n Erro: 'docker' não encontrado. Você instalou o Docker Desktop?")
            return False
        except Exception as e:
            print(f"\n Erro inesperado ao executar o comando: {e}")
            return False

    def up(self, build=False, detach=False, force_recreate=False):
        """
        Inicia todos os serviços (equivalente a 'docker compose up').
        
        Args:
            build (bool): Se True, reconstrói as imagens (adiciona '--build').
            detach (bool): Se True, roda em modo "detached" (adiciona '-d').
            force_recreate (bool): Se True, força a recriação dos containers
                mesmo sem mudança de config (adiciona '--force-recreate').
                Útil depois de alterar algo como o tmpfs do kafka: o compose
                normalmente já detecta e recria sozinho, mas isso garante.
        """
        command = ["up"]
        if build:
            command.append("--build")
        if detach:
            command.append("-d")
        if force_recreate:
            command.append("--force-recreate")
            
        print("Subindo contêineres...")
        return self._run_command(command)

    def recreate_service(self, service_name):
        """
        Recria um único serviço do zero (para, remove e sobe de novo),
        útil para o kafka: como os dados agora vivem em tmpfs, recriar
        o container é a forma mais simples de "zerar" o tópico compactado
        e garantir que a config nova (compaction/tmpfs) esteja em uso.
        
        Args:
            service_name (str): O nome do serviço (ex: 'kafka').
        """
        print(f"Recriando serviço: {service_name}...")
        return self._run_command(["up", "-d", "--force-recreate", "--no-deps", service_name])

    def down(self):
        """
        Para e remove todos os serviços da rede
        (equivalente a 'docker compose down').
        """
        print("Parando e removendo contêineres...")
        # Adiciona '--remove-orphans' para limpar contêineres que possam ter 
        # ficado de uma execução anterior do gerador.
        return self._run_command(["down", "--remove-orphans"])

    def stop_service(self, service_name):
        """
        Para um serviço (contêiner) específico.
        
        Args:
            service_name (str): O nome do serviço (ex: 'worker-cam1').
        """
        print(f"Parando serviço: {service_name}...")
        return self._run_command(["stop", service_name])

    def start_service(self, service_name):
        """
        Inicia um serviço (contêiner) específico.
        
        Args:
            service_name (str): O nome do serviço (ex: 'worker-cam1').
        """
        print(f"Iniciando serviço: {service_name}...")
        return self._run_command(["start", service_name])
    
    def build(self):
        """
        Força a reconstrução das imagens (equivalente a 'docker compose build').
        """
        print("Construindo imagens...")
        return self._run_command(["build"])

    def logs(self, service_name=None, follow=False):
        """
        Mostra os logs.
        
        Args:
            service_name (str, optional): O serviço para ver os logs. Se None, 
                                          mostra logs de todos os serviços.
            follow (bool): Se True, segue os logs em tempo real (adiciona '-f').
        """
        command = ["logs"]
        if follow:
            command.append("--follow")
        if service_name:
            command.append(service_name)
            
        return self._run_command(command)