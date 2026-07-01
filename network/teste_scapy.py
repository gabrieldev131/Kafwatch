import netifaces
import socket
import ipaddress
from scapy.layers.inet import IP, TCP
from scapy.layers.l2 import ARP, Ether
from scapy.all import sr, srp
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time

# --- Função auxiliar para resolver ARP ---
def resolver_arp(ip, iface):
    arp_req = ARP(pdst=ip)
    ether = Ether(dst="ff:ff:ff:ff:ff:ff")
    ans, _ = srp(ether/arp_req, timeout=0.5, iface=iface, verbose=0)
    if ans:
        return ans[0][1].hwsrc  # MAC do destino
    return None

# Descobre interfaces de rede
def obter_redes():
    """
    # Função da qual se obtem os endereços de ip da máquina host (redes das quais a máquina está conectada)

    Retorna uma lista com todas as conexões
    """
    redes = []
    for iface in netifaces.interfaces():
        addrs = netifaces.ifaddresses(iface)
        if netifaces.AF_INET in addrs:

            for ip_info in addrs[netifaces.AF_INET]:
                ip = ip_info['addr']
                netmask = ip_info['netmask']
                network = ipaddress.IPv4Network(f"{ip}/{netmask}", strict=False)

                tabela = {"interface": iface, "ip": ip, "mascara": netmask, "range": str(network)}
                if tabela not in redes:
                    redes.append(tabela)
    return redes

# - descobrir qual dos renges são de uma rede local (LAN) -

def eh_rede_local(ip: str) -> bool: # "-> bool" significa que o retorno vai ser booleano, e "ip: str" significa que só entram IPs salvos em formato string

    # intervalos padrão dos ranges das redes locais 
    REDES_PRIVADAS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ]

    endereco = ipaddress.ip_address(ip) 
    for rede in REDES_PRIVADAS:
        if endereco in rede:
            return True
    return False


# Função de ping otimizada
def ping_host(ip, porta=80, timeout=0.25):
    try:
        socket.setdefaulttimeout(timeout)
        with socket.create_connection((str(ip), porta), timeout=timeout):
            return ip  # IP respondeu
    except (socket.timeout, socket.error):
        return None  # IP não respondeu
    
#ping em paralelo
def filtragem(redes):
    """
    # Passo 1: Filtra apenas as redes locais (LAN)

    nem toda interface da máquina é uma rede local (pode ter VPN, loopback, etc.)
    então percorre todas as redes e guarda só os ranges das que forem locais
    """

    ranges_locais = []
    for rede in redes:
        ip_da_rede = rede["ip"]
        este_ip_é_local = eh_rede_local(ip_da_rede)
        if este_ip_é_local:
            range_da_rede = rede["range"]
            ranges_locais.append(range_da_rede)  # ex: "192.168.1.0/24"
    return ranges_locais

def expande_range_lista_IP (ranges_locais):
    """
    # Expande cada range em uma lista de IPs individuais

    um range como "192.168.1.0/24" representa 254 IPs (192.168.1.1 até 192.168.1.254)
    .hosts() faz essa expansão automaticamente, ignorando o IP de rede e o de broadcast
    todos os IPs de todos os ranges são juntados em uma lista única para que
    um único pool de threads cubra tudo, sem precisar recriar o pool a cada range
    """
    todos_os_ips_de_todos_os_ranges = []
    for range_da_rede in ranges_locais:
        target_network = ipaddress.IPv4Network(range_da_rede, strict=False)
        todos_os_ips_de_todos_os_ranges.extend(target_network.hosts())
    return todos_os_ips_de_todos_os_ranges

def criar_pool_threads(todos_os_ips_de_todos_os_ranges):
    """
    # Cria o pool de threads 

    cada thread vai rodar ping_host(ip) de forma independente
    multiplicar por 8 compensa o tempo ocioso de espera da resposta do ping:
    enquanto uma thread aguarda, outras 7 já estão pingando outros IPs
    """

    icmp_hosts = []
    quantidade_de_threads = (os.cpu_count() or 1) * 8
    with ThreadPoolExecutor(max_workers=quantidade_de_threads) as executor:

        # --- Passo 4: Dispara o ping de cada IP como uma tarefa separada ---
        # executor.submit() agenda ping_host(ip) para rodar em uma thread livre
        # o dicionário futures guarda qual tarefa corresponde a qual IP
        futures = {}
        for ip in todos_os_ips_de_todos_os_ranges:
            tarefa = executor.submit(ping_host, str(ip))
            futures[tarefa] = ip

        # --- Passo 5: Coleta os resultados conforme as tarefas terminam ---
        # as_completed() não espera todas terminarem: já processa cada uma que ficar pronta
        # ping_host retorna o IP se respondeu, ou None se não respondeu
        for tarefa in as_completed(futures):
            ip_que_respondeu = tarefa.result()
            if ip_que_respondeu:
                icmp_hosts.append(ip_que_respondeu)

    return icmp_hosts  # retorna lista de IPs que responderam ao ping


def ping_em_paralelo(redes):

    ranges_locais = filtragem(redes)
    ips_nos_ranges = expande_range_lista_IP (ranges_locais)
    icmp_hosts = criar_pool_threads(ips_nos_ranges)

    return icmp_hosts  # retorna lista de IPs que responderam ao ping

    
def testar_rtsp_describe(ip, port=554):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.1)
        s.connect((ip, port))

        rtsp_request = (
            f"DESCRIBE rtsp://{ip}:{port}/ RTSP/1.0\r\n"
            f"CSeq: 1\r\n"
            f"Accept: application/sdp\r\n\r\n"
        )
        s.send(rtsp_request.encode())
        resposta = s.recv(4096).decode(errors="ignore")
        s.close()

        if "RTSP/1.0 200 OK" in resposta and "application/sdp" in resposta:
            return "camera"
        elif "RTSP/1.0 401 Unauthorized" in resposta:
            # Analisa o cabeçalho WWW-Authenticate
            if "DVR" in resposta or "NVR" in resposta:
                return "dvr"
            elif "Camera" in resposta or "IP Camera" in resposta:
                return "camera"
            else:
                return "unknown"
        else:
            return "invalid"

    except Exception:
        return "error"


# --- Passo 2: TCP SYN scan em múltiplas portas RTSP ---
def scan_multiplas_portas(icmp_hosts, redes):
    rtsp_ports = [554, 8554, 10554]
    camera_result = []

    print("\nVerificando portas RTSP...")

    # Cada IP é testado em paralelo em vez de um por um em série
    quantidade_de_threads = (os.cpu_count() or 1) * 8
    with ThreadPoolExecutor(max_workers=quantidade_de_threads) as executor:

        tarefas = {
            executor.submit(testar_ip_em_todas_as_portas, ip, redes, rtsp_ports): ip
            for ip in icmp_hosts
        }

        for tarefa in as_completed(tarefas):
            cameras_do_ip = tarefa.result()
            camera_result.extend(cameras_do_ip)

    return camera_result  # retorna lista final com todos os IPs confirmados como câmeras IP na rede

def testar_ip_em_todas_as_portas(ip, redes, rtsp_ports):
    #Testa todas as portas RTSP de um único IP — roda em paralelo por IP.

    # Descobre a interface de rede deste IP
    iface = None
    for rede in redes:
        if ipaddress.ip_address(ip) in ipaddress.ip_network(rede["range"], strict=False):
            iface = rede["interface"]
            break

    cameras_encontradas = []
    for port in rtsp_ports:
        pkt = IP(dst=ip)/TCP(dport=port, flags="S")
        ans, _ = sr(pkt, timeout=0.25, retry=0, verbose=0, iface=iface)

        for sent, received in ans:
            tcp_respondeu_com_syn_ack = received.haslayer(TCP) and received[TCP].flags == "SA"
            if tcp_respondeu_com_syn_ack:
                resultado_rtsp = testar_rtsp_describe(received[IP].src, port)
                if resultado_rtsp == "camera":
                    cameras_encontradas.append(ip)

    return cameras_encontradas  # retorna lista de IPs confirmados como câmera neste host

def main():
    start_time = time.time()
    
    lista_de_redes = obter_redes() #redes em que a máquina está conectada
    lista_de_respondedores = ping_em_paralelo(lista_de_redes) #responderam ao broadcast
    entertime = time.time()
    
    """
    lista_de_redes é o parâmetro de entrada, ou seja, a função ping_em_paralelo
    utiliza o que há dentro dela como se fosse o parametro local "redes", quando "redes" é
    utilizada dentro de ping_em_paralelo o valor que será utilizado agora será o de
    "lista_de_redes"

    """
    ip_camera = scan_multiplas_portas(lista_de_respondedores, lista_de_redes) #são cameras da rede local identificada

    final_time = time.time()
    
    print(f'{entertime - start_time}')
    
    print(f'{final_time - start_time}')
    print(f'{ip_camera}')

    return 0
if __name__ == "__main__":
    main()
