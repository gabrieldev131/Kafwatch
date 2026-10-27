import asyncio
import netifaces
import socket
import ipaddress
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time

# Prefixos de interfaces virtuais/containers/VPN que devem ser ignoradas no scan
IGNORED_INTERFACE_PREFIXES = (
    "lo", "docker", "br-", "veth", "virbr", "tun", "tap",
    "wg", "vmnet", "vboxnet", "tailscale", "zt", "cni", "flannel", "cali"
)

# Intervalos padrão dos ranges das redes locais (RFC 1918) pré-alocados
REDES_PRIVADAS = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
)

# Limite seguro de conexões simultâneas (abaixo do ulimit -n padrão de 1024)
MAX_CONCURRENT_PROBES = 512
DEFAULT_PING_TIMEOUT = 0.15
DEFAULT_RTSP_TIMEOUT = 0.20


def _eh_interface_virtual(iface: str) -> bool:
    iface_lower = iface.lower()
    return iface_lower.startswith(IGNORED_INTERFACE_PREFIXES)


# --- Função auxiliar para resolver ARP ---
def resolver_arp(ip, iface):
    from scapy.all import ARP, Ether, srp
    arp_req = ARP(pdst=ip)
    ether = Ether(dst="ff:ff:ff:ff:ff:ff")
    ans, _ = srp(ether / arp_req, timeout=0.5, iface=iface, verbose=0)
    if ans:
        return ans[0][1].hwsrc  # MAC do destino
    return None


# Descobre interfaces de rede
def obter_redes():
    """
    Obtém os endereços de IP da máquina host (redes físicas/LAN às quais a máquina está conectada),
    ignorando interfaces de loopback, Docker bridges e VPNs.

    Retorna uma lista com todas as conexões válidas.
    """
    redes = []
    for iface in netifaces.interfaces():
        if _eh_interface_virtual(iface):
            continue

        addrs = netifaces.ifaddresses(iface)
        if netifaces.AF_INET in addrs:
            for ip_info in addrs[netifaces.AF_INET]:
                ip = ip_info.get("addr")
                netmask = ip_info.get("netmask")
                if not ip or not netmask:
                    continue

                network = ipaddress.IPv4Network(f"{ip}/{netmask}", strict=False)
                tabela = {
                    "interface": iface,
                    "ip": ip,
                    "mascara": netmask,
                    "range": str(network),
                }
                if tabela not in redes:
                    redes.append(tabela)
    return redes


# - Descobrir quais dos ranges são de uma rede local (LAN) -
def eh_rede_local(ip: str) -> bool:
    endereco = ipaddress.ip_address(ip)
    return any(endereco in rede for rede in REDES_PRIVADAS)


# Função de ping TCP otimizada (sem mutar timeout global de socket)
def ping_host(ip, porta=554, timeout=DEFAULT_PING_TIMEOUT):
    ip_str = str(ip)
    try:
        with socket.create_connection((ip_str, porta), timeout=timeout):
            return ip_str  # Porta aberta: host ativo
    except ConnectionRefusedError:
        # TCP RST recebido imediatamente: o host existe e está ativo na rede!
        return ip_str
    except (socket.timeout, OSError):
        return None  # IP não respondeu


def filtragem(redes):
    """
     Passo 1: Filtra apenas as redes locais (LAN) válidas,
    ignorando interfaces virtuais (Docker, loopback, VPN) e duplicatas.
    """
    ranges_locais = []
    for rede in redes:
        iface = rede.get("interface", "")
        if _eh_interface_virtual(iface):
            continue
        ip_da_rede = rede["ip"]
        if eh_rede_local(ip_da_rede):
            range_da_rede = rede["range"]
            if range_da_rede not in ranges_locais:
                ranges_locais.append(range_da_rede)  # ex: "192.168.1.0/24"
    return ranges_locais


def expande_range_lista_IP(ranges_locais):
    """
    Expande cada range em uma lista de IPs individuais (em formato string).
    """
    todos_os_ips_de_todos_os_ranges = []
    for range_da_rede in ranges_locais:
        target_network = ipaddress.IPv4Network(range_da_rede, strict=False)
        todos_os_ips_de_todos_os_ranges.extend(str(ip) for ip in target_network.hosts())
    return todos_os_ips_de_todos_os_ranges


async def _async_probe_host(ip_str: str, sem: asyncio.Semaphore, porta: int = 554, timeout: float = DEFAULT_PING_TIMEOUT):
    async with sem:
        try:
            conn = asyncio.open_connection(ip_str, porta)
            reader, writer = await asyncio.wait_for(conn, timeout=timeout)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return ip_str
        except ConnectionRefusedError:
            # Host respondeu com TCP RST, portanto está ativo na LAN
            return ip_str
        except Exception:
            return None


async def _async_ping_hosts(ips_list, porta=554, timeout=DEFAULT_PING_TIMEOUT):
    sem = asyncio.Semaphore(MAX_CONCURRENT_PROBES)
    tasks = [_async_probe_host(str(ip), sem, porta=porta, timeout=timeout) for ip in ips_list]
    results = await asyncio.gather(*tasks)
    return [ip for ip in results if ip is not None]


def criar_pool_threads(todos_os_ips_de_todos_os_ranges):
    """
    Executa a descoberta de hosts ativos em alta concorrência usando I/O assíncrono
    (capaz de sondar centenas de IPs simultaneamente com baixo custo de CPU).
    """
    if not todos_os_ips_de_todos_os_ranges:
        return []
    return asyncio.run(_async_ping_hosts(todos_os_ips_de_todos_os_ranges))


def ping_em_paralelo(redes):
    ranges_locais = filtragem(redes)
    ips_nos_ranges = expande_range_lista_IP(ranges_locais)
    icmp_hosts = criar_pool_threads(ips_nos_ranges)
    return icmp_hosts


def testar_rtsp_describe(ip, port=554, timeout=DEFAULT_RTSP_TIMEOUT):
    ip_str = str(ip)
    try:
        with socket.create_connection((ip_str, port), timeout=timeout) as s:
            s.settimeout(timeout)
            rtsp_request = (
                f"DESCRIBE rtsp://{ip_str}:{port}/ RTSP/1.0\r\n"
                f"CSeq: 1\r\n"
                f"Accept: application/sdp\r\n\r\n"
            )
            s.sendall(rtsp_request.encode())
            resposta = s.recv(4096).decode(errors="ignore")

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


async def _async_testar_rtsp_describe(ip_str: str, port: int, sem: asyncio.Semaphore, timeout: float = DEFAULT_RTSP_TIMEOUT) -> str:
    async with sem:
        try:
            conn = asyncio.open_connection(ip_str, port)
            reader, writer = await asyncio.wait_for(conn, timeout=timeout)
            try:
                rtsp_request = (
                    f"DESCRIBE rtsp://{ip_str}:{port}/ RTSP/1.0\r\n"
                    f"CSeq: 1\r\n"
                    f"Accept: application/sdp\r\n\r\n"
                )
                writer.write(rtsp_request.encode())
                await asyncio.wait_for(writer.drain(), timeout=timeout)
                data = await asyncio.wait_for(reader.read(4096), timeout=timeout)
                resposta = data.decode(errors="ignore")
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

            if "RTSP/1.0 200 OK" in resposta and "application/sdp" in resposta:
                return "camera"
            elif "RTSP/1.0 401 Unauthorized" in resposta:
                if "DVR" in resposta or "NVR" in resposta:
                    return "dvr"
                elif "Camera" in resposta or "IP Camera" in resposta:
                    return "camera"
                else:
                    return "unknown"
            return "invalid"
        except Exception:
            return "error"


async def _async_testar_ip_portas(ip_str: str, rtsp_ports, sem: asyncio.Semaphore):
    for port in rtsp_ports:
        resultado = await _async_testar_rtsp_describe(ip_str, port, sem)
        if resultado == "camera":
            return ip_str
    return None


async def _async_scan_multiplas_portas(icmp_hosts, rtsp_ports):
    sem = asyncio.Semaphore(MAX_CONCURRENT_PROBES)
    tasks = [_async_testar_ip_portas(str(ip), rtsp_ports, sem) for ip in icmp_hosts]
    results = await asyncio.gather(*tasks)
    # Preserva a ordem e remove duplicatas
    return list(dict.fromkeys(ip for ip in results if ip is not None))


# --- Passo 2: Scan rápido em múltiplas portas RTSP ---
def scan_multiplas_portas(icmp_hosts, redes=None):
    rtsp_ports = [554, 8554, 10554]
    if not icmp_hosts:
        return []

    print("\nVerificando portas RTSP...")
    return asyncio.run(_async_scan_multiplas_portas(icmp_hosts, rtsp_ports))


def testar_ip_em_todas_as_portas(ip, redes=None, rtsp_ports=None):
    """Testa todas as portas RTSP de um único IP diretamente via socket TCP."""
    if rtsp_ports is None:
        rtsp_ports = [554, 8554, 10554]

    ip_str = str(ip)
    cameras_encontradas = []
    for port in rtsp_ports:
        resultado_rtsp = testar_rtsp_describe(ip_str, port)
        if resultado_rtsp == "camera":
            cameras_encontradas.append(ip_str)
            break

    return cameras_encontradas


def discover():
    lista_de_redes = obter_redes()
    lista_de_respondedores = ping_em_paralelo(lista_de_redes)
    ip_camera = scan_multiplas_portas(lista_de_respondedores, lista_de_redes)
    return ip_camera


def main():
    start_time = time.time()

    lista_de_redes = obter_redes()  # redes em que a máquina está conectada
    lista_de_respondedores = ping_em_paralelo(lista_de_redes)  # responderam na rede local
    entertime = time.time()

    ip_camera = scan_multiplas_portas(lista_de_respondedores, lista_de_redes)  # câmeras confirmadas

    final_time = time.time()

    print(f"{entertime - start_time:.3f}s")
    print(f"{final_time - start_time:.3f}s")
    print(f"{ip_camera}")

    return 0


if __name__ == "__main__":
    main()