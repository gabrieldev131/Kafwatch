import asyncio
import ipaddress
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed


class NetworkScanner:
    def __init__(self, network_base, ip_range, port, timeout=0.25, max_workers=64, pace_delay=0.0015):
        """
        Inicializa o scanner.
        
        Args:
            network_base (str | iterable): A base da rede (ex: "192.168.1" ou ["10.145.80", "10.145.81"]).
            ip_range (iterable): Um range ou lista de IPs (ex: range(1, 255)).
            port (int): A porta a ser verificada.
            timeout (float): Timeout em segundos para cada tentativa de conexão.
            max_workers (int): Número máximo de conexões paralelas.
            pace_delay (float): Intervalo entre disparos de conexão para evitar overflow ARP.
        """
        if isinstance(network_base, str):
            self._network_base = [network_base]
        else:
            self._network_base = list(network_base)

        self._ip_range = list(ip_range)
        self._port = port
        self._timeout = timeout
        self._pace_delay = pace_delay
        total_ips = max(1, len(self._network_base) * len(self._ip_range))
        # Ajustamos o número de workers considerando o total real de IPs a escanear
        self._max_workers = min(max_workers, total_ips)

    def _check_ip(self, ip):
        """
        Função "worker" que tenta conectar a um único IP e porta.
        Retorna o IP se for bem-sucedido.
        """
        try:
            with socket.create_connection((ip, self._port), timeout=self._timeout):
                return ip
        except (socket.timeout, ConnectionRefusedError, OSError):
            return None

    async def _async_check_ip(self, ip, sem, progress_callback=None, delay_s=0.0):
        if delay_s > 0:
            await asyncio.sleep(delay_s)
        async with sem:
            try:
                conn = asyncio.open_connection(ip, self._port)
                reader, writer = await asyncio.wait_for(conn, timeout=self._timeout)
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                if progress_callback:
                    progress_callback(ip)
                return ip
            except Exception:
                return None

    async def _async_discover(self, ips_to_check, progress_callback=None):
        sem = asyncio.Semaphore(self._max_workers)
        tasks = [
            self._async_check_ip(ip, sem, progress_callback, delay_s=idx * self._pace_delay)
            for idx, ip in enumerate(ips_to_check)
        ]
        results = await asyncio.gather(*tasks)
        return [ip for ip in results if ip is not None]

    def discover(self, progress_callback=None):
        """
        Escaneia a rede em paralelo usando I/O assíncrono de alta performance.
        Retorna uma lista ordenada de IPs de câmeras encontradas.
        """
        ips_to_check = [
            f"{base}.{host}"
            for base in self._network_base
            for host in self._ip_range
        ]

        if not ips_to_check:
            return []

        found_ips = asyncio.run(self._async_discover(ips_to_check, progress_callback))
        return sorted(found_ips, key=lambda ip: ipaddress.IPv4Address(ip))
    
if __name__ == "__main__":
    found_cameas = NetworkScanner(["10.145.80", "10.145.81", "10.145.82"], range(1, 255), 554, timeout=0.12)
    cameras = found_cameas.discover(progress_callback = lambda ip: print(f"Camera found at: {ip}"))