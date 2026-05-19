# NetworkScanner: Sistema de encontrar câmeras conectadas nas redes em comum com a máquina

# Funções:

### resolver_arp
Descobre o endereço MAC de um dispositivo na rede local a partir do seu endereço IP e da resposta à um broadcast. Assim, garantindo a comunicação com o dispositivo através do registro de seu endereço MAC.

### obter_redes
Obtém informações (interface, IPs, máscaras e range) das redes das quais a máquina possui conexão e guarda os dados em uma lista de dicionários. 

### eh_rede_local
Possui uma lista com os intervalos padrão dos ranges das redes locais e avalia, com base nessa lista, quais ranges obtidos se encaixam nesses intervalos.

### ping_host
Ela tenta abrir uma conexão TCP (pois é a utilizada pelas câmeras) com um IP em uma porta específica para verificar se o host está ativo.

### filtragem
Filtra e adiciona a uma lista apenas as redes locais (LAN) a partir da comparação dos ranges obtidos com o intervalo dos ranges locais registrados na função "eh_rede_local".

### expande_range_lista_IP
Adiciona todos os IPs, que estão dentro dos ranges denominados locais, em uma lista.

### criar_pool_threads
Faz com que cada thread (multiplicada por 8) da máquina ping um IP da lista de IPs das redes locais. Cria outra lista com unicamente os IPs que responderam ao ping.

### ping_em_paralelo
Chama as funções: filtragem, expande_range_lista_IP e criar_pool_threads. A partir do processo das funções em conjunto, retorna a lista de IPs respondedores. 

### testar_rtsp_describe
Cria uma conexão TCP com o IP e a porta 554 (usada pelo RTSP), envia ao servidor RTSP o comando DESCRIBE. Se o servidor respondeu "200 OK" e aceitou SDP, é identificado como câmera IP e a função retorna "camera". Se a resposta foi "401 Unauthorized" há a analise do cabeçalho RTSP, se mencionar "Camera" ou "IP Camera", retorna "camera". Caso contrário, retorna "invalid".

### testar_ip_em_todas_as_portas
Testa um único IP em várias portas RTSP (554, 8554, 10554). Se a porta responde, chama testar_rtsp_describe para verificar se é câmera e apenas retorna os IPs confirmados que são. 

### scan_multiplas_portas
Para cada IP na lista dos IPs respondedores, chama testar_ip_em_todas_as_portas. Retorna os IPs que são confirmados como câmeras.

### main
Chama a função obter_redes e atrela a lista retornada à uma variável. Executa ping_em_paralelo para descobrir quais IPs das redes locais responderam ao broadcast ICMP. Chama scan_multiplas_portas para confirmar quais IPs respondedores são, de fato, câmeras.

# Bibliotecas Utilizadas:

### Netifaces:
Biblioteca utilizada para 

### Socket:

### Ipaddress:

### Scapy.all:

### Concurrent.futures:

### Os:

### Time


# KafWatch: Sistema de Streaming de Vídeo Distribuído com Kafka

Este documento descreve a arquitetura, classes e funcionamento do sistema de captura, transmissão e exibição de vídeo em tempo real utilizando Docker, MediaMTX, Apache Kafka e Python (OpenCV).

Arquitetura do Sistema

O sistema foi desenhado para ser escalável e desacoplado. O fluxo de dados segue o seguinte caminho:

[ Câmeras IP ] --> (RTSP) --> [ MediaMTX (Proxy) ] --> (RTSP) --> [ Worker Python (Producer) ]
                                                                        |
                                                                     (JPEG Bytes)
                                                                        v
[ Visualizador (Consumer) ] <-- (JPEG Bytes) <-- [ Apache Kafka Cluster ]


# Componentes Principais

### MediaMTX (RTSP Proxy): 
Multiplexa o sinal das câmeras. Garante que cada câmera receba apenas uma conexão, protegendo o hardware da câmera contra sobrecarga.

### Kafka (Broker): 
Atua como buffer de alta performance. Permite que múltiplos consumidores (Visualizadores, IAs, Gravadores) consumam o vídeo sem impactar a fonte.

### Worker (Producer): 
Container Docker leve que captura frames, comprime em JPEG e envia para o Kafka.

### Visualizador (Consumer): 
Aplicação Desktop que lê do Kafka e exibe o vídeo com latência mínima.

## Módulo 1: Orquestração e Configuração (Generator)

Este módulo é responsável por escanear a rede e gerar automaticamente os arquivos docker-compose.yml e configurações do MediaMTX. Ele segue estritamente os princípios S.O.L.I.D.

1. ComposeGenerator (Lógica de Negócio)

- 1.1 Responsabilidade (SRP): Gerar as configurações em memória (dicionários Python). Não faz I/O (leitura/escrita em disco).

- 1.2 Funcionamento: Recebe uma lista de IPs, credenciais e portas. Constrói a estrutura de serviços do Docker (um serviço worker para cada câmera encontrada) e as rotas do MediaMTX.

- 1.3 Extensibilidade (OCP): Utiliza templates base que podem ser estendidos sem modificar a estrutura do código.

2. FileSystemWriter (Camada de I/O)

- 2.1 Responsabilidade: Lidar exclusivamente com o sistema de arquivos.

- 2.2 Funcionamento: Cria diretórios, verifica caminhos e escreve arquivos YAML ou Texto.

- 2.3 Benefício: Se decidirmos salvar as configs na nuvem (S3) ou em banco de dados, alteramos apenas esta classe ou criamos uma nova implementação, sem quebrar o ComposeGenerator.

3. NetworkScanner (Descoberta)

- 3.1 Responsabilidade: Varrer sub-redes em busca de portas RTSP (554) abertas.

- 3.2 Funcionamento: Tenta conexões TCP em IPs sequenciais. Retorna uma lista de IPs ativos para o gerador.

## Módulo 2: Captura e Envio (Producer)

O script produtor.py roda dentro de containers Docker (worker-camX). O foco aqui é throughput (vazão).

### Otimizações Implementadas

- CameraBufferCleaner (Threaded Capture):

- - Problema: O método cap.read() do OpenCV é bloqueante. Se o Kafka demorar para responder, o buffer da câmera enche e o vídeo fica com delay acumulado.


- - Solução: Uma thread dedicada lê a câmera em loop infinito e descarta frames antigos, mantendo sempre apenas o último frame na memória (self.last_frame). O produtor principal lê dessa variável, garantindo latência zero na captura.

- Compressão JPEG:

- - Em vez de enviar frames RAW (Bitmap), que ocupariam ~900KB/frame, comprimimos em JPEG (~20KB/frame). Isso reduz o uso de banda de rede em quase 50x, permitindo altas taxas de FPS.

# Kafka Batching:

- Utilizamos linger_ms e compressão gzip no nível do produtor para enviar pacotes de frames, aumentando a eficiência do TCP.

## Módulo 3: Visualização (Consumer Turbo)

- O consumidor.py é a peça mais crítica para a experiência do usuário. Ele precisa decodificar e exibir vídeo a 30+ FPS sem travar a interface.

#### Classe TurboKafkaReader

- Esta classe substitui o consumo padrão por uma abordagem de alta performance usando a biblioteca confluent-kafka (baseada em C).

### 1. Estratégia de "Drenagem de Fila" (Drain Strategy)

* Conceito: O vídeo ao vivo não precisa de histórico. Se o consumidor ficar 1 segundo atrasado, não queremos ver o que aconteceu há 1 segundo; queremos ver o agora.

* Implementação: Em vez de ler uma mensagem por vez, o método _update_loop consome lotes de 50 mensagens (consume(num_messages=50)).

* Lógica: Ele itera sobre o lote e sobrescreve o dicionário local. Se houver 10 frames da "Câmera 1" no lote, os 9 primeiros são descartados (sem gastar CPU decodificando) e apenas o 10º é processado.

### 2. Multithreading (Producer-Consumer Pattern)

* Thread de Background (_update_loop): Focada exclusivamente em I/O de rede e Decodificação de CPU (cv2.imdecode). Ela atualiza uma variável compartilhada.

* Thread Principal (main): Focada exclusivamente em GUI (cv2.imshow). Ela não bloqueia esperando a rede. Ela apenas "tira uma foto" do estado atual e desenha na tela.

### 3. Confluent Kafka (Librdkafka)

* Mudamos da biblioteca kafka-python (pura, lenta) para confluent-kafka (wrapper C++, rápida).

* Ajustamos buffers de socket (socket.receive.buffer.bytes) para 10MB para evitar que o Kernel do SO limite a recepção de dados em fluxos de vídeo HD.

#### Guia de Configuração (.env)

* O sistema depende de variáveis de ambiente para saber onde buscar e onde entregar os dados para rodar DENTRO do Docker (Workers)

* Usa o nome do serviço definido no docker-compose
* * KAFKA_BOOTSTRAP_SERVERS="kafka:29092"
* * RTSP_URL="rtsp://rtsp-proxy:8554/cam1"
* * CAMERA_ID="cam1"


Para rodar NO HOST (Visualizador)

#### Usa localhost, pois a porta 9092 está exposta no host
* * KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
#### Opcional: Filtra para exibir apenas uma câmera específica
* * CAMERA_ID="cam3" 


# Troubleshooting Comum

 | Sintoma | Causa Provável | Solução |  
| :---: | :---: | :---: |
| Erro: Connect to ipv4#127.0.0.1:9092 failed | O script está rodando no Docker mas configurado para localhost, ou vice-versa. | Ajuste o .env. Use kafka:29092 dentro do Docker e localhost:9092 fora. |
| Erro: input/output error no Docker | Corrupção do disco virtual do Docker (WSL2). | Executar wsl --shutdown ou, em casos graves, deletar o arquivo ext4.vhdx. |
| Vídeo com "Delay" (Atraso) | Acúmulo de buffer no Kafka ou no OpenCV. | A classe TurboKafkaReader resolve isso descartando frames antigos automaticamente. |
| FPS Baixo (< 15) | Compressão JPEG muito alta ou limitação da própria câmera. | Verifique se a câmera está configurada para emitir 30fps. Reduza a qualidade JPEG no Produtor. |
