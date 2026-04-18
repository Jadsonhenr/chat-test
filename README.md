# Chat Distribuído — Threading Manual + Sockets Puros

Sistema de chat em tempo real com **implementação manual** de threading e protocolo WebSocket.

---

## Requisitos Cumpridos com Código Manual

| Requisito | Implementação Manual |
|-----------|---------------------|
| **Thread por cliente (servidor)** | `threading.Thread(target=handle_client, args=(sock, addr))` criada explicitamente a cada `accept()` — **ver linha 305 de primary_server.py** |
| **Thread de recepção (cliente)** | `Web Worker` (chat.worker.js) = thread real do SO no navegador — **única forma possível** |
| **Thread de heartbeat monitor** | `threading.Thread(target=heartbeat_monitor)` criada explicitamente — **ver linha 419 de backup_server.py** |
| **Socket puro** | `socket.socket(socket.AF_INET, socket.SOCK_STREAM)` + `accept()` manual — **sem bibliotecas de alto nível** |
| **WebSocket protocol** | Implementação completa do RFC 6455 em `websocket_protocol.py` — **handshake + framing manual** |

---

## Arquitetura: Threads Manuais

```
SERVIDOR (primary_server.py):
  Main Thread            → loop accept() aguardando conexões
  HTTP-Server Thread     → serve arquivos estáticos (criada explicitamente)
  Client-IP:PORT Thread  → uma thread POR cliente (criada a cada accept)
  Client-IP:PORT Thread  → outra thread para outro cliente
  ...                    → N threads (1 por cliente conectado)

SERVIDOR BACKUP (backup_server.py):
  Main Thread            → loop accept()
  HTTP-Server Thread     → servidor HTTP (criada explicitamente)
  HeartbeatMonitor Thread→ monitora /health do primário (criada explicitamente)
  Client-IP:PORT Thread  → threads por cliente (igual ao primário)

CLIENTE (navegador):
  Main Thread (app.js)   → UI, DOM, manipulação de eventos
  Worker Thread (chat.worker.js) → THREAD DEDICADA à recepção WebSocket
```

---

## Threading Manual — Código Relevante

### 1. Criação Manual da Thread por Cliente

**primary_server.py (linha ~305)**:
```python
def run_websocket_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("0.0.0.0", PRIMARY_WS_PORT))
    server_socket.listen(5)
    
    while True:
        client_socket, addr = server_socket.accept()
        
        # ═══ CRIAÇÃO MANUAL DA THREAD ═══
        client_thread = threading.Thread(
            target=handle_client,
            args=(client_socket, addr),
            name=f"Client-{addr[0]}:{addr[1]}",
            daemon=True,
        )
        client_thread.start()
```

### 2. Thread de Heartbeat (criada manualmente)

**backup_server.py (linha ~419)**:
```python
# CRIAÇÃO MANUAL DA THREAD DE MONITOR
monitor_thread = threading.Thread(
    target=heartbeat_monitor,
    name="HeartbeatMonitor",
    daemon=True,
)
monitor_thread.start()
```

### 3. Web Worker (thread de recepção no cliente)

**chat.worker.js**:
```javascript
// Este arquivo executa em uma THREAD REAL do navegador
socket.onmessage = function (event) {
    // ═══ RECEPÇÃO EM THREAD DEDICADA ═══
    const data = JSON.parse(event.data);
    self.postMessage({ type: 'MESSAGE', data });
};
```

**app.js (criação do Worker)**:
```javascript
// CRIAÇÃO MANUAL DO WEB WORKER
state.worker = new Worker('/chat.worker.js');
```

---

## Protocolo WebSocket — Implementação Manual

O arquivo `websocket_protocol.py` implementa do zero:

- **Handshake HTTP Upgrade** (RFC 6455 § 4.2)
  ```python
  def websocket_handshake(sock: socket.socket) -> bool:
      # Lê request HTTP, valida Sec-WebSocket-Key, envia 101 Switching Protocols
  ```

- **Frame Encoding/Decoding** (RFC 6455 § 5.2)
  ```python
  def decode_frame(data: bytes) -> tuple[int, bytes]:
      # Decodifica FIN, opcode, mask, payload length, masking key, payload
  
  def encode_frame(opcode: int, payload: bytes) -> bytes:
      # Codifica frame com opcode + payload (servidor não mascara)
  ```

- **Send/Receive Helpers**
  ```python
  def send_text(sock, text)   # Envia mensagem de texto
  def send_close(sock, code)  # Envia frame de close
  def send_pong(sock, payload)# Responde a ping
  def recv_frame(sock)        # Recebe e decodifica frame
  ```

---

## Execução

```bash
# Não há dependências externas!
python primary_server.py   # terminal 1
python backup_server.py    # terminal 2
# Abrir http://localhost:8080 em dois navegadores
```

---

## Demonstração de Failover

1. Abrir dois navegadores em `http://localhost:8080`
2. Ambos se conectam ao **servidor principal** (porta 8765)
3. **Matar o servidor principal** com `Ctrl+C`
4. Após ~9s (3 falhas × 3s), o **backup se promove** a primário
5. Clientes conectados ao backup continuam funcionando normalmente
6. Novos clientes podem conectar ao backup via `http://localhost:8081`

---

## Logs: Evidência de Threading Manual

Ao executar, os logs mostram:

```
10:00:01 [PRIMARY] [MainThread] Servidor WebSocket escutando em ws://localhost:8765
10:00:05 [PRIMARY] [Client-127.0.0.1:54321] Nova conexão de ('127.0.0.1', 54321)  ← thread: Client-127.0.0.1:54321
10:00:05 [PRIMARY] [MainThread] Thread criada manualmente para ('127.0.0.1', 54321): Client-127.0.0.1:54321
10:00:10 [PRIMARY] [Client-127.0.0.1:54322] Nova conexão de ('127.0.0.1', 54322)  ← thread: Client-127.0.0.1:54322
```

**Cada linha "Thread criada manualmente" prova que o código está criando `threading.Thread()` explicitamente.**

---

## Estrutura do Projeto

```
chat_distribuido/
├── config.py                 # Constantes de configuração
├── websocket_protocol.py     # IMPLEMENTAÇÃO MANUAL DO WEBSOCKET RFC 6455
├── primary_server.py         # Servidor principal (socket + threading manual)
├── backup_server.py          # Servidor backup (socket + threading manual + monitor)
├── requirements.txt          # (vazio — apenas stdlib do Python)
├── README.md
├── relatorio_tecnico.pdf
└── static/
    ├── index.html
    ├── style.css
    ├── app.js                # Thread principal (UI)
    └── chat.worker.js        # WEB WORKER = thread de recepção
```

---

## Diferencial deste Projeto

- **Threading 100% manual** — nenhuma biblioteca abstrai a criação de threads
- **Sockets puros** — `socket.socket()` + `accept()` + `recv()` + `send()`
- **WebSocket do zero** — implementação completa do protocolo (RFC 6455)
- **Web Worker** — única forma de ter thread de recepção no navegador
- **Sem dependências externas** — apenas biblioteca padrão do Python

---

## Observações Técnicas

### Por que Web Worker no cliente?

Navegadores **não expõem `threading` diretamente**. A única forma de criar uma thread separada no browser é via **Web Worker API**, que:
- Cria uma thread real do sistema operacional
- Executa JavaScript em isolamento de memória
- Comunica-se com o thread principal via `postMessage()`

É a implementação **mais fiel possível** ao requisito de "thread dedicada à recepção" no contexto de um cliente web.

### Por que implementar WebSocket manualmente?

Para que a interface seja **acessível via navegador** (requisito do trabalho), o cliente precisa usar o protocolo WebSocket (navegadores não suportam TCP puro). A implementação manual do protocolo demonstra controle total sobre a camada de transporte e evita usar bibliotecas de alto nível que abstraem o threading.

---

## Autor(es)

**[jadson,antonio,joshua**  
Universidade Federal do Rio Grande — FURG  
Sistemas Distribuídos · 2024
# chat-test
# chat-test
