"""
primary_server.py — Servidor Principal com Threading Manual
============================================================
Implementação usando:
  • socket.socket() puro (sem bibliotecas de alto nível)
  • threading.Thread() manual para cada cliente
  • WebSocket protocol implementado do zero
"""

import socket
import threading
import json
import time
import logging
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os

from websocket_protocol import (
    websocket_handshake, recv_frame, send_text, send_close, send_pong,
    OP_TEXT, OP_CLOSE, OP_PING
)
from config import (
    PRIMARY_WS_PORT, PRIMARY_HTTP_PORT,
    MAX_HISTORY, MAX_USERNAME_LEN, MAX_MSG_LEN
)

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRIMARY] [%(threadName)-24s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("primary")

# ── Estado Compartilhado ───────────────────────────────────────────────────────
# Dicionário: {socket: username}
clients: dict[socket.socket, str] = {}
clients_lock = threading.Lock()

# Histórico de mensagens
message_history: list[dict] = []
history_lock = threading.Lock()


# ══════════════════════════════════════════════════════════════════════════════
# Funções Auxiliares
# ══════════════════════════════════════════════════════════════════════════════

def timestamp() -> str:
    return time.strftime("%H:%M")


def add_to_history(msg: dict) -> None:
    with history_lock:
        message_history.append(msg)
        if len(message_history) > MAX_HISTORY:
            message_history.pop(0)


def broadcast(payload: dict, exclude: socket.socket = None) -> None:
    """Envia mensagem JSON para todos os clientes conectados."""
    raw = json.dumps(payload)
    
    with clients_lock:
        targets = list(clients.keys())
    
    for sock in targets:
        if sock is exclude:
            continue
        try:
            send_text(sock, raw)
        except Exception:
            pass  # Conexão já foi encerrada


def broadcast_user_list() -> None:
    with clients_lock:
        users = sorted(clients.values())
    broadcast({"type": "user_list", "users": users})


# ══════════════════════════════════════════════════════════════════════════════
# Handler de Cliente — EXECUTADO EM THREAD DEDICADA (criada manualmente)
# ══════════════════════════════════════════════════════════════════════════════

def handle_client(client_socket: socket.socket, addr: tuple) -> None:
    """
    Gerencia a conexão de um cliente WebSocket.
    
    Esta função é executada em uma thread criada manualmente via
    threading.Thread(target=handle_client, args=(sock, addr)).start()
    
    Cada cliente tem sua própria thread dedicada.
    """
    tname = threading.current_thread().name
    log.info(f"Nova conexão de {addr}  ← thread: {tname}")
    
    username: str | None = None
    
    try:
        # 1. Handshake WebSocket
        if not websocket_handshake(client_socket):
            log.warning(f"Handshake falhou para {addr}")
            return
        
        log.info(f"Handshake completo com {addr}")
        
        # 2. Loop de recepção de mensagens
        while True:
            frame = recv_frame(client_socket, timeout=120.0)
            
            if frame is None:
                # Timeout ou erro de conexão
                break
            
            opcode, payload = frame
            
            # ── CLOSE ────────────────────────────────────────────────────
            if opcode == OP_CLOSE:
                send_close(client_socket)
                break
            
            # ── PING → responde com PONG ────────────────────────────────
            elif opcode == OP_PING:
                send_pong(client_socket, payload)
                continue
            
            # ── TEXT: mensagem JSON do cliente ──────────────────────────
            elif opcode == OP_TEXT:
                try:
                    data = json.loads(payload.decode('utf-8'))
                except json.JSONDecodeError:
                    continue
                
                kind = data.get("type", "")
                
                # ── join ─────────────────────────────────────────────────
                if kind == "join":
                    name = data.get("username", "").strip()
                    
                    if not name or len(name) > MAX_USERNAME_LEN:
                        send_text(client_socket, json.dumps({
                            "type": "error",
                            "msg": "Nome inválido (1-20 caracteres).",
                        }))
                        continue
                    
                    # Verifica unicidade (OPERAÇÃO ATÔMICA com lock)
                    with clients_lock:
                        if name in clients.values():
                            send_text(client_socket, json.dumps({
                                "type": "error",
                                "msg": f'Nome "{name}" já está em uso.',
                            }))
                            continue
                        clients[client_socket] = name
                    
                    username = name
                    log.info(f'"{username}" entrou no chat.')
                    
                    # Envia info do servidor
                    send_text(client_socket, json.dumps({
                        "type": "server_info",
                        "role": "primary",
                        "port": PRIMARY_WS_PORT,
                    }))
                    
                    # Envia histórico
                    with history_lock:
                        hist = list(message_history)
                    send_text(client_socket, json.dumps({
                        "type": "history",
                        "messages": hist,
                    }))
                    
                    # Notifica os demais
                    sys_msg = {
                        "type": "system",
                        "text": f"{username} entrou no chat.",
                        "time": timestamp(),
                    }
                    add_to_history(sys_msg)
                    broadcast(sys_msg)
                    broadcast_user_list()
                
                # ── message ──────────────────────────────────────────────
                elif kind == "message":
                    if not username:
                        continue
                    
                    text = data.get("msg", "").strip()
                    if not text or len(text) > MAX_MSG_LEN:
                        continue
                    
                    chat_msg = {
                        "type": "chat",
                        "username": username,
                        "text": text,
                        "time": timestamp(),
                    }
                    add_to_history(chat_msg)
                    broadcast(chat_msg)
                    log.info(f'[MSG] {username}: {text[:60]}')
                
                # ── private_message ──────────────────────────────────────
                elif kind == "private_message":
                    if not username:
                        continue
                    
                    target = data.get("to", "").strip()
                    text   = data.get("msg", "").strip()
                    
                    if not target or not text:
                        continue
                    
                    # Encontra socket do destinatário
                    with clients_lock:
                        target_sock = next(
                            (s for s, n in clients.items() if n == target),
                            None
                        )
                    
                    if not target_sock:
                        send_text(client_socket, json.dumps({
                            "type": "error",
                            "msg": f'Usuário "{target}" não encontrado.',
                        }))
                        continue
                    
                    priv = {
                        "type": "private",
                        "from": username,
                        "to": target,
                        "text": text,
                        "time": timestamp(),
                    }
                    
                    send_text(target_sock, json.dumps(priv))
                    send_text(client_socket, json.dumps(priv))
                    log.info(f"[PRIV] {username} → {target}")
                
                # ── ping ─────────────────────────────────────────────────
                elif kind == "ping":
                    send_text(client_socket, json.dumps({"type": "pong"}))
    
    except Exception as exc:
        log.error(f"Erro no handler de {addr}: {exc}")
    
    finally:
        # Remove cliente e notifica
        with clients_lock:
            removed = clients.pop(client_socket, None)
        
        if removed:
            log.info(f'"{removed}" desconectou.')
            sys_msg = {
                "type": "system",
                "text": f"{removed} saiu do chat.",
                "time": timestamp(),
            }
            add_to_history(sys_msg)
            broadcast(sys_msg)
            broadcast_user_list()
        
        try:
            client_socket.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# Servidor HTTP — serve arquivos estáticos e /health
# ══════════════════════════════════════════════════════════════════════════════

class ChatHTTPHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        super().__init__(*args, directory=static_dir, **kwargs)
    
    def do_GET(self):
        if self.path == "/health":
            self._serve_health()
        else:
            if self.path == "/":
                self.path = "/index.html"
            super().do_GET()
    
    def _serve_health(self):
        with clients_lock:
            user_count = len(clients)
        body = json.dumps({
            "status": "ok",
            "role": "primary",
            "ws_port": PRIMARY_WS_PORT,
            "users_online": user_count,
            "timestamp": time.time(),
        }).encode()
        
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)
    
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()
    
    def log_message(self, fmt, *args):
        if args and str(args[1]) not in ("200", "304"):
            log.debug("HTTP %s %s %s" % args)


def run_http_server() -> None:
    """Thread daemon para servir arquivos estáticos."""
    httpd = HTTPServer(("0.0.0.0", PRIMARY_HTTP_PORT), ChatHTTPHandler)
    log.info(f"Interface web em http://localhost:{PRIMARY_HTTP_PORT}")
    httpd.serve_forever()


# ══════════════════════════════════════════════════════════════════════════════
# Servidor WebSocket — socket puro + threading manual
# ══════════════════════════════════════════════════════════════════════════════

def run_websocket_server() -> None:
    """
    Servidor WebSocket usando socket puro.
    
    Para cada accept(), cria uma Thread manualmente:
      t = threading.Thread(target=handle_client, args=(sock, addr))
      t.start()
    """
    # Cria socket TCP
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("0.0.0.0", PRIMARY_WS_PORT))
    server_socket.listen(5)
    
    log.info(f"Servidor WebSocket escutando em ws://localhost:{PRIMARY_WS_PORT}")
    log.info("Aguardando conexões…  (Ctrl+C para encerrar)")
    
    try:
        while True:
            # Aceita nova conexão (BLOQUEIA até um cliente conectar)
            client_socket, addr = server_socket.accept()
            
            # CRIA UMA THREAD MANUALMENTE para este cliente
            client_thread = threading.Thread(
                target=handle_client,
                args=(client_socket, addr),
                name=f"Client-{addr[0]}:{addr[1]}",
                daemon=True,  # Thread daemon — encerra com o processo
            )
            client_thread.start()
            
            log.info(f"Thread criada manualmente para {addr}: {client_thread.name}")
    
    except KeyboardInterrupt:
        log.info("Encerrando servidor…")
    finally:
        server_socket.close()


# ══════════════════════════════════════════════════════════════════════════════
# Ponto de Entrada
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("  Chat Distribuído — Servidor PRINCIPAL")
    log.info(f"  WebSocket : ws://localhost:{PRIMARY_WS_PORT}")
    log.info(f"  Interface : http://localhost:{PRIMARY_HTTP_PORT}")
    log.info("=" * 60)
    
    # ── Thread daemon para servidor HTTP ──────────────────────────────────────
    http_thread = threading.Thread(
        target=run_http_server,
        name="HTTP-Server",
        daemon=True,
    )
    http_thread.start()
    
    # ── Servidor WebSocket (main thread) ──────────────────────────────────────
    # Cada accept() → cria Thread manualmente
    run_websocket_server()
