"""
backup_server.py — Servidor Backup com Threading Manual
========================================================
Threading manual:
  • Uma thread por cliente (criada explicitamente com threading.Thread)
  • Uma thread dedicada ao monitor de heartbeat
  • Socket puro (socket.socket)
"""

import socket
import threading
import json
import time
import logging
import os
from http.server import HTTPServer, SimpleHTTPRequestHandler
import urllib.request
import urllib.error

from websocket_protocol import (
    websocket_handshake, recv_frame, send_text, send_close, send_pong,
    OP_TEXT, OP_CLOSE, OP_PING
)
from config import (
    PRIMARY_WS_PORT, PRIMARY_HTTP_PORT,
    BACKUP_WS_PORT, BACKUP_HTTP_PORT,
    HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, MAX_FAILURES,
    MAX_HISTORY, MAX_USERNAME_LEN, MAX_MSG_LEN
)

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BACKUP ] [%(threadName)-24s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backup")

# ── Estado Compartilhado ───────────────────────────────────────────────────────
clients: dict[socket.socket, str] = {}
clients_lock = threading.Lock()

message_history: list[dict] = []
history_lock = threading.Lock()

server_role: str = "backup"  # "backup" ou "primary"
primary_alive: bool = True
role_lock = threading.Lock()


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
    raw = json.dumps(payload)
    with clients_lock:
        targets = list(clients.keys())
    
    for sock in targets:
        if sock is exclude:
            continue
        try:
            send_text(sock, raw)
        except Exception:
            pass


def broadcast_user_list() -> None:
    with clients_lock:
        users = sorted(clients.values())
    broadcast({"type": "user_list", "users": users})


def get_current_role() -> str:
    with role_lock:
        return server_role


# ══════════════════════════════════════════════════════════════════════════════
# Monitor de Heartbeat — THREAD DEDICADA (criada manualmente)
# ══════════════════════════════════════════════════════════════════════════════

def heartbeat_monitor() -> None:
    """
    Thread dedicada que monitora o servidor principal.
    
    Comportamento:
      • A cada HEARTBEAT_INTERVAL segundos, faz GET em /health do primário
      • Se falhar MAX_FAILURES vezes consecutivas, promove o backup a primário
      • Se o primário voltar, notifica os clientes
    
    Esta thread é criada manualmente com:
      threading.Thread(target=heartbeat_monitor, daemon=True).start()
    """
    global server_role, primary_alive
    
    failures = 0
    url = f"http://localhost:{PRIMARY_HTTP_PORT}/health"
    
    log.info(
        f"Monitor de heartbeat iniciado — verificando {url} "
        f"a cada {HEARTBEAT_INTERVAL}s"
    )
    
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        
        try:
            # Tenta acessar /health do servidor principal
            req = urllib.request.Request(url, method='GET')
            with urllib.request.urlopen(req, timeout=HEARTBEAT_TIMEOUT) as resp:
                if resp.status == 200:
                    failures = 0
                    
                    with role_lock:
                        was_down = not primary_alive
                        primary_alive = True
                    
                    if was_down:
                        log.info("Servidor primário voltou ao ar.")
                        broadcast({
                            "type": "server_status",
                            "status": "primary_restored",
                            "primary_ws_port": PRIMARY_WS_PORT,
                            "msg": "Servidor principal restaurado.",
                        })
        
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
            failures += 1
            log.warning(
                f"Primário não respondeu — falha {failures}/{MAX_FAILURES}"
            )
            
            if failures >= MAX_FAILURES:
                with role_lock:
                    already_promoted = not primary_alive
                    if not already_promoted:
                        primary_alive = False
                        server_role = "primary"
                
                if not already_promoted:
                    log.critical(
                        "=" * 54 + "\n"
                        "  FAILOVER: Servidor primário falhou!\n"
                        f"  Backup promovido a primário (porta {BACKUP_WS_PORT})\n"
                        + "=" * 54
                    )
                    broadcast({
                        "type": "server_status",
                        "status": "backup_promoted",
                        "backup_ws_port": BACKUP_WS_PORT,
                        "msg": "Servidor principal indisponível. Você está no backup.",
                    })
                
                failures = 0  # Reseta para evitar promoções repetidas


# ══════════════════════════════════════════════════════════════════════════════
# Handler de Cliente — THREAD DEDICADA (criada manualmente)
# ══════════════════════════════════════════════════════════════════════════════

def handle_client(client_socket: socket.socket, addr: tuple) -> None:
    """
    Gerencia a conexão de um cliente WebSocket.
    Executado em uma thread criada manualmente.
    """
    tname = threading.current_thread().name
    log.info(f"Nova conexão de {addr}  ← thread: {tname}")
    
    username: str | None = None
    
    try:
        # Handshake WebSocket
        if not websocket_handshake(client_socket):
            log.warning(f"Handshake falhou para {addr}")
            return
        
        log.info(f"Handshake completo com {addr}")
        
        # Loop de recepção
        while True:
            frame = recv_frame(client_socket, timeout=120.0)
            
            if frame is None:
                break
            
            opcode, payload = frame
            
            if opcode == OP_CLOSE:
                send_close(client_socket)
                break
            
            elif opcode == OP_PING:
                send_pong(client_socket, payload)
                continue
            
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
                    
                    with clients_lock:
                        if name in clients.values():
                            send_text(client_socket, json.dumps({
                                "type": "error",
                                "msg": f'Nome "{name}" já está em uso.',
                            }))
                            continue
                        clients[client_socket] = name
                    
                    username = name
                    log.info(f'"{username}" entrou no backup.')
                    
                    send_text(client_socket, json.dumps({
                        "type": "server_info",
                        "role": get_current_role(),
                        "port": BACKUP_WS_PORT,
                    }))
                    
                    with history_lock:
                        hist = list(message_history)
                    send_text(client_socket, json.dumps({
                        "type": "history",
                        "messages": hist,
                    }))
                    
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
        with clients_lock:
            removed = clients.pop(client_socket, None)
        
        if removed:
            log.info(f'"{removed}" desconectou do backup.')
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
# Servidor HTTP
# ══════════════════════════════════════════════════════════════════════════════

class BackupHTTPHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        super().__init__(*args, directory=static_dir, **kwargs)
    
    def do_GET(self):
        if self.path == "/health":
            self._serve_health()
        elif self.path == "/status":
            self._serve_status()
        else:
            if self.path == "/":
                self.path = "/index.html"
            super().do_GET()
    
    def _serve_health(self):
        with clients_lock:
            user_count = len(clients)
        with role_lock:
            role = server_role
            p_alive = primary_alive
        
        body = json.dumps({
            "status": "ok",
            "role": role,
            "ws_port": BACKUP_WS_PORT,
            "primary_alive": p_alive,
            "users_online": user_count,
            "timestamp": time.time(),
        }).encode()
        
        self._json_response(body)
    
    def _serve_status(self):
        with role_lock:
            body = json.dumps({
                "role": server_role,
                "primary_alive": primary_alive,
                "backup_ws_port": BACKUP_WS_PORT,
            }).encode()
        self._json_response(body)
    
    def _json_response(self, body: bytes):
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
    httpd = HTTPServer(("0.0.0.0", BACKUP_HTTP_PORT), BackupHTTPHandler)
    log.info(f"Interface web (backup) em http://localhost:{BACKUP_HTTP_PORT}")
    httpd.serve_forever()


# ══════════════════════════════════════════════════════════════════════════════
# Servidor WebSocket — socket puro + threading manual
# ══════════════════════════════════════════════════════════════════════════════

def run_websocket_server() -> None:
    """
    Servidor WebSocket usando socket puro.
    Cada accept() → cria Thread manualmente.
    """
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("0.0.0.0", BACKUP_WS_PORT))
    server_socket.listen(5)
    
    log.info(f"Servidor WebSocket escutando em ws://localhost:{BACKUP_WS_PORT}")
    log.info("Aguardando conexões…  (Ctrl+C para encerrar)")
    
    try:
        while True:
            client_socket, addr = server_socket.accept()
            
            # CRIA THREAD MANUALMENTE
            client_thread = threading.Thread(
                target=handle_client,
                args=(client_socket, addr),
                name=f"Client-{addr[0]}:{addr[1]}",
                daemon=True,
            )
            client_thread.start()
            
            log.info(f"Thread criada manualmente para {addr}: {client_thread.name}")
    
    except KeyboardInterrupt:
        log.info("Encerrando servidor backup…")
    finally:
        server_socket.close()


# ══════════════════════════════════════════════════════════════════════════════
# Ponto de Entrada
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("  Chat Distribuído — Servidor BACKUP")
    log.info(f"  WebSocket : ws://localhost:{BACKUP_WS_PORT}")
    log.info(f"  Interface : http://localhost:{BACKUP_HTTP_PORT}")
    log.info(f"  Monitorando primário na porta {PRIMARY_HTTP_PORT}")
    log.info("=" * 60)
    
    # ── Thread: monitor de heartbeat (CRIADA MANUALMENTE) ─────────────────────
    monitor_thread = threading.Thread(
        target=heartbeat_monitor,
        name="HeartbeatMonitor",
        daemon=True,
    )
    monitor_thread.start()
    log.info(f"Thread de heartbeat criada: {monitor_thread.name}")
    
    # ── Thread: servidor HTTP (CRIADA MANUALMENTE) ────────────────────────────
    http_thread = threading.Thread(
        target=run_http_server,
        name="HTTP-Server",
        daemon=True,
    )
    http_thread.start()
    log.info(f"Thread HTTP criada: {http_thread.name}")
    
    # ── Servidor WebSocket (main thread) ──────────────────────────────────────
    run_websocket_server()
