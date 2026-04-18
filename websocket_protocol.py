"""
websocket_protocol.py — Implementação Manual do Protocolo WebSocket
====================================================================
Implementa o handshake e framing do protocolo WebSocket RFC 6455
sem usar bibliotecas de alto nível.
"""

import base64
import hashlib
import struct
import socket
from typing import Optional

# Constante mágica do protocolo WebSocket (RFC 6455)
WEBSOCKET_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# Opcodes do protocolo
OP_CONTINUATION = 0x0
OP_TEXT         = 0x1
OP_BINARY       = 0x2
OP_CLOSE        = 0x8
OP_PING         = 0x9
OP_PONG         = 0xA


def parse_http_request(data: bytes) -> dict[str, str]:
    """
    Parseia um request HTTP simples e retorna os headers.
    
    Exemplo:
        GET / HTTP/1.1
        Host: localhost:8765
        Upgrade: websocket
        ...
    """
    lines = data.decode('utf-8', errors='ignore').split('\r\n')
    headers = {}
    
    for line in lines[1:]:  # Pula a primeira linha (request line)
        if ': ' in line:
            key, value = line.split(': ', 1)
            headers[key] = value
    
    return headers


def create_handshake_response(sec_key: str) -> bytes:
    """
    Cria a resposta do handshake WebSocket.
    
    Protocolo:
    1. Concatena Sec-WebSocket-Key + MAGIC
    2. Calcula SHA-1 hash
    3. Codifica em base64
    4. Retorna HTTP 101 Switching Protocols
    """
    accept_key = base64.b64encode(
        hashlib.sha1((sec_key + WEBSOCKET_MAGIC).encode()).digest()
    ).decode()
    
    response = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept_key}\r\n"
        "\r\n"
    )
    
    return response.encode()


def websocket_handshake(sock: socket.socket) -> bool:
    """
    Executa o handshake WebSocket.
    Retorna True se bem-sucedido, False caso contrário.
    """
    try:
        # Recebe o request HTTP
        data = sock.recv(4096)
        if not data:
            return False
        
        headers = parse_http_request(data)
        
        # Valida que é um upgrade para WebSocket
        if headers.get('Upgrade', '').lower() != 'websocket':
            return False
        
        sec_key = headers.get('Sec-WebSocket-Key')
        if not sec_key:
            return False
        
        # Envia resposta do handshake
        response = create_handshake_response(sec_key)
        sock.sendall(response)
        
        return True
        
    except Exception:
        return False


def decode_frame(data: bytes) -> Optional[tuple[int, bytes]]:
    """
    Decodifica um frame WebSocket.
    
    Formato do frame (simplificado):
      0               1               2               3
      0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
     +-+-+-+-+-------+-+-------------+-------------------------------+
     |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
     |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
     |N|V|V|V|       |S|             |   (if payload len==126/127)   |
     | |1|2|3|       |K|             |                               |
     +-+-+-+-+-------+-+-------------+-------------------------------+
     |     Extended payload length continued, if payload len == 127  |
     +---------------------------------------------------------------+
     |                     Masking-key (32 bits)                     |
     +---------------------------------------------------------------+
     |                          Payload Data                         |
     +---------------------------------------------------------------+
    
    Retorna: (opcode, payload) ou None se inválido
    """
    if len(data) < 2:
        return None
    
    # Byte 0: FIN + opcode
    fin = (data[0] & 0b10000000) >> 7
    opcode = data[0] & 0b00001111
    
    # Byte 1: MASK + payload length
    mask_bit = (data[1] & 0b10000000) >> 7
    payload_len = data[1] & 0b01111111
    
    offset = 2
    
    # Payload length estendido
    if payload_len == 126:
        if len(data) < 4:
            return None
        payload_len = struct.unpack('>H', data[2:4])[0]
        offset = 4
    elif payload_len == 127:
        if len(data) < 10:
            return None
        payload_len = struct.unpack('>Q', data[2:10])[0]
        offset = 10
    
    # Masking key (4 bytes) — clientes DEVEM mascarar
    if mask_bit:
        if len(data) < offset + 4:
            return None
        masking_key = data[offset:offset + 4]
        offset += 4
    else:
        masking_key = None
    
    # Payload
    if len(data) < offset + payload_len:
        return None
    
    payload = bytearray(data[offset:offset + payload_len])
    
    # Desmascara o payload
    if masking_key:
        for i in range(len(payload)):
            payload[i] ^= masking_key[i % 4]
    
    return (opcode, bytes(payload))


def encode_frame(opcode: int, payload: bytes) -> bytes:
    """
    Codifica um frame WebSocket (servidor → cliente, SEM masking).
    """
    frame = bytearray()
    
    # Byte 0: FIN=1 + opcode
    frame.append(0b10000000 | opcode)
    
    # Byte 1: MASK=0 + payload length
    payload_len = len(payload)
    
    if payload_len < 126:
        frame.append(payload_len)
    elif payload_len < 65536:
        frame.append(126)
        frame.extend(struct.pack('>H', payload_len))
    else:
        frame.append(127)
        frame.extend(struct.pack('>Q', payload_len))
    
    # Payload (sem masking)
    frame.extend(payload)
    
    return bytes(frame)


def recv_frame(sock: socket.socket, timeout: float = 60.0) -> Optional[tuple[int, bytes]]:
    """
    Recebe e decodifica um frame WebSocket do socket.
    Retorna (opcode, payload) ou None em caso de erro/timeout.
    """
    sock.settimeout(timeout)
    
    try:
        # Lê os primeiros 2 bytes
        header = sock.recv(2)
        if len(header) < 2:
            return None
        
        # Determina o tamanho total do frame
        payload_len = header[1] & 0b01111111
        mask_bit = (header[1] & 0b10000000) >> 7
        
        extra_len_bytes = 0
        if payload_len == 126:
            extra_len_bytes = 2
        elif payload_len == 127:
            extra_len_bytes = 8
        
        # Lê o restante do header
        if extra_len_bytes > 0:
            extra = sock.recv(extra_len_bytes)
            header += extra
        
        # Lê masking key (4 bytes se presente)
        if mask_bit:
            mask = sock.recv(4)
            header += mask
        
        # Determina payload length real
        if payload_len < 126:
            actual_len = payload_len
        elif payload_len == 126:
            actual_len = struct.unpack('>H', header[2:4])[0]
        else:
            actual_len = struct.unpack('>Q', header[2:10])[0]
        
        # Lê payload
        payload_data = b''
        while len(payload_data) < actual_len:
            chunk = sock.recv(min(4096, actual_len - len(payload_data)))
            if not chunk:
                return None
            payload_data += chunk
        
        # Decodifica o frame completo
        full_frame = header + payload_data
        return decode_frame(full_frame)
        
    except socket.timeout:
        return None
    except Exception:
        return None


def send_frame(sock: socket.socket, opcode: int, payload: bytes) -> bool:
    """
    Envia um frame WebSocket.
    Retorna True se bem-sucedido, False caso contrário.
    """
    try:
        frame = encode_frame(opcode, payload)
        sock.sendall(frame)
        return True
    except Exception:
        return False


def send_text(sock: socket.socket, text: str) -> bool:
    """Envia uma mensagem de texto (opcode TEXT)."""
    return send_frame(sock, OP_TEXT, text.encode('utf-8'))


def send_close(sock: socket.socket, code: int = 1000) -> bool:
    """Envia frame de close (opcode CLOSE)."""
    payload = struct.pack('>H', code)
    return send_frame(sock, OP_CLOSE, payload)


def send_pong(sock: socket.socket, payload: bytes = b'') -> bool:
    """Envia frame de pong (opcode PONG)."""
    return send_frame(sock, OP_PONG, payload)
