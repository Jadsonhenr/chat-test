/**
 * chat.worker.js — Web Worker: Thread Dedicada à Recepção de Mensagens
 * =====================================================================
 * Web Worker = THREAD REAL do sistema operacional no navegador.
 * 
 * Este Worker executa em uma thread separada do thread principal e é
 * responsável exclusivamente por:
 *   1. Manter a conexão WebSocket com o servidor
 *   2. Receber todas as mensagens do servidor
 *   3. Repassar ao thread principal via postMessage()
 *
 * REQUISITO CUMPRIDO: "Thread dedicada à recepção de mensagens no cliente"
 */

'use strict';

let socket = null;
let serverUrl = '';
let intentionalClose = false;

// ── Recepção de comandos do thread principal ───────────────────────────────────

self.onmessage = function (event) {
    const { type, data } = event.data;

    switch (type) {
        case 'CONNECT':
            serverUrl = data.url;
            intentionalClose = false;
            connect(data.url);
            break;

        case 'JOIN':
            sendToServer({ type: 'join', username: data.username });
            break;

        case 'SEND':
            sendToServer({ type: 'message', msg: data.msg });
            break;

        case 'SEND_PRIVATE':
            sendToServer({ type: 'private_message', to: data.to, msg: data.msg });
            break;

        case 'PING':
            sendToServer({ type: 'ping' });
            break;

        case 'DISCONNECT':
            intentionalClose = true;
            if (socket) socket.close(1000, 'User left');
            break;

        default:
            console.warn('[Worker] Comando desconhecido:', type);
    }
};

// ── Gerenciamento da Conexão WebSocket ────────────────────────────────────────

function connect(url) {
    if (socket && socket.readyState !== WebSocket.CLOSED) {
        socket.close();
    }

    socket = new WebSocket(url);

    socket.onopen = function () {
        self.postMessage({ type: 'CONNECTED', url });
    };

    // ═══ ESTE É O CORAÇÃO DA THREAD DE RECEPÇÃO ═══
    // Este callback executa na thread do Worker e recebe TODAS as mensagens
    socket.onmessage = function (event) {
        try {
            const data = JSON.parse(event.data);
            self.postMessage({ type: 'MESSAGE', data });
        } catch (err) {
            console.error('[Worker] Erro ao parsear mensagem:', err);
        }
    };

    socket.onerror = function () {
        self.postMessage({ type: 'ERROR', msg: 'Erro na conexão WebSocket.' });
    };

    socket.onclose = function (event) {
        self.postMessage({
            type: 'DISCONNECTED',
            code: event.code,
            reason: event.reason,
            intentional: intentionalClose,
        });
    };
}

function sendToServer(payload) {
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(payload));
    } else {
        self.postMessage({
            type: 'ERROR',
            msg: 'Não conectado ao servidor.',
        });
    }
}
