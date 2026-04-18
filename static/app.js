/**
 * app.js — Thread Principal: Lógica de UI
 * ========================================
 * Responsabilidades:
 *   • Criar e controlar o Web Worker (chat.worker.js) — THREAD DE RECEPÇÃO
 *   • Gerenciar a interface de login e a sala de chat
 *   • Processar mensagens recebidas do Worker e atualizar o DOM
 */

'use strict';

const SERVERS = {
    primary: { ws: 'ws://localhost:8765', http: 'http://localhost:8080', label: 'Servidor Principal' },
    backup:  { ws: 'ws://localhost:8766', http: 'http://localhost:8081', label: 'Servidor Backup' },
};

const state = {
    worker: null, username: '', currentServer: 'primary', connected: false,
    privateTarget: null, pingInterval: null, reconnectTimer: null, reconnectAttempts: 0,
};

const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_DELAY_MS = 3000;
const PING_INTERVAL_MS = 25000;

const $ = id => document.getElementById(id);
const dom = {
    loginScreen: $('login-screen'), chatScreen: $('chat-screen'), usernameInput: $('username-input'),
    loginBtn: $('login-btn'), loginError: $('login-error'), serverSelect: $('server-select'),
    messages: $('messages'), msgInput: $('msg-input'), sendBtn: $('send-btn'),
    userList: $('user-list'), userCount: $('user-count'), usernameDisplay: $('username-display'),
    serverStatus: $('server-status'), privateBadge: $('private-mode'), privateTarget: $('private-target'),
    failoverNotif: $('failover-notification'),
};

function initWorker() {
    if (state.worker) state.worker.terminate();
    state.worker = new Worker('/chat.worker.js');
    state.worker.onmessage = handleWorkerMessage;
    state.worker.onerror = (err) => {
        console.error('[Worker Error]', err);
        showLoginError('Erro interno no worker de comunicação.');
    };
}

function handleWorkerMessage(event) {
    const { type, data } = event.data;
    switch (type) {
        case 'CONNECTED':
            state.worker.postMessage({ type: 'JOIN', data: { username: state.username } });
            break;
        case 'DISCONNECTED':
            handleDisconnection(event.data);
            break;
        case 'ERROR':
            console.warn('[WS Error]', data.msg);
            break;
        case 'MESSAGE':
            routeServerMessage(data);
            break;
    }
}

function routeServerMessage(msg) {
    switch (msg.type) {
        case 'server_info': onServerInfo(msg); break;
        case 'history': msg.messages.forEach(renderMessage); scrollToBottom(); break;
        case 'chat': renderMessage(msg); scrollToBottom(); break;
        case 'system': renderMessage(msg); scrollToBottom(); break;
        case 'private': renderMessage(msg); scrollToBottom(); break;
        case 'user_list': renderUserList(msg.users); break;
        case 'error': handleServerError(msg.msg); break;
        case 'server_status': handleServerStatus(msg); break;
        case 'pong': break;
        default: console.warn('[App] Tipo desconhecido:', msg.type);
    }
}

function joinChat() {
    const name = dom.usernameInput.value.trim();
    if (!name || name.length > 20) {
        showLoginError('Digite um nome de usuário (1-20 caracteres).');
        return;
    }
    state.username = name;
    state.currentServer = dom.serverSelect.value;
    hideLoginError();
    dom.loginBtn.disabled = true;
    dom.loginBtn.textContent = 'Conectando…';
    connectToServer(state.currentServer);
}

function connectToServer(serverKey) {
    state.currentServer = serverKey;
    initWorker();
    state.worker.postMessage({ type: 'CONNECT', data: { url: SERVERS[serverKey].ws } });
}

function onServerInfo(msg) {
    state.connected = true;
    state.reconnectAttempts = 0;
    dom.loginScreen.classList.add('hidden');
    dom.chatScreen.classList.remove('hidden');
    dom.usernameDisplay.textContent = state.username;
    dom.loginBtn.disabled = false;
    dom.loginBtn.textContent = 'Entrar no Chat';
    updateServerStatus(msg.role);
    clearInterval(state.pingInterval);
    state.pingInterval = setInterval(() => {
        if (state.connected) state.worker.postMessage({ type: 'PING' });
    }, PING_INTERVAL_MS);
    dom.msgInput.focus();
}

function sendMessage() {
    const text = dom.msgInput.value.trim();
    if (!text || !state.connected) return;
    if (state.privateTarget) {
        state.worker.postMessage({ type: 'SEND_PRIVATE', data: { to: state.privateTarget, msg: text } });
    } else {
        state.worker.postMessage({ type: 'SEND', data: { msg: text } });
    }
    dom.msgInput.value = '';
    dom.msgInput.focus();
}

function startPrivateMessage(targetName) {
    if (targetName === state.username) return;
    state.privateTarget = targetName;
    dom.privateTarget.textContent = `Privado → ${targetName}`;
    dom.privateBadge.classList.remove('hidden');
    dom.msgInput.placeholder = `Mensagem privada para ${targetName}…`;
    dom.msgInput.focus();
}

function cancelPrivate() {
    state.privateTarget = null;
    dom.privateBadge.classList.add('hidden');
    dom.msgInput.placeholder = 'Digite uma mensagem…';
    dom.msgInput.focus();
}

function renderMessage(msg) {
    const div = document.createElement('div');
    switch (msg.type) {
        case 'system':
            div.className = 'msg msg-system';
            div.innerHTML = `<span class="msg-text">${escapeHtml(msg.text)}</span>`;
            break;
        case 'chat': {
            const isMine = msg.username === state.username;
            div.className = `msg msg-chat ${isMine ? 'msg-mine' : 'msg-other'}`;
            div.innerHTML = `<div class="msg-header"><span class="msg-author">${escapeHtml(msg.username)}</span><span class="msg-time">${msg.time}</span></div><div class="msg-bubble">${escapeHtml(msg.text)}</div>`;
            break;
        }
        case 'private': {
            const isSender = msg.from === state.username;
            div.className = `msg msg-private ${isSender ? 'msg-mine' : 'msg-other'}`;
            const label = isSender ? `Privado → ${escapeHtml(msg.to)}` : `🔒 Privado de ${escapeHtml(msg.from)}`;
            div.innerHTML = `<div class="msg-header"><span class="msg-author private-label">${label}</span><span class="msg-time">${msg.time}</span></div><div class="msg-bubble private-bubble">${escapeHtml(msg.text)}</div>`;
            break;
        }
        default: return;
    }
    dom.messages.appendChild(div);
}

function renderUserList(users) {
    dom.userCount.textContent = users.length;
    dom.userList.innerHTML = '';
    users.forEach(name => {
        const li = document.createElement('li');
        li.className = `user-item ${name === state.username ? 'user-self' : ''}`;
        li.innerHTML = `<span class="user-dot"></span><span class="user-name">${escapeHtml(name)}</span>${name !== state.username ? `<button class="pm-btn" onclick="startPrivateMessage('${escapeHtml(name)}')" title="Mensagem privada">✉</button>` : '<span class="user-you">(você)</span>'}`;
        dom.userList.appendChild(li);
    });
}

function handleDisconnection({ intentional }) {
    state.connected = false;
    clearInterval(state.pingInterval);
    if (intentional) { showLoginScreen(); return; }
    if (state.reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
        state.reconnectAttempts++;
        const nextServer = state.currentServer === 'primary' ? 'backup' : 'primary';
        showFailoverNotification(`Conexão perdida. Tentando ${SERVERS[nextServer].label} (tentativa ${state.reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})…`);
        state.reconnectTimer = setTimeout(() => {
            connectToServer(nextServer);
            state.worker.onmessage = (e) => {
                if (e.data.type === 'CONNECTED') {
                    state.worker.postMessage({ type: 'JOIN', data: { username: state.username } });
                    state.worker.onmessage = handleWorkerMessage;
                    hideFailoverNotification();
                }
                handleWorkerMessage(e);
            };
        }, RECONNECT_DELAY_MS);
    } else {
        showFailoverNotification('Não foi possível reconectar. Recarregue a página.');
    }
}

function handleServerStatus(msg) {
    if (msg.status === 'backup_promoted') {
        updateServerStatus('primary');
        showFailoverNotification(msg.msg);
        setTimeout(hideFailoverNotification, 6000);
    } else if (msg.status === 'primary_restored') {
        showFailoverNotification(msg.msg);
        setTimeout(hideFailoverNotification, 6000);
    }
}

function handleServerError(errorMsg) {
    if (!dom.loginScreen.classList.contains('hidden')) {
        showLoginError(errorMsg);
        dom.loginBtn.disabled = false;
        dom.loginBtn.textContent = 'Entrar no Chat';
    } else {
        appendSystemAlert(errorMsg);
    }
}

function showLoginScreen() {
    dom.chatScreen.classList.add('hidden');
    dom.loginScreen.classList.remove('hidden');
    dom.messages.innerHTML = '';
    dom.userList.innerHTML = '';
    dom.usernameInput.value = '';
    dom.msgInput.value = '';
    cancelPrivate();
    state.username = '';
    state.connected = false;
}

function leaveChat() {
    clearInterval(state.pingInterval);
    clearTimeout(state.reconnectTimer);
    if (state.worker) state.worker.postMessage({ type: 'DISCONNECT' });
    showLoginScreen();
}

function updateServerStatus(role) {
    const isPrimary = role === 'primary';
    dom.serverStatus.className = `server-status ${isPrimary ? 'status-primary' : 'status-backup'}`;
    dom.serverStatus.textContent = `● ${isPrimary ? 'Servidor Principal' : 'Servidor Backup'}`;
}

function showFailoverNotification(text) {
    dom.failoverNotif.querySelector('span').textContent = text;
    dom.failoverNotif.classList.remove('hidden');
}

function hideFailoverNotification() { dom.failoverNotif.classList.add('hidden'); }
function showLoginError(msg) { dom.loginError.textContent = msg; dom.loginError.classList.remove('hidden'); }
function hideLoginError() { dom.loginError.classList.add('hidden'); }
function appendSystemAlert(text) {
    const div = document.createElement('div');
    div.className = 'msg msg-system msg-alert';
    div.textContent = text;
    dom.messages.appendChild(div);
    scrollToBottom();
}
function scrollToBottom() { dom.messages.scrollTop = dom.messages.scrollHeight; }
function escapeHtml(text) { return String(text).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }

dom.usernameInput.addEventListener('keydown', e => { if (e.key === 'Enter') joinChat(); });
dom.msgInput.addEventListener('keydown', e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); } });

window.joinChat = joinChat;
window.leaveChat = leaveChat;
window.sendMessage = sendMessage;
window.startPrivateMessage = startPrivateMessage;
window.cancelPrivate = cancelPrivate;
