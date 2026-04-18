#!/usr/bin/env bash
# start.sh — Inicia os servidores principal e backup em segundo plano
# Uso: bash start.sh
# Para encerrar: kill $(cat .pids)

echo "=== Chat Distribuído — FURG ==="

# Inicia servidor principal
python primary_server.py &
PID_PRIMARY=$!
echo "Servidor principal iniciado (PID $PID_PRIMARY)"

sleep 1

# Inicia servidor backup
python backup_server.py &
PID_BACKUP=$!
echo "Servidor backup    iniciado (PID $PID_BACKUP)"

# Salva PIDs para encerramento
echo "$PID_PRIMARY $PID_BACKUP" > .pids

echo ""
echo "Interface principal : http://localhost:8080"
echo "Interface backup    : http://localhost:8081"
echo ""
echo "Para encerrar: kill \$(cat .pids)"

wait
