#!/bin/bash
set -e

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "Запускаю Xvfb..."
Xvfb :99 -screen 0 1920x1080x24 &
sleep 2

log "Запускаю VNC сервер..."
x11vnc -display :99 -forever -shared -nopw -q &
sleep 1

log "Запускаю noVNC..."
websockify --web /usr/share/novnc 6080 localhost:5900 &
sleep 1

log "=== VNC готов ==="
log "VNC: порт 5900"
log "noVNC web: порт 6080"

log "Запускаю yandex_parser.py..."
exec python yandex_parser.py