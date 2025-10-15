#!/bin/bash
set -e

echo "==============================================="
echo "🚀 Instalador del entorno Docker - VeryLargeDatabases"
echo "==============================================="

# --- 1) Verificar privilegios ---
if [ "$EUID" -ne 0 ]; then
  echo "❌ Este script requiere privilegios de superusuario."
  echo "   Ejecútalo con: sudo ./setup_vldb.sh"
  exit 1
fi

# --- 2) Instalar dependencias básicas ---
echo "📦 Instalando dependencias base (curl, gnupg, ca-certificates)..."
apt-get update -qq
apt-get install -y ca-certificates curl gnupg lsb-release apt-transport-https

# --- 3) Agregar el repositorio oficial de Docker (si no existe) ---
if [ ! -f /etc/apt/keyrings/docker.gpg ]; then
  echo "🔑 Añadiendo clave GPG y repositorio oficial de Docker..."
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
    https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    > /etc/apt/sources.list.d/docker.list
fi

# --- 4) Instalar Docker Engine + Compose Plugin ---
echo "🐳 Instalando Docker Engine y Docker Compose v2..."
apt-get update -qq
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# --- 5) Habilitar e iniciar el servicio ---
echo "⚙️ Habilitando el servicio Docker..."
systemctl enable --now docker

# --- 6) Añadir usuario actual al grupo docker ---
CURRENT_USER=${SUDO_USER:-$USER}
if id -nG "$CURRENT_USER" | grep -qw docker; then
  echo "👤 El usuario '$CURRENT_USER' ya pertenece al grupo docker."
else
  echo "👤 Añadiendo '$CURRENT_USER' al grupo docker..."
  usermod -aG docker "$CURRENT_USER"
  echo "ℹ️ Debes cerrar sesión o ejecutar 'newgrp docker' para aplicar los cambios."
fi

# --- 7) Construir y levantar los contenedores ---
echo "🏗️ Construyendo e iniciando los contenedores..."
cd "$(dirname "$0")"  # ir a la carpeta del proyecto
docker compose build
docker compose up -d

# --- 8) Mostrar estado ---
echo "✅ Contenedores activos:"
docker ps

echo "==============================================="
echo "🎉 Entorno Docker configurado correctamente"
echo "-----------------------------------------------"
echo "Para usar la app:"
echo "   docker compose run --rm app python example.py"
echo "-----------------------------------------------"
echo "Si cambias el código Python, reconstruye con:"
echo "   docker compose build app"
echo "==============================================="
