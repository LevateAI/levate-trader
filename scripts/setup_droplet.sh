#!/usr/bin/env bash
set -euo pipefail

: "${REPO_URL:?Set REPO_URL to your GitHub repository URL}"
: "${APP_DIR:=/home/levateai/levate-trader}"
: "${APP_USER:=levateai}"

sudo apt-get update
sudo apt-get install -y software-properties-common git curl
sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install -y python3.11 python3.11-venv python3.11-dev build-essential

if ! id "${APP_USER}" >/dev/null 2>&1; then
  sudo useradd --system --create-home --shell /bin/bash "${APP_USER}"
fi

sudo mkdir -p "${APP_DIR}"
sudo chown "${APP_USER}:${APP_USER}" "${APP_DIR}"

if [ ! -d "${APP_DIR}/.git" ]; then
  sudo -u "${APP_USER}" git clone "${REPO_URL}" "${APP_DIR}"
fi

sudo -u "${APP_USER}" python3.11 -m venv "${APP_DIR}/.venv"
sudo -u "${APP_USER}" "${APP_DIR}/.venv/bin/pip" install --upgrade pip
sudo -u "${APP_USER}" "${APP_DIR}/.venv/bin/pip" install -r "${APP_DIR}/requirements.txt"

sudo cp "${APP_DIR}/systemd/levate-trader@.service" /etc/systemd/system/levate-trader@.service
sudo systemctl daemon-reload
sudo systemctl enable levate-trader@balanced

if [ ! -f "${APP_DIR}/envs/balanced.env" ]; then
  echo "Missing ${APP_DIR}/envs/balanced.env. Pull the tournament env templates before starting."
else
  echo "Fill Supabase credentials in ${APP_DIR}/envs/*.env, then start levate-trader@<account>."
fi
