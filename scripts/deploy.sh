#!/usr/bin/env bash
set -euo pipefail

: "${DROPLET_HOST:?Set DROPLET_HOST}"
: "${DROPLET_USER:=levateai}"
: "${DROPLET_APP_DIR:=/home/levateai/levate-trader}"
: "${BOT_ACCOUNTS:=conservative balanced aggressive scalp_only swing_only chaos}"

ssh "${DROPLET_USER}@${DROPLET_HOST}" "cd ${DROPLET_APP_DIR} && \
  git pull --ff-only && \
  ${DROPLET_APP_DIR}/.venv/bin/pip install -r requirements.txt && \
  sudo cp ${DROPLET_APP_DIR}/systemd/levate-trader@.service /etc/systemd/system/levate-trader@.service && \
  sudo systemctl daemon-reload && \
  for bot in ${BOT_ACCOUNTS}; do sudo systemctl restart levate-trader@\${bot}; done && \
  for bot in ${BOT_ACCOUNTS}; do sudo systemctl status levate-trader@\${bot} --no-pager; done"
