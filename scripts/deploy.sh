#!/usr/bin/env bash
set -euo pipefail

: "${DROPLET_HOST:?Set DROPLET_HOST}"
: "${DROPLET_USER:=levatetrader}"
: "${DROPLET_APP_DIR:=/opt/levate-trader}"

ssh "${DROPLET_USER}@${DROPLET_HOST}" "cd ${DROPLET_APP_DIR} && \
  git pull --ff-only && \
  ${DROPLET_APP_DIR}/venv/bin/pip install -r requirements.txt && \
  sudo systemctl restart levate-trader && \
  sudo systemctl status levate-trader --no-pager"
