#!/bin/zsh

set -u

PROJECT_DIR="/Users/inverse/Applications/信息大爆炸"
CODEX_BIN="/opt/homebrew/bin/codex"
SESSION_ID="019d2911-27b5-7652-81c4-950bd60f39e7"

if [[ ! -x "$CODEX_BIN" ]]; then
  echo "未找到 codex 可执行文件：$CODEX_BIN"
  echo "请先确认 Codex CLI 已安装。"
  read "dummy?按回车键退出..."
  exit 1
fi

if [[ ! -d "$PROJECT_DIR" ]]; then
  echo "项目目录不存在：$PROJECT_DIR"
  read "dummy?按回车键退出..."
  exit 1
fi

cd "$PROJECT_DIR" || exit 1
exec "$CODEX_BIN" resume "$SESSION_ID" -C "$PROJECT_DIR"
