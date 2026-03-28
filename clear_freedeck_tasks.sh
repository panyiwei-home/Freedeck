#!/usr/bin/env bash

set -euo pipefail

STATE_FILE="/home/deck/.local/share/Freedeck/tianyi/state.json"

if [[ ! -f "$STATE_FILE" ]]; then
  echo "未找到状态文件: $STATE_FILE"
  exit 1
fi

BACKUP_FILE="${STATE_FILE}.bak.$(date +%Y%m%d_%H%M%S)"
cp -f "$STATE_FILE" "$BACKUP_FILE"

python3 - "$STATE_FILE" <<'PY'
import json
import sys

state_file = sys.argv[1]

with open(state_file, "r", encoding="utf-8") as f:
    data = json.load(f)

if not isinstance(data, dict):
    raise SystemExit("state.json 格式异常，不是 JSON 对象")

data["tasks"] = []

with open(state_file, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
    f.write("\n")
PY

echo "已清空下载任务列表。"
echo "备份文件: $BACKUP_FILE"
echo "如果 Freedeck 当前正在打开，先退出再重新进入插件。"
