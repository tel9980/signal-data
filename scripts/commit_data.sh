#!/usr/bin/env bash
# 把 out/ 的最新产出滚动提交到 main。
#   1. 先跟上远端最新提交，避免 non-fast-forward
#   2. 内容没变化就不提交
#   3. tip 已经是滚动提交就 --amend 覆盖，历史不增长
set -euo pipefail

git config user.name "actions-user"
git config user.email "actions@github.com"

# 闸门兜底校验（pipeline 正常时已 exit 3，这里再确认一次）
python3 -c "import json;d=json.load(open('out/stats.json'));assert d['gate']['ok'], d['gate']"

git fetch origin main -q
git add -A out

if git diff --cached origin/main --quiet -- out; then
  echo "输出无变化，跳过提交"
  exit 0
fi

# 对齐远端：保留暂存区内容，只把 HEAD 挪到 origin/main
git reset --soft origin/main

MSG="chore: 行情数据更新 $(date -u +%FT%TZ)"
LAST=$(git log -1 --pretty=%s)
if [ "${LAST#chore: 行情数据更新}" != "$LAST" ]; then
  git commit -q --amend -m "$MSG"     # 上一次就是滚动提交 → 原地覆盖
else
  git commit -q -m "$MSG"
fi

git push --force origin HEAD:main
