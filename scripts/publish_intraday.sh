#!/usr/bin/env bash
# 把盘中快照发布到独立的 intraday 分支（孤儿提交 + force-push，永远只有一条提交）。
# 只放 px / indices / stats 三个小文件，不进 main，不污染历史，也不会让仓库膨胀。
set -euo pipefail

git config user.name "actions-user"
git config user.email "actions@github.com"

git checkout -q --orphan intraday-tmp
git rm -rf --cached -q . || true

rm -rf out
mkdir -p out
cp intraday/px.json intraday/indices.json intraday/stats.json out/

git add -f out
git commit -q -m "chore: 盘中行情 $(date -u +%FT%TZ)"
git push --force origin HEAD:refs/heads/intraday
echo "[+] 已发布 intraday 分支（$(date -u +%TZ)）"
