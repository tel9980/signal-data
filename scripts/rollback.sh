#!/usr/bin/env bash
# 一键回退到上一版行情数据
#
# 用法：
#   bash scripts/rollback.sh            # 回到上一版
#   bash scripts/rollback.sh --dry-run  # 只看会改哪些文件
#
# 原理：每次发布前 pipeline 会把旧文件复制到 out/.prev/，
# 这里把它拷回去再提交一次（不 force、不改历史，随时可以再 rollback 回来）。
set -euo pipefail
cd "$(dirname "$0")/.."

DRY=0
[ "${1:-}" = "--dry-run" ] && DRY=1

[ -d out/.prev ] || { echo "[x] 没有 out/.prev，无从回退（新仓库或刚清过）"; exit 1; }
FILES=$(cd out/.prev && ls *.json.gz 2>/dev/null | sed "s/\.gz$//" || true)
[ -n "$FILES" ] || { echo "[x] out/.prev 是空的"; exit 1; }

echo "将回退这些文件："
for f in $FILES; do
  old=$(wc -c < "out/$f" 2>/dev/null || echo 0)
  new=$(gunzip -c "out/.prev/$f.gz" | wc -c)
  echo "  $f：当前 ${old}B → 上一版 ${new}B"
done

if [ "$DRY" = "1" ]; then echo "[*] --dry-run，未改动任何文件"; exit 0; fi

for f in $FILES; do gunzip -c "out/.prev/$f.gz" > "out/$f"; done

git add -A out
if git diff --cached --quiet; then echo "[*] 内容与当前一致，无需提交"; exit 0; fi
git -c user.name="actions-user" -c user.email="actions-user@users.noreply.github.com" \
    commit -q -m "chore: 回退行情数据到上一版（$(date -u +%Y-%m-%dT%H:%M:%SZ)）"
echo "[+] 已提交。推送：git push origin HEAD:main"
