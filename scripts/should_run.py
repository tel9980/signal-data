#!/usr/bin/env python3
"""判断这一轮是否值得跑：最近 LOOKBACK_MIN 分钟内已有成功更新就跳过。

为什么需要：GitHub 免费 runner 的 cron 会延迟（实测 4–6 小时），也可能重复触发。
手里有 11:35 / 15:35 / 18:05 三个时刻，靠这个脚本做"去重 + 补跑"：
  - 11:35 正常跑 → 15:35 正常跑 → 18:05 发现 90 分钟内已成功过 → 跳过
  - 11:35 被延迟到 16:00 才跑 → 18:05 那次仍会跑（间隔 > 90 分钟），把当天补上

退出码：0 = 该跑，1 = 跳过。
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone

LOOKBACK_MIN = int(os.environ.get("LOOKBACK_MIN", "90"))
WORKFLOW = os.environ.get("WORKFLOW_FILE", "update.yml")


def api(url: str, token: str):
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "signal-data-guard",
    })
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def main() -> int:
    token = os.environ.get("GITHUB_TOKEN", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if not token or not repo:
        print("[guard] 缺少 token/repo，直接放行")
        return 0

    now = datetime.now(timezone.utc)
    url = (f"https://api.github.com/repos/{repo}/actions/workflows/{WORKFLOW}"
           f"/runs?branch=main&per_page=20")
    try:
        data = api(url, token)
    except Exception as exc:                       # 查询失败不该阻断更新
        print(f"[guard] 查询历史 run 失败（{type(exc).__name__}: {exc}），直接放行")
        return 0

    for run in data.get("workflow_runs", []):
        if str(run.get("id")) == str(run_id):
            continue
        if run.get("conclusion") != "success":
            continue
        try:
            created = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
        except Exception:
            continue
        gap = (now - created).total_seconds() / 60
        if gap < LOOKBACK_MIN:
            print(f"[guard] 跳过：{gap:.0f} 分钟前刚成功更新过"
                  f"（run {run['id']} @ {created:%Y-%m-%d %H:%M}Z）")
            return 1

    print(f"[guard] 放行：最近 {LOOKBACK_MIN} 分钟内没有成功更新")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
