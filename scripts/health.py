#!/usr/bin/env python3
"""线上体检：8 项 + 数据新鲜度，任何一项挂了就以退出码 1 结束，并自动开 issue。

为什么需要：数据是自动跑的，坏了没人看仓库日志。这个脚本每 30 分钟跑一次，
出问题会开一个带 health 标签的 issue，修好后下一次成功会自动关掉它。
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone

UA = "Mozilla/5.0 (compatible; signal-data-health/1.0)"
TZ_CN = timezone(timedelta(hours=8))

# (名字, URL, 必须包含的片段, 最少字节)
HTTP_CHECKS = [
    ("反代·站点首页", "https://ag.1211.cc.cd/", "异动榜", 20000),
    ("反代·异动榜页", "https://ag.1211.cc.cd/live.html", "异动榜", 5000),
    ("反代·首屏数据", "https://ag.1211.cc.cd/sig/live.json", '"px"', 200000),
    ("反代·订阅(CFnodes)", "https://ag.1211.cc.cd/sub/sub.txt", "dmxlc3M6", 10000),
    ("GitHub·站点首页", "https://tel9980.github.io/signal/", None, 20000),
    ("GitHub·行情数据", "https://raw.githubusercontent.com/tel9980/signal-data/main/out/px.json",
     '"px"', 50000),
]


def fetch(url: str) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return r.status, r.read()


def check_http() -> list[str]:
    bad = []
    for name, url, needle, min_bytes in HTTP_CHECKS:
        try:
            status, body = fetch(url)
            ok = status == 200 and len(body) >= min_bytes and (
                needle is None or needle.encode() in body)
            print(f"  {'✅' if ok else '❌'} {name} {status} {len(body)}B")
            if not ok:
                bad.append(f"{name}：状态 {status} / {len(body)}B / 缺少标记 {needle!r}")
        except Exception as exc:
            print(f"  ❌ {name} {type(exc).__name__}: {exc}")
            bad.append(f"{name}：{type(exc).__name__}: {exc}")
    return bad


def check_freshness() -> list[str]:
    """数据新鲜度：北京时间的交易日 16:00 之后，px 必须是当天的。"""
    try:
        _, body = fetch("https://raw.githubusercontent.com/tel9980/signal-data/main/out/px.json")
        px = json.loads(body.decode("utf-8"))
    except Exception as exc:
        return [f"新鲜度检查失败：{type(exc).__name__}: {exc}"]
    at = str(px.get("at", ""))
    try:
        stamp = datetime.strptime(at, "%Y-%m-%d %H:%M").replace(tzinfo=TZ_CN)
    except ValueError:
        print(f"  ❌ 数据时间戳异常：{at!r}")
        return [f"px.json 时间戳异常：{at!r}"]

    now = datetime.now(TZ_CN)
    age_h = (now - stamp).total_seconds() / 3600
    count = len(px.get("px") or {})
    print(f"  ℹ️  数据时间 {at}（{age_h:.1f} 小时前），{count} 只")

    bad = []
    if count < 4000:
        bad.append(f"股票数只有 {count} 只（正常约 5575）")
    # 工作日 16:00 后，数据必须是当天的；周末/节假日不判
    if now.weekday() < 5 and now.hour >= 16 and stamp.date() != now.date():
        bad.append(f"数据陈旧：现在是 {now:%m-%d %H:%M}，数据还是 {stamp:%m-%d %H:%M}")
    return bad


def gh_api(path: str, token: str, method: str = "GET", payload: dict | None = None):
    url = path if path.startswith("http") else f"https://api.github.com{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "signal-data-health",
    })
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read().decode("utf-8")
        return json.loads(raw) if raw.strip() else {}


def notify(bad: list[str]) -> None:
    token = os.environ.get("GITHUB_TOKEN", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not token or not repo:
        print("[health] 缺少 token/repo，跳过 issue")
        return
    try:
        open_issues = gh_api(f"/repos/{repo}/issues?state=open&labels=health&per_page=5", token)
        if open_issues:
            print(f"[health] 已有开放的 health issue #{open_issues[0]['number']}，不重复开")
            return
        title = f"数据体检失败 {datetime.now(TZ_CN):%Y-%m-%d %H:%M}"
        body = "以下检查未通过：\n\n" + "\n".join(f"- {b}" for b in bad)
        body += "\n\n（自动创建；下次体检全绿会自动关闭）\n"
        issue = gh_api(f"/repos/{repo}/issues", token, "POST",
                       {"title": title, "body": body, "labels": ["health"]})
        print(f"[health] 已开 issue #{issue.get('number')}")
    except Exception as exc:
        print(f"[health] 开 issue 失败：{type(exc).__name__}: {exc}")


def close_issue_if_green() -> None:
    token = os.environ.get("GITHUB_TOKEN", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not token or not repo:
        return
    try:
        for issue in gh_api(f"/repos/{repo}/issues?state=open&labels=health&per_page=5", token):
            gh_api(f"/repos/{repo}/issues/{issue['number']}", token, "PATCH",
                   {"state": "closed", "body": (issue.get("body") or "") + "\n\n✅ 已恢复正常（自动关闭）"})
            print(f"[health] 已关闭 issue #{issue['number']}")
    except Exception as exc:
        print(f"[health] 关闭 issue 失败：{type(exc).__name__}: {exc}")


def main() -> int:
    print(f"=== 体检 {datetime.now(TZ_CN):%Y-%m-%d %H:%M} (北京时间) ===")
    bad = check_http() + check_freshness()
    if bad:
        print(f"\n== 失败 {len(bad)} 项 ==")
        for b in bad:
            print(f"  - {b}")
        notify(bad)
        return 1
    print("\n== 全部通过 ==")
    close_issue_if_green()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
