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


def fetch(url: str, attempts: int = 3) -> tuple[int, bytes]:
    """带重试的取数。反代偶尔会瞬时 TLS EOF（Cloudflare 边缘抖动），
    单次失败就开 issue 会造成假警报，所以失败重试 3 次、间隔递进。"""
    import time
    last: Exception | None = None
    for i in range(attempts):
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"})
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                return r.status, r.read()
        except Exception as exc:
            last = exc
            if i + 1 < attempts:
                time.sleep(1.5 * (i + 1))
    raise last  # type: ignore[misc]


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


def in_trading() -> bool:
    now = datetime.now(TZ_CN)
    if now.weekday() >= 5:
        return False
    m = now.hour * 60 + now.minute
    return (575 <= m <= 695) or (785 <= m <= 905)   # 09:35-11:35 / 13:05-15:05


def check_intraday() -> list[str]:
    """盘中快照新鲜度：交易时段内 intraday/px.json 不应超过 40 分钟没更新。"""
    if not in_trading():
        print("  ℹ️  非交易时段，跳过盘中快照检查")
        return []
    try:
        _, body = fetch("https://raw.githubusercontent.com/tel9980/signal-data/intraday/out/px.json")
        px = json.loads(body.decode("utf-8"))
    except Exception as exc:
        return [f"盘中快照读取失败：{type(exc).__name__}: {exc}"]
    at = str(px.get("at", ""))
    try:
        stamp = datetime.strptime(at, "%Y-%m-%d %H:%M").replace(tzinfo=TZ_CN)
    except ValueError:
        return [f"盘中快照时间戳异常：{at!r}"]
    age_m = (datetime.now(TZ_CN) - stamp).total_seconds() / 60
    count = len(px.get("px") or {})
    print(f"  ℹ️  盘中快照 {at}（{age_m:.0f} 分钟前），{count} 只")
    bad = []
    if count < 4000:
        bad.append(f"盘中快照只有 {count} 只")
    if age_m > 40:
        bad.append(f"盘中快照 {age_m:.0f} 分钟没更新（交易时段应 ≤20 分钟）")
    return bad


def check_regression() -> list[str]:
    """数据不许往回退。

    留档 history.json 是只追加的：它的最后一行就是"跑到过的最新一次"。
    如果线上 px.json 的时间比它还早，说明数据被旧版本覆盖了（改代码时误把本地旧
    out/ 一起提交，就会造成这种"看起来正常、其实是昨天数据"的事故）。
    """
    try:
        _, body = fetch("https://raw.githubusercontent.com/tel9980/signal-data/main/out/px.json")
        at_px = str(json.loads(body.decode("utf-8")).get("at") or "")
        _, hbody = fetch("https://raw.githubusercontent.com/tel9980/signal-data/main/out/history.json")
        rows = json.loads(hbody.decode("utf-8")).get("rows") or []
    except Exception as exc:
        return [f"回退检查失败：{type(exc).__name__}: {exc}"]
    if not rows:
        return []
    at_hist = str(rows[-1].get("at") or "")
    if at_px and at_hist and at_px < at_hist:
        print(f"  ❌ 数据回退：px={at_px} 早于留档最新 {at_hist}")
        return [f"数据被回退到更早的一版：px.json 是 {at_px}，但留档里已到过 {at_hist}（有旧 out/ 被提交了？）"]
    print(f"  ✅ 没有回退（px {at_px} ≥ 留档 {at_hist}）")
    return []


def check_gate() -> list[str]:
    """闸门结论：stats.json 里 gate.ok=false 说明最近一轮被拦下了（数据没更新）。

    这是"数据没更新"背后最常见的原因，直接读出原因写进 issue，省得去翻 Actions 日志。
    """
    try:
        _, body = fetch("https://raw.githubusercontent.com/tel9980/signal-data/main/out/stats.json")
        st = json.loads(body.decode("utf-8"))
    except Exception as exc:
        return [f"闸门检查失败：{type(exc).__name__}: {exc}"]
    g = st.get("gate") or {}
    if g.get("ok") is False:
        print(f"  ❌ 最近一轮被闸门拦下：{g.get('reason')}")
        return [f"最近一轮数据被拦下，线上仍是旧数据：{g.get('reason')}"]
    print(f"  ✅ 闸门通过（at {st.get('at')}，{st.get('quotes_ok')} 只）")
    return []


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


def kick_update(reason: str, workflow: str = "update.yml", quiet_window: int = 1200) -> None:
    """自愈：数据明显落后且最近没有跑过，就手动触发一次对应 workflow。"""
    token = os.environ.get("GITHUB_TOKEN", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not token or not repo:
        print("[heal] 缺少 token/repo，跳过自愈触发")
        return
    try:
        runs = gh_api(f"/repos/{repo}/actions/workflows/{workflow}/runs?per_page=5", token)
        now = datetime.now(timezone.utc)
        for run in runs.get("workflow_runs", []):
            started = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
            if (now - started).total_seconds() < quiet_window:
                print(f"[heal] {quiet_window // 60} 分钟内刚跑过 {run['id']}（{run['status']}），不重复触发")
                return
        gh_api(f"/repos/{repo}/actions/workflows/{workflow}/dispatches", token, "POST",
               {"ref": "main"})
        print(f"[heal] 已重新触发 {workflow} —— 原因：{reason}")
    except Exception as exc:
        print(f"[heal] 触发失败：{type(exc).__name__}: {exc}")


def main() -> int:
    print(f"=== 体检 {datetime.now(TZ_CN):%Y-%m-%d %H:%M} (北京时间) ===")
    bad = check_http() + check_intraday() + check_freshness() + check_gate() + check_regression()
    if bad:
        print(f"\n== 失败 {len(bad)} 项 ==")
        for b in bad:
            print(f"  - {b}")
        notify(bad)
        if any("盘中快照" in b for b in bad):
            kick_update("；".join(b for b in bad if "盘中快照" in b), "intraday.yml", 600)
        if any("数据陈旧" in b or "数据时间戳异常" in b for b in bad):
            kick_update("；".join(b for b in bad if "陈旧" in b or "时间戳" in b))
        return 1
    print("\n== 全部通过 ==")
    close_issue_if_green()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
