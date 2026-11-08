#!/usr/bin/env python3
"""signal-data · 用 eltdx（通达信协议）生成 signal 前端需要的行情类 JSON。

产出（默认写入 out/）：
  px.json       全市场 A 股现价 + 当日涨跌幅，schema 与 signal/data/px.json 完全一致
  indices.json  四大指数（上证 / 创业板 / 科创50 / 北证50）涨跌幅 + 距近一年高点
  stats.json    本次抓取的自检信息

用法：
  python pipeline.py                 # 抓全市场
  python pipeline.py --limit 300     # 只抓前 300 只，用于联调
  python pipeline.py --no-indices    # 只出 px.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from pathlib import Path

CHUNK = 80                    # 通达信单次快照上限，80 只/次
MARKETS = ("sz", "sh", "bj")
INDICES = (
    ("上证指数", "sh000001"),
    ("创业板指", "sz399006"),
    ("科创 50", "sh000688"),
    ("北证 50", "bj899050"),
)
TZ_CN = timezone(timedelta(hours=8))


def log(msg: str) -> None:
    print(msg, flush=True)


def fetch_universe(client, limit: int) -> list[str]:
    """取全市场 A 股代码（按 sz / sh / bj 三个市场）。"""
    codes: list[str] = []
    for market in MARKETS:
        try:
            rows = client.codes.a_shares(market)
            got = [s.code for s in rows]
            log(f"[*] {market} 市场 {len(got)} 只")
            codes.extend(got)
        except Exception as exc:              # 单个市场失败不影响整体
            log(f"[!] {market} 市场取列表失败：{type(exc).__name__}: {exc}")
    if limit:
        codes = codes[:limit]
    return codes


def fetch_quotes(client, codes: list[str], workers: int = 8) -> dict[str, list[float]]:
    """并发抓快照，返回 {code: [last, chg_pct]}；停牌/无成交（价格为 0）的直接丢弃。"""
    chunks = [codes[i:i + CHUNK] for i in range(0, len(codes), CHUNK)]
    out: dict[str, list[float]] = {}
    zero = 0
    lock = __import__("threading").Lock()
    done = 0

    def work(chunk: list[str]) -> None:
        nonlocal zero, done
        try:
            rows = client.quotes.get_snapshots(chunk, on_missing="skip")
        except Exception as exc:
            log(f"[!] 快照失败（{len(chunk)} 只）：{type(exc).__name__}: {exc}")
            return
        for r in rows:
            last = float(r.last_price or 0)
            pre = float(r.pre_close_price or 0)
            if last <= 0 or pre <= 0:
                with lock:
                    zero += 1
                continue
            chg = round((last - pre) / pre * 100, 2)
            out[r.code] = [round(last, 2), chg]
        with lock:
            done += 1

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(work, chunks))

    log(f"[*] 快照完成：{len(chunks)} 批 → {len(out)} 只有效，丢弃停牌/零价 {zero} 只")
    return out


def fetch_indices(client) -> list[dict]:
    """指数：涨跌幅 + 距近一年高点。近一年高点用日 K 的 250 根 high 计算。"""
    rows: list[dict] = []
    for name, code in INDICES:
        item = {"name": name, "code": code, "last": None, "chg": None, "from_high": None}
        try:
            q = client.quotes.get_snapshots([code], on_missing="skip")
            if q:
                last = float(q[0].last_price or 0)
                pre = float(q[0].pre_close_price or 0)
                if last > 0:
                    item["last"] = round(last, 2)
                    if pre > 0:
                        item["chg"] = round((last - pre) / pre * 100, 2)
        except Exception as exc:
            log(f"[!] {name} 快照失败：{type(exc).__name__}: {exc}")
        try:
            series = client.bars.get(code, period="day", count=250, adjust=None)
            highs = [float(b.high) for b in series.bars if b.high]
            if highs and item["last"]:
                item["from_high"] = round((item["last"] / max(highs) - 1) * 100, 1)
        except Exception as exc:
            log(f"[!] {name} 日K失败：{type(exc).__name__}: {exc}")
        rows.append(item)
        log(f"[*] {name}: {item}")
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="用 eltdx 生成 signal 行情 JSON")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--limit", type=int, default=0, help="只抓前 N 只（0=全部）")
    ap.add_argument("--no-indices", action="store_true", help="跳过指数")
    ap.add_argument("--timeout", type=float, default=8.0)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()
    now_cn = datetime.now(TZ_CN)

    try:
        import eltdx
        from eltdx import TdxClient
    except ImportError:
        log("[x] 缺少依赖：pip install eltdx")
        return 2

    try:
        from importlib.metadata import version as _pkg_version
        version = _pkg_version("eltdx")
    except Exception:
        version = getattr(eltdx, "__version__", "?")
    log(f"=== signal-data {now_cn:%Y-%m-%d %H:%M} (eltdx {version}) ===")

    client = TdxClient(timeout=args.timeout, probe_hosts=True, server_count=4)
    stats = {"generated_at_utc": now_cn.astimezone(timezone.utc).isoformat(timespec="seconds"),
             "eltdx_version": str(version)}
    try:
        codes = fetch_universe(client, args.limit)
        if not codes:
            log("[x] 没有取到股票列表，退出")
            return 1
        stats["universe"] = len(codes)

        px = fetch_quotes(client, codes)
        if px:
            payload = {"at": now_cn.strftime("%Y-%m-%d %H:%M"), "px": px}
            (out_dir / "px.json").write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
            log(f"[+] px.json：{len(px)} 只")
        else:
            log("[!] px 为空，不写文件")
        stats["quotes_ok"] = len(px)

        if not args.no_indices:
            items = fetch_indices(client)
            ok = [i for i in items if i["chg"] is not None]
            if ok:
                (out_dir / "indices.json").write_text(
                    json.dumps({"updated": now_cn.strftime("%Y-%m-%d %H:%M"), "items": items},
                               ensure_ascii=False, indent=1), encoding="utf-8")
                log(f"[+] indices.json：{len(ok)}/{len(items)} 个指数")
            stats["indices_ok"] = len(ok)
    finally:
        try:
            client.close()
        except Exception:
            pass

    stats["duration_s"] = round(time.time() - started, 1)
    (out_dir / "stats.json").write_text(json.dumps(stats, ensure_ascii=False, indent=1), encoding="utf-8")
    log(f"[*] 完成，耗时 {stats['duration_s']}s → {out_dir}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
