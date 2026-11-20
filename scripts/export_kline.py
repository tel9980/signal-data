#!/usr/bin/env python3
"""可选增强：为台账里的股票导出日 K，供独立 K 线页 kline.html 使用。

设计原则：完全独立、失败不影响主管线。依赖已有 eltdx（主依赖），adata 不需要。
输出：out/kline/<code>.json = {"code","name","updated","bars":[[date,o,h,l,c,vol], ...]}
用法：python3 scripts/export_kline.py --out-dir out --count 250
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

TZ_CN = timezone(timedelta(hours=8))


def log(m: str) -> None:
    print(m, flush=True)


def market_prefix(code: str) -> str:
    if code.startswith(("60", "68", "9")):
        return "sh"
    if code.startswith(("4", "8")):
        return "bj"
    return "sz"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--count", type=int, default=250)
    args = ap.parse_args()

    ledger = Path(args.out_dir).parent / "ledger" / "entries.json"
    if not ledger.exists():
        ledger = Path("ledger/entries.json")
    if not ledger.exists():
        log("[skip] 没有 ledger/entries.json，无观察标的")
        return 0

    try:
        entries = json.loads(ledger.read_text(encoding="utf-8")).get("entries", [])
    except Exception as exc:
        log(f"[skip] 读台账失败：{exc}")
        return 0
    codes = [(str(e.get("code", "")), str(e.get("name", ""))) for e in entries if e.get("code")]
    if not codes:
        log("[skip] 台账为空")
        return 0

    try:
        from eltdx import TdxClient
    except ImportError:
        log("[skip] 缺少 eltdx")
        return 0

    client = TdxClient(timeout=10, probe_hosts=True)
    out_dir = Path(args.out_dir) / "kline"
    out_dir.mkdir(parents=True, exist_ok=True)
    updated = datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M")
    done = 0
    try:
        for code, name in codes:
            full = market_prefix(code) + code
            try:
                series = client.bars.get(full, period="day", count=args.count, adjust=None)
                bars = []
                for b in (series.bars or []):
                    d = b.time.strftime("%Y-%m-%d") if getattr(b, "time", None) else ""
                    bars.append([d, round(float(b.open), 2), round(float(b.high), 2),
                                 round(float(b.low), 2), round(float(b.close), 2),
                                 round(float(b.volume_lots or 0), 0)])
                if len(bars) < 2:
                    log(f"[!] {code} 只有 {len(bars)} 根 K，跳过")
                    continue
                payload = {"code": code, "name": name, "updated": updated, "bars": bars}
                (out_dir / f"{code}.json").write_text(
                    json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
                done += 1
                log(f"[+] {code} {name} {len(bars)} 根")
            except Exception as exc:
                log(f"[!] {code} 导出失败：{type(exc).__name__}: {exc}")
    finally:
        try:
            client.close()
        except Exception:
            pass

    log(f"[*] K 线导出完成：{done}/{len(codes)} 只 → {out_dir}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
