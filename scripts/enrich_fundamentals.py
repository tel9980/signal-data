#!/usr/bin/env python3
"""可选增强：用 adata 生成 fundamentals.json（财务核心指标 / 板块 / 股本）。

设计原则（重要）：
  * 完全独立于主行情管线：本脚本失败、超时、没装 adata 都不影响 out/ 里的任何现有文件。
  * adata 不是 requirements.txt 的依赖；手动安装到隔离目录后设置 PYTHONPATH 才生效。
  * 网络（东方财富）经常 SSL EOF，所以每个接口单独 try，拿不到就跳过该字段。

用法：
  PYTHONPATH=/tmp/adata-lib python3 scripts/enrich_fundamentals.py --out-dir out --limit 60
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

TZ_CN = timezone(timedelta(hours=8))
CORE_FIELDS = {
    "eps": "每股收益", "roe": "净资产收益率", "net_profit": "净利润",
    "revenue": "营业收入", "gross_margin": "毛利率", "debt_ratio": "资产负债率",
}


def log(m: str) -> None:
    print(m, flush=True)


def pick_row(df, code: str) -> dict:
    """DataFrame → 最新一条 dict（只保留可 JSON 序列化的标量）。"""
    if df is None or not hasattr(df, "iterrows"):
        return {}
    rows = list(df.iterrows())
    if not rows:
        return {}
    _, row = rows[0]
    out = {}
    for k, v in row.to_dict().items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[str(k)] = _clean(v)
        elif hasattr(v, "item"):
            try:
                out[str(k)] = _clean(v.item())
            except Exception:
                pass
    return out


def _clean(v):
    """NaN / inf 不是合法 JSON，统一转成 None。"""
    if isinstance(v, float):
        if v != v or v in (float("inf"), float("-inf")):
            return None
    return v


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--limit", type=int, default=60, help="最多补充多少只（按成交额前 N 只）")
    ap.add_argument("--timeout", type=float, default=8)
    args = ap.parse_args()

    try:
        import adata
    except ImportError:
        log("[skip] 未安装 adata（本脚本是可选增强，主行情不受影响）")
        return 0

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    px_path = out_dir / "px.json"
    if not px_path.exists():
        log("[skip] 没有 px.json，无法决定补充哪些股票")
        return 0
    px = json.loads(px_path.read_text(encoding="utf-8"))
    # 按成交额（signals 里 mn 字段）排序不可靠，简单取代码顺序前 N
    codes = sorted((px.get("px") or {}).keys())[: args.limit]
    if not codes:
        log("[skip] px.json 为空")
        return 0

    result = {"updated": datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M"),
              "source": "adata (Apache-2.0, 个人研究)", "fields": CORE_FIELDS, "stocks": {}}
    log(f"[*] adata {getattr(adata,'__version__','?')}，补充 {len(codes)} 只")

    done = 0
    for code in codes:
        item: dict = {}
        try:
            r = adata.stock.info.get_stock_shares(code)
            d = pick_row(r, code)
            if d:
                shares = d.get("list_a_shares") or d.get("total_shares")
                if shares:
                    item["shares"] = int(float(shares))
                item["share_date"] = str(d.get("change_date") or "")
        except Exception as exc:
            pass
        try:
            r = adata.stock.finance.get_core_index(code)
            d = pick_row(r, code)
            if d:
                for k in ("eps", "roe", "net_profit", "revenue", "gross_margin", "debt_ratio"):
                    for key in (k, k.upper()):
                        if key in d and d[key] is not None:
                            item[k] = d[key]
                            break
        except Exception:
            pass
        if item:
            result["stocks"][code] = item
            done += 1
        if done >= args.limit:
            break

    target = out_dir / "fundamentals.json"
    target.write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    log(f"[+] {target.name}：{done} 只（{target.stat().st_size // 1024}KB）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
