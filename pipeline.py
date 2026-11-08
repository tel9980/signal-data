#!/usr/bin/env python3
"""signal-data · 用 eltdx（通达信协议）生成 signal 前端需要的行情类 JSON。

产出（默认写入 out/）：
  px.json       全市场 A 股现价 + 当日涨跌幅，schema 与 signal/data/px.json 完全一致
  indices.json  四大指数（上证 / 创业板 / 科创50 / 北证50）涨跌幅 + 距近一年高点
  signals.json  逐股行情派生指标（涨跌幅 / 5·20·60 日涨幅 / 距 250 日高·低点 / 量比 / 换手 / 主力净流入）
  momentum.json 由 signals.json 派生的「异动榜」分组（季末拉升、放量上攻、新高、主力大买）
  stats.json    本次抓取的自检信息

用法：
  python pipeline.py                  # 抓全市场（含派生指标）
  python pipeline.py --limit 300      # 只抓前 300 只，用于联调
  python pipeline.py --no-indices     # 跳过指数
  python pipeline.py --no-signals     # 只出 px.json / indices.json（最快）
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from pathlib import Path

CHUNK = 80                    # 通达信单次快照上限，80 只/次
BAR_CHUNK = 300               # 日 K 每批代码数（一批＝一个请求；19 批并发跑完）
FLOW_CHUNK = 300              # 资金流每批代码数（一批＝一个请求，比库默认的 75 少 4 倍往返）
MARKETS = ("sz", "sh", "bj")
INDICES = (
    ("上证指数", "sh000001"),
    ("创业板指", "sz399006"),
    ("科创 50", "sh000688"),
    ("北证 50", "bj899050"),
)
TZ_CN = timezone(timedelta(hours=8))

# 异动榜规则：与 signal 的「季末拉升」口径保持一致，其余为补充维度
RULES = {
    "季末拉升": "近 20 个交易日涨幅超过 30%（与 signal 同口径）",
    "放量上攻": "当日量比 ≥ 2 且涨幅 ≥ 3%",
    "创一年新高": "收盘价站上近 250 日最高价的 99.5%",
    "主力大买": "当日主力净流入 ≥ 5000 万元",
}


def log(msg: str) -> None:
    print(msg, flush=True)


def _parallel(fn, chunks, workers, label=""):
    """并发跑 fn(chunk)，结果按顺序收集；单批异常只记日志，不中断整体。"""
    out = []
    lock = threading.Lock()

    def work(chunk):
        try:
            res = fn(chunk)
        except Exception as exc:                       # 单批失败不影响整体
            log(f"[!] {label}失败（{len(chunk)} 只）：{type(exc).__name__}: {exc}")
            return
        with lock:
            out.append(res)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(work, chunks))
    return out


def fetch_universe(client, limit: int) -> list[str]:
    """取全市场 A 股代码，统一成 sz000001 / sh600000 / bj430047 这种带前缀的形式。"""
    codes: list[str] = []
    for market in MARKETS:
        try:
            rows = client.codes.a_shares(market)
            got = [f"{market}{s.code}" for s in rows]
            log(f"[*] {market} 市场 {len(got)} 只")
            codes.extend(got)
        except Exception as exc:              # 单个市场失败不影响整体
            log(f"[!] {market} 市场取列表失败：{type(exc).__name__}: {exc}")
    if limit:
        codes = codes[:limit]
    return codes


def fetch_names(client) -> dict[str, str]:
    """代码 -> 名称（通达信证券列表，含 ST 标记）。"""
    try:
        rows = client.helpers.latest_stock_list()
        return {f"{r.exchange}{r.code}": (r.name or "") for r in rows}
    except Exception as exc:
        log(f"[!] 取名称失败：{type(exc).__name__}: {exc}")
        return {}


def fetch_quotes(client, codes: list[str], workers: int = 10) -> dict[str, dict]:
    """并发抓快照，返回 {code: {last, chg, hand, amount, high, low, open}}；
    停牌/无成交（价格为 0）的直接丢弃。"""
    chunks = [codes[i:i + CHUNK] for i in range(0, len(codes), CHUNK)]
    out: dict[str, dict] = {}
    zero = 0
    lock = threading.Lock()

    def work(chunk: list[str]) -> None:
        nonlocal zero
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
            with lock:
                out[getattr(r, "full_code", None) or r.code] = {
                    "last": round(last, 2),
                    "chg": round((last - pre) / pre * 100, 2),
                    "hand": float(r.total_hand or 0),
                    "amount": float(r.amount or 0),
                    "high": float(r.high_price or 0),
                    "low": float(r.low_price or 0),
                    "open": float(r.open_price or 0),
                }

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


# ---------------------------------------------------------------- 派生指标 ----

def fetch_daily(client, codes: list[str], count: int = 250, workers: int = 8) -> dict[str, dict]:
    """日 K → 各周期涨幅、距 250 日高/低点、量比。返回 {code: {...}}。

    口径：
      d5/d20/d60  收盘价相对 5/20/60 个交易日前收盘价的涨跌幅（%）
      hi250       收盘价距近 250 日最高价的百分比（负值＝在高点下方）
      lo250       收盘价距近 250 日最低价的百分比（正值＝在低点上方）
      vr          量比（当日成交量 / 前 5 个交易日均量）
    """
    chunks = [codes[i:i + BAR_CHUNK] for i in range(0, len(codes), BAR_CHUNK)]

    def work(chunk: list[str]) -> dict[str, dict]:
        got = client.bars.get(chunk, period="day", count=count, adjust=None, batch_size=len(chunk))
        # 传 list 时返回的是 {code: KlineSeries} 的 dict，不是 list
        items = got.items() if isinstance(got, dict) else [(s.code, s) for s in got]
        res: dict[str, dict] = {}
        for code, series in items:
            bars = [b for b in (series.bars or ()) if b.close]
            if len(bars) < 6:
                continue
            closes = [float(b.close) for b in bars]
            highs = [float(b.high) for b in bars if b.high]
            lows = [float(b.low) for b in bars if b.low]
            vols = [float(b.volume_lots or 0) for b in bars]
            last = closes[-1]

            def pct(n: int):
                return round((last / closes[-1 - n] - 1) * 100, 2) if len(closes) > n and closes[-1 - n] > 0 else None

            prev5 = [v for v in vols[-6:-1] if v > 0]
            v5 = sum(prev5) / len(prev5) if prev5 else 0
            # 盘中抓取时最后一根 K 可能是当日未收盘的，量比偏小；收盘后跑是准的
            res[code] = {
                "d5": pct(5),
                "d20": pct(20),
                "d60": pct(60),
                "hi250": round((last / max(highs) - 1) * 100, 2) if highs else None,
                "lo250": round((last / min(lows) - 1) * 100, 2) if lows else None,
                "vr": round(vols[-1] / v5, 2) if v5 > 0 else None,
            }
        return res

    merged: dict[str, dict] = {}
    for part in _parallel(work, chunks, workers, "日K"):
        merged.update(part)
    log(f"[*] 日K：{len(chunks)} 批 → {len(merged)} 只有指标")
    return merged


def fetch_main_net(client, codes: list[str], workers: int = 8) -> dict[str, float]:
    """当日主力净流入（万元）。取 money_flow 最新一条记录的 main_net。"""
    chunks = [codes[i:i + FLOW_CHUNK] for i in range(0, len(codes), FLOW_CHUNK)]

    def work(chunk: list[str]) -> dict[str, float]:
        batch = client.money_flow.daily(chunk, batch_size=len(chunk))
        res: dict[str, float] = {}
        for block in batch.blocks:
            recs = [r for r in (block.records or ()) if r.date]
            if not recs:
                continue
            latest = max(recs, key=lambda r: r.date)
            res[f"{block.exchange}{block.code}"] = round(float(latest.main_net or 0) / 1e4, 1)
        return res

    merged: dict[str, float] = {}
    for part in _parallel(work, chunks, workers, "资金流"):
        merged.update(part)
    log(f"[*] 资金流：{len(chunks)} 批 → {len(merged)} 只有主力净流入")
    return merged


def fetch_circulating(client, codes: list[str]) -> dict[str, float]:
    """流通股本（股），用于算换手率。"""
    try:
        table = client.helpers.daily_share_capital(codes)
    except Exception as exc:
        log(f"[!] 取流通股本失败：{type(exc).__name__}: {exc}")
        return {}
    return {r.full_code: float(r.circulating_shares or 0) for r in table.rows if r.circulating_shares}


def build_signals(snap: dict[str, dict], daily: dict[str, dict], main_net: dict[str, float],
                  circulating: dict[str, float], names: dict[str, str]) -> dict[str, dict]:
    """把各路数据合成逐股信号表（输出用 6 位代码作 key）。只有拿到快照的股票才输出。"""
    out: dict[str, dict] = {}
    for full, q in snap.items():
        code = full[-6:]
        d = daily.get(full, {})
        share = circulating.get(full) or 0
        hand = q.get("hand") or 0
        # 换手率 = 成交量(股) / 流通股本；A 股 1 手 = 100 股
        turnover = round(hand * 100 / share * 100, 2) if share > 0 else None
        out[code] = {
            "n": names.get(full, ""),
            "c": q["last"],
            "p": q["chg"],
            "d5": d.get("d5"),
            "d20": d.get("d20"),
            "d60": d.get("d60"),
            "hi": d.get("hi250"),
            "lo": d.get("lo250"),
            "vr": d.get("vr"),
            "to": turnover,
            "mn": main_net.get(full),
        }
    return out


def build_momentum(signals: dict[str, dict], cap: int = 300) -> dict:
    """按 RULES 生成异动榜分组。"""
    def item(code: str, s: dict) -> dict:
        return {"code": code, "name": s["n"], "c": s["c"], "p": s["p"],
                "d20": s["d20"], "hi": s["hi"], "vr": s["vr"], "to": s["to"], "mn": s["mn"]}

    groups = []
    picks = [
        ("季末拉升", lambda s: s.get("d20") is not None and s["d20"] > 30, lambda s: -(s["d20"] or 0)),
        ("放量上攻", lambda s: (s.get("vr") or 0) >= 2 and (s.get("p") or 0) >= 3,
         lambda s: -((s.get("vr") or 0) * max(s.get("p") or 0, 0))),
        ("创一年新高", lambda s: s.get("hi") is not None and s["hi"] >= -0.5, lambda s: -(s.get("p") or 0)),
        ("主力大买", lambda s: s.get("mn") is not None and s["mn"] >= 5000, lambda s: -(s["mn"] or 0)),
    ]
    for key, test, order in picks:
        rows = [(c, s) for c, s in signals.items() if test(s)]
        rows.sort(key=lambda kv: order(kv[1]))
        groups.append({
            "key": key,
            "rule": RULES[key],
            "count": len(rows),
            "items": [item(c, s) for c, s in rows[:cap]],
        })
        log(f"[*] 异动榜「{key}」：{len(rows)} 只（输出前 {min(len(rows), cap)} 只）")
    return {"rules": RULES, "groups": groups}


def main() -> int:
    ap = argparse.ArgumentParser(description="用 eltdx 生成 signal 行情 JSON")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--limit", type=int, default=0, help="只抓前 N 只（0=全部）")
    ap.add_argument("--no-indices", action="store_true", help="跳过指数")
    ap.add_argument("--no-signals", action="store_true", help="跳过派生指标（signals/momentum）")
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

    client = TdxClient(timeout=args.timeout, probe_hosts=True, server_count=4, pool_size=16)
    stats = {"generated_at_utc": now_cn.astimezone(timezone.utc).isoformat(timespec="seconds"),
             "eltdx_version": str(version)}
    try:
        codes = fetch_universe(client, args.limit)
        if not codes:
            log("[x] 没有取到股票列表，退出")
            return 1
        stats["universe"] = len(codes)

        snap = fetch_quotes(client, codes)
        if snap:
            px = {c[-6:]: [v["last"], v["chg"]] for c, v in snap.items()}
            payload = {"at": now_cn.strftime("%Y-%m-%d %H:%M"), "px": px}
            (out_dir / "px.json").write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
            log(f"[+] px.json：{len(px)} 只")
        else:
            log("[!] px 为空，不写文件")
        stats["quotes_ok"] = len(snap)

        if snap and not args.no_signals:
            names = fetch_names(client)
            daily = fetch_daily(client, list(snap))
            main_net = fetch_main_net(client, list(snap))
            circulating = fetch_circulating(client, list(snap))
            signals = build_signals(snap, daily, main_net, circulating, names)
            payload = {"updated": now_cn.strftime("%Y-%m-%d %H:%M"), "fields": {
                "n": "名称", "c": "收盘价", "p": "当日涨跌幅%", "d5": "近5日涨幅%",
                "d20": "近20日涨幅%", "d60": "近60日涨幅%", "hi": "距250日高点%",
                "lo": "距250日低点%", "vr": "量比（当日/前5日均量）", "to": "换手率%",
                "mn": "主力净流入(万元)"}, "stocks": signals}
            (out_dir / "signals.json").write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
            log(f"[+] signals.json：{len(signals)} 只")
            mom = build_momentum(signals)
            mom["updated"] = payload["updated"]
            (out_dir / "momentum.json").write_text(
                json.dumps(mom, ensure_ascii=False, indent=1), encoding="utf-8")
            log(f"[+] momentum.json：{len(mom['groups'])} 个分组")
            stats["signals_ok"] = len(signals)
            stats["momentum"] = {g["key"]: g["count"] for g in mom["groups"]}

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
