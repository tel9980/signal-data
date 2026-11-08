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
import os
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from pathlib import Path

CHUNK = 80                    # 通达信单次快照上限，80 只/次
BAR_CHUNK = 300               # 日 K 每批代码数（一批＝一个请求；19 批并发跑完）
FLOW_CHUNK = 300              # 资金流每批代码数（一批＝一个请求，比库默认的 75 少 4 倍往返）
QUOTE_WORKERS = int(os.environ.get("QUOTE_WORKERS", "10"))   # 快照并发（Actions 上延迟受限）
DERIVE_WORKERS = int(os.environ.get("DERIVE_WORKERS", "10"))  # 日 K / 资金流并发
SERVER_COUNT = int(os.environ.get("SERVER_COUNT", "4"))       # 服务器数（多了反而抢容量，实测 4 最稳）
POOL_SIZE = int(os.environ.get("POOL_SIZE", "32"))            # 连接池（复用建连，省掉每次握手）
FLOW_RETRY_CHUNK = 60         # 资金流重试批大小（通道重建后先用小批试探）
RETRY = 3                     # 单批失败重试次数（PoolBusy / 瞬时断连）
MIN_COVERAGE = 0.90           # 快照覆盖率低于此值视为异常，整轮不发布
STAGE_DIR = ".staging"        # 暂存目录：全部产出先写这里，闸门通过才原子替换
NAMES_CACHE = "names.json"    # 股票名称缓存（名称一天只变几只）
NAMES_TTL_DAYS = 3
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


def _retry(fn, attempts: int = RETRY, backoff: float = 1.0):
    """瞬时错误（PoolBusy / 断连）重试，指数退避；最后一次仍失败才抛出。"""
    last = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            last = exc
            if i < attempts - 1:
                time.sleep(backoff * (i + 1))
    raise last


def _parallel(fn, chunks, workers, label=""):
    """并发跑 fn(chunk)，结果按顺序收集；单批异常只记日志，不中断整体。"""
    out = []
    lock = threading.Lock()

    def work(chunk):
        try:
            res = _retry(lambda: fn(chunk))
        except Exception as exc:                       # 单批失败不影响整体
            log(f"[!] {label}失败（{len(chunk)} 只）：{type(exc).__name__}: {exc}")
            return
        with lock:
            out.append(res)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(work, chunks))
    return out


def fetch_universe(client, limit: int, markets: tuple[str, ...] | list[str] = MARKETS) -> list[str]:
    """取 A 股代码，统一成 sz000001 / sh600000 / bj430047 这种带前缀的形式。"""
    codes: list[str] = []
    for market in markets:
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


def fetch_quotes(client, codes: list[str], workers: int = QUOTE_WORKERS) -> dict[str, dict]:
    """并发抓快照，返回 {code: {last, chg, hand, amount, high, low, open}}；
    停牌/无成交（价格为 0）的直接丢弃。"""
    chunks = [codes[i:i + CHUNK] for i in range(0, len(codes), CHUNK)]
    out: dict[str, dict] = {}
    zero = 0
    lock = threading.Lock()

    def work(chunk: list[str]) -> None:
        nonlocal zero
        try:
            rows = _retry(lambda: client.quotes.get_snapshots(chunk, on_missing="skip"))
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

def fetch_daily(client, codes: list[str], count: int = 250, workers: int = DERIVE_WORKERS) -> dict[str, dict]:
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


def fetch_main_net(client, codes: list[str], workers: int = DERIVE_WORKERS, factory=None):
    """当日主力净流入（万元）。取 money_flow 最新一条记录的 main_net。

    通达信的 runtime 偶发会整条命令通道关闭（ConnectionClosedError: 7709 runtime
    command channel is closed），这时所有批次会同时失败。检测到"几乎全灭"就重建
    客户端、把失败批次拆小重试一次；重建出的客户端会返回给调用方继续用。

    返回 (净流入字典, 当前可用客户端)。
    """

    def work_with(cl):
        def work(chunk: list[str]) -> dict[str, float]:
            batch = cl.money_flow.daily(chunk, batch_size=len(chunk))
            res: dict[str, float] = {}
            for block in batch.blocks:
                recs = [r for r in (block.records or ()) if r.date]
                if not recs:
                    continue
                latest = max(recs, key=lambda r: r.date)
                # 北交所等部分标的没有资金流数据，total_amount 为 0 时留空（前端显示 —），不要写成 0 亿
                if not float(latest.total_amount or 0):
                    continue
                res[f"{block.exchange}{block.code}"] = round(float(latest.main_net or 0) / 1e4, 1)
            return res
        return work

    def run(cl, chunks):
        """跑一批分片，返回 (结果, 失败分片列表)。"""
        got: dict[str, float] = {}
        failed: list[list[str]] = []
        lock = threading.Lock()
        work = work_with(cl)

        def one(chunk):
            try:
                part = _retry(lambda: work(chunk))
            except Exception as exc:
                log(f"[!] 资金流失败（{len(chunk)} 只）：{type(exc).__name__}: {exc}")
                with lock:
                    failed.append(chunk)
                return
            with lock:
                got.update(part)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(one, chunks))
        return got, failed

    chunks = [codes[i:i + FLOW_CHUNK] for i in range(0, len(codes), FLOW_CHUNK)]
    merged, failed = run(client, chunks)

    # 全军覆没（或大面积失败）通常是命令通道挂了，换一条新连接再试一次
    if failed and factory and len(merged) < 0.5 * len(codes):
        log(f"[!] 资金流整体异常（{len(merged)}/{len(codes)}），新建连接后重试 {len(failed)} 批")
        try:
            fresh = factory()
        except Exception as exc:
            log(f"[!] 重建连接失败：{type(exc).__name__}: {exc}")
        else:
            smaller = [c[i:i + FLOW_RETRY_CHUNK]
                       for c in failed for i in range(0, len(c), FLOW_RETRY_CHUNK)]
            retry_got, still = run(fresh, smaller)
            merged.update(retry_got)
            log(f"[*] 重试后补充 {len(retry_got)} 只，仍失败 {len(still)} 批")
            if len(merged) >= 0.5 * len(codes):
                old, client = client, fresh          # 老连接已废，后面改用新连接
                try:
                    old.close()
                except Exception:
                    pass
            else:
                try:
                    fresh.close()
                except Exception:
                    pass

    log(f"[*] 资金流：{len(chunks)} 批 → {len(merged)} 只有主力净流入")
    return merged, client


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


# ---------------------------------------------------------------- 发布工具 ----

def _write_json(path: Path, obj, *, indent=None) -> None:
    text = (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) if indent is None
            else json.dumps(obj, ensure_ascii=False, indent=indent))
    path.write_text(text, encoding="utf-8")


def publish(stage: Path, out_dir: Path) -> list[str]:
    """把暂存目录里的文件原子替换到 out_dir（os.replace 同盘原子，前端不会读到半个文件）。"""
    done = []
    for f in sorted(stage.glob("*.json")):
        target = out_dir / f.name
        os.replace(f, target)
        done.append(f"{target.name} {target.stat().st_size / 1024:.0f}KB")
    shutil.rmtree(stage, ignore_errors=True)
    return done


def gate(stats: dict, args) -> tuple[bool, str]:
    """数据闸门：覆盖率不达标就整轮不发布，避免"半份数据"静默上线。"""
    universe = int(stats.get("universe") or 0)
    quotes = int(stats.get("quotes_ok") or 0)
    if not universe:
        return False, "没有取到股票列表"
    need = int(universe * args.min_coverage)
    if quotes < need:
        return False, f"快照覆盖不足：{quotes}/{universe} < {need}（阈值 {args.min_coverage:.0%}）"
    if not args.no_signals:
        sig = int(stats.get("signals_ok") or 0)
        if sig < quotes * 0.95:
            return False, f"派生指标缺失：signals {sig} / 快照 {quotes}"
    if not args.no_signals:
        only_bj = [m for m in (stats.get("markets") or [])] == ["bj"]
        daily = int(stats.get("daily_ok") or 0)
        sig = int(stats.get("signals_ok") or 0)
        if sig and daily < sig * 0.9:
            return False, f"日K缺失：只有 {daily}/{sig} 只有历史数据（正常应在 99% 以上）"
        flow = int(stats.get("flow_ok") or 0)
        if sig and flow < sig * 0.5 and not only_bj:
            return False, f"资金流异常：只有 {flow}/{sig} 只有主力净流入（正常应在 90% 以上）"
    if not args.no_indices and int(stats.get("indices_ok") or 0) < len(INDICES):
        return False, f"指数只拿到 {stats.get('indices_ok')}/{len(INDICES)} 个"
    return True, "ok"


def load_names(client, out_dir: Path) -> tuple[dict[str, str], bool]:
    """名称：优先用 out/names.json（NAMES_TTL_DAYS 天内），否则现拉。返回 (names, 是否新拉)。"""
    cache = out_dir / NAMES_CACHE
    if cache.exists():
        try:
            data = json.loads(cache.read_text(encoding="utf-8"))
            stamp = datetime.fromisoformat(str(data.get("updated_utc", "")).replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - stamp).total_seconds() / 86400
            if age < NAMES_TTL_DAYS and data.get("names"):
                log(f"[*] 名称走缓存：{len(data['names'])} 只（{age:.1f} 天前）")
                return data["names"], False
        except Exception:
            pass
    return fetch_names(client), True


def _finish(stage: Path, out_dir: Path, stats: dict, args, started: float) -> int:
    """统一的收尾：闸门 → 写 stats → 发布 / 丢弃。返回进程退出码。"""
    ok, why = gate(stats, args)
    stats["gate"] = {"ok": ok, "reason": why}
    stats["duration_s"] = round(time.time() - started, 1)
    _write_json(out_dir / "stats.json", stats, indent=1)
    if not ok:
        log(f"[x] 数据闸门未通过：{why}")
        log("[x] 本轮不发布，线上保持上一份数据（stats.json 已写明原因）")
        if not args.keep_partial:
            shutil.rmtree(stage, ignore_errors=True)
        return 3
    for line in publish(stage, out_dir):
        log(f"[+] 发布 {line}")
    log(f"[*] 完成，耗时 {stats['duration_s']}s → {out_dir}/")
    return 0


FIELDS = {
    "n": "名称", "c": "收盘价", "p": "当日涨跌幅%", "d5": "近5日涨幅%",
    "d20": "近20日涨幅%", "d60": "近60日涨幅%", "hi": "距250日高点%",
    "lo": "距250日低点%", "vr": "量比（当日/前5日均量）", "to": "换手率%",
    "mn": "主力净流入(万元)",
}


def fetch_all(client, out_dir: Path, args, now_cn, version: str, started: float,
              factory=None) -> int:
    stage = out_dir / STAGE_DIR
    shutil.rmtree(stage, ignore_errors=True)
    stage.mkdir(parents=True, exist_ok=True)
    stats = {"generated_at_utc": now_cn.astimezone(timezone.utc).isoformat(timespec="seconds"),
             "eltdx_version": str(version)}
    markets = [m.strip() for m in args.markets.split(",") if m.strip()]
    stats["markets"] = markets

    codes = fetch_universe(client, args.limit, markets=markets)
    if not codes:
        log("[x] 没有取到股票列表，退出")
        return 1
    stats["universe"] = len(codes)

    snap = fetch_quotes(client, codes, workers=args.workers)
    stats["quotes_ok"] = len(snap)
    at = now_cn.strftime("%Y-%m-%d %H:%M")
    px = {c[-6:]: [v["last"], v["chg"]] for c, v in snap.items()}
    signals: dict[str, dict] = {}
    mom = None
    names: dict[str, str] = {}
    names_fresh = False

    if snap and not args.no_signals:
        names, names_fresh = load_names(client, out_dir)
        daily = fetch_daily(client, list(snap), workers=args.workers)
        stats["daily_ok"] = len(daily)
        main_net, client = fetch_main_net(client, list(snap), workers=args.workers, factory=factory)
        circulating = fetch_circulating(client, list(snap))
        signals = build_signals(snap, daily, main_net, circulating, names)
        stats["signals_ok"] = len(signals)
        stats["flow_ok"] = len(main_net)
        mom = build_momentum(signals)
        stats["momentum"] = {g["key"]: g["count"] for g in mom["groups"]}

    items: list[dict] = []
    if not args.no_indices:
        items = fetch_indices(client)
        stats["indices_ok"] = len([i for i in items if i["chg"] is not None])
    else:
        stats["indices_ok"] = len(INDICES)

    if names_fresh and names:
        _write_json(stage / NAMES_CACHE,
                    {"updated_utc": now_cn.astimezone(timezone.utc).isoformat(timespec="seconds"),
                     "names": names})

    if args.shard:
        name = "-".join(markets) + ".json"
        _write_json(stage / name, {"at": at, "markets": markets, "px": px,
                                   "stocks": signals, "names": names})
        log(f"[+] 分片 {name}：{len(px)} 只现价 / {len(signals)} 只指标")
    else:
        _write_json(stage / "px.json", {"at": at, "px": px})
        if signals:
            _write_json(stage / "signals.json", {"updated": at, "fields": FIELDS, "stocks": signals})
            mom["updated"] = at
            _write_json(stage / "momentum.json", mom, indent=1)
            # 首屏单文件：px + signals + momentum + indices 一次请求拿全
            _write_json(stage / "live.json", {"updated": at, "at": at, "px": px,
                                              "stocks": signals, "momentum": mom,
                                              "indices": items})
        if items:
            _write_json(stage / "indices.json", {"updated": at, "items": items}, indent=1)

    return _finish(stage, out_dir, stats, args, started)


def merge_shards(shard_dir: Path, out_dir: Path, client, args, now_cn, version: str,
                 started: float) -> int:
    """把 matrix 各分片（每个市场一个 json）合并成最终产出。"""
    stage = out_dir / STAGE_DIR
    shutil.rmtree(stage, ignore_errors=True)
    stage.mkdir(parents=True, exist_ok=True)
    px: dict[str, list] = {}
    stocks: dict[str, dict] = {}
    names: dict[str, str] = {}
    at = ""
    files = [f for f in sorted(shard_dir.glob("*.json"))
             if f.name not in (NAMES_CACHE, "stats.json")]
    for f in files:
        d = json.loads(f.read_text(encoding="utf-8"))
        px.update(d.get("px") or {})
        stocks.update(d.get("stocks") or {})
        names.update(d.get("names") or {})
        at = d.get("at") or at
    log(f"[*] 合并 {len(files)} 个分片 → {len(px)} 只现价 / {len(stocks)} 只指标")
    if not at:
        at = now_cn.strftime("%Y-%m-%d %H:%M")

    mom = build_momentum(stocks) if stocks else None
    items = fetch_indices(client) if not args.no_indices else []
    if stocks:
        _write_json(stage / "px.json", {"at": at, "px": px})
        _write_json(stage / "signals.json", {"updated": at, "fields": FIELDS, "stocks": stocks})
        mom["updated"] = at
        _write_json(stage / "momentum.json", mom, indent=1)
        _write_json(stage / "live.json", {"updated": at, "at": at, "px": px,
                                          "stocks": stocks, "momentum": mom, "indices": items})
    if items:
        _write_json(stage / "indices.json", {"updated": at, "items": items}, indent=1)
    if names:
        _write_json(stage / NAMES_CACHE,
                    {"updated_utc": now_cn.astimezone(timezone.utc).isoformat(timespec="seconds"),
                     "names": names})

    stats = {"generated_at_utc": now_cn.astimezone(timezone.utc).isoformat(timespec="seconds"),
             "eltdx_version": str(version), "mode": "merge", "shards": len(files),
             "markets": [f.stem for f in files], "at": at,
             "universe": len(px), "quotes_ok": len(px),
             "signals_ok": len(stocks), "daily_ok": len(stocks),
             "flow_ok": sum(1 for v in stocks.values() if v.get("mn") is not None),
             "indices_ok": len([i for i in items if i["chg"] is not None]) if items else len(INDICES)}
    if mom:
        stats["momentum"] = {g["key"]: g["count"] for g in mom["groups"]}
    return _finish(stage, out_dir, stats, args, started)


def main() -> int:
    ap = argparse.ArgumentParser(description="用 eltdx 生成 signal 行情 JSON")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--limit", type=int, default=0, help="只抓前 N 只（0=全部）")
    ap.add_argument("--no-indices", action="store_true", help="跳过指数")
    ap.add_argument("--no-signals", action="store_true", help="跳过派生指标（signals/momentum）")
    ap.add_argument("--timeout", type=float, default=8.0)
    ap.add_argument("--workers", type=int, default=QUOTE_WORKERS, help="并发数（快照/日K/资金流共用）")
    ap.add_argument("--min-coverage", type=float, default=MIN_COVERAGE,
                    help="快照覆盖率下限，低于此值本轮不发布（默认 0.9）")
    ap.add_argument("--markets", default=",".join(MARKETS), help="只抓指定市场，如 sz,sh")
    ap.add_argument("--shard", action="store_true",
                    help="分片模式：按市场各写一个 <market>.json（配合 matrix 并行）")
    ap.add_argument("--merge", default="", metavar="DIR",
                    help="合并模式：把 DIR/*.json 分片合并成最终产出")
    ap.add_argument("--keep-partial", action="store_true", help="闸门未通过时保留暂存文件便于排查")
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

    def make_client():
        return TdxClient(timeout=args.timeout, probe_hosts=True,
                         server_count=SERVER_COUNT, pool_size=POOL_SIZE)

    client = make_client()
    try:
        if args.merge:
            return merge_shards(Path(args.merge), out_dir, client, args, now_cn, version, started)
        return fetch_all(client, out_dir, args, now_cn, version, started, factory=make_client)
    finally:
        try:
            client.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
