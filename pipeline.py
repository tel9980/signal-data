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
import gzip
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
HISTORY = "history.json"      # 每日一行留档：指数点位、涨跌家数、异动榜数量
HISTORY_MAX = 250             # 保留最近 250 个交易日（约 60KB）
PREV_DIR = ".prev"            # 上一版数据备份（发布前自动留，出问题一条命令回去）
SANITY_MAX_AVG_JUMP = 8.0     # 平均涨幅单日跳变上限（百分点），超过基本是抓错了
SANITY_MAX_MOM_RATIO = 10     # 异动榜数量相对上一日放大上限（倍）
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


class _AttrPath:
    """把 client.a.b(...) 这样的两三级属性路径解析成真正的调用目标。"""

    @staticmethod
    def resolve(obj, path: tuple[str, ...]):
        for name in path:
            obj = getattr(obj, name)
        return obj


class _LazyMember:
    """延迟解析的成员：属性访问和调用都推迟到真正执行时，再在「当前」连接上解析。

    这样即使客户端本身已经挂了（连属性访问都抛错），也能先换连接再重做这次调用。
    支持任意深度：client.quotes.get_snapshots、client.helpers.latest_stock_list …
    """

    def __init__(self, root: "HealingClient", path: tuple[str, ...]):
        object.__setattr__(self, "_root", root)
        object.__setattr__(self, "_path", path)

    def __call__(self, *a, **kw):
        root = object.__getattribute__(self, "_root")
        path = object.__getattribute__(self, "_path")
        return root.invoke(lambda cli: _AttrPath.resolve(cli, path)(*a, **kw))

    def __getattr__(self, name: str):
        return _LazyMember(object.__getattribute__(self, "_root"),
                           object.__getattribute__(self, "_path") + (name,))


class HealingClient:
    """客户端代理：命令通道被挤断时自动重建连接并重试，调用方完全无感。

    背景：通达信的 7709 runtime 会整条命令通道关闭（ConnectionClosedError /
    PoolBusyError）。一个分片里 20 多批请求是并发的，通道一挂所有批次会同时失败，
    整个分片就报废。用代理包一层后，遇到这类错误就换一条新连接把这次调用重做，
    单个分片的失败率从「全灭」降到「个别批次慢一点」。
    """

    CONN_HINTS = (
        "channel is closed",          # 7709 runtime command channel is closed
        "connectionclosed",
        "poolbusy",
        "capacity is full",
        "reset by peer",
        "broken pipe",
        "connection reset",
        "connection aborted",
        "timed out",
        "timeout",
    )

    def __init__(self, factory, attempts: int = 4):
        self._factory = factory
        self._attempts = attempts
        self._lock = threading.Lock()
        self._client = factory()
        self.reconnects = 0

    # ---- 内部 ----------------------------------------------------------
    @property
    def client(self):
        return self._client

    @classmethod
    def _is_conn_error(cls, exc: Exception) -> bool:
        text = f"{type(exc).__name__}: {exc}".lower()
        return any(h in text for h in cls.CONN_HINTS)

    def _swap(self, dead):
        """只换一次：多个线程同时发现通道挂了时，后到的直接用别人换好的连接。"""
        with self._lock:
            if self._client is not dead:
                return self._client
            try:
                dead.close()
            except Exception:
                pass
            self._client = self._factory()
            self.reconnects += 1
            log(f"[*] 连接已重建（第 {self.reconnects} 次）")
            return self._client

    def invoke(self, fn):
        for i in range(self._attempts):
            cli = self._client
            try:
                return fn(cli)
            except Exception as exc:
                if not self._is_conn_error(exc) or i == self._attempts - 1:
                    raise
                log(f"[!] {type(exc).__name__}: {exc} → 重建连接后重试（{i + 1}/{self._attempts - 1}）")
                self._swap(cli)
                time.sleep(min(1.0 * (2 ** i), 6.0))
        raise RuntimeError("unreachable")

    # ---- 对外：像 TdxClient 一样用 --------------------------------------
    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return _LazyMember(self, (name,))

    def close(self):
        try:
            self._client.close()
        except Exception:
            pass


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


def fetch_universe(client, limit: int, markets: tuple[str, ...] | list[str] = MARKETS,
                   prefixes: tuple[str, ...] = ()) -> list[str]:
    """取 A 股代码，统一成 sz000001 / sh600000 / bj430047 这种带前缀的形式。

    prefixes 不为空时按 6 位代码前缀过滤（如创业板 300/301、科创 688），用于把
    一个市场拆成多个分片并行跑，单片更小、总时长由最慢的一片决定。
    """
    codes: list[str] = []
    for market in markets:
        try:
            rows = client.codes.a_shares(market)
            got = [f"{market}{s.code}" for s in rows]
            if prefixes:
                got = [c for c in got if c[2:5] in prefixes]
            log(f"[*] {market} 市场 {len(got)} 只" + (f"（前缀 {','.join(prefixes)}）" if prefixes else ""))
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

    整片失败时（通道抖动）把失败批次拆成 FLOW_RETRY_CHUNK 小批再补一次。

    返回 (净流入字典, 当前可用客户端) —— 客户端由 HealingClient 代理维持，这里原样返回。
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

    # 断连由 HealingClient 代理兜底重建，这里只需要把整片失败的情况再拆小补一次
    if failed and len(merged) < 0.5 * len(codes):
        log(f"[!] 资金流整体异常（{len(merged)}/{len(codes)}），把 {len(failed)} 批拆成小批重试")
        smaller = [c[i:i + FLOW_RETRY_CHUNK]
                   for c in failed for i in range(0, len(c), FLOW_RETRY_CHUNK)]
        retry_got, still = run(client, smaller)
        merged.update(retry_got)
        log(f"[*] 重试后补充 {len(retry_got)} 只，仍失败 {len(still)} 批")

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


def market_stats(px: dict) -> dict:
    """涨跌家数与平均涨幅：既是留档内容，也是合理性校验的依据。"""
    chgs = [v[1] for v in px.values() if v and v[1] is not None]
    up = sum(1 for v in px.values() if v and (v[1] or 0) > 0)
    down = sum(1 for v in px.values() if v and (v[1] or 0) < 0)
    return {"n": len(px), "up": up, "down": down,
            "flat": len(px) - up - down,
            "avg_chg": round(sum(chgs) / len(chgs), 2) if chgs else None}


def prev_row(out_dir: Path) -> dict:
    """上一份留档（不含今天），用于和今天比。"""
    path = out_dir / HISTORY
    if not path.exists():
        return {}
    try:
        rows = (json.loads(path.read_text(encoding="utf-8")) or {}).get("rows") or []
    except Exception:
        return {}
    return rows[-1] if rows else {}


def sanity_check(stats: dict, out_dir: Path) -> str:
    """数据合理性校验：体检只查"够不够/新不新"，这里查"对不对"。

    拦住这三类"覆盖率 100% 但内容是错的"：
      1. 全市场涨跌家数一侧为 0（字段抓错、全平）
      2. 平均涨幅相对上一日跳变过大（口径写错、单位错）
      3. 异动榜数量相对上一日放大十倍（规则写错，比如比较符号反了）
    没有上一份留档时（首次运行、历史被清空）一律放行。
    """
    prev = prev_row(out_dir)
    if not prev or not prev.get("n"):
        return ""
    today = stats.get("at") or ""
    if prev.get("d") and today and prev["d"] == today[:10]:
        return ""                                   # 同一天补跑，没有可比性
    n = int(stats.get("quotes_ok") or 0)
    if n < 1000 or int(prev.get("n") or 0) < 1000:
        return ""                                   # 样本太小（分片/联调），不做判断
    up, down = stats.get("up"), stats.get("down")
    if up is not None and down is not None and (up == 0 or down == 0):
        return f"涨跌家数异常：涨 {up} / 跌 {down}（全市场不可能只有一边）"
    avg, prev_avg = stats.get("avg_chg"), prev.get("avg")
    if avg is not None and prev_avg is not None and abs(avg - prev_avg) > SANITY_MAX_AVG_JUMP:
        return f"平均涨幅跳变：{prev_avg:+.2f}% → {avg:+.2f}%（差 {abs(avg - prev_avg):.2f} 个百分点）"
    mom, prev_mom = stats.get("momentum") or {}, prev.get("mom") or {}
    for key, count in mom.items():
        base = prev_mom.get(key)
        if base and base >= 20 and count > base * SANITY_MAX_MOM_RATIO:
            return f"异动榜「{key}」暴增：昨日 {base} 只 → 今日 {count} 只"
    return ""


def append_history(out_dir: Path, at: str, px: dict, signals: dict, items: list,
                   mom: dict | None) -> dict:
    """把当天的市场概况追加进 out/history.json（同一天重复跑就覆盖当天那行）。

    只存"以后想回看才有用"的聚合值，一天一行（约 250 字节），一年不到 100KB。
    """
    path = out_dir / HISTORY
    rows: list[dict] = []
    if path.exists():
        try:
            rows = (json.loads(path.read_text(encoding="utf-8")) or {}).get("rows") or []
        except Exception:
            rows = []
    ms = market_stats(px)
    row = {
        "d": at[:10],
        "at": at,
        "n": ms["n"],
        "up": ms["up"],
        "down": ms["down"],
        "flat": ms["flat"],
        "avg": ms["avg_chg"],
        "idx": {i["name"]: i.get("last") for i in (items or [])},
        "mom": {g["key"]: g["count"] for g in (mom or {}).get("groups", [])},
        "flow_ok": sum(1 for v in signals.values() if v.get("mn") is not None),
    }
    rows = [r for r in rows if r.get("d") != row["d"]] + [row]
    rows.sort(key=lambda r: r.get("d") or "")
    rows = rows[-HISTORY_MAX:]
    return {"updated": at, "rows": rows}


def publish(stage: Path, out_dir: Path) -> list[str]:
    """把暂存目录里的文件原子替换到 out_dir（os.replace 同盘原子，前端不会读到半个文件）。"""
    done = []
    prev = out_dir / PREV_DIR
    prev.mkdir(parents=True, exist_ok=True)
    for f in sorted(stage.glob("*.json")):
        target = out_dir / f.name
        if target.exists():                         # 先留一份上一版（gzip 后约 1/3 体积），出问题能一条命令回去
            with gzip.open(prev / (f.name + ".gz"), "wb", compresslevel=9) as g:
                g.write(target.read_bytes())
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
    if ok:
        bad = sanity_check(stats, out_dir)          # 覆盖率达标但内容明显不对，同样拦下
        if bad:
            ok, why = False, bad
            stats["sanity"] = {"ok": False, "reason": bad}
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
    prefixes = tuple(p.strip() for p in (args.prefix or "").split(",") if p.strip())
    stats["markets"] = markets
    if prefixes:
        stats["prefixes"] = list(prefixes)

    codes = fetch_universe(client, args.limit, markets=markets, prefixes=prefixes)
    if not codes:
        log("[x] 没有取到股票列表，退出")
        return 1
    stats["universe"] = len(codes)

    snap = fetch_quotes(client, codes, workers=args.workers)
    stats["quotes_ok"] = len(snap)
    at = now_cn.strftime("%Y-%m-%d %H:%M")
    px = {c[-6:]: [v["last"], v["chg"]] for c, v in snap.items()}
    stats["at"] = at
    stats.update({k: v for k, v in market_stats(px).items() if k != "n"})
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
        # 分片文件名必须唯一：sz 市场拆成两个分片时不能都叫 sz.json（合并时会互相覆盖）
        name = "-".join(markets + list(prefixes)) + ".json"
        _write_json(stage / name, {"at": at, "markets": markets, "prefixes": list(prefixes),
                                   "px": px, "stocks": signals, "names": names})
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
            # 首屏轻包：不带逐只明细（stocks 占 78% 体积），手机首屏从 914KB 降到约 125KB
            _write_json(stage / "live-core.json", {"updated": at, "at": at, "px": px,
                                                   "momentum": mom, "indices": items,
                                                   "stocks_ok": len(signals)})
        _write_json(stage / HISTORY, append_history(out_dir, at, px, signals, items, mom), indent=1)
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
        _write_json(stage / "live-core.json", {"updated": at, "at": at, "px": px,
                                               "momentum": mom, "indices": items,
                                               "stocks_ok": len(stocks)})
        _write_json(stage / HISTORY, append_history(out_dir, at, px, stocks, items, mom), indent=1)
    if items:
        _write_json(stage / "indices.json", {"updated": at, "items": items}, indent=1)
    if names:
        _write_json(stage / NAMES_CACHE,
                    {"updated_utc": now_cn.astimezone(timezone.utc).isoformat(timespec="seconds"),
                     "names": names})

    stats = {"generated_at_utc": now_cn.astimezone(timezone.utc).isoformat(timespec="seconds"),
             "eltdx_version": str(version), "mode": "merge", "shards": len(files), "at": at,
             "markets": [f.stem for f in files], "at": at,
             "universe": len(px), "quotes_ok": len(px),
             "signals_ok": len(stocks), "daily_ok": len(stocks),
             "flow_ok": sum(1 for v in stocks.values() if v.get("mn") is not None),
             "indices_ok": len([i for i in items if i["chg"] is not None]) if items else len(INDICES)}
    stats.update({k: v for k, v in market_stats(px).items() if k != "n"})
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
    ap.add_argument("--prefix", default="", metavar="PREFIX",
                    help="只抓指定代码前缀（逗号分隔，如 300,301=创业板；688=科创），用于细分分片")
    ap.add_argument("--shard", action="store_true",
                    help="分片模式：按市场（+前缀）各写一个 <market>[-<prefix>].json（配合 matrix 并行）")
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

    # 用代理包一层：通达信命令通道被挤断时自动换连接重试，避免整片报废
    client = HealingClient(make_client)
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
