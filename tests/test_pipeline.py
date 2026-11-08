#!/usr/bin/env python3
"""pipeline.py 的回归测试。

不联网、不依赖 eltdx（pipeline 只在 main() 里 import eltdx），
所以 CI 里只需要 pip install pytest 就能跑。

覆盖的是「改错了会让线上出事」的那些逻辑：
  * 数据闸门（覆盖率不够就不许发布）
  * 分片合并（各市场 JSON 合成一份，且不能把 names.json 当分片）
  * 异动榜规则（季末拉升 / 放量上攻 / 新高 / 主力大买）
  * 断线自愈（HealingClient 换连接重试，非连接类异常不许重试）
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pipeline  # noqa: E402

TZ_CN = timezone(timedelta(hours=8))


def mk_args(**kw):
    base = dict(
        out_dir="out", limit=None, markets="sz,sh,bj", prefix=None, shard=False,
        no_indices=True, no_signals=False, keep_partial=False, timeout=8.0,
        workers=4, min_coverage=pipeline.MIN_COVERAGE, merge=None,
    )
    base.update(kw)
    return argparse.Namespace(**base)


def mk_stock(**kw):
    s = {"n": "测试股", "c": 10.0, "p": 1.0, "d5": 1.0, "d20": 1.0, "d60": 1.0,
         "hi": -10.0, "lo": 20.0, "vr": 1.0, "to": 1.0, "mn": None}
    s.update(kw)
    return s


# ------------------------------------------------------------------ 闸门 ----

def test_gate_passes_on_healthy_stats():
    ok, why = pipeline.gate({"universe": 5581, "quotes_ok": 5575, "signals_ok": 5575,
                             "daily_ok": 5573, "flow_ok": 5217, "markets": ["sz"],
                             "indices_ok": 4}, mk_args())
    assert ok, why


def test_gate_blocks_half_data():
    """快照只到 57% 时必须拒绝发布（真实事故：195/356）。"""
    ok, why = pipeline.gate({"universe": 356, "quotes_ok": 195}, mk_args())
    assert not ok and "快照覆盖不足" in why


def test_gate_blocks_on_empty_universe():
    ok, why = pipeline.gate({"universe": 0}, mk_args())
    assert not ok and "股票列表" in why


def test_gate_blocks_when_flow_channel_died():
    """资金流整片失败（0/2318）必须拦住，不能发布一份没有主力资金的榜。"""
    ok, why = pipeline.gate({"universe": 2318, "quotes_ok": 2318, "signals_ok": 2318,
                             "daily_ok": 2310, "flow_ok": 0, "markets": ["sz"],
                             "indices_ok": 4}, mk_args())
    assert not ok and "资金流" in why


def test_gate_excuses_bj_only_shard():
    """北交所本来就没有资金流，单独分片时不该被判失败。"""
    ok, why = pipeline.gate({"universe": 356, "quotes_ok": 356, "signals_ok": 356,
                             "daily_ok": 340, "flow_ok": 0, "markets": ["bj"],
                             "indices_ok": 4}, mk_args())
    assert ok, why


def test_gate_blocks_missing_indices():
    ok, why = pipeline.gate({"universe": 5581, "quotes_ok": 5575, "signals_ok": 5575,
                             "daily_ok": 5573, "flow_ok": 5217, "markets": ["sz"],
                             "indices_ok": 2}, mk_args(no_indices=False))
    assert not ok and "指数" in why


# -------------------------------------------------------------- 异动榜 ----

def test_build_momentum_rules():
    signals = {
        "300001": mk_stock(d20=45.0),                                   # 季末拉升
        "300002": mk_stock(vr=2.5, p=4.0),                              # 放量上攻
        "300003": mk_stock(hi=-0.1),                                    # 创一年新高
        "300004": mk_stock(mn=8000.0),                                  # 主力大买
        "300005": mk_stock(d20=5.0, vr=0.8, p=0.2, hi=-30.0),           # 都不符合
    }
    mom = pipeline.build_momentum(signals)
    got = {g["key"]: [i["code"] for i in g["items"]] for g in mom["groups"]}
    assert got["季末拉升"] == ["300001"]
    assert got["放量上攻"] == ["300002"]
    assert got["创一年新高"] == ["300003"]
    assert got["主力大买"] == ["300004"]
    assert mom["rules"]["季末拉升"]


def test_build_momentum_caps_items_but_keeps_count():
    signals = {f"3000{i:02d}": mk_stock(d20=50.0 + i) for i in range(10)}
    mom = pipeline.build_momentum(signals, cap=3)
    g = [x for x in mom["groups"] if x["key"] == "季末拉升"][0]
    assert g["count"] == 10          # 计数是真实数量
    assert len(g["items"]) == 3      # 输出有上限
    assert g["items"][0]["d20"] == 59.0   # 按 20 日涨幅从大到小


# ----------------------------------------------------------- 分片合并 ----

def _write_shard(path: Path, market: str, codes: dict, at: str = "2026-11-08 15:35"):
    path.write_text(json.dumps({
        "at": at, "markets": [market], "prefixes": [],
        "px": {c: [10.0, 1.0] for c in codes},
        "stocks": {c: mk_stock(n=f"{market}{c}", mn=1000.0) for c in codes},
        "names": {c: f"{market}{c}" for c in codes},
    }, ensure_ascii=False), encoding="utf-8")


def test_merge_shards_combines_markets_and_skips_names(tmp_path):
    shard_dir = tmp_path / "shard"
    shard_dir.mkdir()
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    _write_shard(shard_dir / "sz.json", "sz", {"000001": 1, "000002": 1})
    _write_shard(shard_dir / "sh.json", "sh", {"600000": 1})
    # names.json / stats.json 不是分片，必须被忽略（否则会当成 px 数据合并进来）
    (shard_dir / pipeline.NAMES_CACHE).write_text(json.dumps({"names": {"999999": "不该出现"}}), encoding="utf-8")
    (shard_dir / "stats.json").write_text(json.dumps({"universe": 999}), encoding="utf-8")

    code = pipeline.merge_shards(shard_dir, out_dir, client=None, args=mk_args(),
                                 now_cn=datetime.now(TZ_CN), version="test", started=pipeline.time.time())
    assert code == 0

    px = json.loads((out_dir / "px.json").read_text(encoding="utf-8"))
    assert set(px["px"]) == {"000001", "000002", "600000"}
    stats = json.loads((out_dir / "stats.json").read_text(encoding="utf-8"))
    assert stats["mode"] == "merge" and stats["shards"] == 2
    assert stats["gate"]["ok"] is True
    live_core = json.loads((out_dir / "live-core.json").read_text(encoding="utf-8"))
    assert "stocks" not in live_core            # 首屏轻包不许带逐只明细
    assert live_core["stocks_ok"] == 3
    names = json.loads((out_dir / "names.json").read_text(encoding="utf-8"))
    assert "999999" not in names["names"]


def test_finish_does_not_publish_when_gate_fails(tmp_path):
    """闸门不过时：上一份数据必须原样保留，进程退出码 3。"""
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "px.json").write_text('{"at":"old","px":{"000001":[1,1]}}', encoding="utf-8")
    stage = out_dir / pipeline.STAGE_DIR
    stage.mkdir()
    (stage / "px.json").write_text('{"at":"new","px":{}}', encoding="utf-8")
    code = pipeline._finish(stage, out_dir, {"universe": 0}, mk_args(), pipeline.time.time())
    assert code == 3
    assert json.loads((out_dir / "px.json").read_text(encoding="utf-8"))["at"] == "old"
    assert not stage.exists()                    # 暂存目录已清理


# --------------------------------------------------------- 断线自愈 ----

class DeadClient:
    """一条已经挂掉的连接：任何属性访问都抛通道关闭。"""

    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True

    def __getattr__(self, name):
        raise RuntimeError("7709 runtime command channel is closed")


class FakeClient:
    def __init__(self):
        self.quotes = self
        self.helpers = self

    def get_snapshots(self, codes, on_missing="skip"):
        return [{"code": c} for c in codes]

    def latest_stock_list(self):
        return []


def test_attr_path_resolve():
    class A:
        class B:
            @staticmethod
            def c(*a, **kw):
                return ("hit", a, kw)

    assert pipeline._AttrPath.resolve(A, ("B", "c"))(1, k=2) == ("hit", (1,), {"k": 2})


def test_healing_client_swaps_dead_connection():
    made = []

    def factory():
        made.append(1)
        return FakeClient() if len(made) > 1 else DeadClient()

    box = pipeline.HealingClient(factory, attempts=3)
    out = box.quotes.get_snapshots(["000001"])
    assert out == [{"code": "000001"}]
    assert box.reconnects == 1 and len(made) == 2


def test_healing_client_does_not_retry_logic_errors():
    class Boom:
        def __getattr__(self, name):
            raise ValueError("参数写错了")     # 不是连接类错误

    box = pipeline.HealingClient(lambda: Boom(), attempts=3)
    with pytest.raises(ValueError):
        box.quotes.get_snapshots([])
    assert box.reconnects == 0


def test_conn_error_classification():
    assert pipeline.HealingClient._is_conn_error(
        RuntimeError("7709 runtime command channel is closed"))
    assert pipeline.HealingClient._is_conn_error(RuntimeError("PoolBusyError: capacity is full"))
    assert not pipeline.HealingClient._is_conn_error(KeyError("0"))


# ------------------------------------------------------- 历史留档 ----

def test_append_history_replaces_same_day(tmp_path):
    px = {"000001": [10.0, 1.0], "000002": [20.0, -2.0], "000003": [30.0, 0.0]}
    items = [{"name": "上证指数", "last": 3600.0, "chg": -1.1}]
    mom = {"groups": [{"key": "季末拉升", "count": 47, "items": []}]}
    h1 = pipeline.append_history(tmp_path, "2026-11-07 15:35", px, {}, items, mom)
    (tmp_path / pipeline.HISTORY).write_text(json.dumps(h1), encoding="utf-8")
    h2 = pipeline.append_history(tmp_path, "2026-11-08 15:35", px, {}, items, mom)
    (tmp_path / pipeline.HISTORY).write_text(json.dumps(h2), encoding="utf-8")
    # 同一天再跑一次（补跑）→ 覆盖当天那行，不产生重复
    h3 = pipeline.append_history(tmp_path, "2026-11-08 18:05", px, {}, items, mom)
    assert [r["d"] for r in h3["rows"]] == ["2026-11-07", "2026-11-08"]
    assert h3["rows"][-1]["at"] == "2026-11-08 18:05"
    row = h3["rows"][-1]
    assert (row["up"], row["down"], row["flat"], row["n"]) == (1, 1, 1, 3)
    assert row["avg"] == pytest.approx(-0.33, abs=0.01)
    assert row["idx"]["上证指数"] == 3600.0
    assert row["mom"]["季末拉升"] == 47


def test_append_history_caps_rows(tmp_path):
    px = {"000001": [10.0, 1.0]}
    for i in range(pipeline.HISTORY_MAX + 20):
        h = pipeline.append_history(tmp_path, f"2025-01-01 15:35", px, {}, [], None)
        (tmp_path / pipeline.HISTORY).write_text(json.dumps(h), encoding="utf-8")
    # 全同一天 → 只剩 1 行，且不报错
    rows = json.loads((tmp_path / pipeline.HISTORY).read_text(encoding="utf-8"))["rows"]
    assert len(rows) == 1


def test_append_history_survives_corrupt_file(tmp_path):
    (tmp_path / pipeline.HISTORY).write_text("{ 坏掉的 json", encoding="utf-8")
    h = pipeline.append_history(tmp_path, "2026-11-08 15:35", {"000001": [1.0, 0.0]}, {}, [], None)
    assert len(h["rows"]) == 1


# ------------------------------------------------- 合理性校验（防"对不上"） ----

def _seed_history(out_dir: Path, n=5000, up=2500, down=2400, avg=0.5, mom=None):
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / pipeline.HISTORY).write_text(json.dumps({"updated": "x", "rows": [{
        "d": "2026-11-07", "at": "2026-11-07 15:35", "n": n, "up": up, "down": down,
        "flat": n - up - down, "avg": avg, "idx": {},
        "mom": mom or {"季末拉升": 50, "放量上攻": 60, "创一年新高": 10, "主力大买": 80},
    }]}, ensure_ascii=False), encoding="utf-8")


def test_sanity_passes_on_normal_day(tmp_path):
    _seed_history(tmp_path)
    stats = {"at": "2026-11-08 15:35", "quotes_ok": 5575, "up": 2400, "down": 3100,
             "avg_chg": -1.2, "momentum": {"季末拉升": 47, "放量上攻": 68}}
    assert pipeline.sanity_check(stats, tmp_path) == ""


def test_sanity_blocks_one_sided_breadth(tmp_path):
    _seed_history(tmp_path)
    stats = {"at": "2026-11-08 15:35", "quotes_ok": 5575, "up": 0, "down": 0, "avg_chg": 0}
    assert "涨跌家数异常" in pipeline.sanity_check(stats, tmp_path)


def test_sanity_blocks_avg_jump(tmp_path):
    _seed_history(tmp_path, avg=0.5)
    stats = {"at": "2026-11-08 15:35", "quotes_ok": 5575, "up": 2400, "down": 3100, "avg_chg": -9.5}
    assert "平均涨幅跳变" in pipeline.sanity_check(stats, tmp_path)


def test_sanity_blocks_momentum_explosion(tmp_path):
    _seed_history(tmp_path)
    stats = {"at": "2026-11-08 15:35", "quotes_ok": 5575, "up": 2400, "down": 3100,
             "avg_chg": -1.2, "momentum": {"季末拉升": 900}}
    assert "暴增" in pipeline.sanity_check(stats, tmp_path)


def test_sanity_skips_first_run_and_small_samples(tmp_path):
    stats = {"at": "2026-11-08 15:35", "quotes_ok": 5575, "up": 0, "down": 0}
    assert pipeline.sanity_check(stats, tmp_path) == ""      # 没有历史 → 放行
    _seed_history(tmp_path)
    small = {"at": "2026-11-08 15:35", "quotes_ok": 300, "up": 0, "down": 300}
    assert pipeline.sanity_check(small, tmp_path) == ""      # 联调小样本 → 不判断


def test_sanity_skips_same_day_rerun(tmp_path):
    _seed_history(tmp_path)
    stats = {"at": "2026-11-07 18:05", "quotes_ok": 5575, "up": 0, "down": 0}
    assert pipeline.sanity_check(stats, tmp_path) == ""      # 同一天补跑不比


# ------------------------------------------------ 上一版备份（能回退） ----

def test_publish_keeps_previous_version(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    (out / "px.json").write_text('{"at":"v1"}', encoding="utf-8")
    stage = out / pipeline.STAGE_DIR
    stage.mkdir()
    (stage / "px.json").write_text('{"at":"v2"}', encoding="utf-8")
    pipeline.publish(stage, out)
    assert json.loads((out / "px.json").read_text())["at"] == "v2"
    import gzip
    prev = out / pipeline.PREV_DIR / "px.json.gz"
    assert json.loads(gzip.decompress(prev.read_bytes()))["at"] == "v1"


def test_market_stats():
    ms = pipeline.market_stats({"000001": [10.0, 2.0], "000002": [5.0, -1.0], "000003": [7.0, 0.0]})
    assert (ms["n"], ms["up"], ms["down"], ms["flat"]) == (3, 1, 1, 1)
    assert ms["avg_chg"] == pytest.approx(0.33, abs=0.01)
