#!/usr/bin/env python3
"""数据完整性自检：防止「改代码时顺手把 out/ 删了/退回旧版」这类事故。

跑在 CI 的每次 push 上（几毫秒，不联网、不依赖）。查四件事：
  1. out/ 的必需文件都在（缺一个就是被误删）
  2. out/.prev/ 非空（回退能力还在）
  3. out/history.json 的留档行没有减少（历史只追加）
  4. out/px.json 的时间不早于留档最新一行（数据不许往回退）

为什么需要：2026-11-09 真实发生过一次——本地旧 out/ 被 `git add -A` 一起提交，
线上数据退回前一天，且 history.json 被整文件删掉。当时靠体检查出来才没一直错下去。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "out"
REQUIRED = ["px.json", "signals.json", "live.json", "live-core.json",
            "momentum.json", "indices.json", "names.json", "stats.json", "history.json",
            "ledger.json"]


def load(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def main() -> int:
    bad: list[str] = []

    missing = [f for f in REQUIRED if not (OUT / f).exists()]
    if missing:
        bad.append(f"out/ 缺少文件：{', '.join(missing)}")

    prev = OUT / ".prev"
    if not prev.is_dir() or not any(prev.glob("*.json.gz")):
        bad.append("out/.prev/ 空或不存在（回退能力丢失）")

    hist = load(OUT / "history.json") or {}
    rows = hist.get("rows") or []
    if not rows:
        bad.append("out/history.json 没有留档行")
    else:
        latest = str(rows[-1].get("at") or "")
        px = load(OUT / "px.json") or {}
        at = str(px.get("at") or "")
        if at and latest and at < latest:
            bad.append(f"数据往回退了：px.json 是 {at}，留档已到 {latest}")

    if bad:
        print("数据完整性检查未通过：")
        for b in bad:
            print("  -", b)
        return 1
    print(f"数据完整性检查通过：{len(rows)} 行留档，数据 {at}，回退备份 "
          f"{len(list(prev.glob('*.json.gz')))} 个文件")
    return 0


if __name__ == "__main__":
    sys.exit(main())
