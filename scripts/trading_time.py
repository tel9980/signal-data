#!/usr/bin/env python3
"""交易时段守卫：只在 A 股连续竞价时段（北京时间）放行盘中任务。

用法：python3 scripts/trading_time.py          # 在时段内 exit 0，否则 exit 1
      python3 scripts/trading_time.py --force  # 手动触发时强制放行

注意：只判断周末，不判断法定节假日 —— 节假日跑一轮抓到的就是上一交易日的收盘快照，
数据本身无害，只是白跑一次（免费额度内），不值得为此引入节假日表。
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

TZ_CN = timezone(timedelta(hours=8))
# 连续竞价：09:30-11:30、13:00-15:00，两头各留 2 分钟容忍（快照取到的是最新成交价）
WINDOWS = ((9 * 60 + 30, 11 * 60 + 32), (13 * 60, 15 * 60 + 2))


def main(argv: list[str]) -> int:
    now = datetime.now(TZ_CN)
    if "--force" in argv:
        print(f"[force] 忽略时段检查 {now:%Y-%m-%d %H:%M}")
        return 0
    if now.weekday() >= 5:
        print(f"周末（周{now.weekday() + 1}），跳过")
        return 1
    minutes = now.hour * 60 + now.minute
    for start, end in WINDOWS:
        if start <= minutes <= end:
            print(f"交易时段 {now:%H:%M}，继续")
            return 0
    print(f"非交易时段 {now:%H:%M}，跳过")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
