# signal-data · signal 的行情数据管线

用 [eltdx](https://github.com/electkismet/eltdx)（通达信 7709 协议，Rust 引擎）抓 A 股行情，产出 [signal](https://github.com/zentaox/signal) 前端能直接读的 JSON，外加一批 signal 自己没有的行情派生指标。

三个仓库的分工：

```
eltdx（数据源，别人的库）  →  本仓库 pipeline.py  →  out/*.json  →  signal（展示层）
```

不改 signal 的取数逻辑，也不动 eltdx 的库，只把"行情这一层"补上。

## 产出

| 文件 | 内容 | 与 signal 的关系 |
|---|---|---|
| `out/px.json` | 全市场 A 股现价 + 当日涨跌幅，**schema 与 `signal/data/px.json` 完全一致** | 可直接替换 |
| `out/indices.json` | 上证 / 创业板 / 科创50 / 北证50 的涨跌幅 + 距近一年高点 | 可作为 `signal/data/market.json` 里 `indices` 的来源 |
| `out/signals.json` | 逐股行情派生指标：5/20/60 日涨幅、距 250 日高·低点、量比、换手率、主力净流入 | signal 没有，属新增 |
| `out/momentum.json` | 由 `signals.json` 派生的 4 组「异动榜」（含与 signal 同口径的「季末拉升」） | signal 没有，属新增 |
| `out/live.json` | **整包**：px + signals + momentum + indices 打包在一起，前端一次请求拿全 | signal 没有，属新增 |
| `out/live-core.json` | **首屏轻包**：px + indices + momentum（不含逐只明细），约 146KB，手机首屏用这个 | signal 没有，属新增 |
| `out/names.json` | 代码 → 名称缓存（3 天有效，避免每次都拉证券列表） | — |
| `out/stats.json` | 本次抓取自检：时间、耗时、股票数、指数数、各组数量、**闸门结论**、eltdx 版本 | — |
| `out/history.json` | **每日一行留档**（最多 250 个交易日）：指数点位、涨跌家数、平均涨幅、异动榜数量、有资金流的只数 | — |
| `out/.prev/` | **上一版数据备份**（gzip，约 540KB）：每次发布前自动留，出问题 `bash scripts/rollback.sh` 一条命令回去 | — |

### px.json

```json
{"at": "2026-11-08 08:25", "px": {"000001": [11.81, 1.46], "000002": [3.24, -2.7]}}
```

即 `代码: [现价, 当日涨跌幅%]`。

### signals.json

```json
{"updated": "2026-11-08 16:02",
 "fields": {"n": "名称", "c": "收盘价", "p": "当日涨跌幅%", "d5": "近5日涨幅%",
            "d20": "近20日涨幅%", "d60": "近60日涨幅%", "hi": "距250日高点%",
            "lo": "距250日低点%", "vr": "量比", "to": "换手率%", "mn": "主力净流入(万元)"},
 "stocks": {"000001": {"n": "平安银行", "c": 11.81, "p": 1.46, "d5": 1.46, "d20": 1.9,
                       "d60": 4.79, "hi": -2.24, "lo": 18.22, "vr": 0.99, "to": 0.85,
                       "mn": 5643.2}}}
```

字段口径：

| 字段 | 算法 |
|---|---|
| `d5 / d20 / d60` | 收盘价 ÷ 5 / 20 / 60 个交易日前的收盘价 − 1 |
| `hi` | 收盘价 ÷ 近 250 个交易日最高价 − 1（负值＝在高点下方） |
| `lo` | 收盘价 ÷ 近 250 个交易日最低价 − 1 |
| `vr` | 当日成交量 ÷ 前 5 个交易日均量 |
| `to` | 当日成交量(手)×100 ÷ 流通股本 |
| `mn` | 通达信资金流当日 `main_net`（主力＝超大单＋大单）÷ 10000 |

### momentum.json

四组规则：

| 分组 | 规则 |
|---|---|
| 季末拉升 | 近 20 个交易日涨幅 > 30%（**与 signal 同口径**） |
| 放量上攻 | 量比 ≥ 2 且当日涨幅 ≥ 3% |
| 创一年新高 | 收盘价站上近 250 日最高价的 99.5% |
| 主力大买 | 当日主力净流入 ≥ 5000 万元 |

## 稳定性设计
| 合理性校验 | 闸门只能判断"够不够"，这里判断"对不对"：涨跌家数一侧为 0、平均涨幅单日跳变 >8 个百分点、异动榜数量放大 10 倍，一律拦下并写明原因 |
| 上一版备份 | 发布前把旧文件 gzip 存到 `out/.prev/`；出问题 `bash scripts/rollback.sh` 拷回并提交，随时可再回退一次 |

| 机制 | 说明 |
|---|---|
| **数据闸门** | 产出先写进 `out/.staging/`，只有「快照覆盖 ≥ 90% + 日 K ≥ 90% + 资金流 ≥ 50% + 指数 4/4」全部满足才 `os.replace` 原子发布；任何一项不达标 → 退出码 3、**线上保持上一份数据**，原因写进 `stats.json` |
| **分片并行** | 工作流把 sz / sh / bj 拆成 3 个 job 跑在不同 runner（不同出口 IP），各抓自己市场，最后一个 job 合并。3 个分片合并结果与单次全量**逐字段一致（5575/5575）** |
| **去重 + 补跑** | `scripts/should_run.py`：90 分钟内已有成功 run 就整轮跳过；配合 11:35 / 15:35 / 18:05 三个 cron，既不重复跑，也不怕 cron 延迟漏跑 |
| **重试** | 单批请求失败（`PoolBusyError` / 瞬时断连）自动重试 3 次，指数退避 1s→2s |
| **通道自愈** | 通达信偶发整条命令通道关闭（`ConnectionClosedError: 7709 runtime command channel is closed`）→ 检测到资金流大面积失败就**新建连接、拆成 60 只小批重试**，恢复后后续阶段改用新连接 |
| **分片限流** | 三个分片会同时连同一个节点，所以分片内并发压到 `QUOTE_WORKERS=8 / DERIVE_WORKERS=6 / POOL_SIZE=16 / SERVER_COUNT=2`（都可环境变量覆盖） |
| **线上体检** | `scripts/health.py` 每 30 分钟检查 8 项 + 数据新鲜度（工作日 16:00 后必须是当天数据），失败自动开 `health` 标签 issue，恢复后自动关闭 |
| **滚动提交** | 上一提交是 `chore: 行情数据更新` 时用 `--amend` 原地覆盖，仓库历史不膨胀 |

## 实测数据

以 2026-11-06 收盘数据与 signal 现有 `data/px.json` 逐只核对：

| 指标 | 结果 |
|---|---|
| 我们的有效股票数 | **5575** 只（停牌/零价 6 只已丢弃） |
| 与上游共有的代码 | 5570 只 |
| 其中数值**完全一致** | **5564 只（99.89%）** |
| 仅上游有、我们没有 | 602 只（主要是退市/长期停牌股，如 `000003`） |
| 本机全量耗时 | **约 24–28 秒**（70 批快照 + 19 批日 K + 19 批资金流，10 线程并发） |
| Actions 耗时 | 见仓库 Actions 页面（分片并行后约 1 分钟量级） |
| 四大指数 | 涨跌幅与 signal 完全一致（-1.10 / -3.96 / -2.60 / -2.37） |

> 顺便发现：signal 现在 `market.json` 里北证50 的"距高点"写成 **-1.3%**，而按 250 日日线高点算是 **-45.1%**，与其它三个指数（-14 ~ -44）明显不同量级，疑似上游计算有 bug。本管线输出的 `from_high` 是按日 K 实算的。

## 能覆盖多少？诚实版

把 signal 的 `data/` 逐个拆开看，能不能用行情数据重算：

| signal 的数据 | 本管线 | 说明 |
|---|---|---|
| `px.json` 现价 | ✅ 完全覆盖 | 我们 5575 只，且数值 99.89% 一致 |
| `market.json` 的 `indices` | ✅ 完全覆盖 | 顺便修正了北证50 的 bug |
| `s0–9` 的「季末拉升」标签 | ✅ 完全覆盖 | 近 20 日涨幅 > 30%，见 `momentum.json` |
| `s0–9` 的 解禁压力 / 近月解禁 | ❌ | 限售股解禁表，来自公告与交易所，不是行情 |
| `s0–9` 的 外资扎堆 / 低于机构估算成本 | ❌ | 需要十大流通股东名册（季报），行情只能算成本、算不出名册 |
| `s0–9` 的 龙虎榜常客 | ❌ | 龙虎榜席位数据，通达信协议里没有 |
| `s0–9` 的 国资在十大股东 | ❌ | 同上，股东名册类 |
| `risk0–9` 的 财报四项 + 质押比例 | ❌ | 财务科目（`f10` 只能补一部分）与股权质押表 |
| `pool.json` 机构季报持仓 | ❌ | 季报持仓，非行情 |
| `seats.json` 龙虎榜席位 | ❌ | 非行情 |
| `audit.json` 定增/重组审核 | ❌ | 交易所审核进度，非行情 |
| `themes.json` 题材 | ⚠️ 部分 | 通达信 F10 题材可以补一部分 |

结论：**signal 里能被"行情数据"重算的部分，本管线已经全部覆盖**（现价、指数、季末拉升），另外新增了量比/换手/资金流/距高点这些它原来没有的维度；解禁、股东名册、龙虎榜、财报质押这批**不是行情数据**，任何行情源都替代不了。

## 怎么用

**方式一：直接把 `px.json` 放进 signal 的 `data/`**
schema 完全一样，`cp out/px.json <signal仓库>/data/px.json` 即可，前端不用改一行代码。

**方式二：前端直接 fetch 本仓库的 raw 地址**
raw.githubusercontent.com 允许跨域（`Access-Control-Allow-Origin: *`），signal 站点可以把 `load('px.json')` 改成指向：

```
https://raw.githubusercontent.com/tel9980/signal-data/main/out/px.json
```

**方式三（已落地）：fork 里改了 `load()`，行情自动走本仓库**
见 [tel9980/signal](https://github.com/tel9980/signal) 的 `app.js`：`px.json`、`market.json` 的 `indices` 优先用本仓库数据，失败则回退到原 `data/`。

## 本地跑

```bash
pip install -r requirements.txt
python pipeline.py                  # 全市场，约 2 分钟
python pipeline.py --limit 300      # 联调只抓 300 只
python pipeline.py --no-signals     # 只出 px.json / indices.json（约 5 秒）
python pipeline.py --no-indices     # 跳过指数
python pipeline.py --out-dir /tmp/x # 指定输出目录
```

## 自动更新

`.github/workflows/update.yml`：交易日 **北京时间 11:35 与 15:35** 各跑一次（cron 已换算成 UTC `35 3` / `35 7`），也可手动触发。

产物用**滚动提交**（`git commit --amend` 覆盖上一条），所以仓库历史永远是常量，不会像普通数据仓库那样每天膨胀。

## 免责

eltdx 为 Research-Only 许可，本项目仅用于个人学习与协议研究，请勿用于商业或付费服务。

## 测试

```bash
python -m pip install pytest
python -m pytest -q tests        # 25 个用例，全程离线，不需要 eltdx
```

CI 见 `.github/workflows/test.yml`：只改 `pipeline.py` / `tests/` / `scripts/` 时才触发，每天的数据滚动提交不会反复跑。
锁住的是「改错就让线上出事」的那几块：数据闸门（覆盖率不足不许发布）、合理性校验、分片合并、异动榜规则、断线自愈、上一版备份、每日留档。

