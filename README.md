# signal-data · signal 的行情数据管线

用 [eltdx](https://github.com/electkismet/eltdx)（通达信 7709 协议，Rust 引擎）抓 A 股行情，产出 [signal](https://github.com/zentaox/signal) 前端能直接读的 JSON。

三个仓库的分工：

```
eltdx（数据源，别人的库）  →  本仓库 pipeline.py  →  out/*.json  →  signal（展示层）
```

不改 signal 的前端代码，也不动 eltdx 的库，只把"行情这一层"补上。

## 产出

| 文件 | 内容 | 与 signal 的关系 |
|---|---|---|
| `out/px.json` | 全市场 A 股现价 + 当日涨跌幅，**schema 与 `signal/data/px.json` 完全一致** | 可直接替换 |
| `out/indices.json` | 上证 / 创业板 / 科创50 / 北证50 的涨跌幅 + 距近一年高点 | 可作为 `signal/data/market.json` 里 `indices` 的来源 |
| `out/stats.json` | 本次抓取自检：时间、耗时、股票数、指数数、eltdx 版本 | — |

`px.json` 的格式：

```json
{"at": "2026-11-08 08:25", "px": {"000001": [11.81, 1.46], "000002": [3.24, -2.7]}}
```

即 `代码: [现价, 当日涨跌幅%]`。

## 实测对齐情况

以 2026-11-06 收盘数据与 signal 现有 `data/px.json` 逐只核对：

| 指标 | 结果 |
|---|---|
| 我们的有效股票数 | **5575** 只（停牌/零价 6 只已丢弃） |
| 与上游共有的代码 | 5570 只 |
| 其中数值**完全一致** | **5564 只（99.89%）** |
| 仅上游有、我们没有 | 602 只（主要是退市/长期停牌股，如上方的 `000003`） |
| 全市场抓取耗时 | **约 3 秒**（70 批快照，8 线程并发） |
| 四大指数 | 涨跌幅与 signal 完全一致（-1.10 / -3.96 / -2.60 / -2.37） |

> 顺便发现：signal 现在 `market.json` 里北证50 的"距高点"写成 **-1.3%**，而按 250 日日线高点算是 **-45.1%**，与其它三个指数（-14 ~ -44）明显不同量级，疑似上游计算有 bug。本管线输出的 `from_high` 是按日 K 实算的。

## 怎么用

**方式一：直接把 `px.json` 放进 signal 的 `data/`**
schema 完全一样，`cp out/px.json <signal仓库>/data/px.json` 即可，前端不用改一行代码。

**方式二：前端直接 fetch 本仓库的 raw 地址**
raw.githubusercontent.com 允许跨域（`Access-Control-Allow-Origin: *`），signal 站点可以在 `load('px.json')` 时改指向：

```
https://raw.githubusercontent.com/tel9980/signal-data/main/out/px.json
```

**方式三（推荐长期）：只把指数接进 `market.json`**
自己维护 `indices` 字段，`pool / seats / audit` 这些 eltdx 拿不到的数据继续用原来的源。

## 能覆盖多少？诚实版

eltdx 只有通达信行情/F10 协议，所以：

| signal 的数据 | 本管线 |
|---|---|
| `px.json` 现价（3323 只 → 我们 5575 只） | ✅ 完全覆盖 |
| `market.json` 的 `indices` | ✅ 完全覆盖 |
| `risk0–9` / `s0–9`（逐股风险，需 K线·分时·逐笔） | ✅ 可按同样方式算（`client.bars` / `client.minutes` / `client.trades`） |
| `themes` / `tags` 部分题材 | ⚠️ 部分（7615 F10 题材） |
| `pool.json` 机构季报持仓 7858 行 | ❌ 不是行情数据，拿不到 |
| `seats.json` 龙虎榜席位 | ❌ 拿不到 |
| `audit.json` 定增/重组审核进度 | ❌ 拿不到 |

即：**行情层约 1/3 可以替代**，其余仍需原来的非行情数据源。这正是"不合并仓库、只做数据联动"的原因。

## 本地跑

```bash
pip install -r requirements.txt
python pipeline.py                  # 全市场，约 3 秒
python pipeline.py --limit 300      # 联调只抓 300 只
python pipeline.py --no-indices     # 只出 px.json
python pipeline.py --out-dir /tmp/x # 指定输出目录
```

## 自动更新

`.github/workflows/update.yml`：交易日 **北京时间 11:35 与 15:35** 各跑一次（cron 已换算成 UTC `35 3` / `35 7`），也可手动触发。

产物用**滚动提交**（`git commit --amend` 覆盖上一条），所以仓库历史永远是常量，不会像普通数据仓库那样每天膨胀。

## 免责

eltdx 为 Research-Only 许可，本项目仅用于个人学习与协议研究，请勿用于商业或付费服务。
