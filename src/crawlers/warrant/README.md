# data_sdk.crawlers.warrant — Taiwan warrant terms, point in time

Crawls TWSE MOPS (公開資訊觀測站) and builds a warrant database whose terms can be
looked up *as of any date*, which is what Black-Scholes needs: a warrant's strike,
exercise ratio and maturity all move during its life.

## Layers

Row counts below are from the 2026-09-08 build: 426,172 warrants, 705,163
history rows.

```
<cache-dir>/
  mops_raw/mops_raw/            layer 0a — dlt crawl output
    warrant_basic_info/           t90sb01, incremental on list_date
    warrant_active_snapshot/      t90sb01 current view, replaced whole each run
    warrant_strike_ratio_adjustment/   t95sb02, incremental
    warrant_strike_ratio_reset/        t95sb03, incremental
    warrant_announcement/         t95sb01 + o_t95sb01, incremental
  mops_pipeline_state/          dlt cursors
  warrant_basic_info.parquet    layer 1 — one row per warrant, current terms
  warrant_history.parquet       layer 2 — one row per state change  ← the product

$DATA_SDK_TEJ_WARRANTS_PATH/     layer 0b — frozen TEJ Pro exports, read-only
  tej_warrant_raw_*.parquet          (default /mnt/nfs/backup/tej_warrants)
  tej_warrant_adjustment_*.parquet
```

Paths follow the data_sdk convention: `$DATA_SDK_WARRANT_CACHE_PATH`
(default `/mnt/nfs/backup/warrant_history`) and `$DATA_SDK_TEJ_WARRANTS_PATH`
(default `/mnt/nfs/backup/tej_warrants`). Every CLI takes `--cache-dir` to
override. The newest export matching each glob is used, so dropping a fresher
TEJ file is all it takes to extend the seed.

## Using it

```python
history = pl.read_parquet('cache/warrant_history.parquet').sort('effective_date')

terms = (
    quotes.sort('date')
    .join_asof(history, left_on='date', right_on='effective_date',
               by=['warrant_id', 'warrant_name'], strategy='backward')
    .filter(pl.col('date') <= pl.col('exercise_end_date'))
)
```

**Key on both columns.** `warrant_id` alone is not unique — MOPS reuses a code
once the previous warrant expires, and `043693` has been six different warrants.
Joining on the code alone silently returns a predecessor's strike for any date
before the current warrant listed.

**Keep the trailing filter.** An as-of join has no idea a warrant expires and
will happily return the final terms for a date years afterwards. Filtering on
the matched row's own `exercise_end_date` is what makes the answer "as known at
the time". The other side needs no guard: a date before the listing matches
nothing and comes back null.

There is no `valid_to` — "the latest row at or before this date" already defines
the interval. Current terms are `history.filter(pl.col('is_current'))`, which is
what `warrant_basic_info.parquet` holds.

## `warrant_history.parquet` schema

One row per warrant per state change. Primary key `(warrant_id, warrant_name,
effective_date, sequence)`.

| column | type | null | 說明 |
|---|---|---|---|
| `warrant_id` | str | 0 | 權證代號，已去掉回收尾碼。**單獨不唯一**，見上面的 key 說明 |
| `warrant_name` | str | 0 | 權證簡稱，如 `台光電元大5B購02`。key 的另一半 |
| `effective_date` | datetime | 0 | 此狀態生效日，as-of join 用這欄。`expiry_change` 例外：填的是**公告日**，不是新到期日生效那天 |
| `sequence` | int | 0 | 同一天多個事件的排序。`0` = 合成的 issuance、`1` = 一般、`2`+ = TEJ 原本的序號 |
| **狀態欄（as-of 取值）** | | | |
| `strike` | float | 0 | 履約價。重設型權證此欄為**重設後**的值 |
| `ratio` | float | 0 | 行使比例，每單位權證可換股數（= `alloc_qty_per_1k / 1000`） |
| `cap` | float | 704,830 | 上限價。只有牛熊証與 43 檔展延型有值 |
| `floor` | float | 704,707 | 下限價。同上族群。展延型的**上限**價被 MOPS 填在這欄，是它自己的欄位語意問題，不是解析錯誤 |
| `exercise_end_date` | datetime | 0 | 到期日／履約截止日（台灣權證兩者同日，歐式權證實測 100% 相等）。提前到期時會變動 |
| `last_trade_date` | datetime | 0 | 最後交易日，隨到期日一起變動 |
| **來源欄** | | | |
| `event_type` | str | 0 | `issuance` 388,626 · `change` 313,301 · `expiry_change` 2,817 · `adjustment` 387 · `reset` 32 |
| `source` | str | 0 | `tej` 680,115 · `dim_synthesised` 21,812 · `mops_announcement` 2,817 · `mops_strike` 419 |
| `source_rank` | int | 0 | 兩個來源描述同一時點時的優先序：1 TEJ、2 公告、3 MOPS 事件、4 合成 |
| `is_current` | bool | 0 | 是否為該檔最後一列。每檔恰好一列為 True |
| **靜態欄（同一檔每列相同）** | | | |
| `issuer` | str | 0 | 發行商，如 `凱基` |
| `type` | str | 0 | `認購` 616,948 · `認售` 88,215 |
| `target_stock_id` | str | 0 | 標的代號。指數權證是 `IX0001`，ETF 是 `00xxx` |
| `target_name` | str | 0 | 標的名稱 |
| `market` | str | 0 | `twse` 541,509 · `otc` 163,654 |
| `list_date` | datetime | 0 | 上市日。MOPS 壞成 2023-12-26 的那批已用 TEJ 修正 |
| `exercise_start_date` | datetime | 0 | 履約開始日。美式等於 `list_date`，歐式等於 `exercise_end_date` |
| `original_strike` | float | 0 | 發行時履約價，**重設前**的值。重設型權證不能拿來當可交易的履約價，要用 `strike` |
| `is_bull_bear` | bool | 0 | 牛證/熊證標記，746 檔。研究時排除 |

`warrant_basic_info.parquet`（dim 表）是一檔一列的現況表，靜態欄相同，另有現行的
`latest_strike` / `alloc_qty_per_1k` / `exercise_end_date`，以及三個來源標記
`list_date_source`、`exercise_start_date_source`、`term_source`。

## Refreshing

```bash
bash update.sh cache   # crawl (incremental) → build → validate → verify
```

`validate.py` is offline and checks internal consistency. `verify_openapi.py`
fetches the TWSE and TPEx OpenAPI and compares the current strike, ratio and
expiry of every live warrant against an independent publication of the same
facts — the check that catches a stale or misparsed number rather than an
inconsistent one. It exits non-zero below 98% on any field.

Curated tables are pure functions of the raw layer and are rebuilt whole each
run, so there is no build state to keep in sync. The TEJ seed is never touched.

**Never delete `mops_raw/warrant_strike_ratio_*`.** MOPS serves only a rolling
~18 months of those reports, so the accumulated append-only copy is the only
record of anything older.

## Why each source exists

| Source | Supplies | Without it |
|---|---|---|
| TEJ adjustment export | strike/ratio history, 2020-01-02 to the export date | MOPS's own t95sb02 covers 5% of warrants — 229k warrants would show a flat strike that demonstrably moved |
| `warrant_announcement` | expiry changes, dated by *announcement* | An early-terminated warrant would look alive until its original maturity, mispricing every day after the announcement |
| `warrant_active_snapshot` | current expiry / last trade date / latest terms | `warrant_basic_info` is incremental on `list_date`, so an already-listed warrant is never re-read and its mutable fields freeze at first-crawl values |
| TEJ basic-info export | repairs `list_date`, `exercise_start_date` | MOPS itself serves ~19,889 rows with both set to the literal 2023-12-26 |
| `warrant_strike_ratio_adjustment/_reset` | strike history after the frozen seed | History would stop at the seed date |

The key is `(warrant_id, warrant_name)`, where `warrant_id` is the listing code
with its recycling suffix stripped (MOPS `703055b` and TEJ `703055Y` are the same
warrant; `03001T` is already 6 characters and is left alone). It is also what
joins the sources together: `exercise_end_date` shifts on early termination and
`list_date` is the field being repaired, so the name is the only other stable
identifier both vendors share.

## Known limits

- **Before 2020-01-02**: no strike history. Warrants that expired before the TEJ
  window get a single synthesised issuance row (21,812 rows carry
  `source = dim_synthesised`), and a further ~2,000 have their first TEJ row
  already post-change rather than an issuance.
- **Extensions (展延)**: not modelled. The announcement document names an
  ambiguous date — `03029X`'s 2026-09-07 extension names 2026-03-04, six months
  in the past — so extension rows are dropped rather than guessed at. Affects
  only the 43 extendable warrants, which studies exclude anyway.
- **Bull/bear certificates (牛證/熊證)**: 746 warrants, flagged `is_bull_bear`.
  TEJ never covers them, so their history is MOPS-only.
- **Cap/floor**: not carried on history rows. Outside bull/bear only 43 warrants
  have them.
- **Early terminations before ~2026-01**: `t95sb01` keeps a rolling ~8 months, so
  roughly 3,000 historical early terminations have no announcement date. Their
  history shows the original maturity until the final row.

- **Leading-edge drift**: the exchange OpenAPI is occasionally fresher than the
  MOPS snapshot, so ~48 live warrants (0.09%) carry a strike one adjustment
  behind until the next crawl. `verify_openapi.py` is what surfaces these.

`validate.py` and `verify_openapi.py` check all of the above that can be checked;
`DATA_DICTIONARY.md` holds the field-by-field semantics and the measured gaps
behind these choices.

## Layout

```
data-sdk/src/crawlers/warrant/     imports as `data_sdk.crawlers.warrant`
  warrant_reports.py   all crawling — session, ROC parsing, five dlt resources
  __main__.py          the crawl CLI
  build_basic_info.py  layer 1
  build_history.py     layer 2
  validate.py          raw and curated checks (offline)
  verify_openapi.py    cross-check against TWSE/TPEx OpenAPI (online)
  update.sh            crawl → build → validate → verify
```

Ships as part of `data-sdk`; a consuming repo installs it with
`pip install -e ../data-sdk`.
