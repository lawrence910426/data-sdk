# data_sdk.crawlers.warrant — Taiwan warrant terms, point in time

Crawls TWSE MOPS (公開資訊觀測站) and builds a warrant database whose terms can be
looked up *as of any date*, which is what Black-Scholes needs: a warrant's strike,
exercise ratio and maturity all move during its life.

## Layers

Row counts below are from the 2026-09-14 build: 650,559 warrants (expiries
2003-01-22 to 2028-09-15), 968,175 history rows.

```
<cache-dir>/
  mops_raw/mops_raw/            layer 0a — dlt crawl output
    warrant_basic_info/           t90sb01 expired warrants, incremental on exercise_end_date
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

$DATA_SDK_FINMIND_WARRANTS_PATH/ layer 0c — frozen FinMind warrant summary, read-only
  warrant_summary_*.parquet          (default /mnt/nfs/backup/finmind_warrants)
```

Paths follow the data_sdk convention: `$DATA_SDK_WARRANT_CACHE_PATH`
(default `/mnt/nfs/backup/warrant_history`), `$DATA_SDK_TEJ_WARRANTS_PATH`
(default `/mnt/nfs/backup/tej_warrants`) and `$DATA_SDK_FINMIND_WARRANTS_PATH`
(default `/mnt/nfs/backup/finmind_warrants`). Every CLI takes `--cache-dir` to
override. The newest file matching each glob is used, so dropping a fresher
export is all it takes to extend a seed. The FinMind seed is
`WarrantInfoWrapper.get_warrant_summary()`'s parquet, copied in and dated.

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
the time". On the other side, rows start at the *issue* date, 2-4 days before
`list_date`; there are no quotes to join in that gap, but add
`pl.col('date') >= pl.col('list_date')` if the input can carry such dates.

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
| `effective_date` | datetime | 2,476 | 此狀態生效日，as-of join 用這欄。issuance 列是**發行日**，比 `list_date` 早 2-4 天；重設型權證的重設多半在上市前一天，也照實際日期放。要「可交易」語意請自己加 `list_date <= date`（wrapper 的 `as_of` 已有）。`expiry_change` 例外：填的是**公告日**，不是新到期日生效那天。空值 = 該檔 `list_date` 未知（見下） |
| `sequence` | int | 0 | 同一天多個事件的排序。`0` = 合成的 issuance、`1` = 一般、`2`+ = TEJ 原本的序號 |
| **狀態欄（as-of 取值）** | | | |
| `strike` | float | 0 | 履約價。重設型權證此欄為**重設後**的值 |
| `ratio` | float | 0 | 行使比例，每單位權證可換股數（= `alloc_qty_per_1k / 1000`） |
| `cap` | float | 966,705 | 上限價。只有牛熊証與 43 檔展延型有值 |
| `floor` | float | 966,947 | 下限價。同上族群。展延型的**上限**價被 MOPS 填在這欄，是它自己的欄位語意問題，不是解析錯誤 |
| `exercise_end_date` | datetime | 0 | 到期日／履約截止日（台灣權證兩者同日，歐式權證實測 100% 相等）。提前到期時會變動 |
| `last_trade_date` | datetime | 72 | 最後交易日，隨到期日一起變動。72 列空值是 2003-04 MOPS 本身空白 |
| **來源欄** | | | |
| `event_type` | str | 0 | `issuance` 648,400 · `change` 314,771 · `expiry_change` 2,869 · `adjustment` 1,566 · `snapshot_diff` 497 · `reset` 72 |
| `source` | str | 0 | `tej` 717,018 · `dim_synthesised` 246,153 · `mops_announcement` 2,869 · `mops_strike` 1,638 · `mops_snapshot` 497 |
| `source_rank` | int | 0 | 兩個來源描述同一時點時的優先序：1 TEJ、2 公告、3 MOPS 事件、4 合成、5 snapshot diff |
| `is_current` | bool | 0 | 是否為該檔最後一列。每檔恰好一列為 True |
| **靜態欄（同一檔每列相同）** | | | |
| `issuer` | str | 0 | 發行商，如 `凱基` |
| `type` | str | 20 | `認購` 834,577 · `認售` 133,578。20 列空值是 2003-04 MOPS 本身空白 |
| `target_stock_id` | str | 0 | 標的代號。指數權證是 `IX0001`，ETF 是 `00xxx` |
| `target_name` | str | 0 | 標的名稱 |
| `market` | str | 0 | `twse` 740,491 · `otc` 227,684 |
| `list_date` | datetime | 2,476 | 上市日。MOPS 壞成 2023-12-26 的 51,537 檔（幾乎全是 2011-2019 到期的上櫃權證）：19,932 用 TEJ 修、48,647 用 FinMind 修、2,476 兩邊都沒有 → 空值。來源標在 dim 表的 `list_date_source` |
| `exercise_start_date` | datetime | 2,072 | 履約開始日。美式等於 `list_date`，歐式等於 `exercise_end_date`。空值同上 |
| `original_strike` | float | 0 | 發行時履約價，**重設前**的值。重設型權證不能拿來當可交易的履約價，要用 `strike` |
| `is_bull_bear` | bool | 0 | 牛證/熊證標記，1,748 檔。研究時排除 |
| `is_american` | bool | 2,476 | 美式（上市日起可履約）vs 歐式（只能到期日履約）。`list_date` 未知時為空值。**沒有任何來源對全母體標示這件事**（`t90sb01` 無此欄、交易所 OpenAPI 也沒有、TEJ 的 `權證類型` 與 `t95sb02` 的 `exercise_method` 只涵蓋部分），所以由日期推導：`exercise_start_date == list_date`。對 TEJ 有涵蓋的 418,527 檔驗證 100% 吻合、零例外 |

`warrant_basic_info.parquet`（dim 表）是一檔一列的現況表，靜態欄相同，另有現行的
`latest_strike` / `alloc_qty_per_1k` / `exercise_end_date`，以及三個來源標記
`list_date_source`、`exercise_start_date_source`、`term_source`。

## Refreshing

```bash
bash update.sh daily   # snapshot + events → build → validate → verify  (~3 min)
bash update.sh full    # also sweeps newly expired warrants into warrant_basic_info
```

`warrant_basic_info`'s sweep is keyed on 到期日 with the cursor on
`exercise_end_date`, so `full` re-reads only the current year's window. A
from-scratch backfill (`--history-start-year 2003`) must be run in chunks with
`--history-end-year`: MOPS drops the connection after ~30 minutes and dlt
discards the whole package on failure. With the dlt state on NFS the cleanup
step also intermittently fails with `Directory not empty`; crawling a chunk
into a local `--cache-dir` and copying the parquet into
`mops_raw/mops_raw/warrant_basic_info/` works around it (the build reads every
file there and de-duplicates).

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
| `warrant_active_snapshot` | every live warrant's current expiry / last trade date / latest terms | `warrant_basic_info` records a warrant only once it has expired (swept by 到期日, cursor on `exercise_end_date`), so live warrants would be absent and, once recorded, never re-read |
| TEJ basic-info export | repairs `list_date`, `exercise_start_date` | MOPS itself serves ~51,500 rows with both set to the literal 2023-12-26 |
| FinMind warrant summary | the same repair for the ~48,700 corrupted rows expired before TEJ's window (nearly all OTC, 2011-2019) | Their listing date would be null. Matched on code + last trading day; agrees with TEJ to the day on 97.7% of the 19,687 warrants all three sources share |
| `warrant_strike_ratio_adjustment/_reset` | strike history after the frozen seed | History would stop at the seed date |

The key is `(warrant_id, warrant_name)`, where `warrant_id` is the listing code
with its recycling suffix stripped (MOPS `703055b` and TEJ `703055Y` are the same
warrant; `03001T` is already 6 characters and is left alone; pre-2005 codes
are 4 digits, `0680a` -> `0680`). It is also what
joins the sources together: `exercise_end_date` shifts on early termination and
`list_date` is the field being repaired, so the name is the only other stable
identifier both vendors share.

## Known limits

- **Before 2020-01-02**: no strike history. Warrants that expired before the TEJ
  window get a single synthesised issuance row (245,026 rows carry
  `source = dim_synthesised`) at the *issuance* strike; for the 41% of them
  whose strike later moved, the final strike is in `warrant_basic_info` but no
  date for the move exists anywhere. A further ~2,000 have their first TEJ row
  already post-change rather than an issuance.
- **2,476 warrants with no listing date**: MOPS corrupted it and neither TEJ
  nor FinMind has the warrant (mostly expired 2005-2012). Their history row is
  undated, so they never appear in an as-of lookup; they do appear in the
  current view.
- **2003-2004**: MOPS's oldest rows are patchy -- 20 lack a type, 72 lack a
  last trading day, 4-digit codes (`0680`). Kept as served.
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
