# data-sdk

## Integration w/ Jupyter

In your first block, you are required to install this repo via pip.
```
!pip install git+https://github.com/lawrence910426/data-sdk.git --force-reinstall
```

Then, you may now import the classes and use the data.
```python
from data_sdk import (
    FinMindWrapper,
    ShioajiWrapper,
    TEJWrapper,
    WarrantInfoWrapper,
    get_order_book_odd_lots,
    get_order_book_stocks,
    get_order_book_warrant,
)
from pathlib import Path

shioaji_wrapper = ShioajiWrapper()
finmind_wrapper = FinMindWrapper()

df_ob = get_order_book_stocks("2026-01-02", is_twse=True, sid="2330")
df_odd = get_order_book_odd_lots("2026-01-02", is_twse=True, sid="2330")
df_warrant = get_order_book_warrant("2026-01-02", is_twse=True)
df_w_sid   = get_order_book_warrant("2026-01-02", is_twse=True, sid="700339")
```

## Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/lawrence910426/data-sdk.git
    cd data-sdk
    ```

2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3.  Install the package:
    ```bash
    pip install .
    ```

## Configuration

Set the following environment variables to configure the SDK:

### Cache Storage Paths
These variables govern where downloaded data is cached and read from.
If not set, they default to the current working directory, except
`DATA_SDK_TEJ_CACHE_PATH`, which defaults to `/mnt/nfs/backup/tej_cache`.

```bash
export DATA_SDK_FINMIND_BROKER_PATH="/mnt/nfs/backup/finmind_broker"
export DATA_SDK_SHIOAJI_TICKS_PATH="/mnt/nfs/backup/shioaji_ticks"
export DATA_SDK_SHIOAJI_FUTURES_TICKS_PATH="/mnt/nfs/backup/shioaji_futures_ticks"
export DATA_SDK_ORDER_BOOK_PARQUET_PATH="/mnt/nfs/backup/parquets"
export DATA_SDK_TEJ_CACHE_PATH="/mnt/nfs/backup/tej_cache"
```

### API Keys
Required for fetching data from FinMind, Shioaji and TEJ.

```bash
export FINMIND_API_TOKEN=your_token
export SHIOAJI_API_KEY=your_api_key
export SHIOAJI_SECRET_KEY=your_secret_key
export TEJ_API_TOKEN=your_tej_token
```

## Usage

### Wrappers

```python
from data_sdk import (
    FinMindWrapper,
    ShioajiWrapper,
    TEJWrapper,
    WarrantInfoWrapper,
    get_order_book_odd_lots,
    get_order_book_stocks,
    get_order_book_warrant,
)
from pathlib import Path

# Get FinMind broker data (downloads if missing)
finmind = FinMindWrapper()
df = finmind.get_broker("2024-01-02", "2330")

# Get Shioaji order book data (downloads if missing)
shioaji = ShioajiWrapper()
df_ticks = shioaji.get_order_book("2024-01-02", "2330")

# Get Shioaji futures ticks (downloads if missing; code = product, continuous
# near-month alias, or month symbol)
df_fut = shioaji.get_futures_ticks("2024-08-01", "CDFR1")
df_contracts = shioaji.get_futures_contracts()  # currently-listed contracts only

# TEJ monthly revenue announcements (TWN/EWSALE, incrementally cached as CSV;
# optionally set TEJ_API_TOKEN / DATA_SDK_TEJ_CACHE_PATH). Columns: coid,
# mdate (revenue month), annd_s (announcement date), d0001/d0002/d0003
# (revenue, prior-year revenue, YoY %)
tej = TEJWrapper()
df_rev = tej.get_ewsale()                        # from 2021-01-01 (subscription floor)
df_rev = tej.get_ewsale(min_date="2023-01-01")   # custom start for a cold fetch

# Fetch Taiwan warrant metadata (cached to disk, requires FINMIND_API_TOKEN)
warrant_info = WarrantInfoWrapper(cache_dir=Path("/tmp/warrant_cache"))
df_summary = warrant_info.get_warrant_summary()   # all warrants with strike/expiry
df_names   = warrant_info.get_warrant_names()     # warrant code → stock_name
issuer_map = warrant_info.build_issuer_map()      # {warrant_id: issuer_name}

# Order book from parquet (requires DATA_SDK_ORDER_BOOK_PARQUET_PATH)
df_ob = get_order_book_stocks("2026-01-02", is_twse=True, sid="2330")
df_ob_day = get_order_book_stocks("2026-01-02", is_twse=True)  # entire day
df_odd = get_order_book_odd_lots("2026-01-02", is_twse=True, sid="2330")
df_warrant = get_order_book_warrant("2026-01-02", is_twse=True)           # all warrants
df_w_sid   = get_order_book_warrant("2026-01-02", is_twse=True, sid="700339")  # single warrant
```

### Use LazyFrame for parquet reads (recommended)

When reading order-book parquet files, prefer `lazy=True` so the SDK returns a Polars `LazyFrame`.
This is recommended because it can significantly reduce parquet I/O by pushing filters/projections down to the parquet scan before materialization.

```python
from data_sdk import get_order_book_stocks
import polars as pl

# Return LazyFrame instead of eager pandas DataFrame
lf = get_order_book_stocks("2026-01-02", is_twse=True, lazy=True)

# Apply additional filters/columns lazily, then collect only when needed
df = (
    lf.filter(
        (pl.col("stock_code") == "2330")
        & (pl.col("match_time") > "13:25:00.00000")
    )
      .select(["match_time", "stock_code", "bid_price", "ask_price"])
      .collect()
      .to_pandas()
)
```

If you already know the stock id, you can also pass `sid` directly to reduce scanned data:

```python
lf = get_order_book_stocks("2026-01-02", is_twse=True, sid="2330", lazy=True)
df = lf.collect().to_pandas()
```

### Crawlers

```python
from data_sdk.crawlers import get_intraday_lending_info

df_combined = get_intraday_lending_info("2024-01-02")
```

See `examples/example_crawler.py` for a complete example.
