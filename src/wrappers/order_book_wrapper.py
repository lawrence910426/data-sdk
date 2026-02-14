import os
import sys
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import polars as pl

def _order_book_base_path() -> Path:
    base = Path(os.environ.get("DATA_SDK_ORDER_BOOK_PARQUET_PATH", "."))
    if not os.environ.get("DATA_SDK_ORDER_BOOK_PARQUET_PATH"):
        print(
            "Warning: DATA_SDK_ORDER_BOOK_PARQUET_PATH not set. Using current directory.",
            file=sys.stderr,
        )
    return base


def _format_match_time(df_lazy: pl.LazyFrame) -> pl.LazyFrame:
    mt = (
        pl.col("match_time")
        .cast(pl.Int64)
        .cast(pl.Utf8)
        .str.zfill(12)
    )
    return df_lazy.with_columns(
        pl.concat_str(
            [
                mt.str.slice(0, 2), pl.lit(":"),
                mt.str.slice(2, 2), pl.lit(":"),
                mt.str.slice(4, 2), pl.lit("."),
                mt.str.slice(6),
            ]
        ).alias("match_time")
    )

def get_order_book_stocks(
    day: str,
    is_twse: bool,
    sid: Optional[str] = None,
    lazy: bool = False,
) -> Union[pd.DataFrame, pl.LazyFrame]:
    """Load order book (regular lots) from parquet for a given day, optionally filtered by stock.

    Parquet path is taken from DATA_SDK_ORDER_BOOK_PARQUET_PATH (e.g. /mnt/nfs/backup/parquets).
    Files are named twseudp_YYYYMMDD_0825_all.parquet (TWSE) or otcudp_YYYYMMDD_0825_all.parquet (OTC).

    Args:
        day: Trading day, e.g. "2026-02-11" or "20260211".
        is_twse: True for TWSE, False for OTC.
        sid: Stock code. If None, returns the entire day for all stocks.
        lazy: If True, return a polars LazyFrame; otherwise collect and return pandas DataFrame.

    Returns:
        pandas DataFrame or polars LazyFrame with match_time formatted as HH:MM:SS.mmm.
    """
    base = _order_book_base_path()
    day_plain = day.replace("-", "")
    prefix = "twseudp" if is_twse else "otcudp"
    path = base / f"{prefix}_{day_plain}_0825_all.parquet"

    df_lazy = pl.scan_parquet(str(path))
    if sid is not None:
        df_lazy = df_lazy.filter(pl.col("stock_code") == sid)
    df_lazy = _format_match_time(df_lazy)

    if lazy:
        return df_lazy
    return df_lazy.collect().to_pandas()


def get_order_book_odd_lots(
    day: str,
    is_twse: bool,
    sid: Optional[str] = None,
    lazy: bool = False,
) -> Union[pd.DataFrame, pl.LazyFrame]:
    """Load order book (odd lots) from parquet for a given day, optionally filtered by stock.

    Parquet path is taken from DATA_SDK_ORDER_BOOK_PARQUET_PATH.
    Files are named twse_ip5_YYYYMMDD_0825_all.parquet (TWSE) or otc_ip5_YYYYMMDD_0825_all.parquet (OTC).

    Args:
        day: Trading day, e.g. "2026-02-11" or "20260211".
        is_twse: True for TWSE, False for OTC.
        sid: Stock code. If None, returns the entire day for all stocks.
        lazy: If True, return a polars LazyFrame; otherwise collect and return pandas DataFrame.

    Returns:
        pandas DataFrame or polars LazyFrame with match_time formatted as HH:MM:SS.mmm.
    """
    base = _order_book_base_path()
    day_plain = day.replace("-", "")
    prefix = "twse_ip5" if is_twse else "otc_ip5"
    path = base / f"{prefix}_{day_plain}_0825_all.parquet"

    df_lazy = pl.scan_parquet(str(path))
    if sid is not None:
        df_lazy = df_lazy.filter(pl.col("stock_code") == sid)
    df_lazy = _format_match_time(df_lazy)

    if lazy:
        return df_lazy
    return df_lazy.collect().to_pandas()


def get_order_book_warrant(
    day: str,
    is_twse: bool,
    lazy: bool = False,
) -> Union[pd.DataFrame, pl.LazyFrame]:
    """Load order book (warrant) from parquet for a given day.

    Parquet path is taken from DATA_SDK_ORDER_BOOK_PARQUET_PATH.
    Files are named twse_warranty_YYYYMMDD_0825_all.parquet (TWSE) or otc_warranty_YYYYMMDD_0825_all.parquet (OTC).

    Args:
        day: Trading day, e.g. "2026-02-11" or "20260211".
        is_twse: True for TWSE, False for OTC.
        lazy: If True, return a polars LazyFrame; otherwise collect and return pandas DataFrame.

    Returns:
        pandas DataFrame or polars LazyFrame with match_time formatted as HH:MM:SS.mmm.
    """
    base = _order_book_base_path()
    day_plain = day.replace("-", "")
    prefix = "twse_warranty" if is_twse else "otc_warranty"
    path = base / f"{prefix}_{day_plain}_0825_all.parquet"

    df_lazy = pl.scan_parquet(str(path))
    df_lazy = _format_match_time(df_lazy)

    if lazy:
        return df_lazy
    return df_lazy.collect().to_pandas()