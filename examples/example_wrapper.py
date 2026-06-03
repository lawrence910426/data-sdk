import os
import pandas as pd
from pathlib import Path
from data_sdk import (
    FinMindWrapper,
    ShioajiWrapper,
    WarrantInfoWrapper,
    get_order_book_odd_lots,
    get_order_book_stocks,
    get_order_book_warrant,
)

def main():
    print("Fetching FinMind broker data...")
    try:
        finmind = FinMindWrapper()
        df_finmind = finmind.get_broker("2026-01-02", "2330")
        print(f"FinMind data shape: {df_finmind.shape}")
        print(df_finmind.head())
    except Exception as e:
        print(f"FinMind error: {e}")

    print("\nFetching order book from parquet (stocks)...")
    try:
        df_ob = get_order_book_stocks("2026-01-02", is_twse=True, sid="2330")
        print(f"Order book stocks shape: {df_ob.shape}")
        print(df_ob.head())
    except Exception as e:
        print(f"Order book stocks error: {e}")

    print("\nFetching order book from parquet (odd lots)...")
    try:
        df_odd = get_order_book_odd_lots("2026-01-02", is_twse=True, sid="2330")
        print(f"Order book odd lots shape: {df_odd.shape}")
        print(df_odd.head())
    except Exception as e:
        print(f"Order book odd lots error: {e}")

    print("\nFetching order book from parquet (warrant, single sid)...")
    try:
        df_w_sid = get_order_book_warrant("2026-06-02", is_twse=True, sid="058998")
        print(f"Order book warrant (sid) shape: {df_w_sid.shape}")
        print(df_w_sid.head())
    except Exception as e:
        print(f"Order book warrant (sid) error: {e}")

    print("\nFetching Shioaji order book data...")
    try:
        shioaji = ShioajiWrapper()
        df_shioaji = shioaji.get_order_book("2026-01-02", "2330")
        print(f"Shioaji data shape: {df_shioaji.shape}")
        print(df_shioaji.head())
    except Exception as e:
        print(f"Shioaji error: {e}")

    print("\nFetching warrant info (summary + names + issuer map)...")
    try:
        warrant_info = WarrantInfoWrapper(cache_dir=Path("/tmp/warrant_cache"))
        df_summary = warrant_info.get_warrant_summary()
        print(f"Warrant summary shape: {df_summary.shape}")
        print(df_summary.head())
        issuer_map = warrant_info.build_issuer_map()
        print(f"Issuer map sample: {str(issuer_map)[:50]}")
    except Exception as e:
        print(f"WarrantInfoWrapper error: {e}")

if __name__ == "__main__":
    main()
