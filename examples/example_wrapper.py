import os
import pandas as pd
from data_sdk import (
    FinMindWrapper,
    ShioajiWrapper,
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

    print("\nFetching order book from parquet (warrant)...")
    try:
        df_warrant = get_order_book_warrant("2026-01-02", is_twse=True)
        print(f"Order book warrant shape: {df_warrant.shape}")
        print(df_warrant.head())
    except Exception as e:
        print(f"Order book warrant error: {e}")

    print("\nFetching Shioaji order book data...")
    try:
        shioaji = ShioajiWrapper()
        df_shioaji = shioaji.get_order_book("2026-01-02", "2330")
        print(f"Shioaji data shape: {df_shioaji.shape}")
        print(df_shioaji.head())
    except Exception as e:
        print(f"Shioaji error: {e}")

if __name__ == "__main__":
    main()
