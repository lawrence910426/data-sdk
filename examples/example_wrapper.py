import os
import pandas as pd
from data_sdk import FinMindWrapper, ShioajiWrapper
from data_sdk.crawlers import get_twse_intraday_lending_info

def main():
    print("Fetching FinMind broker data...")
    try:
        finmind = FinMindWrapper()
        df_finmind = finmind.get_broker("2026-01-02", "2330")
        print(f"FinMind data shape: {df_finmind.shape}")
        print(df_finmind.head())
    except Exception as e:
        print(f"FinMind error: {e}")

    print("\nFetching Shioaji order book data...")
    try:
        shioaji = ShioajiWrapper()
        df_shioaji = shioaji.get_order_book("2026-01-02", "2330")
        print(f"Shioaji data shape: {df_shioaji.shape}")
        print(df_shioaji.head())
        # shioaji instance will be destroyed here or at end of scope functionality
    except Exception as e:
        print(f"Shioaji error: {e}")

if __name__ == "__main__":
    main()
