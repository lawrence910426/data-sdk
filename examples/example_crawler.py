from data_sdk.crawlers import get_intraday_lending_info
import pandas as pd

def main():
    date = "2024-01-02"
    print(f"Fetching intraday lending info for {date}...")
    try:
        df = get_intraday_lending_info(date)
        print("Data fetched successfully!")
        print(df.head())
        print(f"\nTotal records: {len(df)}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
