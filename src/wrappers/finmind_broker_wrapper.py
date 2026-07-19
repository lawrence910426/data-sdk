import os
import sys
import time
import pandas as pd
import requests
from FinMind.data import DataLoader

FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"

class FinMindWrapper:
    _api = None
    _ref_count = 0

    def __init__(self):
        if FinMindWrapper._api is None:
            token = os.environ.get("FINMIND_API_TOKEN")
            if not token:
                print("Warning: FINMIND_API_TOKEN not set. Download may fail.", file=sys.stderr)
            
            FinMindWrapper._api = DataLoader()
            if token:
                FinMindWrapper._api.login_by_token(api_token=token)
        FinMindWrapper._ref_count += 1

    def __del__(self):
        FinMindWrapper._ref_count -= 1
        if FinMindWrapper._ref_count == 0:
            FinMindWrapper._api = None

    def _download_broker(self, day, output_dir):
        """Download broker data for a specific day and save to parquet."""
        print(f"[{day}] Downloading broker data...")
        try:
            df = FinMindWrapper._api.taiwan_stock_trading_daily_report(date=day, use_async=True)
            if df is None or (hasattr(df, "empty") and df.empty):
                 raise ValueError(f"Empty result for {day}")
            
            os.makedirs(output_dir, exist_ok=True)
            path = os.path.join(output_dir, f"{day}.parquet")
            df.to_parquet(path, index=False)
            print(f"[{day}] saved {path}")
            return path
        except Exception as e:
            raise RuntimeError(f"Download failed for {day}: {e}")

    def get_broker(self, day, sid):
        """Read broker data for (day, sid). Downloads if missing."""
        output_dir = os.environ.get("DATA_SDK_FINMIND_BROKER_PATH", ".")
        if not os.environ.get("DATA_SDK_FINMIND_BROKER_PATH"):
             print("Warning: DATA_SDK_FINMIND_BROKER_PATH not set. Using current directory.", file=sys.stderr)

        path = f"{output_dir}/{day}.parquet"
        if not os.path.isfile(path):
            try:
                self._download_broker(day, output_dir)
            except Exception as e:
                raise FileNotFoundError(
                    f"FinMind broker file not found: {path} and download failed: {e}"
                )
                
        sid_str = str(sid)
        out = pd.read_parquet(path, filters=[("stock_id", "==", sid_str)])
        return out.reset_index(drop=True)

    # ------------------------------------------------------------------
    # Generic FinMind dataset access (same cache-then-read idea as get_broker).
    # Downloads a dataset over [start_date, end_date] (whole market unless
    # data_id is given), caches to parquet, returns a DataFrame.
    # ------------------------------------------------------------------
    def cache_dir(self):
        d = os.environ.get("DATA_SDK_FINMIND_CACHE_PATH")
        if not d:
            print("Warning: DATA_SDK_FINMIND_CACHE_PATH not set. Using current directory.", file=sys.stderr)
            d = "."
        return d

    def download_dataset(self, dataset, start_date, end_date, data_id):
        token = os.environ.get("FINMIND_API_TOKEN", "")
        params = {"dataset": dataset, "start_date": start_date,
                  "end_date": end_date, "token": token}
        if data_id:
            params["data_id"] = data_id
        for attempt in range(6):
            r = requests.get(FINMIND_API_URL, params=params, timeout=60)
            if r.status_code == 402 or (r.ok and r.json().get("status") == 402):
                # rate limited: FinMind resets the hourly quota; back off and retry
                print(f"[{dataset} {start_date}] rate limited, sleeping 60s...", file=sys.stderr)
                time.sleep(60)
                continue
            r.raise_for_status()
            body = r.json()
            if body.get("status") != 200:
                raise RuntimeError(f"FinMind error {body.get('status')}: {body.get('msg')}")
            return pd.DataFrame(body.get("data", []))
        raise RuntimeError(f"FinMind still rate limited after retries: {dataset} {start_date}")

    def get_dataset(self, dataset, start_date, end_date=None, data_id=None):
        """Download any FinMind dataset for [start_date, end_date], cache, return DataFrame.

        Whole-market when data_id is None. Cached under
        {DATA_SDK_FINMIND_CACHE_PATH}/{dataset}/{start}_{end}[_{data_id}].parquet
        so a backtest loop pays each (dataset, range) download only once.
        """
        end_date = end_date or start_date
        cache_dir = os.path.join(self.cache_dir(), dataset)
        key = f"{start_date}_{end_date}" + (f"_{data_id}" if data_id else "")
        path = os.path.join(cache_dir, f"{key}.parquet")
        if not os.path.isfile(path):
            df = self.download_dataset(dataset, start_date, end_date, data_id)
            os.makedirs(cache_dir, exist_ok=True)
            df.to_parquet(path, index=False)
        return pd.read_parquet(path)

    def get_price(self, day, sid=None):
        """Whole-market daily OHLCV for one day (TaiwanStockPrice), cached per day."""
        df = self.get_dataset("TaiwanStockPrice", day)
        return df if sid is None else df[df["stock_id"] == str(sid)].reset_index(drop=True)

    def get_margin_short(self, day, sid=None):
        """信用額度總量管制餘額表 (TaiwanStockMarginPurchaseShortSale) for one day.

        A stock appearing here on `day` is credit-eligible (融資融券) that day;
        ShortSaleLimit > 0 means short selling is permitted.
        """
        df = self.get_dataset("TaiwanStockMarginPurchaseShortSale", day)
        return df if sid is None else df[df["stock_id"] == str(sid)].reset_index(drop=True)

    def get_short_suspension(self, start_date, end_date=None, sid=None):
        """暫停融券賣出表(融券回補日) (TaiwanStockMarginShortSaleSuspension).

        Rows have (stock_id, date, end_date, reason): short selling is suspended
        for that stock over [date, end_date]. Fetched over a range in one call.
        """
        df = self.get_dataset("TaiwanStockMarginShortSaleSuspension", start_date, end_date)
        return df if sid is None else df[df["stock_id"] == str(sid)].reset_index(drop=True)