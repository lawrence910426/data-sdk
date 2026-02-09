import os
import sys
import time
import pandas as pd
from FinMind.data import DataLoader

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