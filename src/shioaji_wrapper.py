import os
import pandas as pd
import shioaji as sj
from pathlib import Path
import sys

class ShioajiWrapper:
    _api = None
    _ref_count = 0

    def __init__(self):
        if ShioajiWrapper._api is None:
            ShioajiWrapper._api = sj.Shioaji(simulation=False)
            ShioajiWrapper._api.login(
                api_key=os.environ.get("SHIOAJI_API_KEY"),
                secret_key=os.environ.get("SHIOAJI_SECRET_KEY")
            )
        ShioajiWrapper._ref_count += 1

    def __del__(self):
        ShioajiWrapper._ref_count -= 1
        if ShioajiWrapper._ref_count == 0 and ShioajiWrapper._api is not None:
            try:
                ShioajiWrapper._api.logout()
            except Exception as e:
                # API might be already closed or network issues
                pass
            ShioajiWrapper._api = None

    def get_order_book(self, day, sid):
        # Determine parquet path
        data_dir = Path(os.environ.get("DATA_SDK_SHIOAJI_TICKS_PATH", "."))
        if not os.environ.get("DATA_SDK_SHIOAJI_TICKS_PATH"):
            print("Warning: DATA_SDK_SHIOAJI_TICKS_PATH not set. Using current directory.", file=sys.stderr)
            
        dest_path = data_dir / f"{sid}_{day}.parquet"

        # Check if parquet exists
        if dest_path.exists():
            df = pd.read_parquet(dest_path)
        else:
            # Download if not exists
            if ShioajiWrapper._api is None:
                 raise RuntimeError("Shioaji API is not initialized.")

            ticks = ShioajiWrapper._api.ticks(
                contract=ShioajiWrapper._api.Contracts.Stocks[sid], 
                date=str(day)
            )
            print(f"Downloading {sid} {day}")
            print(ShioajiWrapper._api.usage())
            df = pd.DataFrame({**ticks})
            
            # Save to parquet (raw data)
            data_dir.mkdir(parents=True, exist_ok=True)
            df.to_parquet(dest_path)
        
        return df