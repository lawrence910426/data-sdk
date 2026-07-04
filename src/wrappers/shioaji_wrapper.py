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

    def _download_ticks_to_parquet(self, day, sid, dest_path: Path, data_dir: Path) -> pd.DataFrame:
        if ShioajiWrapper._api is None:
            raise RuntimeError("Shioaji API is not initialized.")

        ticks = ShioajiWrapper._api.ticks(
            contract=ShioajiWrapper._api.Contracts.Stocks[sid],
            date=str(day),
        )
        print(f"Downloading {sid} {day}")
        print(ShioajiWrapper._api.usage())
        df = pd.DataFrame({**ticks})
        data_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(dest_path)
        return df

    def get_order_book(self, day, sid):
        # Determine parquet path
        data_dir = Path(os.environ.get("DATA_SDK_SHIOAJI_TICKS_PATH", "."))
        if not os.environ.get("DATA_SDK_SHIOAJI_TICKS_PATH"):
            print("Warning: DATA_SDK_SHIOAJI_TICKS_PATH not set. Using current directory.", file=sys.stderr)

        dest_path = data_dir / f"{sid}_{day}.parquet"

        if dest_path.exists():
            if dest_path.stat().st_size == 0:
                dest_path.unlink(missing_ok=True)
            else:
                try:
                    return pd.read_parquet(dest_path)
                except Exception:
                    dest_path.unlink(missing_ok=True)

        return self._download_ticks_to_parquet(day, sid, dest_path, data_dir)

    def _download_futures_ticks_to_parquet(self, day, code, dest_path: Path, data_dir: Path) -> pd.DataFrame:
        if ShioajiWrapper._api is None:
            raise RuntimeError("Shioaji API is not initialized.")

        contract = ShioajiWrapper._api.Contracts.Futures[code]
        if contract is None:
            raise KeyError(f"Unknown futures contract: {code}")
        ticks = ShioajiWrapper._api.ticks(
            contract=contract,
            date=str(day),
        )
        print(f"Downloading futures {code} {day}")
        print(ShioajiWrapper._api.usage())
        df = pd.DataFrame({**ticks})
        data_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(dest_path)
        return df

    def get_futures_ticks(self, day, code):
        """Ticks for one futures contract on one day, cached as parquet.

        ``code`` is a Shioaji futures contract code: a product ("TXF"), a
        continuous near-month alias ("CDFR1"), or a month symbol ("CDF202609").
        An empty tick day is cached as an empty parquet, so it is not
        re-downloaded.
        """
        # Determine parquet path
        data_dir = Path(os.environ.get("DATA_SDK_SHIOAJI_FUTURES_TICKS_PATH", "."))
        if not os.environ.get("DATA_SDK_SHIOAJI_FUTURES_TICKS_PATH"):
            print("Warning: DATA_SDK_SHIOAJI_FUTURES_TICKS_PATH not set. Using current directory.", file=sys.stderr)

        dest_path = data_dir / f"{code}_{day}.parquet"

        if dest_path.exists():
            if dest_path.stat().st_size == 0:
                dest_path.unlink(missing_ok=True)
            else:
                try:
                    return pd.read_parquet(dest_path)
                except Exception:
                    dest_path.unlink(missing_ok=True)

        return self._download_futures_ticks_to_parquet(day, code, dest_path, data_dir)

    def get_futures_contracts(self) -> pd.DataFrame:
        """One row per listed futures contract.

        Columns: code, symbol, name, category, delivery_month, delivery_date,
        underlying_kind, underlying_code, unit.

        Shioaji downloads the live contract file at login, so this covers
        currently-listed contracts only; expired or delisted contracts are
        absent.
        """
        if ShioajiWrapper._api is None:
            raise RuntimeError("Shioaji API is not initialized.")

        # The contract file downloads asynchronously after login; __getitem__
        # blocks until it is complete, plain iteration does not. Index once so
        # the loop below sees the full contract set.
        ShioajiWrapper._api.Contracts.Futures["TXF"]

        fields = ["code", "symbol", "name", "category", "delivery_month",
                  "delivery_date", "underlying_kind", "underlying_code", "unit"]
        rows = []
        for product in ShioajiWrapper._api.Contracts.Futures:
            for contract in product:
                rows.append({f: getattr(contract, f, None) for f in fields})
        return pd.DataFrame(rows, columns=fields)