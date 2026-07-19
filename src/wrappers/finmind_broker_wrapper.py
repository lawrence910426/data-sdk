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
    # Generic whole-market FinMind access.
    #
    # Design: every dataset has ONE base accessor that downloads a single
    # day's whole-market snapshot (cached as
    # {DATA_SDK_FINMIND_CACHE_PATH}/{dataset}/{date}.parquet), and ONE range
    # accessor that assembles [start_date, end_date] by iterating the trading
    # calendar over the per-day cache. Dataset-specific helpers
    # (get_price / get_margin_short / get_short_suspension) simply call the
    # range accessor. The per-day cache is therefore whole-market and
    # reusable across studies and stocks.
    # ------------------------------------------------------------------
    def cache_directory(self) -> str:
        directory = os.environ.get("DATA_SDK_FINMIND_CACHE_PATH")
        if not directory:
            print("Warning: DATA_SDK_FINMIND_CACHE_PATH not set. Using current directory.", file=sys.stderr)
            directory = "."
        return directory

    def request_finmind(self, params: dict) -> pd.DataFrame:
        """One FinMind v4 REST call with retry on rate limiting (status 402)."""
        token = os.environ.get("FINMIND_API_TOKEN", "")
        for attempt in range(6):
            response = requests.get(FINMIND_API_URL, params={**params, "token": token}, timeout=60)
            if response.status_code == 402 or (response.ok and response.json().get("status") == 402):
                print(f"[FinMind {params.get('dataset')}] rate limited, sleeping 60s...", file=sys.stderr)
                time.sleep(60)
                continue
            response.raise_for_status()
            body = response.json()
            if body.get("status") != 200:
                raise RuntimeError(f"FinMind error {body.get('status')}: {body.get('msg')}")
            return pd.DataFrame(body.get("data", []))
        raise RuntimeError(f"FinMind still rate limited after retries: {params.get('dataset')}")

    def get_dataset_by_date(self, dataset: str, date: str) -> pd.DataFrame:
        """Base accessor: one day's whole-market data for any dataset, cached per day.

        An empty day is cached as an empty parquet so it is not re-downloaded.
        """
        path = os.path.join(self.cache_directory(), dataset, f"{date}.parquet")
        if not os.path.isfile(path):
            frame = self.request_finmind({"dataset": dataset, "start_date": date, "end_date": date})
            os.makedirs(os.path.dirname(path), exist_ok=True)
            frame.to_parquet(path, index=False)
        return pd.read_parquet(path)

    def get_trading_dates(self, start_date: str, end_date: str) -> list:
        """Trading calendar in [start_date, end_date] (TaiwanStockTradingDate), cached per range."""
        path = os.path.join(self.cache_directory(), "TaiwanStockTradingDate",
                            f"{start_date}_{end_date}.parquet")
        if not os.path.isfile(path):
            frame = self.request_finmind({"dataset": "TaiwanStockTradingDate",
                                          "start_date": start_date, "end_date": end_date})
            os.makedirs(os.path.dirname(path), exist_ok=True)
            frame.to_parquet(path, index=False)
        calendar = pd.read_parquet(path)
        return sorted(calendar["date"].tolist())

    def get_dataset_by_range(self, dataset: str, start_date: str, end_date: str,
                             use_trading_calendar: bool = True) -> pd.DataFrame:
        """Range accessor: whole-market data over [start_date, end_date].

        Iterates day by day and concatenates the per-day snapshots, so every day
        is downloaded at most once ever. use_trading_calendar=False iterates
        every calendar day instead — needed for datasets keyed to announcement
        dates that can fall on market holidays (e.g. a 暫停融券 window starting
        on a typhoon closure day).
        """
        if use_trading_calendar:
            dates = self.get_trading_dates(start_date, end_date)
        else:
            dates = [stamp.strftime("%Y-%m-%d")
                     for stamp in pd.date_range(start_date, end_date, freq="D")]
        frames = [self.get_dataset_by_date(dataset, date) for date in dates]
        frames = [frame for frame in frames if not frame.empty]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def get_price(self, start_date: str, end_date: str = None) -> pd.DataFrame:
        """Whole-market daily OHLCV (TaiwanStockPrice) over a date range."""
        return self.get_dataset_by_range("TaiwanStockPrice", start_date, end_date or start_date)

    def get_margin_short(self, start_date: str, end_date: str = None) -> pd.DataFrame:
        """信用額度總量管制餘額表 (TaiwanStockMarginPurchaseShortSale) over a date range.

        A stock present on a date is credit-eligible (融資融券) that day;
        ShortSaleLimit > 0 means short selling is permitted.
        """
        return self.get_dataset_by_range("TaiwanStockMarginPurchaseShortSale",
                                         start_date, end_date or start_date)

    def get_short_suspension(self, start_date: str, end_date: str = None) -> pd.DataFrame:
        """暫停融券賣出表(融券回補日) (TaiwanStockMarginShortSaleSuspension) over a date range.

        Each per-day snapshot holds the suspension windows STARTING that day
        (stock_id, date, end_date, reason); expand [date, end_date] yourself to
        test whether a given day is inside a window. Windows starting before
        start_date are not included — fetch with a buffer if you need them.
        Iterates calendar days (not trading days): windows can start on a
        market holiday (e.g. typhoon closure).
        """
        return self.get_dataset_by_range("TaiwanStockMarginShortSaleSuspension",
                                         start_date, end_date or start_date,
                                         use_trading_calendar=False)
