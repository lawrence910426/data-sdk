import os
import pandas as pd
from pathlib import Path
from typing import Optional


KNOWN_ISSUERS = [
    "元大", "富邦", "統一", "群益", "中信", "兆豐", "國泰",
    "永豐", "凱基", "臺新", "華南", "第一", "玉山", "大華",
    "台銀", "合庫", "摩根", "野村",
]

class WarrantInfoWrapper:
    """
    Fetches Taiwan warrant metadata from FinMind's TaiwanStockInfoWithWarrantSummary.

    Requires FINMIND_API_TOKEN environment variable.

    Usage:
        w = WarrantInfoWrapper()
        summary = w.get_warrant_summary()          # all warrants (cached)
    """

    def __init__(self, cache_dir):
        from FinMind.data import DataLoader
        self._api = DataLoader()
        token = os.environ.get("FINMIND_API_TOKEN")
        if not token:
            raise EnvironmentError("FINMIND_API_TOKEN is not set")
        self._api.login_by_token(api_token=token)
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._summary_cache: Optional[pd.DataFrame] = None
        self._names_cache: Optional[pd.DataFrame] = None

    def get_warrant_summary(self, refresh: bool = False) -> pd.DataFrame:
        """
        Returns TaiwanStockInfoWithWarrantSummary as a DataFrame.

        Known columns (may include more — print .columns to discover):
          stock_id          (str)   warrant code
          date              (str)   listing date
          target_stock_id   (str)   underlying stock code
          type              (str)   "認購" (call) | "認售" (put)
          fulfillment_method(str)   e.g. "美式"
          end_date          (str)   last trading date (YYYY-MM-DD)
          fulfillment_start_date (str)
          fulfillment_end_date   (str)
          exercise_ratio    (float) warrants per share (e.g. 0.119)
          fulfillment_price (float) strike price

        Result is cached in <cache_dir>/warrant_summary.parquet.
        """
        cache_path = self._cache_dir / "warrant_summary.parquet"
        if not refresh and self._summary_cache is not None:
            return self._summary_cache
        if not refresh and cache_path.exists():
            self._summary_cache = pd.read_parquet(cache_path)
            return self._summary_cache

        df = self._api.taiwan_stock_info_with_warrant_summary()
        print(f"[WarrantInfoWrapper] TaiwanStockInfoWithWarrantSummary columns: {df.columns.tolist()}")
        df.to_parquet(cache_path, index=False)
        self._summary_cache = df
        return df
