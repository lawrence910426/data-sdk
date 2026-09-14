import os
import pandas as pd
from pathlib import Path
from typing import Optional

from ..crawlers.warrant import cache_directory as warrant_cache_directory


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

    def __init__(self, cache_dir=None):
        # FinMind login is lazy (only in get_warrant_summary): get_warrant_history
        # doesn't touch FinMind at all, and shouldn't need FINMIND_API_TOKEN set
        # or a writable cache_dir just to construct the wrapper.
        self._api = None
        # str is accepted as well as Path: the FinMind cache path is built with
        # `/`, which a str would only fail on much later, inside
        # get_warrant_summary().
        self._cache_dir = Path(cache_dir) if cache_dir is not None else None
        self._summary_cache: Optional[pd.DataFrame] = None
        self._names_cache: Optional[pd.DataFrame] = None
        self._history_cache: Optional[pd.DataFrame] = None

    def _finmind_api(self):
        if self._api is None:
            from FinMind.data import DataLoader
            token = os.environ.get("FINMIND_API_TOKEN")
            if not token:
                raise EnvironmentError("FINMIND_API_TOKEN is not set")
            if self._cache_dir is None:
                raise ValueError("cache_dir is required for get_warrant_summary()")
            api = DataLoader()
            api.login_by_token(api_token=token)
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            self._api = api
        return self._api

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

        Known issue: fulfillment_price is 0 for every warrant market-wide, and
        end_date never updates after e.g. early termination. See
        get_warrant_history() for the fixed, point-in-time replacement.
        """
        cache_path = self._cache_dir / "warrant_summary.parquet"
        if not refresh and self._summary_cache is not None:
            return self._summary_cache
        if not refresh and cache_path.exists():
            self._summary_cache = pd.read_parquet(cache_path)
            return self._summary_cache

        df = self._finmind_api().taiwan_stock_info_with_warrant_summary()
        print(f"[WarrantInfoWrapper] TaiwanStockInfoWithWarrantSummary columns: {df.columns.tolist()}")
        df.to_parquet(cache_path, index=False)
        self._summary_cache = df
        return df

    def get_warrant_history(
        self,
        as_of: Optional[str] = None,
        cache_dir: Optional[str | Path] = None,
        refresh: bool = False,
    ) -> pd.DataFrame:
        """
        One row per warrant, from data_sdk.crawlers.warrant's built tables
        (MOPS + TEJ, point-in-time) instead of FinMind — strike/ratio are real
        (get_warrant_summary()'s are 0), expiry stays current, no token needed.
        Bull/bear certificates (牛證/熊證) and extendable warrants (展延型)
        are not included.

        Every column of warrant_history.parquet is returned as-is:
          warrant_id        (str)   recycling suffix stripped; not unique alone
          warrant_name      (str)   pairs with warrant_id as the key
          effective_date    (date)  when these terms took effect
          sequence          (int)   tie-break for two events on one date
          strike            (float)
          ratio             (float) shares per warrant unit
          event_type        (str)   issuance | change | expiry_change | adjustment | reset | snapshot_diff
          source            (str)   tej | mops_strike | mops_announcement | mops_snapshot | dim_synthesised
          source_rank       (int)   precedence used to resolve same-moment rows
          exercise_end_date (date)
          cap / floor       (float) pre-2010 capped calls only
          last_trade_date   (date)
          issuer            (str)
          type              (str)   "認購" (call) | "認售" (put)
          target_stock_id   (str)   underlying stock code
          target_name       (str)   underlying stock name
          market            (str)   "twse" | "otc"
          list_date         (date)  null when MOPS corrupted it and neither
                                     TEJ nor FinMind could repair it
          exercise_start_date (date) same
          original_strike   (float) strike at issuance, pre-reset
          is_american       (bool)  exercisable from listing, not only at
                                     maturity; derived from the dates, null
                                     when list_date is
          is_current        (bool)  last row of the warrant

        as_of=None: current terms. as_of="YYYY-MM-DD": terms as known then; no
        row means not yet listed, already expired, or list_date unknown.

        Reads <cache_dir>/warrant_history.parquet (default
        $DATA_SDK_WARRANT_CACHE_PATH). Refresh with
        `python -m data_sdk.crawlers.warrant.update`.
        """
        resolved_cache_dir = warrant_cache_directory(cache_dir)
        if refresh or self._history_cache is None or getattr(self, "_history_cache_dir", None) != resolved_cache_dir:
            path = resolved_cache_dir / "warrant_history.parquet"
            if not path.exists():
                raise FileNotFoundError(
                    f"{path} does not exist -- run "
                    "`python -m data_sdk.crawlers.warrant.update` (or point "
                    "cache_dir / DATA_SDK_WARRANT_CACHE_PATH at an existing build)"
                )
            self._history_cache = pd.read_parquet(path)
            self._history_cache_dir = resolved_cache_dir
        history = self._history_cache

        if as_of is None:
            summary = history[history["is_current"]]
        else:
            as_of_date = pd.Timestamp(as_of)
            candidates = history[
                (history["effective_date"] <= as_of_date)
                & (history["list_date"] <= as_of_date)
                & (history["exercise_end_date"] >= as_of_date)
            ]
            summary = candidates.sort_values(
                ["warrant_id", "warrant_name", "effective_date", "sequence"]
            ).drop_duplicates(subset=["warrant_id", "warrant_name"], keep="last")

        return summary.reset_index(drop=True)
