import os
import tempfile

import pandas as pd


class TEJWrapper:
    """TEJ (tejapi) datasets with an incremental CSV cache.

    The cache directory comes from ``DATA_SDK_TEJ_CACHE_PATH`` (default
    ``/mnt/nfs/backup/tej_cache``); the API key from ``TEJ_API_TOKEN``
    (required). tejapi configuration is global module state, so it is
    applied once per process.
    """

    _configured = False
    _DEFAULT_CACHE_DIR = "/mnt/nfs/backup/tej_cache"

    #: Subscription floor for TWN/EWSALE (dataStartYear = 2021).
    EWSALE_MIN_DATE = "2021-01-01"

    def __init__(self):
        if not TEJWrapper._configured:
            import tejapi

            api_key = os.environ.get("TEJ_API_TOKEN")
            if not api_key:
                raise RuntimeError(
                    "TEJ_API_TOKEN environment variable is not set"
                )
            tejapi.ApiConfig.api_key = api_key
            tejapi.ApiConfig.ignoretz = True
            TEJWrapper._configured = True

    @staticmethod
    def _cache_dir():
        path = os.environ.get("DATA_SDK_TEJ_CACHE_PATH", TEJWrapper._DEFAULT_CACHE_DIR)
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def _write_atomic(df, path):
        """Write CSV via a temp file + rename: the cache lives on shared
        NFS, so a concurrent reader must never see a torn file."""
        fd, tmp_path = tempfile.mkstemp(
            dir=os.path.dirname(path), suffix=".csv.tmp"
        )
        os.close(fd)
        try:
            df.to_csv(tmp_path, index=False)
            os.replace(tmp_path, path)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    @staticmethod
    def _read_cache(path):
        """Read the CSV cache re-imposing the dtypes CSV cannot carry:
        ``coid`` must stay str at parse time (numeric-looking ids -- a
        leading-zero coid inferred as int is unrepairable afterwards),
        ``mdate``/``annd_s`` datetime64[ns] (parse_dates may infer a coarser
        resolution; consumers compare against ns DatetimeIndexes)."""
        df = pd.read_csv(path, dtype={"coid": str}, parse_dates=["mdate", "annd_s"])
        for col in ("mdate", "annd_s"):
            df[col] = df[col].astype("datetime64[ns]")
        return df

    def get_ewsale(self, min_date: str = EWSALE_MIN_DATE) -> pd.DataFrame:
        """TWN/EWSALE monthly-revenue announcements, incrementally cached.

        Columns: ``coid`` (str), ``mdate`` (revenue month), ``annd_s``
        (announcement date), ``d0001``/``d0002``/``d0003`` (revenue, prior-year
        revenue, YoY %). Cold start fetches ``annd_s >= min_date``; warm calls
        fetch only ``annd_s > max(cached annd_s)`` and merge, deduping on
        ``(coid, annd_s)`` keep-last.

        Cached as ``ewsale.csv`` (dtypes re-imposed on read); a legacy
        ``ewsale.parquet`` from <= 0.4.0 is migrated in place on first read
        and left for older installs sharing the NFS cache dir.
        """
        import tejapi

        cache_dir = self._cache_dir()
        path = os.path.join(cache_dir, "ewsale.csv")
        legacy_parquet = os.path.join(cache_dir, "ewsale.parquet")

        df = None
        if os.path.isfile(path):
            df = self._read_cache(path)
        elif os.path.isfile(legacy_parquet):
            # One-time migration from the pre-0.4.1 parquet cache. The parquet
            # is deliberately left in place: the cache dir is shared NFS and
            # machines on older data-sdk still read/write it; new code never
            # looks at it again once ewsale.csv exists.
            df = pd.read_parquet(legacy_parquet)
            self._write_atomic(df, path)
            print(f"[TEJ EWSALE] migrated {legacy_parquet} -> {path} ({len(df)} rows)")

        if df is not None:
            max_annd = df["annd_s"].max().strftime("%Y-%m-%d")
            df_new = tejapi.get("TWN/EWSALE", annd_s={"gt": max_annd}, paginate=True)
            if len(df_new) > 0:
                df_new = self._normalize_ewsale(df_new)
                df = pd.concat([df, df_new], ignore_index=True)
                df = df.drop_duplicates(subset=["coid", "annd_s"], keep="last")
                df = df.sort_values("annd_s").reset_index(drop=True)
                self._write_atomic(df, path)
                print(f"[TEJ EWSALE] merged {len(df_new)} new rows, total {len(df)}")
        else:
            print(f"[TEJ EWSALE] cold fetch (annd_s >= {min_date})...")
            df = tejapi.get("TWN/EWSALE", annd_s={"gte": min_date}, paginate=True)
            df = self._normalize_ewsale(df)
            df = df.sort_values("annd_s").reset_index(drop=True)
            self._write_atomic(df, path)
            print(f"[TEJ EWSALE] cached {len(df)} rows at {path}")

        return df

    @staticmethod
    def _normalize_ewsale(df):
        df = df.reset_index(drop=True)
        df["coid"] = df["coid"].astype(str)
        df["mdate"] = pd.to_datetime(df["mdate"])
        df["annd_s"] = pd.to_datetime(df["annd_s"])
        return df
