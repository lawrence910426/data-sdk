"""Build ``<cache-dir>/warrant_basic_info.parquet`` -- the warrant dimension table.

One row per warrant, holding the attributes that do not change over its life
plus its *current* terms. Point-in-time strike/ratio/expiry live in
``warrant_history.parquet`` instead (see :mod:`build_history`).

Three sources, in precedence order per field:

1. ``mops_raw/warrant_basic_info`` -- the base population and every issuance-time
   field (``original_strike``, ``issuer``, ``target_stock_id``, ...).
2. ``mops_raw/warrant_active_snapshot`` -- overwrites the *mutable* term fields
   (``exercise_end_date``, ``last_trade_date``, ``latest_*``). The basic-info
   resource is incremental on ``list_date``, so a warrant already past the
   cursor is never re-read and its mutable fields freeze at first-crawl values;
   an early termination or extension silently rots them. The snapshot has no
   cursor and is replaced whole every run, so it always reflects MOPS today.
3. the TEJ Pro export (``$DATA_SDK_TEJ_WARRANTS_PATH``) -- repairs ``list_date`` /
   ``exercise_start_date``, which MOPS itself serves corrupted (~19,889 rows all
   set to the literal 2023-12-26; confirmed by live-replaying the MOPS query).

The key is ``(warrant_id, warrant_name)`` with ``warrant_id`` stripped of its
recycling suffix. It is what joins to TEJ too: ``exercise_end_date`` shifts on
early termination and ``list_date`` is the very field being repaired, so the
name is the only other stable identifier both vendors share.

Usage:
    python -m data_sdk.crawlers.warrant.build_basic_info --cache-dir cache
"""
from __future__ import annotations

import argparse
import glob
from pathlib import Path

import pandas as pd

from . import (
    DEFAULT_CACHE_PATH,
    TEJ_BASIC_INFO_GLOB,
    cache_directory as cache_dir,
    find_tej_seed,
)

#: Primary key. ``warrant_id`` is the 6-character listing code with any
#: recycling suffix stripped; it is NOT unique on its own (MOPS reuses a code
#: once the previous warrant expires -- ``030001`` has been three different
#: warrants), so the name is part of the key.
WARRANT_KEY = ['warrant_id', 'warrant_name']

DATE_COLUMNS = [
    'list_date',
    'exercise_start_date',
    'last_trade_date',
    'exercise_end_date',
]
# Fields the snapshot is allowed to overwrite: everything that can change after
# issuance. original_* and the identity columns are deliberately not here.
MUTABLE_TERM_COLUMNS = [
    'exercise_end_date',
    'last_trade_date',
    'latest_strike',
    'latest_cap',
    'latest_floor',
    'alloc_qty_per_1k',
]


def load_raw_table(cache_directory: Path, table_name: str) -> pd.DataFrame | None:
    files = sorted(
        glob.glob(str(cache_directory / 'mops_raw' / 'mops_raw' / table_name / '*.parquet'))
    )
    if not files:
        return None
    frame = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
    frame = frame.drop(columns=[column for column in frame.columns if column.startswith('_dlt_')])
    for column in frame.columns:
        if column.endswith('_date'):
            frame[column] = pd.to_datetime(frame[column])
    return frame


def add_warrant_key(frame: pd.DataFrame, id_column: str, name_column: str) -> pd.DataFrame:
    """Normalise the identity columns to ``(warrant_id, warrant_name)``.

    The 7th character of a MOPS code is a recycling suffix, not part of the
    traded code, and each vendor assigns its own (MOPS ``703055b`` vs TEJ
    ``703055Y``), so it is stripped. Codes below 7 characters already end at
    the traded code (``03001T``) and are untouched.
    """
    out = frame.copy()
    out['warrant_id'] = out[id_column].str[:6]
    if name_column != 'warrant_name':
        out['warrant_name'] = out[name_column]
    return out


def union_new_listings(basic: pd.DataFrame, snapshot: pd.DataFrame) -> pd.DataFrame:
    """Add warrants the snapshot sees but the incremental table has not.

    The basic-info crawl sweeps every 到期日 year-window and is therefore slow
    enough to run on a longer cycle than the snapshot, so between its runs the
    newest listings exist only in the snapshot. The snapshot is a complete
    current view with the same columns, so those rows can be adopted as-is.
    """
    known_warrants = set(zip(basic['warrant_id'], basic['warrant_name']))
    is_known = [
        key in known_warrants for key in zip(snapshot['warrant_id'], snapshot['warrant_name'])
    ]
    new_listings = snapshot[~pd.Series(is_known, index=snapshot.index)]
    if new_listings.empty:
        return basic
    print(f'adopted {len(new_listings):,} warrants seen only in the snapshot')
    adopted = new_listings.drop(columns=['crawl_date'])
    return pd.concat([basic, adopted[basic.columns]], ignore_index=True)


def overlay_snapshot(basic: pd.DataFrame, snapshot: pd.DataFrame) -> pd.DataFrame:
    """Refresh the mutable term fields from the current-view snapshot."""
    snapshot_terms = snapshot[WARRANT_KEY + MUTABLE_TERM_COLUMNS + ['crawl_date']]
    snapshot_terms = snapshot_terms.drop_duplicates(subset=WARRANT_KEY, keep='last')
    out = basic.merge(snapshot_terms, on=WARRANT_KEY, how='left', suffixes=('', '_snapshot'))

    is_covered = out['crawl_date'].notna()
    changed_count = 0
    for column in MUTABLE_TERM_COLUMNS:
        snapshot_column = f'{column}_snapshot'
        differs = is_covered & (out[column] != out[snapshot_column]) & out[snapshot_column].notna()
        changed_count += int(differs.sum())
        out.loc[differs, column] = out.loc[differs, snapshot_column]
    out['term_source'] = 'mops_raw'
    out.loc[is_covered, 'term_source'] = 'mops_snapshot'

    print(f'snapshot covers {int(is_covered.sum()):,} warrants, refreshed {changed_count:,} field values')
    return out.drop(columns=[f'{column}_snapshot' for column in MUTABLE_TERM_COLUMNS] + ['crawl_date'])


def impute_dates_from_tej(basic: pd.DataFrame, tej_basic: pd.DataFrame) -> pd.DataFrame:
    tej_dates = tej_basic.rename(
        columns={'上市日': 'tej_list_date', '履約開始日': 'tej_exercise_start_date'}
    )
    tej_dates['tej_list_date'] = pd.to_datetime(tej_dates['tej_list_date'])
    tej_dates['tej_exercise_start_date'] = pd.to_datetime(tej_dates['tej_exercise_start_date'])
    tej_dates = add_warrant_key(tej_dates, '權證代號', '權證名')
    tej_dates = tej_dates.drop_duplicates(subset=WARRANT_KEY, keep='first')

    out = basic.merge(
        tej_dates[WARRANT_KEY + ['tej_list_date', 'tej_exercise_start_date']],
        on=WARRANT_KEY,
        how='left',
    )
    list_date_is_wrong = out['tej_list_date'].notna() & (out['list_date'] != out['tej_list_date'])
    start_date_is_wrong = (
        out['tej_exercise_start_date'].notna()
        & (out['exercise_start_date'] != out['tej_exercise_start_date'])
    )

    out['list_date_source'] = 'mops'
    out.loc[list_date_is_wrong, 'list_date_source'] = 'tej_imputed'
    out.loc[list_date_is_wrong, 'list_date'] = out.loc[list_date_is_wrong, 'tej_list_date']

    out['exercise_start_date_source'] = 'mops'
    out.loc[start_date_is_wrong, 'exercise_start_date_source'] = 'tej_imputed'
    out.loc[start_date_is_wrong, 'exercise_start_date'] = out.loc[
        start_date_is_wrong, 'tej_exercise_start_date'
    ]

    print(f'imputed list_date from TEJ: {int(list_date_is_wrong.sum()):,} rows')
    print(f'imputed exercise_start_date from TEJ: {int(start_date_is_wrong.sum()):,} rows')
    return out.drop(columns=['tej_list_date', 'tej_exercise_start_date'])


def build_dim_warrant(cache_directory: Path) -> pd.DataFrame:
    basic = load_raw_table(cache_directory, 'warrant_basic_info')
    if basic is None:
        raise FileNotFoundError(f'no warrant_basic_info under {cache_directory}/mops_raw')
    basic = add_warrant_key(basic, 'warrant_id', 'warrant_name')
    print(f'MOPS basic_info: {len(basic):,} rows')

    snapshot = load_raw_table(cache_directory, 'warrant_active_snapshot')
    if snapshot is None:
        print('WARNING: no warrant_active_snapshot table -- mutable term fields may be stale')
        basic['term_source'] = 'mops_raw'
    else:
        snapshot = add_warrant_key(snapshot, 'warrant_id', 'warrant_name')
        basic = union_new_listings(basic, snapshot)
        basic = overlay_snapshot(basic, snapshot)

    tej_basic_path = find_tej_seed(TEJ_BASIC_INFO_GLOB)
    if tej_basic_path is not None:
        print(f'TEJ seed: {tej_basic_path}')
        basic = impute_dates_from_tej(basic, pd.read_parquet(tej_basic_path))
    else:
        print(f'WARNING: no {TEJ_BASIC_INFO_GLOB} in the TEJ seed dir -- skipping list_date repair')
        basic['list_date_source'] = 'mops'
        basic['exercise_start_date_source'] = 'mops'

    # Bull/bear certificates (牛證/熊證) behave differently and TEJ never covers
    # them, so downstream studies exclude them -- flag rather than drop.
    basic['is_bull_bear'] = basic['warrant_name'].str.contains('牛|熊', regex=True, na=False)
    return basic


def main() -> None:
    parser = argparse.ArgumentParser(description='Build the warrant dimension table.')
    parser.add_argument('--cache-dir', default=None,
                        help=f'default: $DATA_SDK_WARRANT_CACHE_PATH or {DEFAULT_CACHE_PATH}')
    parser.add_argument('--out', default=None)
    arguments = parser.parse_args()

    cache_directory = cache_dir(arguments.cache_dir)
    out_path = (
        Path(arguments.out) if arguments.out else cache_directory / 'warrant_basic_info.parquet'
    )

    dim_warrant = build_dim_warrant(cache_directory)
    dim_warrant.to_parquet(out_path, index=False)
    print(f'wrote {len(dim_warrant):,} rows -> {out_path}')


if __name__ == '__main__':
    main()
