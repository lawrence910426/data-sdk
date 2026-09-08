"""Build ``<cache-dir>/warrant_history.parquet`` -- point-in-time warrant terms.

One row per warrant per *state change*. Each row is a full snapshot of the
warrant's terms as known on ``effective_date``, so a lookup is one as-of join:

    history.join_asof(quotes, on='date', by='warrant_id', strategy='backward')
        .filter(pl.col('date') <= pl.col('exercise_end_date'))

There is deliberately no ``valid_to``: it would be exactly the next row's
``effective_date``, and dropping it turns an insert-plus-update into a plain
append. The trailing filter is what stops an as-of join from returning terms for
a date after the warrant expired.

**Expiry is a state column, not a static one.** Black-Scholes needs the maturity
the market believed at the time; a warrant terminated early was priced against
its original maturity right up to the announcement. So ``exercise_end_date`` and
``last_trade_date`` sit alongside strike and ratio, and an expiry change is its
own event row.

Sources, in precedence order on a ``(warrant_id, warrant_name, effective_date)`` collision:

| rank | source | covers | why |
|---|---|---|---|
| 1 | the TEJ Pro adjustment export | 2020-01-02 .. seed end | The backbone. MOPS's own t95sb02 keeps only a rolling ~18 months, so 95% of historical strike changes exist nowhere else. |
| 2 | ``mops_raw/warrant_announcement`` | ~2026-01 onward | Expiry changes with their *announcement* date -- the only point-in-time expiry source. |
| 3 | ``mops_raw/warrant_strike_ratio_adjustment`` / ``_reset`` | after seed end | Keeps strike history running once the frozen TEJ seed stops. |
| 4 | synthesised issuance from the dimension table | warrants absent from TEJ | New listings, and the 2019 cohort predating the TEJ window. |
| 5 | ``mops_raw/warrant_active_snapshot`` diff | anything the above missed | Today's terms for a warrant whose change fell in a period no event source covers, dated at the crawl. |

Reset events (t95sb03) are not modelled as separate rows: a reset always takes
effect on or before the listing day (verified -- 2,646 of 2,692 exactly on
``list_date``, the rest 3 days earlier, none after), so no trading day is ever
priced against the pre-reset strike. What matters is that the *issuance* row
carries the post-reset strike, which the TEJ listing row already does.

Usage:
    python -m data_sdk.crawlers.warrant.build_history --cache-dir cache
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from . import (
    DEFAULT_CACHE_PATH,
    TEJ_ADJUSTMENT_GLOB,
    TEJ_BASIC_INFO_GLOB,
    cache_directory as cache_dir,
    find_tej_seed,
)
from .build_basic_info import WARRANT_KEY, add_warrant_key, load_raw_table

# Terms carried on every row. Missing values are forward-filled within a
# warrant, so a strike-only event keeps the expiry it inherited and vice versa.
STATE_COLUMNS = [
    'strike',
    'ratio',
    'cap',
    'floor',
    'exercise_end_date',
    'last_trade_date',
]
STATIC_COLUMNS = [
    'issuer',
    'type',
    'target_stock_id',
    'target_name',
    'market',
    'list_date',
    'exercise_start_date',
    'original_strike',
    'is_bull_bear',
]
EVENT_COLUMNS = WARRANT_KEY + ['effective_date', 'sequence'] + STATE_COLUMNS + [
    'event_type',
    'source',
    'source_rank',
]


def empty_events() -> pd.DataFrame:
    return pd.DataFrame(columns=EVENT_COLUMNS)


def events_from_tej(tej_adjustment_path: Path, dim_warrant: pd.DataFrame) -> pd.DataFrame:
    """TEJ's per-state rows -> events (rank 1).

    Two quirks handled here. The ``上市櫃`` row is dated the *issue* date, 2-4
    days before listing, so its date is replaced by the warrant's ``list_date``
    -- there is no trading before then and an event predating the listing would
    open a phantom period. And TEJ labels every strike move ``重設`` regardless
    of whether it was a genuine reset or an ex-rights adjustment, so the label is
    mapped to a neutral ``change`` rather than pretending to know which.
    """
    tej = pd.read_parquet(
        tej_adjustment_path,
        columns=['權證代號', '權證簡稱', '年月日', '序號', '事件說明', '履約價(元)', '行使比例'],
    )
    tej['年月日'] = pd.to_datetime(tej['年月日'])
    tej = add_warrant_key(tej, '權證代號', '權證簡稱')

    events = pd.DataFrame({
        'warrant_id': tej['warrant_id'],
        'warrant_name': tej['warrant_name'],
        'effective_date': tej['年月日'],
        'sequence': pd.to_numeric(tej['序號'], errors='coerce').fillna(1).astype(int),
        'strike': pd.to_numeric(tej['履約價(元)'], errors='coerce'),
        'ratio': pd.to_numeric(tej['行使比例'], errors='coerce'),
        'event_type': tej['事件說明'].map({'上市櫃': 'issuance'}).fillna('change'),
    })
    events['source'] = 'tej'
    events['source_rank'] = 1

    listing_dates = dim_warrant[WARRANT_KEY + ['list_date']]
    events = events.merge(listing_dates, on=WARRANT_KEY, how='left')
    is_issuance = events['event_type'] == 'issuance'
    has_listing_date = events['list_date'].notna()
    events.loc[is_issuance & has_listing_date, 'effective_date'] = events.loc[
        is_issuance & has_listing_date, 'list_date'
    ]
    print(f'TEJ events: {len(events):,} ({int(is_issuance.sum()):,} issuance)')
    return events.drop(columns=['list_date'])


def events_from_mops_strike(cache_directory: Path, seed_end_date: pd.Timestamp) -> pd.DataFrame:
    """t95sb02 / t95sb03 rows after the frozen TEJ seed ends (rank 3)."""
    frames = []
    adjustment = load_raw_table(cache_directory, 'warrant_strike_ratio_adjustment')
    if adjustment is not None:
        adjustment = add_warrant_key(adjustment, 'warrant_id', 'warrant_name')
        frames.append(pd.DataFrame({
            'warrant_id': adjustment['warrant_id'],
            'warrant_name': adjustment['warrant_name'],
            'effective_date': adjustment['adjustment_effective_date'],
            'sequence': 1,
            'strike': adjustment['latest_strike'],
            'ratio': adjustment['latest_ratio'],
            'cap': adjustment.get('latest_cap'),
            'floor': adjustment.get('latest_floor'),
            'event_type': 'adjustment',
        }))
    reset = load_raw_table(cache_directory, 'warrant_strike_ratio_reset')
    if reset is not None:
        reset = add_warrant_key(reset, 'warrant_id', 'warrant_name')
        frames.append(pd.DataFrame({
            'warrant_id': reset['warrant_id'],
            'warrant_name': reset['warrant_name'],
            'effective_date': reset['reset_effective_date'],
            'sequence': 1,
            'strike': reset['reset_strike'],
            'ratio': reset['latest_ratio'],
            'cap': reset.get('reset_cap'),
            'floor': reset.get('reset_floor'),
            'event_type': 'reset',
        }))
    if not frames:
        return empty_events()

    events = pd.concat(frames, ignore_index=True)
    events = events[events['effective_date'] > seed_end_date]
    events['source'] = 'mops_strike'
    events['source_rank'] = 3
    print(f'MOPS strike events after {seed_end_date.date()}: {len(events):,}')
    return events


def events_from_announcements(cache_directory: Path) -> pd.DataFrame:
    """Expiry changes dated by their announcement, not their effect (rank 2)."""
    announcements = load_raw_table(cache_directory, 'warrant_announcement')
    if announcements is None:
        print('WARNING: no warrant_announcement table -- expiry changes will be undated')
        return empty_events()

    announcements = add_warrant_key(announcements, 'warrant_id', 'warrant_name')
    announcements = announcements[announcements['announced_expiry_date'].notna()]
    # Early terminations only. Their document names were checked against TEJ and
    # the embedded date is always the new expiry. Extension documents use the
    # same field for something else -- 03029X's 2026-09-07 extension names
    # 2026-03-04, six months in the past -- so they are left out rather than
    # guessed at. Extensions affect only the 43 extendable warrants, which are
    # excluded from studies anyway.
    announcements = announcements[announcements['announcement_type'] == 'early_termination']

    # The announcement names the new expiry but not the new last trading day.
    # The settlement gap between them is fixed per warrant, so the last trading
    # day is carried on the same row by shifting the prevailing gap; without it
    # the row would forward-fill the pre-termination last trading day, which now
    # falls after expiry.
    events = pd.DataFrame({
        'warrant_id': announcements['warrant_id'],
        'warrant_name': announcements['warrant_name'],
        'effective_date': announcements['announcement_date'],
        'sequence': 1,
        'exercise_end_date': announcements['announced_expiry_date'],
        'event_type': 'expiry_change',
    })
    events['source'] = 'mops_announcement'
    events['source_rank'] = 2
    events = events.drop_duplicates(subset=WARRANT_KEY + ['effective_date'], keep='last')
    print(f'announcement expiry events: {len(events):,}')
    return events


def scheduled_expiry_dates(cache_directory: Path, dim_warrant: pd.DataFrame) -> pd.DataFrame:
    """Per warrant, the maturity written into it at issuance.

    A warrant terminated early was priced against its *original* maturity until
    the announcement, so the issuance row must not carry the post-termination
    date that the dimension table (correctly) holds as current. TEJ records the
    scheduled last trading day, which is the only surviving record of the
    original schedule; the expiry sits a fixed 2-4 days after it, so the gap is
    taken from the warrant's own actual pair.

    Falls back to the dimension table's current dates where TEJ has no row --
    correct for every warrant that was never rescheduled, which is 99% of them.
    """
    fallback = dim_warrant[WARRANT_KEY + ['exercise_end_date', 'last_trade_date']].rename(
        columns={
            'exercise_end_date': 'scheduled_exercise_end_date',
            'last_trade_date': 'scheduled_last_trade_date',
        }
    )
    fallback['current_exercise_end_date'] = fallback['scheduled_exercise_end_date']
    tej_basic_path = find_tej_seed(TEJ_BASIC_INFO_GLOB)
    if tej_basic_path is None:
        return fallback

    tej = pd.read_parquet(
        tej_basic_path, columns=['權證代號', '權證名', '預定最後交易日', '最後交易日', '到期日']
    )
    for column in ['預定最後交易日', '最後交易日', '到期日']:
        tej[column] = pd.to_datetime(tej[column])
    tej = add_warrant_key(tej, '權證代號', '權證名')
    tej = tej.drop_duplicates(subset=WARRANT_KEY, keep='first')

    settlement_gap = tej['到期日'] - tej['最後交易日']
    scheduled = pd.DataFrame({
        'warrant_id': tej['warrant_id'],
        'warrant_name': tej['warrant_name'],
        'tej_scheduled_last_trade_date': tej['預定最後交易日'],
        'tej_scheduled_exercise_end_date': tej['預定最後交易日'] + settlement_gap,
    })

    out = fallback.merge(scheduled, on=WARRANT_KEY, how='left')
    has_schedule = out['tej_scheduled_last_trade_date'].notna()
    out.loc[has_schedule, 'scheduled_last_trade_date'] = out.loc[
        has_schedule, 'tej_scheduled_last_trade_date'
    ]
    out.loc[has_schedule, 'scheduled_exercise_end_date'] = out.loc[
        has_schedule, 'tej_scheduled_exercise_end_date'
    ]
    rescheduled_count = int(
        (out['scheduled_exercise_end_date'] != out['current_exercise_end_date']).sum()
    )
    print(f'warrants whose scheduled expiry differs from current: {rescheduled_count:,}')
    return out.drop(
        columns=[
            'tej_scheduled_last_trade_date',
            'tej_scheduled_exercise_end_date',
            'current_exercise_end_date',
        ]
    )


def events_from_dim(dim_warrant: pd.DataFrame, covered_keys: set[str]) -> pd.DataFrame:
    """Issuance rows for warrants no other source covers (rank 4).

    New listings crawled after the TEJ seed was frozen, plus the 2019 cohort
    that predates the seed window. ``original_strike`` is the issuance strike
    and ``alloc_qty_per_1k / 1000`` the issuance ratio; for a warrant with no
    later events these are also its final terms, so a single row is complete.
    """
    is_covered = [
        key in covered_keys
        for key in zip(dim_warrant['warrant_id'], dim_warrant['warrant_name'])
    ]
    uncovered = dim_warrant[~pd.Series(is_covered, index=dim_warrant.index)]
    events = pd.DataFrame({
        'warrant_id': uncovered['warrant_id'],
        'warrant_name': uncovered['warrant_name'],
        'effective_date': uncovered['list_date'],
        'sequence': 0,
        'strike': uncovered['original_strike'],
        'ratio': uncovered['alloc_qty_per_1k'] / 1000.0,
        'cap': uncovered['original_cap'],
        'floor': uncovered['original_floor'],
        'event_type': 'issuance',
    })
    events['source'] = 'dim_synthesised'
    events['source_rank'] = 4
    print(f'synthesised issuance for uncovered warrants: {len(events):,}')
    return events


def events_from_snapshot_diff(
    cache_directory: Path,
    dim_warrant: pd.DataFrame,
    events: pd.DataFrame,
) -> pd.DataFrame:
    """Close the gap between the last known event and what MOPS reports today.

    A warrant whose strike moved in a period no event source covers -- most of
    all the extendable warrants listed before 2020 and still alive, whose only
    event is a synthesised issuance from years ago -- would otherwise carry a
    stale strike right up to its current row. The snapshot knows today's terms
    but not when they changed, so the event is dated at the crawl: the change is
    recorded no later than it actually happened, and never invented earlier.
    """
    snapshot = load_raw_table(cache_directory, 'warrant_active_snapshot')
    if snapshot is None:
        return empty_events()

    snapshot = add_warrant_key(snapshot, 'warrant_id', 'warrant_name')
    crawl_date = pd.to_datetime(snapshot['crawl_date']).max()
    current_terms = snapshot[WARRANT_KEY + ['latest_strike', 'alloc_qty_per_1k']]
    current_terms = current_terms.drop_duplicates(subset=WARRANT_KEY, keep='last')

    last_events = (
        events.sort_values(WARRANT_KEY + ['effective_date', 'sequence'])
        .groupby(WARRANT_KEY, as_index=False)
        .last()[WARRANT_KEY + ['strike', 'ratio']]
    )
    compared = current_terms.merge(last_events, on=WARRANT_KEY, how='inner')
    compared['snapshot_ratio'] = compared['alloc_qty_per_1k'] / 1000.0

    strike_moved = (compared['latest_strike'] - compared['strike']).abs() > 0.01
    ratio_moved = (compared['snapshot_ratio'] - compared['ratio']).abs() > 1e-4
    unexplained = compared[strike_moved | ratio_moved]
    if unexplained.empty:
        return empty_events()

    diff_events = pd.DataFrame({
        'warrant_id': unexplained['warrant_id'],
        'warrant_name': unexplained['warrant_name'],
        'effective_date': crawl_date,
        'sequence': 9,
        'strike': unexplained['latest_strike'],
        'ratio': unexplained['snapshot_ratio'],
        'event_type': 'snapshot_diff',
    })
    diff_events['source'] = 'mops_snapshot'
    diff_events['source_rank'] = 5
    print(f'snapshot diffs no event explained: {len(diff_events):,} (dated {crawl_date.date()})')
    return diff_events


def chain_events(
    events: pd.DataFrame,
    dim_warrant: pd.DataFrame,
    scheduled_expiry: pd.DataFrame,
) -> pd.DataFrame:
    """Order, de-duplicate and forward-fill events into point-in-time rows."""
    events = events.dropna(subset=WARRANT_KEY + ['effective_date'])
    known_warrants = set(zip(dim_warrant['warrant_id'], dim_warrant['warrant_name']))
    is_known = [key in known_warrants for key in zip(events['warrant_id'], events['warrant_name'])]
    events = events[pd.Series(is_known, index=events.index)]

    # An event dated before the warrant existed is join noise on a recycled code.
    listing_dates = dim_warrant[WARRANT_KEY + ['list_date']]
    events = events.merge(listing_dates, on=WARRANT_KEY, how='left')
    before_listing = events['effective_date'] < events['list_date']
    if before_listing.any():
        print(f'dropped {int(before_listing.sum()):,} events dated before list_date')
    events = events[~before_listing].drop(columns=['list_date'])

    # Two rows can share a moment for two different reasons, resolved in order:
    #   1. A reset-type warrant's strike is reset on its own listing day, so the
    #      issuance row and the reset land on the same date. The reset is the
    #      state that actually traded, so a non-issuance row outranks issuance.
    #   2. The same event reaches us from two sources near the seed boundary;
    #      the lower source_rank (TEJ, then announcements, then MOPS) wins.
    events['is_issuance'] = events['event_type'] == 'issuance'
    events = events.sort_values(
        WARRANT_KEY + ['effective_date', 'sequence', 'is_issuance', 'source_rank'],
    )
    events = events.drop_duplicates(
        subset=WARRANT_KEY + ['effective_date', 'sequence'], keep='first'
    )
    events = events.drop(columns=['is_issuance'])

    # Seed each warrant's first row with the maturity it was issued with, so
    # that forward-filling carries the originally scheduled expiry until an
    # announcement moves it -- rather than back-dating today's expiry to
    # issuance, which would misprice every early-terminated warrant.
    events = events.merge(scheduled_expiry, on=WARRANT_KEY, how='left')
    is_first_row = ~events.duplicated(subset=WARRANT_KEY, keep='first')
    events.loc[is_first_row, 'exercise_end_date'] = events.loc[
        is_first_row, 'exercise_end_date'
    ].fillna(events.loc[is_first_row, 'scheduled_exercise_end_date'])
    events.loc[is_first_row, 'last_trade_date'] = events.loc[
        is_first_row, 'last_trade_date'
    ].fillna(events.loc[is_first_row, 'scheduled_last_trade_date'])
    events = events.drop(columns=['scheduled_exercise_end_date', 'scheduled_last_trade_date'])

    # Each row is a full state snapshot: an event that only moved the expiry
    # inherits the prevailing strike, and vice versa.
    grouped = events.groupby(WARRANT_KEY, sort=False)
    for column in STATE_COLUMNS:
        events[column] = grouped[column].ffill()

    # An expiry_change carries only the new expiry, so its last_trade_date was
    # forward-filled from before the change and can now sit after expiry.
    # Re-derive it from the warrant's own settlement gap, taken at issuance.
    settlement_gap = (events['exercise_end_date'] - events['last_trade_date'])
    issuance_gap = settlement_gap.groupby([events['warrant_id'], events['warrant_name']]).transform('first')
    last_trade_date_is_stale = events['last_trade_date'] > events['exercise_end_date']
    events.loc[last_trade_date_is_stale, 'last_trade_date'] = (
        events.loc[last_trade_date_is_stale, 'exercise_end_date']
        - issuance_gap[last_trade_date_is_stale]
    )

    # A handful of TEJ rows are dated a day or two after the warrant expired.
    # An as-of lookup filters on expiry anyway, so such a row can never be
    # returned; dropping it keeps the table self-consistent. A warrant's first
    # row is always kept, so no warrant loses its history entirely.
    is_first_event = ~events.duplicated(subset=WARRANT_KEY, keep='first')
    is_after_expiry = events['effective_date'] > events['exercise_end_date']
    droppable = is_after_expiry & ~is_first_event
    if droppable.any():
        print(f'dropped {int(droppable.sum()):,} events dated after exercise_end_date')
    events = events[~droppable]

    history = events.merge(
        dim_warrant[WARRANT_KEY + STATIC_COLUMNS], on=WARRANT_KEY, how='left'
    )
    history['is_current'] = ~history.duplicated(subset=WARRANT_KEY, keep='last')
    return history


def build_history(cache_directory: Path, dim_warrant: pd.DataFrame) -> pd.DataFrame:
    tej_adjustment_path = find_tej_seed(TEJ_ADJUSTMENT_GLOB)
    frames = []
    seed_end_date = pd.Timestamp.min

    if tej_adjustment_path is not None:
        print(f'TEJ seed: {tej_adjustment_path}')
        tej_events = events_from_tej(tej_adjustment_path, dim_warrant)
        seed_end_date = tej_events['effective_date'].max()
        frames.append(tej_events)
        print(f'TEJ seed ends {seed_end_date.date()}')
    else:
        print(f'WARNING: no {TEJ_ADJUSTMENT_GLOB} in the TEJ seed dir'
              ' -- history will be MOPS-only (~5% coverage)')

    frames.append(events_from_announcements(cache_directory))
    frames.append(events_from_mops_strike(cache_directory, seed_end_date))

    events = pd.concat([frame for frame in frames if len(frame)], ignore_index=True)
    for column in STATE_COLUMNS:
        if column not in events.columns:
            events[column] = pd.NA

    covered_keys = set(zip(events['warrant_id'], events['warrant_name']))
    events = pd.concat([events, events_from_dim(dim_warrant, covered_keys)], ignore_index=True)
    events = pd.concat(
        [events, events_from_snapshot_diff(cache_directory, dim_warrant, events)],
        ignore_index=True,
    )

    for column in ['exercise_end_date', 'last_trade_date']:
        events[column] = pd.to_datetime(events[column], errors='coerce')
    scheduled_expiry = scheduled_expiry_dates(cache_directory, dim_warrant)
    return chain_events(events, dim_warrant, scheduled_expiry)


def main() -> None:
    parser = argparse.ArgumentParser(description='Build point-in-time warrant term history.')
    parser.add_argument('--cache-dir', default=None,
                        help=f'default: $DATA_SDK_WARRANT_CACHE_PATH or {DEFAULT_CACHE_PATH}')
    parser.add_argument('--out', default=None)
    arguments = parser.parse_args()

    cache_directory = cache_dir(arguments.cache_dir)
    out_path = (
        Path(arguments.out) if arguments.out else cache_directory / 'warrant_history.parquet'
    )

    dim_warrant = pd.read_parquet(cache_directory / 'warrant_basic_info.parquet')
    history = build_history(cache_directory, dim_warrant)
    history.to_parquet(out_path, index=False)
    print(f'wrote {len(history):,} rows for {len(history[WARRANT_KEY].drop_duplicates()):,} warrants -> {out_path}')


if __name__ == '__main__':
    main()
