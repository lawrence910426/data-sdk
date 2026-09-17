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

**Rows can predate ``list_date``.** The issuance row sits on the issue date,
2-4 days before listing, and a reset-type warrant's reset lands on or just
before the listing day (2,646 of 2,692 MOPS resets exactly on ``list_date``,
the rest up to 3 days earlier; TEJ dates 1,443 of them to the day before).
Both are kept as dated: the reset is the state that traded from day one, and
the issuance row before it carries the pre-reset strike. Whether a date is
tradable is the reader's concern -- ``list_date`` is on every row, and
``WarrantInfoWrapper.get_warrant_history(as_of=...)`` already filters on it.

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
    taiwan_today,
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
    'is_american',
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

    Dates are kept as TEJ records them: the ``上市櫃`` row on the *issue*
    date (2-4 days before listing) and a reset on the day it took effect,
    which for 1,443 warrants is the day before listing. Both are real states
    -- the issue-time strike, then the reset one that traded from day one --
    and whether a date is tradable is the reader's concern (``list_date`` is
    on every row). TEJ labels every strike move ``重設`` regardless of whether
    it was a genuine reset or an ex-rights adjustment, so the label is mapped
    to a neutral ``change`` rather than pretending to know which.
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
    is_issuance = events['event_type'] == 'issuance'
    print(f'TEJ events: {len(events):,} ({int(is_issuance.sum()):,} issuance)')
    return events


def events_from_mops_strike(
    cache_directory: Path,
    seed_end_date: pd.Timestamp,
    tej_keys: set,
) -> pd.DataFrame:
    """t95sb02 / t95sb03 rows (rank 3): from the frozen TEJ seed's last day
    on, plus everything for warrants TEJ never covers (bull/bear, the
    extendable ones, MOPS-only listings), whose only strike history is here.
    """
    frames = []
    # Announced but not yet in force rows carry their own 生效日, which is what
    # keeps the change off the days before it (see the resource's docstring).
    adjustment_tables = [
        table
        for table in (
            load_raw_table(cache_directory, 'warrant_strike_ratio_adjustment'),
            load_raw_table(cache_directory, 'warrant_pending_adjustment'),
        )
        if table is not None
    ]
    adjustment = pd.concat(adjustment_tables, ignore_index=True) if adjustment_tables else None
    if adjustment is not None and len(adjustment):
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
    reset_tables = [
        table
        for table in (
            load_raw_table(cache_directory, 'warrant_strike_ratio_reset'),
            load_raw_table(cache_directory, 'warrant_pending_reset'),
        )
        if table is not None
    ]
    reset = pd.concat(reset_tables, ignore_index=True) if reset_tables else None
    if reset is not None and len(reset):
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
    # Inclusive: the seed's last day may be partial (an export pulled
    # mid-day), and a same-day collision is settled by source_rank anyway.
    after_seed = events['effective_date'] >= seed_end_date
    tej_covered = pd.Series(
        [key in tej_keys for key in zip(events['warrant_id'], events['warrant_name'])],
        index=events.index,
    )
    events = events[after_seed | ~tej_covered]
    events['source'] = 'mops_strike'
    events['source_rank'] = 3
    print(f'MOPS strike events: {int(after_seed.sum()):,} from {seed_end_date.date()},'
          f' {int((~after_seed & ~tej_covered).sum()):,} earlier for warrants outside TEJ')
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
        # Sequence 2, after any strike event on the same day: this row carries
        # only the expiry, and on a shared (date, sequence) the dedup would
        # keep it over the adjustment and forward-fill a stale strike.
        'sequence': 2,
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

    # 預定最後交易日 is not reliable to the day: on ~6,800 warrants it sits 1-7
    # days off the actual last trading day with no termination involved (the
    # shifts cluster on market closures -- 2025-10-22, 2026-02-10 -- and TEJ's
    # own schedule arithmetic), and taking it literally put 0.9% of live
    # warrants one day off the exchange's expiry. A real early termination
    # moves the date by weeks, and those cluster per underlying event (the 35
    # warrants all ending 2026-08-20 on a 2026-09-11 schedule), so only a
    # shift beyond a week is treated as a reschedule; otherwise the actual
    # dates are the schedule.
    # ponytail: 7-day cut-off; use the announcement table to classify if a
    # termination inside a week ever matters.
    settlement_gap = tej['到期日'] - tej['最後交易日']
    is_rescheduled = (tej['最後交易日'] - tej['預定最後交易日']).dt.days < -7
    scheduled_last_trade_date = tej['預定最後交易日'].where(is_rescheduled, tej['最後交易日'])
    scheduled = pd.DataFrame({
        'warrant_id': tej['warrant_id'],
        'warrant_name': tej['warrant_name'],
        'tej_scheduled_last_trade_date': scheduled_last_trade_date,
        'tej_scheduled_exercise_end_date': scheduled_last_trade_date + settlement_gap,
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


def first_priced_event(events: pd.DataFrame) -> pd.DataFrame:
    """Per warrant, the earliest event carrying both a strike and a ratio."""
    priced = events.dropna(subset=['strike', 'ratio', 'effective_date'])
    priced = priced.sort_values(WARRANT_KEY + ['effective_date', 'sequence'])
    return (
        priced.groupby(WARRANT_KEY, as_index=False)
        .head(1)[WARRANT_KEY + ['strike', 'ratio', 'effective_date']]
        .rename(columns={
            'strike': 'later_strike',
            'ratio': 'later_ratio',
            'effective_date': 'later_date',
        })
    )


def events_from_dim(
    dim_warrant: pd.DataFrame,
    covered_keys: set[str],
    other_events: pd.DataFrame,
) -> pd.DataFrame:
    """Issuance rows for warrants no other source covers (rank 4).

    New listings crawled after the TEJ seed was frozen, plus the 2019 cohort
    that predates the seed window. ``original_strike`` is the issuance strike
    and ``alloc_qty_per_1k / 1000`` the issuance ratio.

    ``t90sb01`` publishes only the *latest* allocation, so on a warrant that
    has been adjusted since it listed that ratio is not the one it issued
    with. An ex-rights adjustment conserves ``strike x ratio``, so where a
    later event is known the issuance ratio is backed out from it instead
    (``strike' x ratio' / original_strike``); ``validate`` measures how well
    the invariant holds. A warrant with no later event keeps the published
    allocation, which for it is both the issuance and the current ratio.
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

    priced = first_priced_event(other_events)
    events = events.merge(priced, on=WARRANT_KEY, how='left')
    is_backed_out = (
        events['later_strike'].notna()
        & events['later_ratio'].notna()
        & (events['strike'] > 0)
        & (events['later_date'] > events['effective_date'])
    )
    events.loc[is_backed_out, 'ratio'] = (
        events.loc[is_backed_out, 'later_strike']
        * events.loc[is_backed_out, 'later_ratio']
        / events.loc[is_backed_out, 'strike']
    ).round(3)  # t90sb01 publishes the allocation as an integer per 1000 units
    events = events.drop(columns=['later_strike', 'later_ratio', 'later_date'])
    print(f'synthesised issuance for uncovered warrants: {len(events):,}'
          f' ({int(is_backed_out.sum()):,} ratios backed out of a later event)')
    return events



# TODO: MOPS's t90sb01 applies an ex-dividend adjustment a day before it takes
# effect, so a diff dated at the crawl is a day early (德微, 2026-09-14). Plan:
# a daily TWSE/TPEx OpenAPI snapshot resource as the diff's reference (the
# exchange applies on the ex-date), plus t95sb02's future-effective rows for
# the true dates; MOPS's snapshot becomes verify's reference instead.
def events_from_snapshot_diff(
    cache_directory: Path,
    dim_warrant: pd.DataFrame,
    events: pd.DataFrame,
    scheduled_expiry: pd.DataFrame,
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
    current_terms = snapshot[
        WARRANT_KEY + ['latest_strike', 'alloc_qty_per_1k', 'exercise_end_date', 'last_trade_date']
    ]
    current_terms = current_terms.drop_duplicates(subset=WARRANT_KEY, keep='last')
    # A warrant that has not listed yet is not trading on anything, and MOPS's
    # snapshot carries a provisional strike for it; there is nothing to diff.
    not_yet_listed = dim_warrant.loc[dim_warrant['list_date'] > crawl_date, WARRANT_KEY]
    current_terms = current_terms.merge(not_yet_listed, on=WARRANT_KEY, how='left', indicator=True)
    current_terms = current_terms[current_terms['_merge'] == 'left_only'].drop(columns=['_merge'])

    # The last known state per warrant, with expiry forward-filled the way
    # chain_events will do it: a strike-only event carries no expiry.
    ordered = events.sort_values(WARRANT_KEY + ['effective_date', 'sequence'])
    ordered['exercise_end_date'] = pd.to_datetime(ordered['exercise_end_date'], errors='coerce')
    ordered['exercise_end_date'] = ordered.groupby(WARRANT_KEY)['exercise_end_date'].ffill()
    last_events = (
        ordered.groupby(WARRANT_KEY, as_index=False)
        .last()[WARRANT_KEY + ['strike', 'ratio', 'exercise_end_date']]
        .rename(columns={'exercise_end_date': 'known_exercise_end_date'})
    )
    compared = current_terms.merge(last_events, on=WARRANT_KEY, how='inner')
    compared['snapshot_ratio'] = compared['alloc_qty_per_1k'] / 1000.0
    compared['exercise_end_date'] = pd.to_datetime(compared['exercise_end_date'])
    # Strike events carry no expiry, so for most warrants the last known
    # expiry is the one the chain will seed the first row with: the schedule.
    compared = compared.merge(
        scheduled_expiry[WARRANT_KEY + ['scheduled_exercise_end_date']], on=WARRANT_KEY, how='left'
    )
    compared['known_exercise_end_date'] = compared['known_exercise_end_date'].fillna(
        compared['scheduled_exercise_end_date']
    )

    strike_moved = (compared['latest_strike'] - compared['strike']).abs() > 0.01
    ratio_moved = (compared['snapshot_ratio'] - compared['ratio']).abs() > 1e-4
    # An expiry the snapshot reports that no announcement explained: MOPS
    # posts a termination on an underlying's delisting under 終止上市/上櫃
    # rather than 提前終止, which the announcement resource does not read.
    expiry_moved = (
        compared['known_exercise_end_date'].notna()
        & (compared['exercise_end_date'] != compared['known_exercise_end_date'])
    )
    unexplained = compared[strike_moved | ratio_moved | expiry_moved]
    if unexplained.empty:
        return empty_events()

    # Dated at the crawl, or at expiry if the warrant has already expired:
    # the snapshot still lists it for a few days after, and an ex-dividend
    # between its last trading day and expiry (t95sb02 rarely records those)
    # is exactly what its settlement terms then reflect.
    effective_date = pd.to_datetime(unexplained['exercise_end_date']).clip(upper=crawl_date)
    # Restate only what moved. A warrant whose expiry changed still has the
    # strike it had yesterday, and MOPS applies an ex-dividend adjustment to
    # the snapshot a day early -- carrying that strike onto an expiry-only diff
    # would date the adjustment a day before it is true.
    terms_moved = (strike_moved | ratio_moved)[unexplained.index]
    expiry_only = ~terms_moved
    diff_events = pd.DataFrame({
        'warrant_id': unexplained['warrant_id'],
        'warrant_name': unexplained['warrant_name'],
        'effective_date': effective_date.fillna(crawl_date),
        'sequence': 9,
        'strike': unexplained['latest_strike'].where(terms_moved),
        'ratio': unexplained['snapshot_ratio'].where(terms_moved),
        'exercise_end_date': unexplained['exercise_end_date'].where(expiry_moved[unexplained.index]),
        'last_trade_date': pd.to_datetime(unexplained['last_trade_date']).where(expiry_moved[unexplained.index]),
        'event_type': 'snapshot_diff',
    })
    diff_events['source'] = 'mops_snapshot'
    diff_events['source_rank'] = 5
    expiry_only_count = int(expiry_only.sum())
    print(f'snapshot diffs no event explained: {len(diff_events):,}'
          f' ({expiry_only_count:,} expiry only;'
          f' {int((effective_date < crawl_date).sum()):,} on an already-expired warrant, dated at its expiry)')
    return diff_events


def chain_events(
    events: pd.DataFrame,
    dim_warrant: pd.DataFrame,
    scheduled_expiry: pd.DataFrame,
) -> pd.DataFrame:
    """Order, de-duplicate and forward-fill events into point-in-time rows.

    An undated event is kept only when it is a synthesised issuance -- the
    warrant's list_date is unknown (see build_basic_info), so its single row
    is undated too. It still gives the warrant its current terms; an as-of
    lookup never matches it, which is the right answer for "listed when?".
    """
    is_dated = events['effective_date'].notna()
    is_undated_issuance = ~is_dated & (events['source'] == 'dim_synthesised')
    events = events[is_dated | is_undated_issuance].dropna(subset=WARRANT_KEY)
    known_warrants = set(zip(dim_warrant['warrant_id'], dim_warrant['warrant_name']))
    is_known = [key in known_warrants for key in zip(events['warrant_id'], events['warrant_name'])]
    events = events[pd.Series(is_known, index=events.index)]

    # Events before list_date are kept: the issuance on the issue date and any
    # reset/adjustment applied between issue and first trade are real states.
    # Only the count is reported, as a tripwire for join noise on a recycled code.
    listing_dates = dim_warrant[WARRANT_KEY + ['list_date']]
    with_listing = events.merge(listing_dates, on=WARRANT_KEY, how='left')
    before_listing = with_listing['effective_date'] < with_listing['list_date']
    print(f'events dated before list_date (kept): {int(before_listing.sum()):,}')

    # Two rows can share a moment for two different reasons, resolved in order:
    #   1. A reset-type warrant's strike is reset on its own listing day, so the
    #      issuance row and the reset land on the same date. The reset is the
    #      state that actually traded, so a non-issuance row outranks issuance.
    #   2. The same event reaches us from two sources near the seed boundary;
    #      the lower source_rank (TEJ, then announcements, then MOPS) wins.
    events['is_issuance'] = events['event_type'] == 'issuance'
    # na_position: an issuance whose list_date is unknown (see
    # build_basic_info) is undated and must still open the chain.
    events = events.sort_values(
        WARRANT_KEY + ['effective_date', 'sequence', 'is_issuance', 'source_rank'],
        na_position='first',
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
    # forward-filled from before the change and can now sit after expiry. An
    # early termination's last trading day is two business days before the new
    # expiry (every one of the 60 checked against the exchange), so re-derive
    # it that way; the calendar gap taken at issuance spans weekends and was
    # off by one or two days on most of them.
    last_trade_date_is_stale = events['last_trade_date'] > events['exercise_end_date']
    events.loc[last_trade_date_is_stale, 'last_trade_date'] = (
        events.loc[last_trade_date_is_stale, 'exercise_end_date'] - 2 * pd.offsets.BDay()
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
    # "Current" means in force today, not the last row: an adjustment announced
    # for a future date sits in the table with that date, and until it arrives
    # the terms that trade are the ones before it.
    today = pd.Timestamp(taiwan_today())
    in_force = history['effective_date'].isna() | (history['effective_date'] <= today)
    current_rows = history[in_force].groupby(WARRANT_KEY, sort=False).tail(1).index
    # A warrant that lists tomorrow has no row in force yet, and every warrant
    # must have exactly one: give it its issuance row. That is also what the
    # exchange publishes for an unlisted warrant -- a reset or an adjustment
    # dated on the listing day has not happened yet either.
    not_yet_listed = ~history[WARRANT_KEY].agg(tuple, axis=1).isin(
        set(map(tuple, history.loc[current_rows, WARRANT_KEY].values))
    )
    current_rows = current_rows.union(
        history[not_yet_listed].groupby(WARRANT_KEY, sort=False).head(1).index
    )
    history['is_current'] = history.index.isin(current_rows)
    pending = int((~in_force).sum())
    if pending:
        print(f'rows effective after today: {pending:,}'
              f' ({int(not_yet_listed.groupby([history["warrant_id"], history["warrant_name"]]).any().sum()):,}'
              ' warrants not yet listed)')
    return history


def build_history(cache_directory: Path, dim_warrant: pd.DataFrame) -> pd.DataFrame:
    # Bull/bear certificates and extendable warrants are not a trading target
    # and their terms (knock-out barrier and financing cost; a maturity that
    # moves by extension) do not fit this table's model. They stay in the
    # dimension table, flagged, and are left out of the history entirely.
    is_excluded = (
        dim_warrant['is_bull_bear'].fillna(False).astype(bool)
        | dim_warrant['is_extendable'].fillna(False).astype(bool)
    )
    print(f'excluding {int(is_excluded.sum()):,} bull/bear and extendable warrants from the history')
    dim_warrant = dim_warrant[~is_excluded]

    tej_adjustment_path = find_tej_seed(TEJ_ADJUSTMENT_GLOB)
    frames = []
    seed_end_date = pd.Timestamp.min

    if tej_adjustment_path is not None:
        print(f'TEJ seed: {tej_adjustment_path}')
        tej_events = events_from_tej(tej_adjustment_path, dim_warrant)
        # The seed's end is the last day TEJ actually recorded events on --
        # taken from the raw 年月日, not from the events, whose issuance rows
        # have been re-dated to list_date and can sit a few days later. Using
        # the re-dated maximum once put the end at 09-02 when TEJ stopped on
        # 09-01, and every MOPS adjustment effective 09-02 (989 of them) was
        # dropped as "covered by TEJ".
        seed_end_date = pd.to_datetime(
            pd.read_parquet(tej_adjustment_path, columns=['年月日'])['年月日']
        ).max()
        frames.append(tej_events)
        print(f'TEJ seed ends {seed_end_date.date()}')
    else:
        print(f'WARNING: no {TEJ_ADJUSTMENT_GLOB} in the TEJ seed dir'
              ' -- history will be MOPS-only (~5% coverage)')

    tej_keys = set(zip(frames[0]['warrant_id'], frames[0]['warrant_name'])) if frames else set()
    frames.append(events_from_announcements(cache_directory))
    frames.append(events_from_mops_strike(cache_directory, seed_end_date, tej_keys))

    events = pd.concat([frame for frame in frames if len(frame)], ignore_index=True)
    for column in STATE_COLUMNS:
        if column not in events.columns:
            events[column] = pd.NA

    # A warrant is covered when some event opens its life -- one dated on or
    # before its list_date. An adjustment that arrived after listing (a warrant
    # listed after the TEJ seed ended, or one whose first TEJ row is already a
    # change) does not stand in for the issuance: without a synthesised one the
    # days between listing and that adjustment would have no terms at all.
    dated = events.merge(dim_warrant[WARRANT_KEY + ['list_date']], on=WARRANT_KEY, how='left')
    opens_life = dated['list_date'].isna() | (dated['effective_date'] <= dated['list_date'])
    covered_keys = set(zip(dated.loc[opens_life, 'warrant_id'], dated.loc[opens_life, 'warrant_name']))
    events = pd.concat([events, events_from_dim(dim_warrant, covered_keys, events)], ignore_index=True)
    scheduled_expiry = scheduled_expiry_dates(cache_directory, dim_warrant)
    events = pd.concat(
        [events, events_from_snapshot_diff(cache_directory, dim_warrant, events, scheduled_expiry)],
        ignore_index=True,
    )

    for column in ['exercise_end_date', 'last_trade_date']:
        events[column] = pd.to_datetime(events[column], errors='coerce')
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
