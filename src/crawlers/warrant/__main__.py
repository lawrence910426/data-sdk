"""CLI entry point: ``python -m mops_crawler --cache-dir <path>``.

Writes raw parquet tables under ``<cache-dir>/mops_raw/`` and keeps the dlt
pipeline's incremental-cursor state under ``<cache-dir>/mops_pipeline_state/``.
The destination is plain ``filesystem`` (parquet files) — every resource is
append-only (see :mod:`mops_crawler.warrant_reports` for why), so no
merge/upsert support, and therefore no SQL-capable destination, is needed.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import dlt

from . import DEFAULT_CACHE_PATH, cache_directory as cache_dir
from .warrant_reports import HISTORY_START_YEAR, warrant_reports_source

RESOURCE_NAMES = [
    'warrant_basic_info',
    'warrant_active_snapshot',
    'warrant_strike_ratio_adjustment',
    'warrant_strike_ratio_reset',
    'warrant_announcement',
]


def main() -> None:
    parser = argparse.ArgumentParser(description='Crawl TWSE MOPS warrant reports into raw parquet tables.')
    parser.add_argument('--cache-dir', default=None,
                        help=f'writes mops_raw/ and mops_pipeline_state/ under this'
                             f' (default: $DATA_SDK_WARRANT_CACHE_PATH or {DEFAULT_CACHE_PATH})')
    parser.add_argument('--history-start-year', type=int, default=HISTORY_START_YEAR, help='earliest year to sweep for delisted warrants')
    parser.add_argument('--resources', nargs='+', choices=RESOURCE_NAMES, default=None, help='restrict the run to these resources (default: all three)')
    arguments = parser.parse_args()

    cache_directory = cache_dir(arguments.cache_dir)
    pipeline = dlt.pipeline(
        pipeline_name='mops_warrant_reports',
        destination=dlt.destinations.filesystem(
            bucket_url=(cache_directory / 'mops_raw').resolve().as_uri()
        ),
        dataset_name='mops_raw',
        pipelines_dir=str(cache_directory / 'mops_pipeline_state'),
    )
    source = warrant_reports_source(arguments.history_start_year)
    if arguments.resources is not None:
        source = source.with_resources(*arguments.resources)
    print(pipeline.run(source, loader_file_format='parquet'))


if __name__ == '__main__':
    main()
