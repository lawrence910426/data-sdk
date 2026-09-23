"""Offline tests for the warrant side of FinMindWrapper (no network, no NFS)."""
import os

import pandas as pd
import pytest

from data_sdk.wrappers import finmind_broker_wrapper as module
from data_sdk.wrappers.finmind_broker_wrapper import BROKER_COLUMNS, FinMindWrapper


def _rows(day, codes, buy_dtype="int32"):
    frame = pd.DataFrame({
        "securities_trader": ["元大"] * len(codes),
        "price": [1.0] * len(codes),
        "buy": pd.Series([1000] * len(codes), dtype=buy_dtype),
        "sell": pd.Series([0] * len(codes), dtype=buy_dtype),
        "securities_trader_id": ["9800"] * len(codes),
        "stock_id": codes,
        "date": [day] * len(codes),
    })
    return frame


@pytest.fixture
def archive(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SDK_FINMIND_BROKER_PATH", str(tmp_path / "stocks"))
    monkeypatch.setenv("DATA_SDK_FINMIND_BROKER_WARRANT_PATH", str(tmp_path / "warrants"))
    return tmp_path


def test_stock_and_warrant_archives_come_from_their_own_env_vars(archive):
    stock_path = FinMindWrapper.broker_day_path("2026-08-06")
    warrant_path = FinMindWrapper.broker_day_path("2026-08-06", warrant=True)
    assert stock_path == str(archive / "stocks" / "2026-08-06.parquet")
    assert warrant_path == str(archive / "warrants" / "2026-08-06.parquet")


def test_write_pins_share_counts_to_int64_and_reads_back_with_filters(archive):
    wrapper = FinMindWrapper.__new__(FinMindWrapper)   # no DataLoader login needed
    day = "2026-08-06"
    frame = _rows(day, ["030001", "030001", "700448"])   # one duplicate row
    frame.loc[2, "securities_trader_id"] = "9227"
    path = wrapper.write_broker_day(day, frame, warrant=True)
    assert path == FinMindWrapper.broker_day_path(day, warrant=True)

    stored = pd.read_parquet(path)
    assert list(stored.columns) == BROKER_COLUMNS
    assert stored["buy"].dtype == "int64" and stored["sell"].dtype == "int64"
    assert len(stored) == 2                                     # duplicate dropped

    assert wrapper.archived_stock_ids(day, warrant=True) == {"030001", "700448"}
    by_code = wrapper.get_broker(day, sid="700448", warrant=True)
    assert by_code["securities_trader_id"].tolist() == ["9227"]
    by_branch = wrapper.get_broker(day, trader_id="9800", warrant=True)
    assert by_branch["stock_id"].tolist() == ["030001"]
    whole = wrapper.get_broker(day, warrant=True)
    assert len(whole) == 2
    with pytest.raises(FileNotFoundError):
        wrapper.get_broker(day, sid="700448")   # the stock archive has no such day


class _FakeApi:
    def __init__(self, report, traded):
        self._report = report
        self._traded = traded

    def taiwan_stock_warrant_trading_daily_report(self, date, use_object):
        assert use_object
        return self._report

    def taiwan_stock_daily(self, start_date, end_date):
        return pd.DataFrame({"stock_id": sorted(self._traded), "Trading_Volume": [1] * len(self._traded)})


class _FakeLimiter:
    def acquire(self, n=1):
        pass


@pytest.fixture
def fetcher(monkeypatch):
    def build(report, traded):
        wrapper = FinMindWrapper.__new__(FinMindWrapper)
        monkeypatch.setattr(FinMindWrapper, "_api", _FakeApi(report, traded))
        monkeypatch.setattr(FinMindWrapper, "limiter", classmethod(lambda cls: _FakeLimiter()))
        return wrapper
    return build


def test_fetch_warrant_day_accepts_a_full_day(fetcher, monkeypatch):
    monkeypatch.setattr(module, "MIN_EXPECTED_WARRANTS", 3)
    day = "2026-08-06"
    codes = ["030001", "030002", "700448", "700449"]
    result = fetcher(_rows(day, codes), traded={"030001", "700448"}).fetch_warrant_day(day)
    assert result.complete
    assert result.received == set(codes)
    assert result.requests_spent == 2


def test_fetch_warrant_day_refuses_a_thin_or_misdated_object(fetcher, monkeypatch):
    monkeypatch.setattr(module, "MIN_EXPECTED_WARRANTS", 3)
    day = "2026-08-06"
    thin = fetcher(_rows(day, ["030001"]), traded=set()).fetch_warrant_day(day)
    assert not thin.complete and thin.unresolved == {day}
    misdated = fetcher(_rows("2026-08-05", ["030001", "030002", "030003"]), traded=set()).fetch_warrant_day(day)
    assert not misdated.complete and misdated.unresolved == {day}
    empty = fetcher(pd.DataFrame(), traded=set()).fetch_warrant_day(day)
    assert not empty.complete and empty.unresolved == {day}


def test_fetch_warrant_day_refuses_when_traded_warrants_are_missing(fetcher, monkeypatch):
    monkeypatch.setattr(module, "MIN_EXPECTED_WARRANTS", 3)
    day = "2026-08-06"
    report = _rows(day, ["030001", "030002", "030003"])
    traded = {"030001", "030002", "030003", "700448", "700449"}   # 40% missing
    result = fetcher(report, traded).fetch_warrant_day(day)
    assert not result.complete
    assert result.unresolved == {"700448", "700449"}
    within_tolerance = fetcher(report, {"030001", "030002", "030003"}).fetch_warrant_day(day)
    assert within_tolerance.complete and within_tolerance.absent == set()


def test_warrant_code_ranges():
    accepted = [
        "030001",   # TWSE call, first
        "089999",   # TWSE call, last
        "05309U",   # TWSE domestic put
        "03732B",   # TWSE bear
        "700000",   # TPEx call, first
        "739999",   # TPEx call, last
        "73001P",   # TPEx put
        "70123C",   # TPEx bull
    ]
    rejected = [
        "2330",     # stock
        "0050",     # ETF
        "006208",   # ETF
        "00631L",   # leveraged ETF
        "01001T",   # REIT (T suffix, but outside the warrant range)
        "020000",   # ETN
        "02001L",   # leveraged ETN
        "030000",   # below the TWSE call range
        "090001",   # above the TWSE call range
        "740000",   # above the TPEx call range
        "05309A",   # letter that is not a warrant kind
        "TAIEX",
    ]
    assert all(module.is_warrant_code(code) for code in accepted)
    assert not any(module.is_warrant_code(code) for code in rejected)
