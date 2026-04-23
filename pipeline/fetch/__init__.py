"""Modulo fetch — interazione con API EODHD per prezzi, fundamentals, screener."""

from pipeline.fetch.eodhd_client import EODHDClient
from pipeline.fetch.fetch_universe import build_universe, save_universe_parquet
from pipeline.fetch.fetch_prices import (
    fetch_prices_bulk,
    fetch_prices_backfill,
    FetchPricesResult,
)
from pipeline.fetch.fetch_fundamentals import (
    fetch_fundamentals_bulk,
    fetch_fundamentals_single,
    FetchFundamentalsResult,
    parse_snapshot,
    parse_history_rows,
)

__all__ = [
    "EODHDClient",
    "build_universe",
    "save_universe_parquet",
    "fetch_prices_bulk",
    "fetch_prices_backfill",
    "FetchPricesResult",
    "fetch_fundamentals_bulk",
    "fetch_fundamentals_single",
    "FetchFundamentalsResult",
    "parse_snapshot",
    "parse_history_rows",
]
