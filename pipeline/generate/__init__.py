"""Modulo generate — costruzione output JSON/HTML per il sito statico."""

from pipeline.generate.build_screener_json import (
    build_screener_json,
    ScreenerJsonResult,
)
from pipeline.generate.build_ticker_pages import (
    build_all_ticker_pages,
    build_one_ticker,
    TickerPagesResult,
)

__all__ = [
    "build_screener_json",
    "ScreenerJsonResult",
    "build_all_ticker_pages",
    "build_one_ticker",
    "TickerPagesResult",
]
