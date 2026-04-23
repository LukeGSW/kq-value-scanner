"""Metriche di redditività derivate da ``fundamentals_snapshot``.

Calcola:
- ``roic_ttm``    — ROIC TTM (se non già in snapshot, ricavato da EBIT × (1−t))
- ``fcf_margin``  — FCF TTM / Revenue TTM
- ``fcf_yield``   — FCF TTM / MarketCap

+ quality flags per il rank:
- ``flag_fcf_positive``     : 1 se FCF TTM > 0
- ``flag_revenue_growth_ok``: 1 se revenue_growth_yoy ≥ soglia
- ``flag_roic_ok``          : 1 se ROIC ≥ soglia
- ``flag_quality_ok``       : 1 se tutti gli above sono 1

Soglie in ``settings.yaml → compute.quality``.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from pipeline.config import load_settings
from pipeline.storage.db import get_connection

logger = logging.getLogger("kq.compute.profitability")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(
        logging.Formatter(
            "%(asctime)s · %(name)s · %(levelname)s · %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)


# ------------------------------------------------------------------------------
def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        import math
        f = float(v)
        if math.isnan(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def compute_profitability_for_row(
    snap: dict[str, Any],
    quality_thresholds: dict[str, Any],
) -> dict[str, Any]:
    """Calcola metriche di redditività + quality flags per singolo ticker."""
    out: dict[str, Any] = {
        "roic_ttm": None,
        "fcf_margin": None,
        "fcf_yield": None,
        "flag_fcf_positive": None,
        "flag_revenue_growth_ok": None,
        "flag_roic_ok": None,
        "flag_quality_ok": None,
    }

    # ROIC: usa quello già presente nel snapshot (calcolato da EODHD Highlights)
    roic = _to_float(snap.get("roic"))
    out["roic_ttm"] = roic

    # FCF margin
    fcf = _to_float(snap.get("free_cash_flow_ttm"))
    rev = _to_float(snap.get("total_revenue_ttm"))
    if fcf is not None and rev is not None and rev > 0:
        out["fcf_margin"] = fcf / rev

    # FCF yield = FCF / MarketCap
    mcap = _to_float(snap.get("market_cap_usd"))
    if fcf is not None and mcap is not None and mcap > 0:
        out["fcf_yield"] = fcf / mcap

    # Quality flags
    rev_growth = _to_float(snap.get("revenue_growth_yoy"))
    roic_min = float(quality_thresholds.get("roic_min", 0.08))
    rev_growth_min = float(quality_thresholds.get("revenue_growth_min", 0.0))

    if fcf is not None:
        out["flag_fcf_positive"] = 1 if fcf > 0 else 0
    if rev_growth is not None:
        out["flag_revenue_growth_ok"] = 1 if rev_growth >= rev_growth_min else 0
    if roic is not None:
        out["flag_roic_ok"] = 1 if roic >= roic_min else 0

    # Aggregato: tutti e 3 i flag devono essere 1 (e non-None)
    flags = [
        out["flag_fcf_positive"],
        out["flag_revenue_growth_ok"],
        out["flag_roic_ok"],
    ]
    if all(f is not None for f in flags):
        out["flag_quality_ok"] = 1 if all(f == 1 for f in flags) else 0

    return out


def compute_profitability_all(db_path=None) -> list[dict[str, Any]]:
    settings = load_settings()
    quality = (settings.get("compute", {}) or {}).get("quality", {})

    with get_connection(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM fundamentals_snapshot", conn)
    if df.empty:
        logger.warning("fundamentals_snapshot vuoto.")
        return []

    rows: list[dict[str, Any]] = []
    for rec in df.to_dict(orient="records"):
        out = compute_profitability_for_row(rec, quality)
        out["ticker"] = rec["ticker"]
        rows.append(out)
    logger.info("Profitability: calcolate metriche per %d ticker", len(rows))
    return rows
