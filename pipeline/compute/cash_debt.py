"""Metriche cassa e debito da ``fundamentals_snapshot``.

Calcola i rapporti chiave per valutare la solidità finanziaria dell'azienda:

- ``net_debt_ebitda`` = net_debt / ebitda_ttm
- ``net_debt_fcf``    = net_debt / fcf_ttm
- ``interest_coverage`` = ebit_ttm / |interest_expense_ttm|
- ``current_ratio``   = total_current_assets / total_current_liabilities
- ``quick_ratio``     = (total_current_assets − inventory) / total_current_liabilities
- ``cash_ratio``      = total_cash / total_current_liabilities

Tutti i valori ``None`` se il denominatore è zero, negativo dove non ha senso,
o se i componenti mancano.

Ritorna una lista di dict pronti per il merge finale in ``computed_metrics``.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from pipeline.storage.db import get_connection

logger = logging.getLogger("kq.compute.cash_debt")
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
# Utility
# ------------------------------------------------------------------------------
def _safe_ratio(num: float | None, den: float | None,
                 allow_negative_den: bool = False) -> float | None:
    """Ratio sicuro: None se mancano input, den=0, o (di default) den<0."""
    if num is None or den is None:
        return None
    try:
        d = float(den)
        if d == 0:
            return None
        if not allow_negative_den and d < 0:
            return None
        return float(num) / d
    except (TypeError, ValueError):
        return None


# ------------------------------------------------------------------------------
# Core
# ------------------------------------------------------------------------------
def compute_cash_debt_for_row(snap: dict[str, Any]) -> dict[str, Any]:
    """Calcola rapporti cassa/debito per un singolo snapshot ticker."""
    out: dict[str, Any] = {
        "net_debt_ebitda": None,
        "net_debt_fcf": None,
        "interest_coverage": None,
        "current_ratio": None,
        "quick_ratio": None,
        "cash_ratio": None,
    }

    net_debt = snap.get("net_debt")
    ebitda = snap.get("ebitda_ttm")
    fcf = snap.get("free_cash_flow_ttm")
    ebit = snap.get("ebit_ttm")
    interest = snap.get("interest_expense_ttm")
    cur_assets = snap.get("total_current_assets")
    cur_liab = snap.get("total_current_liabilities")
    inventory = snap.get("inventory")
    total_cash = snap.get("total_cash")

    # Net Debt / EBITDA (può essere negativo se cassa netta)
    out["net_debt_ebitda"] = _safe_ratio(net_debt, ebitda, allow_negative_den=False)

    # Net Debt / FCF (evita FCF=0 o negativo per senso finanziario)
    out["net_debt_fcf"] = _safe_ratio(net_debt, fcf, allow_negative_den=False)

    # Interest Coverage: EBIT / |interest_expense|
    if ebit is not None and interest is not None:
        abs_int = abs(float(interest))
        if abs_int > 0:
            out["interest_coverage"] = float(ebit) / abs_int

    # Current ratio
    out["current_ratio"] = _safe_ratio(cur_assets, cur_liab)

    # Quick ratio (senza magazzino)
    if cur_assets is not None and cur_liab is not None:
        inv = float(inventory) if inventory is not None else 0.0
        num = float(cur_assets) - inv
        if float(cur_liab) > 0:
            out["quick_ratio"] = num / float(cur_liab)

    # Cash ratio
    out["cash_ratio"] = _safe_ratio(total_cash, cur_liab)

    return out


def compute_cash_debt_all(db_path=None) -> list[dict[str, Any]]:
    """Calcola metriche cassa/debito per tutti i ticker in ``fundamentals_snapshot``."""
    with get_connection(db_path) as conn:
        df = pd.read_sql_query(
            "SELECT * FROM fundamentals_snapshot", conn,
        )
    if df.empty:
        logger.warning("fundamentals_snapshot vuoto — nulla da calcolare.")
        return []

    rows: list[dict[str, Any]] = []
    for rec in df.to_dict(orient="records"):
        out = compute_cash_debt_for_row(rec)
        out["ticker"] = rec["ticker"]
        rows.append(out)
    logger.info("Cash/Debt: calcolate metriche per %d ticker", len(rows))
    return rows
