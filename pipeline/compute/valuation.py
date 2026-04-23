"""Valutazione derivata — PE percentile storico e PE relativo al settore.

Calcola due metriche chiave NON presenti negli Highlights EODHD:

- ``pe_percentile_5y`` : percentile del PE attuale rispetto al PE rolling
  degli ultimi 5 anni di storia. Richiede:
    * EPS TTM attuale (dal snapshot)
    * Prezzo storico 5Y (da prices_daily)
    → Proxy: PE_storico[d] = price[d] / (EPS TTM corrente). È un proxy
      (non ricostruisce EPS storici), utile per dare all'utente il
      contesto: "il prezzo attuale implica un multiplo alto/basso rispetto
      al tuo range di prezzo storico a parità di EPS".

- ``pe_vs_sector_median`` : ratio PE_ticker / mediana_settore_PE.
  >1 = più caro del settore; <1 = più economico.

Nota: la metrica "pura" (con EPS storici reali) richiederebbe scorrere
``financials_history/income/annual`` e costruire una serie EPS TTM —
rimandata a v2.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from pipeline.config import load_settings
from pipeline.storage.db import get_connection, get_universe

logger = logging.getLogger("kq.compute.valuation")
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


def _load_snapshots(db_path=None) -> pd.DataFrame:
    with get_connection(db_path) as conn:
        return pd.read_sql_query(
            "SELECT ticker, pe_ttm, market_cap_usd, shares_outstanding "
            "FROM fundamentals_snapshot", conn,
        )


def _load_prices_5y(tickers: list[str], years: int = 5, db_path=None) -> pd.DataFrame:
    """Carica prezzi (adjusted_close) degli ultimi N anni per i ticker richiesti."""
    if not tickers:
        return pd.DataFrame()
    cutoff = (datetime.utcnow() - timedelta(days=int(365.25 * years))).strftime("%Y-%m-%d")
    frames: list[pd.DataFrame] = []
    BATCH = 500
    for i in range(0, len(tickers), BATCH):
        chunk = tickers[i:i + BATCH]
        ph = ",".join(["?"] * len(chunk))
        sql = (
            f"SELECT ticker, date, adjusted_close, close FROM prices_daily "
            f"WHERE ticker IN ({ph}) AND date >= ?"
        )
        params = chunk + [cutoff]
        with get_connection(db_path) as conn:
            frames.append(pd.read_sql_query(sql, conn, params=params))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df


# ------------------------------------------------------------------------------
# Core
# ------------------------------------------------------------------------------
def compute_pe_percentile_5y(
    snap: dict[str, Any],
    prices: pd.DataFrame | None,
) -> float | None:
    """PE percentile rolling 5Y per un singolo ticker.

    Strategia (proxy):
    - EPS TTM implicito = MarketCap / shares_outstanding ÷ PE_TTM
      ...in realtà più semplice: EPS = price_today / PE_TTM
    - Serie PE_storica[d] = price[d] / EPS_implicito
    - Percentile = rank(PE_oggi, serie_PE_storica) / n

    Ritorna None se mancano input.
    """
    pe_now = _to_float(snap.get("pe_ttm"))
    if pe_now is None or pe_now <= 0:
        return None
    if prices is None or prices.empty:
        return None

    # EPS implicito dal prezzo più recente e pe_now
    col = "adjusted_close" if prices["adjusted_close"].notna().any() else "close"
    s = prices.sort_values("date").set_index("date")[col].astype(float).dropna()
    if s.empty:
        return None

    price_now = float(s.iloc[-1])
    if price_now <= 0:
        return None
    eps = price_now / pe_now
    if eps <= 0:
        return None

    pe_series = s / eps
    pe_series = pe_series[(pe_series > 0) & (pe_series < 1000)]  # filtro outlier estremi
    if len(pe_series) < 30:  # troppa poca storia
        return None

    # Percentile (frazione di valori ≤ pe_now)
    return float((pe_series <= pe_now).sum() / len(pe_series))


def compute_sector_relative_all(snapshots: pd.DataFrame,
                                  universe_df: pd.DataFrame) -> dict[str, float]:
    """Ritorna mappa ticker → pe_vs_sector_median.

    Media per settore calcolata sull'universo attivo (esclude valori non
    finiti o negativi).
    """
    # Merge snapshot con sector
    merged = snapshots.merge(
        universe_df[["ticker", "sector"]], on="ticker", how="left",
    )
    merged["pe_clean"] = merged["pe_ttm"].where(
        (merged["pe_ttm"].notna()) & (merged["pe_ttm"] > 0) & (merged["pe_ttm"] < 500),
    )
    sector_median = merged.groupby("sector")["pe_clean"].median()

    out: dict[str, float] = {}
    for _, row in merged.iterrows():
        sec = row["sector"]
        pe = row["pe_clean"]
        if pd.isna(pe) or sec is None:
            continue
        med = sector_median.get(sec)
        if pd.isna(med) or med is None or med == 0:
            continue
        out[row["ticker"]] = float(pe / med)
    return out


def compute_valuation_all(db_path=None) -> list[dict[str, Any]]:
    """Calcola valuation derivate per tutti i ticker."""
    settings = load_settings()
    lookback_y = int((settings.get("compute", {}) or {}).get(
        "pe_percentile_lookback_years", 5))

    snapshots = _load_snapshots(db_path)
    if snapshots.empty:
        logger.warning("fundamentals_snapshot vuoto.")
        return []

    universe_df = get_universe(active_only=True, db_path=db_path)

    # Sector relative: 1 query su tutto
    sector_rel = compute_sector_relative_all(snapshots, universe_df)

    # PE percentile 5Y per ticker (richiede prezzi storici)
    tickers = snapshots["ticker"].tolist()
    prices_all = _load_prices_5y(tickers, years=lookback_y, db_path=db_path)
    prices_by_tk = dict(iter(prices_all.groupby("ticker", sort=False)))

    rows: list[dict[str, Any]] = []
    for rec in snapshots.to_dict(orient="records"):
        tk = rec["ticker"]
        px = prices_by_tk.get(tk)
        out = {
            "ticker": tk,
            "pe_percentile_5y": compute_pe_percentile_5y(rec, px),
            "pe_vs_sector_median": sector_rel.get(tk),
        }
        rows.append(out)

    logger.info("Valuation: calcolate metriche per %d ticker", len(rows))
    return rows
