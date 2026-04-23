"""Heatmap stagionalità mensile 10Y per ticker.

Produce una matrice mese × anno dei rendimenti mensili % (adjusted close),
più una riga "AVG" (media mensile attraverso gli anni).

Output: dict serializzabile per il JSON di pagina ticker:

```
{
    "years":  [2016, 2017, ..., 2025, 2026],
    "months": ["Gen","Feb",...,"Dic","AVG"],
    "matrix": [[ret_2016_gen, ret_2016_feb, ..., ret_2016_dic],
               ...,
               [avg_gen, avg_feb, ..., avg_dic]]
}
```

La heatmap non va in ``computed_metrics`` — è un artefatto per la pagina
ticker. Esposta via ``compute_seasonality_for_ticker()``; l'orchestratore
opzionalmente chiama ``compute_seasonality_all_json()`` per precomputare
tutti i ticker e scaricarli a file.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from pipeline.storage.db import get_connection, get_universe

logger = logging.getLogger("kq.compute.seasonality")
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


_MONTH_LABELS_IT = ["Gen", "Feb", "Mar", "Apr", "Mag", "Giu",
                     "Lug", "Ago", "Set", "Ott", "Nov", "Dic"]


def _load_prices(ticker: str, years: int = 10, db_path=None) -> pd.Series:
    cutoff = (datetime.utcnow() - timedelta(days=int(365.25 * years))).strftime("%Y-%m-%d")
    with get_connection(db_path) as conn:
        df = pd.read_sql_query(
            "SELECT date, adjusted_close, close FROM prices_daily "
            "WHERE ticker = ? AND date >= ? ORDER BY date",
            conn, params=[ticker, cutoff],
        )
    if df.empty:
        return pd.Series(dtype=float)
    df["date"] = pd.to_datetime(df["date"])
    col = "adjusted_close" if df["adjusted_close"].notna().any() else "close"
    return df.set_index("date")[col].astype(float).dropna()


def compute_seasonality_for_ticker(
    ticker: str, years: int = 10, db_path=None,
) -> dict[str, Any] | None:
    """Matrice mese × anno dei rendimenti mensili per un ticker.

    Returns
    -------
    dict or None
        Struttura JSON-ready, None se dati insufficienti.
    """
    px = _load_prices(ticker, years=years, db_path=db_path)
    if px.empty or len(px) < 60:
        return None

    # Resample a fine mese (last trading day effettivo del mese)
    monthly = px.resample("ME").last()
    returns = monthly.pct_change().dropna()
    if returns.empty:
        return None

    # Pivot: index=anno, columns=mese (1-12)
    tmp = returns.to_frame("r")
    tmp["year"] = tmp.index.year
    tmp["month"] = tmp.index.month
    pivot = tmp.pivot_table(index="year", columns="month", values="r")

    # Assicura 12 colonne
    for m in range(1, 13):
        if m not in pivot.columns:
            pivot[m] = np.nan
    pivot = pivot[sorted(pivot.columns)]

    # Limita agli anni dell'ultimo lookback
    current_year = datetime.utcnow().year
    min_year = current_year - years
    pivot = pivot.loc[pivot.index >= min_year]

    # Media per mese (AVG row)
    avg = pivot.mean(axis=0, skipna=True)

    years_list = list(pivot.index)
    matrix = []
    for y in years_list:
        row = pivot.loc[y].values
        matrix.append([None if pd.isna(v) else float(v) for v in row])
    avg_row = [None if pd.isna(v) else float(v) for v in avg.values]

    return {
        "ticker": ticker,
        "years": years_list + ["AVG"],
        "months": _MONTH_LABELS_IT,
        "matrix": matrix + [avg_row],
        "n_years": len(years_list),
    }


def compute_seasonality_all_json(
    tickers: list[str] | None = None,
    years: int = 10,
    db_path=None,
) -> dict[str, dict[str, Any]]:
    """Calcola stagionalità per tutti i ticker attivi. Ritorna ``{ticker: heatmap}``."""
    if tickers is None:
        u = get_universe(active_only=True, db_path=db_path)
        tickers = u["ticker"].tolist() if not u.empty else []

    out: dict[str, dict[str, Any]] = {}
    for tk in tickers:
        heatmap = compute_seasonality_for_ticker(tk, years=years, db_path=db_path)
        if heatmap is not None:
            out[tk] = heatmap
    logger.info("Seasonality: heatmap generate per %d/%d ticker", len(out), len(tickers))
    return out
