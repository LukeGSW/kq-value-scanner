"""Metriche tecniche da ``prices_daily``.

Calcola, per ciascun ticker attivo:
- ``last_close`` / ``last_close_date``
- ``sma_50``, ``sma_200``, ``pct_from_sma50``, ``pct_from_sma200``
- ``zscore_90``: z-score sulla SMA 90 delle log-returns
- ``hv_20_annualized``: HV 20 × √252
- ``drawdown_from_ath``, ``ath_date``
- ``rs_126d``: relative strength 6m vs benchmark configurato per exchange
- ``ytd_return``, ``one_year_return``

L'output è una lista di dict pronti per ``upsert_computed_metrics``.

Esempio
-------
>>> from pipeline.compute.technical import compute_technical_all
>>> rows = compute_technical_all()
>>> # …poi l'orchestratore fa upsert su computed_metrics
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

from pipeline.config import get_all_exchanges, load_settings
from pipeline.storage.db import get_connection, get_universe

logger = logging.getLogger("kq.compute.technical")
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
# Helpers
# ------------------------------------------------------------------------------
def _benchmark_map() -> dict[str, str]:
    """Ritorna mappa exchange_code → benchmark ticker (es. US → SPY.US)."""
    return {ex["code"]: ex["benchmark"] for ex in get_all_exchanges()}


def _load_prices(tickers: list[str], db_path=None) -> pd.DataFrame:
    """Carica prezzi (adjusted_close + close) per una lista di ticker in un'unica query.

    Returns
    -------
    pd.DataFrame con colonne: ticker, date (datetime), close, adjusted_close.
    """
    if not tickers:
        return pd.DataFrame()
    placeholders = ",".join(["?"] * len(tickers))
    sql = (
        f"SELECT ticker, date, close, adjusted_close "
        f"FROM prices_daily WHERE ticker IN ({placeholders}) "
        f"ORDER BY ticker, date"
    )
    with get_connection(db_path) as conn:
        df = pd.read_sql_query(sql, conn, params=tickers)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df


def _load_benchmark_series(benchmark_ticker: str, db_path=None) -> pd.Series:
    """Serie adjusted_close del benchmark indicizzata per data."""
    with get_connection(db_path) as conn:
        df = pd.read_sql_query(
            "SELECT date, adjusted_close FROM prices_daily WHERE ticker = ? "
            "ORDER BY date",
            conn, params=[benchmark_ticker],
        )
    if df.empty:
        return pd.Series(dtype=float, name=benchmark_ticker)
    df["date"] = pd.to_datetime(df["date"])
    s = df.set_index("date")["adjusted_close"].astype(float)
    s.name = benchmark_ticker
    return s


# ------------------------------------------------------------------------------
# Core: calcolo per singolo ticker
# ------------------------------------------------------------------------------
def compute_technical_for_series(
    px: pd.Series,
    bench: pd.Series | None,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Calcola metriche tecniche a partire da una serie prezzi (adjusted_close).

    Parameters
    ----------
    px : pd.Series
        Indicizzata per data, valori = adjusted_close.
    bench : pd.Series or None
        Benchmark per calcolo RS (stessa convenzione). None → rs_126d = None.
    params : dict
        Parametri compute da settings.yaml (chiave ``compute``).
    """
    out: dict[str, Any] = {
        "last_close": None, "last_close_date": None,
        "sma_50": None, "sma_200": None,
        "pct_from_sma50": None, "pct_from_sma200": None,
        "zscore_90": None, "hv_20_annualized": None,
        "drawdown_from_ath": None, "ath_date": None,
        "rs_126d": None,
        "ytd_return": None, "one_year_return": None,
    }

    if px is None or px.empty:
        return out

    px = px.astype(float).sort_index()
    last_close = float(px.iloc[-1])
    last_date = px.index[-1]
    out["last_close"] = last_close
    out["last_close_date"] = last_date.strftime("%Y-%m-%d")

    # SMA
    n_short = int(params.get("sma_short", 50))
    n_long = int(params.get("sma_long", 200))
    if len(px) >= n_short:
        sma_s = float(px.tail(n_short).mean())
        out["sma_50"] = sma_s
        out["pct_from_sma50"] = (last_close / sma_s - 1.0) if sma_s else None
    if len(px) >= n_long:
        sma_l = float(px.tail(n_long).mean())
        out["sma_200"] = sma_l
        out["pct_from_sma200"] = (last_close / sma_l - 1.0) if sma_l else None

    # Log returns + zScore sulla finestra
    logret = np.log(px / px.shift(1)).dropna()
    z_win = int(params.get("zscore_window", 90))
    if len(logret) >= z_win:
        recent = logret.tail(z_win)
        mu = float(recent.mean())
        sd = float(recent.std(ddof=1))
        if sd > 0:
            # zScore dell'ULTIMO log-return rispetto alla finestra
            out["zscore_90"] = float((recent.iloc[-1] - mu) / sd)

    # Historical Volatility 20d annualizzata
    hv_win = int(params.get("hv_window", 20))
    hv_ann = float(params.get("hv_annualization_factor", 252))
    if len(logret) >= hv_win:
        hv = float(logret.tail(hv_win).std(ddof=1) * np.sqrt(hv_ann))
        out["hv_20_annualized"] = hv

    # Drawdown from ATH
    cummax = px.cummax()
    dd_series = (px / cummax) - 1.0
    out["drawdown_from_ath"] = float(dd_series.iloc[-1])
    ath_idx = px.idxmax()
    out["ath_date"] = ath_idx.strftime("%Y-%m-%d") if ath_idx is not None else None

    # Returns
    # YTD: da 2 gennaio dell'anno corrente → ultimo
    ytd_start = pd.Timestamp(year=last_date.year, month=1, day=1)
    ytd_slice = px.loc[px.index >= ytd_start]
    if len(ytd_slice) >= 2:
        out["ytd_return"] = float(ytd_slice.iloc[-1] / ytd_slice.iloc[0] - 1.0)

    # 1Y return (252 td back)
    if len(px) >= 252:
        out["one_year_return"] = float(px.iloc[-1] / px.iloc[-252] - 1.0)

    # Relative Strength su rs_window vs benchmark
    rs_win = int(params.get("rs_window", 126))
    if bench is not None and not bench.empty and len(px) >= rs_win:
        # Allinea sulle date comuni, prendi la finestra
        joined = pd.concat([px.rename("p"), bench.rename("b")], axis=1).dropna()
        if len(joined) >= rs_win:
            tail = joined.tail(rs_win)
            p_ret = tail["p"].iloc[-1] / tail["p"].iloc[0] - 1.0
            b_ret = tail["b"].iloc[-1] / tail["b"].iloc[0] - 1.0
            # RS come (1+rp)/(1+rb) - 1
            out["rs_126d"] = float((1 + p_ret) / (1 + b_ret) - 1.0)

    return out


# ------------------------------------------------------------------------------
# Orchestrazione su tutto l'universo
# ------------------------------------------------------------------------------
def compute_technical_all(
    exchange_code: str | None = None,
    db_path=None,
) -> list[dict[str, Any]]:
    """Calcola tutte le metriche tecniche per tutti i ticker attivi.

    Returns
    -------
    list of dict
        Dict con chiavi compatibili con ``computed_metrics``, con almeno
        ``ticker`` + metriche tecniche. Da unire con gli altri moduli compute
        prima dell'upsert finale.
    """
    settings = load_settings()
    params = settings.get("compute", {})

    universe_df = get_universe(active_only=True, exchange_code=exchange_code,
                                db_path=db_path)
    if universe_df.empty:
        logger.warning("Universe vuoto — nulla da calcolare.")
        return []

    # Pre-carica benchmark per exchange presenti
    bench_map = _benchmark_map()
    benches_needed = {
        bench_map[xc] for xc in universe_df["exchange_code"].unique()
        if xc in bench_map
    }
    bench_series: dict[str, pd.Series] = {
        b: _load_benchmark_series(b, db_path) for b in benches_needed
    }
    for b, s in bench_series.items():
        logger.info("Benchmark %s: %d osservazioni", b, len(s))

    tickers = universe_df["ticker"].tolist()
    logger.info("Technical: loading prezzi per %d ticker…", len(tickers))

    # Carica a batch (SQLite param limit ~999)
    BATCH = 500
    frames: list[pd.DataFrame] = []
    for i in range(0, len(tickers), BATCH):
        chunk = tickers[i:i + BATCH]
        frames.append(_load_prices(chunk, db_path))
    prices_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if prices_df.empty:
        logger.warning("Nessun prezzo in DB — esegui prima fetch_prices.")
        return []

    # Map ticker → Series adjusted_close
    rows: list[dict[str, Any]] = []
    grouped = prices_df.groupby("ticker", sort=False)
    ticker_to_exchange = dict(zip(universe_df["ticker"], universe_df["exchange_code"]))

    for ticker, g in grouped:
        # Adjusted close è preferibile; fallback a close
        col = "adjusted_close" if g["adjusted_close"].notna().any() else "close"
        px = g.set_index("date")[col].astype(float)
        xc = ticker_to_exchange.get(ticker)
        bench_tk = bench_map.get(xc) if xc else None
        bench = bench_series.get(bench_tk) if bench_tk else None

        metrics = compute_technical_for_series(px, bench, params)
        metrics["ticker"] = ticker
        rows.append(metrics)

    logger.info("Technical: calcolate metriche per %d ticker", len(rows))
    return rows
