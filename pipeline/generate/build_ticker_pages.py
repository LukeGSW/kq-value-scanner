"""Genera un file JSON per ciascun ticker attivo.

Output: ``site/data/tickers/{ticker}.json`` (es.
``site/data/tickers/AAPL.US.json``). Contiene tutto il necessario per
popolare la pagina di dettaglio single-ticker:

- ``meta``            : identità, timestamp, exchange, benchmark
- ``hero``            : prezzo corrente, market cap, next earnings, meta-chip
- ``snapshot``        : 7 card valutazione (PE/Forward/PEG/EV/EBITDA/…)
- ``cash_debt``       : banner cassa/debito + 8 ratio di solidità
- ``cashflow_quality``: OCF/NI, FCF, FCF margin, CapEx/Revenue
- ``profitability``   : ROE/ROA/ROIC/Gross margin/Operating margin
- ``price_series``    : OHLC 10Y + SMA 50/200 + volumi + earnings markers
- ``technical``       : zScore 90, HV 20, drawdown, RS 126d, rendimento YTD/1Y
- ``diagnostics``     : serie drawdown rolling, HV rolling, RS rolling
- ``seasonality``     : heatmap mensile 10Y (dalla funzione in compute.seasonality)
- ``scores``          : Altman Z, Piotroski F, Beneish M (con label rating)
- ``kq_score``        : score composito 0-100 + ranking globale/settoriale
- ``ai_read``         : lettura sintetica generata meccanicamente

Uso CLI
-------
# Genera TUTTI i ticker attivi (default)
python -m pipeline.generate.build_ticker_pages

# Solo un ticker (debug)
python -m pipeline.generate.build_ticker_pages --ticker AAPL.US

# Solo un exchange
python -m pipeline.generate.build_ticker_pages --exchange US

# Limita per test
python -m pipeline.generate.build_ticker_pages --max-tickers 50
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from pipeline.config import PROJECT_ROOT, get_all_exchanges, load_settings
from pipeline.compute.seasonality import compute_seasonality_for_ticker
from pipeline.storage.db import get_connection, get_universe

logger = logging.getLogger("kq.generate.ticker_pages")
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


SCHEMA_VERSION = "1.0.0"


# ------------------------------------------------------------------------------
@dataclass
class TickerPagesResult:
    tickers_in: int = 0
    tickers_out: int = 0
    tickers_skipped: int = 0
    total_size_bytes: int = 0
    output_dir: str = ""
    started_at: str = ""
    ended_at: str = ""
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        mb = self.total_size_bytes / (1024 * 1024)
        return (
            f"TickerPagesResult(in={self.tickers_in}, out={self.tickers_out}, "
            f"skipped={self.tickers_skipped}, total={mb:.1f} MB, "
            f"errors={len(self.errors)})"
        )


# ------------------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------------------
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean(v: Any, ndigits: int | None = None) -> Any:
    """Vedi build_screener_json._clean."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(v, "item") and not isinstance(v, (str, bytes)):
        try:
            v = v.item()
        except Exception:
            pass
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        if ndigits is not None:
            return round(v, ndigits)
    return v


def _output_dir(cli_override: str | None = None) -> Path:
    if cli_override:
        p = Path(cli_override)
    else:
        settings = load_settings()
        raw = settings["output"]["tickers_dir"]
        p = Path(raw)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    p.mkdir(parents=True, exist_ok=True)
    return p


def _benchmark_for(exchange_code: str) -> str | None:
    for ex in get_all_exchanges():
        if ex["code"] == exchange_code:
            return ex.get("benchmark")
    return None


# ------------------------------------------------------------------------------
# DB loaders (batch — quando possibile — per non aprire N connessioni)
# ------------------------------------------------------------------------------
def _load_full_data(db_path=None) -> dict[str, pd.DataFrame]:
    """Carica in RAM i DF principali usati da tutti i ticker (universe, metrics,
    snapshot, fundamentals history annuale per i trend a 5 anni)."""
    with get_connection(db_path) as conn:
        universe = pd.read_sql_query(
            """
            SELECT * FROM universe WHERE is_active = 1
            """,
            conn,
        )
        metrics = pd.read_sql_query("SELECT * FROM computed_metrics", conn)
        snap = pd.read_sql_query("SELECT * FROM fundamentals_snapshot", conn)
        # Storia annuale — solo balance (per trend debito) + cashflow (OCF, CapEx)
        hist_annual = pd.read_sql_query(
            """
            SELECT ticker, period_end, statement_type,
                   long_term_debt, short_term_debt, cash_and_equivalents,
                   short_term_investments, total_revenue, operating_cashflow,
                   capital_expenditure, free_cash_flow, net_income,
                   total_equity, total_assets, total_liabilities
              FROM financials_history
             WHERE freq = 'annual'
             ORDER BY ticker, period_end
            """,
            conn,
        )
    return {
        "universe":    universe,
        "metrics":     metrics,
        "snap":        snap,
        "hist_annual": hist_annual,
    }


def _load_ohlc(ticker: str, db_path=None) -> pd.DataFrame:
    """Carica OHLCV per un ticker (tutta la storia presente)."""
    with get_connection(db_path) as conn:
        df = pd.read_sql_query(
            """
            SELECT date, open, high, low, close, adjusted_close, volume
              FROM prices_daily
             WHERE ticker = ?
             ORDER BY date ASC
            """,
            conn, params=[ticker],
        )
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


# ------------------------------------------------------------------------------
# Builders per sezione
# ------------------------------------------------------------------------------
def _build_meta(
    u_row: pd.Series, benchmark: str | None,
) -> dict[str, Any]:
    return {
        "schema_version":  SCHEMA_VERSION,
        "generator":       "pipeline.generate.build_ticker_pages",
        "build_ts_utc":    _utc_now_iso(),
        "ticker":          _clean(u_row.get("ticker")),
        "code":            _clean(u_row.get("code")),
        "name":            _clean(u_row.get("name")),
        "exchange":        _clean(u_row.get("exchange_code")),
        "exchange_name":   _clean(u_row.get("exchange_name")),
        "country":         _clean(u_row.get("country")),
        "currency":        _clean(u_row.get("currency")),
        "type":            _clean(u_row.get("type")),
        "sector":          _clean(u_row.get("sector")),
        "industry":        _clean(u_row.get("industry")),
        "isin":            _clean(u_row.get("isin")),
        "benchmark":       benchmark,
    }


def _build_hero(
    u_row: pd.Series, m_row: pd.Series, s_row: pd.Series,
) -> dict[str, Any]:
    last_close = _clean(m_row.get("last_close"), 4)
    # Market cap: preferiamo snapshot, fallback universe
    mcap = s_row.get("market_cap_usd")
    if mcap is None or (isinstance(mcap, float) and math.isnan(mcap)):
        mcap = u_row.get("market_capitalization")
    return {
        "last_close":         last_close,
        "last_close_date":    _clean(m_row.get("last_close_date")),
        "market_cap_usd":     _clean(mcap, 0),
        "shares_outstanding": _clean(s_row.get("shares_outstanding"), 0),
        "beta":               _clean(s_row.get("beta"), 3),
        "next_earnings_date": _clean(s_row.get("next_earnings_date")),
        "most_recent_quarter":_clean(s_row.get("most_recent_quarter")),
        # rendimenti
        "ytd_return":         _clean(m_row.get("ytd_return"), 4),
        "one_year_return":    _clean(m_row.get("one_year_return"), 4),
    }


def _build_snapshot(
    m_row: pd.Series, s_row: pd.Series,
) -> dict[str, Any]:
    """Sette card valutazione + sotto-field di contesto."""
    return {
        "pe_ttm":              _clean(s_row.get("pe_ttm"), 3),
        "forward_pe":          _clean(s_row.get("forward_pe"), 3),
        "peg":                 _clean(s_row.get("peg"), 3),
        "ev_to_ebitda":        _clean(s_row.get("ev_to_ebitda"), 3),
        "ev_to_revenue":       _clean(s_row.get("ev_to_revenue"), 3),
        "price_to_book":       _clean(s_row.get("price_to_book"), 3),
        "price_to_sales_ttm":  _clean(s_row.get("price_to_sales_ttm"), 3),
        "pe_percentile_5y":    _clean(m_row.get("pe_percentile_5y"), 4),
        "pe_vs_sector_median": _clean(m_row.get("pe_vs_sector_median"), 4),
        "fcf_yield":           _clean(m_row.get("fcf_yield"), 4),
        "earnings_yield":      _clean(s_row.get("earnings_yield"), 4),
        "dividend_yield":      _clean(s_row.get("dividend_yield"), 4),
        "payout_ratio":        _clean(s_row.get("payout_ratio"), 4),
        "dividend_per_share":  _clean(s_row.get("dividend_per_share"), 4),
        # Growth di contesto
        "revenue_growth_yoy":       _clean(s_row.get("revenue_growth_yoy"), 4),
        "eps_growth_yoy":           _clean(s_row.get("eps_growth_yoy"), 4),
        "earnings_growth_next_year":_clean(s_row.get("earnings_growth_next_year"), 4),
    }


def _build_cash_debt(
    m_row: pd.Series, s_row: pd.Series,
    history_df: pd.DataFrame,
) -> dict[str, Any]:
    # Trend 5 anni del debito netto (dalla history annuale: LT+ST debt − cash)
    trend = _net_debt_trend_5y(history_df)
    # Debt / Equity (dal più recente annual record se disponibile)
    debt_equity = _debt_equity_latest(history_df)

    return {
        # Totali
        "total_cash":      _clean(s_row.get("total_cash"), 0),
        "total_debt":      _clean(s_row.get("total_debt"), 0),
        "net_debt":        _clean(s_row.get("net_debt"), 0),
        # Ratios
        "net_debt_ebitda":   _clean(m_row.get("net_debt_ebitda"), 3),
        "net_debt_fcf":      _clean(m_row.get("net_debt_fcf"), 3),
        "interest_coverage": _clean(m_row.get("interest_coverage"), 2),
        "debt_equity":       debt_equity,
        "current_ratio":     _clean(m_row.get("current_ratio"), 3),
        "quick_ratio":       _clean(m_row.get("quick_ratio"), 3),
        "cash_ratio":        _clean(m_row.get("cash_ratio"), 3),
        # Trend storico per grafico
        "net_debt_trend_5y": trend,
    }


def _safe_float(v: Any, default: float = 0.0) -> float:
    """Converte a float trattando NaN/None/stringhe vuote come ``default``.

    Nota: ``v or default`` in Python NON funziona per NaN — ``NaN`` è truthy
    e passa attraverso. Questa helper è l'unico modo corretto.
    """
    if v is None:
        return default
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    if math.isnan(f) or math.isinf(f):
        return default
    return f


def _net_debt_trend_5y(history_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Ritorna [{year, net_debt}, …] per gli ultimi 5 annual close disponibili."""
    if history_df is None or history_df.empty:
        return []
    bs = history_df[history_df["statement_type"] == "balance"].copy()
    if bs.empty:
        return []
    bs["period_end"] = pd.to_datetime(bs["period_end"])
    bs = bs.sort_values("period_end")
    bs = bs.tail(5)
    out: list[dict[str, Any]] = []
    for _, r in bs.iterrows():
        ltd = _safe_float(r.get("long_term_debt"))
        std = _safe_float(r.get("short_term_debt"))
        cash = _safe_float(r.get("cash_and_equivalents"))
        sti = _safe_float(r.get("short_term_investments"))
        nd = (ltd + std) - (cash + sti)
        # Se tutti i componenti erano NaN/None → nd=0.0 ma non significativo
        if ltd == 0 and std == 0 and cash == 0 and sti == 0:
            continue
        out.append({
            "year":     int(r["period_end"].year),
            "net_debt": round(nd, 0),
        })
    return out


def _debt_equity_latest(history_df: pd.DataFrame) -> float | None:
    """Debt/Equity all'ultimo annual disponibile (LT+ST debt / total_equity)."""
    if history_df is None or history_df.empty:
        return None
    bs = history_df[history_df["statement_type"] == "balance"].copy()
    if bs.empty:
        return None
    bs = bs.sort_values("period_end")
    r = bs.iloc[-1]
    ltd = _safe_float(r.get("long_term_debt"))
    std = _safe_float(r.get("short_term_debt"))
    eq = _safe_float(r.get("total_equity"))
    if eq == 0:
        return None
    ratio = (ltd + std) / eq
    if math.isnan(ratio) or math.isinf(ratio):
        return None
    return round(ratio, 3)


def _build_cashflow_quality(
    m_row: pd.Series, s_row: pd.Series,
    history_df: pd.DataFrame,
) -> dict[str, Any]:
    """OCF/NI ratio, FCF TTM, FCF margin, CapEx/Revenue — dal più recente annual."""
    ocf_ni = None
    capex_rev = None
    if history_df is not None and not history_df.empty:
        # Most recent annual per ticker (cashflow + income mix)
        cf = history_df[history_df["statement_type"] == "cashflow"].copy()
        inc = history_df[history_df["statement_type"] == "income"].copy()
        if not cf.empty:
            cf = cf.sort_values("period_end").iloc[-1]
            if not inc.empty:
                # match by period_end, else latest income
                target_end = cf["period_end"]
                inc_match = inc[inc["period_end"] == target_end]
                if inc_match.empty:
                    inc_match = inc.sort_values("period_end").iloc[[-1]]
                inc_r = inc_match.iloc[0]
                # Uso _safe_float: scarta NaN/None/stringhe vuote.
                # `x or 0` non basta perché NaN è truthy in Python.
                ni = _safe_float(inc_r.get("net_income"))
                ocf = _safe_float(cf.get("operating_cashflow"))
                rev = _safe_float(inc_r.get("total_revenue"))
                capex = _safe_float(cf.get("capital_expenditure"))
                if ni != 0 and ocf != 0:
                    ratio = ocf / ni
                    if not (math.isnan(ratio) or math.isinf(ratio)):
                        ocf_ni = round(ratio, 3)
                if rev != 0 and capex != 0:
                    # capex tipicamente negativo; abs per ratio intensivo
                    ratio = abs(capex) / rev
                    if not (math.isnan(ratio) or math.isinf(ratio)):
                        capex_rev = round(ratio, 4)

    return {
        "fcf_ttm":    _clean(s_row.get("free_cash_flow_ttm"), 0),
        "fcf_margin": _clean(m_row.get("fcf_margin"), 4),
        "ocf_ni":     ocf_ni,
        "capex_rev":  capex_rev,
    }


def _build_profitability(
    m_row: pd.Series, s_row: pd.Series,
) -> dict[str, Any]:
    return {
        "roe":              _clean(s_row.get("roe"), 4),
        "roa":              _clean(s_row.get("roa"), 4),
        "roic":             _clean(s_row.get("roic"), 4),
        "roic_ttm":         _clean(m_row.get("roic_ttm"), 4),
        "gross_margin":     _clean(s_row.get("gross_margin"), 4),
        "operating_margin": _clean(s_row.get("operating_margin"), 4),
        "profit_margin":    _clean(s_row.get("profit_margin"), 4),
    }


def _build_price_series(
    ticker: str, ohlc: pd.DataFrame,
    history_df: pd.DataFrame,
) -> dict[str, Any]:
    """OHLC + SMA 50/200 + volumi + earnings markers.

    Le serie sono restituite in formato lightweight-charts:
    - candles: [{time: unix_s, open, high, low, close}, …]
    - volume:  [{time, value, color}, …]
    - sma50 / sma200: [{time, value}, …]
    - earnings: [{time, label}, …] — dalle period_end quarterly income
    """
    if ohlc is None or ohlc.empty:
        return {
            "candles":   [],
            "volume":    [],
            "sma50":     [],
            "sma200":    [],
            "earnings":  [],
            "log_returns": [],
            "zscore":    [],
        }

    settings = load_settings()
    sma_short = int(settings["compute"]["sma_short"])
    sma_long = int(settings["compute"]["sma_long"])
    z_window = int(settings["compute"]["zscore_window"])

    df = ohlc.sort_values("date").reset_index(drop=True)
    # Unix seconds
    ts = (df["date"].astype("int64") // 10**9).astype("int64")

    # OHLC (useremo 'close' non adjusted per le candele, come nel mockup)
    candles = []
    for i in range(len(df)):
        if pd.isna(df["close"].iloc[i]):
            continue
        row_c = {
            "time": int(ts.iloc[i]),
            "open": _clean(df["open"].iloc[i], 4),
            "high": _clean(df["high"].iloc[i], 4),
            "low":  _clean(df["low"].iloc[i], 4),
            "close":_clean(df["close"].iloc[i], 4),
        }
        candles.append(row_c)

    # SMA
    close = df["close"].astype(float)
    sma_s = close.rolling(sma_short).mean()
    sma_l = close.rolling(sma_long).mean()
    sma50_series = [
        {"time": int(ts.iloc[i]), "value": round(float(sma_s.iloc[i]), 4)}
        for i in range(len(df)) if not pd.isna(sma_s.iloc[i])
    ]
    sma200_series = [
        {"time": int(ts.iloc[i]), "value": round(float(sma_l.iloc[i]), 4)}
        for i in range(len(df)) if not pd.isna(sma_l.iloc[i])
    ]

    # Volumi con color in base a close>=open
    volume = []
    for i in range(len(df)):
        v = df["volume"].iloc[i]
        if pd.isna(v):
            continue
        o = df["open"].iloc[i]
        c = df["close"].iloc[i]
        color = "rgba(16,185,129,0.45)" if (pd.notna(o) and pd.notna(c) and c >= o) \
            else "rgba(239,68,68,0.45)"
        volume.append({
            "time":  int(ts.iloc[i]),
            "value": int(v),
            "color": color,
        })

    # Log returns + zscore SMA 90 (serve per il terzo pane del grafico)
    log_close = np.log(close.replace(0, np.nan))
    log_ret = log_close.diff()
    roll_mean = log_ret.rolling(z_window).mean()
    roll_std = log_ret.rolling(z_window).std(ddof=1)
    zraw = (log_ret - roll_mean) / roll_std

    log_returns = [
        {"time": int(ts.iloc[i]), "value": round(float(log_ret.iloc[i]), 6)}
        for i in range(1, len(df)) if pd.notna(log_ret.iloc[i])
    ]
    zscore = [
        {"time": int(ts.iloc[i]), "value": round(float(zraw.iloc[i]), 3)}
        for i in range(len(df)) if pd.notna(zraw.iloc[i])
    ]

    # Earnings: usiamo i period_end quarterly dell'income come proxy
    earnings_marks: list[dict[str, Any]] = []
    if history_df is not None and not history_df.empty:
        q = history_df[history_df["statement_type"] == "income"].copy()
        if not q.empty:
            q["period_end"] = pd.to_datetime(q["period_end"])
            for pe in q["period_end"].dropna().unique():
                # Trova la candle più vicina >= pe+30gg (approx earnings release)
                pe_ts = pd.Timestamp(pe) + pd.Timedelta(days=30)
                idx = df["date"].searchsorted(pe_ts)
                if idx >= len(df):
                    continue
                earnings_marks.append({
                    "time":  int(ts.iloc[int(idx)]),
                    "label": pd.Timestamp(pe).strftime("%Y-%m-%d"),
                })

    return {
        "candles":     candles,
        "volume":      volume,
        "sma50":       sma50_series,
        "sma200":      sma200_series,
        "earnings":    earnings_marks,
        "log_returns": log_returns,
        "zscore":      zscore,
    }


def _build_technical(m_row: pd.Series) -> dict[str, Any]:
    return {
        "sma_50":             _clean(m_row.get("sma_50"), 4),
        "sma_200":            _clean(m_row.get("sma_200"), 4),
        "pct_from_sma50":     _clean(m_row.get("pct_from_sma50"), 4),
        "pct_from_sma200":    _clean(m_row.get("pct_from_sma200"), 4),
        "zscore_90":          _clean(m_row.get("zscore_90"), 3),
        "hv_20_annualized":   _clean(m_row.get("hv_20_annualized"), 4),
        "drawdown_from_ath":  _clean(m_row.get("drawdown_from_ath"), 4),
        "ath_date":           _clean(m_row.get("ath_date")),
        "rs_126d":            _clean(m_row.get("rs_126d"), 4),
        "ytd_return":         _clean(m_row.get("ytd_return"), 4),
        "one_year_return":    _clean(m_row.get("one_year_return"), 4),
    }


def _build_diagnostics_series(
    ohlc: pd.DataFrame, bench_ohlc: pd.DataFrame | None,
) -> dict[str, list[dict[str, Any]]]:
    """Serie rolling per i 3 mini-chart di diagnostica:
    - drawdown: drawdown from ATH (tutto lo storico, monthly downsample ≈ step 15)
    - hv_20: HV 20 annualizzata rolling
    - rs_126d: Relative Strength vs benchmark rolling
    """
    out: dict[str, list[dict[str, Any]]] = {"drawdown": [], "hv_20": [], "rs_126d": []}
    if ohlc is None or ohlc.empty:
        return out

    df = ohlc.sort_values("date").reset_index(drop=True)
    ts = (df["date"].astype("int64") // 10**9).astype("int64")
    close = df["close"].astype(float)

    # Drawdown
    running_max = close.cummax()
    dd = close / running_max - 1.0
    step = 15
    for i in range(0, len(df), step):
        if pd.isna(dd.iloc[i]):
            continue
        out["drawdown"].append({
            "time":  int(ts.iloc[i]),
            "value": round(float(dd.iloc[i]), 4),
        })

    # HV 20
    log_ret = np.log(close.replace(0, np.nan)).diff()
    hv = log_ret.rolling(20).std(ddof=1) * np.sqrt(252)
    for i in range(0, len(df), step):
        if pd.isna(hv.iloc[i]):
            continue
        out["hv_20"].append({
            "time":  int(ts.iloc[i]),
            "value": round(float(hv.iloc[i]), 4),
        })

    # Relative strength 126d vs benchmark
    if bench_ohlc is not None and not bench_ohlc.empty:
        bdf = bench_ohlc.sort_values("date").reset_index(drop=True)
        bdf["date"] = pd.to_datetime(bdf["date"])
        df2 = pd.DataFrame({
            "date":  pd.to_datetime(df["date"]),
            "close": close,
        }).merge(
            bdf[["date", "close"]].rename(columns={"close": "bench_close"}),
            on="date", how="inner",
        )
        if not df2.empty:
            ret126 = df2["close"].pct_change(126)
            bret126 = df2["bench_close"].pct_change(126)
            rs = ret126 - bret126
            ts2 = (df2["date"].astype("int64") // 10**9).astype("int64")
            for i in range(0, len(df2), step):
                if pd.isna(rs.iloc[i]):
                    continue
                out["rs_126d"].append({
                    "time":  int(ts2.iloc[i]),
                    "value": round(float(rs.iloc[i]), 4),
                })
    return out


def _rating_altman_z(v: Any) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "n/a"
    settings = load_settings()
    safe = settings["compute"]["altman_z"]["safe"]
    grey = settings["compute"]["altman_z"]["grey"]
    if v >= safe:
        return "safe"
    if v >= grey:
        return "grey"
    return "distress"


def _rating_piotroski(v: Any) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "n/a"
    settings = load_settings()
    strong = settings["compute"]["piotroski_f"]["strong"]
    weak = settings["compute"]["piotroski_f"]["weak"]
    v = int(v)
    if v >= strong:
        return "strong"
    if v <= weak:
        return "weak"
    return "neutral"


def _rating_beneish(v: Any) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "n/a"
    settings = load_settings()
    clean = settings["compute"]["beneish_m"]["clean"]
    return "clean" if v < clean else "alert"


def _build_scores(m_row: pd.Series) -> dict[str, Any]:
    az = _clean(m_row.get("altman_z"), 2)
    pf = _clean(m_row.get("piotroski_f"))
    bm = _clean(m_row.get("beneish_m"), 3)
    return {
        "altman_z":    {"value": az, "rating": _rating_altman_z(az)},
        "piotroski_f": {"value": pf, "rating": _rating_piotroski(pf)},
        "beneish_m":   {"value": bm, "rating": _rating_beneish(bm)},
    }


def _build_kq_score(m_row: pd.Series) -> dict[str, Any]:
    return {
        "value":        _clean(m_row.get("kq_value_score"), 2),
        "rank_global":  _clean(m_row.get("rank_global")),
        "rank_sector":  _clean(m_row.get("rank_sector")),
        # I pesi sono hardcoded nel modulo compute.scores — documentati qui.
        "breakdown": {
            "valuation_weight":  0.40,
            "quality_weight":    0.25,
            "solidity_weight":   0.20,
            "momentum_weight":   0.15,
        },
    }


# ------------------------------------------------------------------------------
# AI read — lettura automatizzata sintetica
# ------------------------------------------------------------------------------
def _ai_read(
    u_row: pd.Series, m_row: pd.Series, s_row: pd.Series,
) -> dict[str, Any]:
    """Costruisce un paragrafo riassuntivo strutturato.

    L'output è un dict con: ``summary`` (stringa), ``bullets`` (lista), e
    ``disclaimer`` — la pagina HTML renderizza in blocco.
    """
    ticker = u_row.get("ticker")
    name = u_row.get("name") or ticker
    bullets: list[str] = []

    pe = _clean(s_row.get("pe_ttm"), 2)
    fpe = _clean(s_row.get("forward_pe"), 2)
    peg = _clean(s_row.get("peg"), 2)
    pe_pct = _clean(m_row.get("pe_percentile_5y"), 2)
    roic = _clean(m_row.get("roic_ttm"), 3)
    fcf_margin = _clean(m_row.get("fcf_margin"), 3)
    nd_ebitda = _clean(m_row.get("net_debt_ebitda"), 2)
    dd = _clean(m_row.get("drawdown_from_ath"), 3)
    hv = _clean(m_row.get("hv_20_annualized"), 3)
    rs = _clean(m_row.get("rs_126d"), 3)
    az = _clean(m_row.get("altman_z"), 2)
    pf = _clean(m_row.get("piotroski_f"))
    bm = _clean(m_row.get("beneish_m"), 2)
    kq = _clean(m_row.get("kq_value_score"), 1)
    rg = _clean(m_row.get("rank_global"))

    # Valutazione
    if pe is not None:
        val_txt = f"PE TTM {pe}x"
        if fpe is not None:
            val_txt += f" (Forward {fpe}x)"
        if pe_pct is not None:
            val_txt += f", percentile storico 5Y {int(pe_pct*100)}%"
        if peg is not None:
            val_txt += f", PEG {peg}"
        bullets.append(f"Valutazione: {val_txt}.")

    # Qualità / redditività
    q_parts = []
    if roic is not None:
        q_parts.append(f"ROIC {roic*100:.1f}%")
    if fcf_margin is not None:
        q_parts.append(f"FCF margin {fcf_margin*100:.1f}%")
    if q_parts:
        bullets.append("Qualità: " + ", ".join(q_parts) + ".")

    # Solidità
    s_parts = []
    if nd_ebitda is not None:
        s_parts.append(f"Net Debt / EBITDA {nd_ebitda}")
    if az is not None:
        s_parts.append(f"Altman Z {az}")
    if pf is not None:
        s_parts.append(f"Piotroski {pf}/9")
    if bm is not None:
        s_parts.append(f"Beneish M {bm}")
    if s_parts:
        bullets.append("Solidità: " + ", ".join(s_parts) + ".")

    # Tecnico
    t_parts = []
    if dd is not None:
        t_parts.append(f"drawdown da ATH {dd*100:.1f}%")
    if hv is not None:
        t_parts.append(f"HV 20 {hv*100:.1f}%")
    if rs is not None:
        t_parts.append(f"RS 6m {rs*100:+.1f}%")
    if t_parts:
        bullets.append("Tecnico: " + ", ".join(t_parts) + ".")

    # KQ score
    if kq is not None:
        rank_txt = f" (rank globale #{rg})" if rg is not None else ""
        bullets.append(f"KQ Value Score {kq}/100{rank_txt}.")

    summary = (
        f"{name} · lettura automatizzata dei segnali sintetizzati dal sistema. "
        "I bullet sotto aggregano meccanicamente le metriche della pagina; "
        "questa non è raccomandazione di investimento."
    )
    return {
        "summary":    summary,
        "bullets":    bullets,
        "disclaimer": (
            "Lettura generata in modo algoritmico aggregando le metriche della pagina. "
            "Non costituisce raccomandazione di investimento. "
            "Approfondire sempre con analisi qualitativa (management, settore, cicli)."
        ),
    }


# ------------------------------------------------------------------------------
# Ticker builder
# ------------------------------------------------------------------------------
def build_one_ticker(
    ticker: str,
    data: dict[str, pd.DataFrame],
    benchmark_ohlc_cache: dict[str, pd.DataFrame],
    db_path=None,
) -> dict[str, Any] | None:
    """Costruisce il payload JSON per un singolo ticker.

    Returns None se i dati non sono sufficienti (niente universe row).
    """
    universe = data["universe"]
    metrics = data["metrics"]
    snap = data["snap"]
    hist_annual = data["hist_annual"]

    u = universe[universe["ticker"] == ticker]
    if u.empty:
        return None
    u_row = u.iloc[0]

    m = metrics[metrics["ticker"] == ticker]
    m_row = m.iloc[0] if not m.empty else pd.Series(dtype=object)

    s = snap[snap["ticker"] == ticker]
    s_row = s.iloc[0] if not s.empty else pd.Series(dtype=object)

    h = hist_annual[hist_annual["ticker"] == ticker] if not hist_annual.empty \
        else pd.DataFrame()

    # OHLC storico (10Y — o quanto disponibile)
    ohlc = _load_ohlc(ticker, db_path=db_path)
    # Benchmark OHLC (cache per exchange)
    exch = u_row.get("exchange_code")
    benchmark = _benchmark_for(exch) if exch else None
    bench_ohlc = None
    if benchmark:
        if benchmark not in benchmark_ohlc_cache:
            benchmark_ohlc_cache[benchmark] = _load_ohlc(benchmark, db_path=db_path)
        bench_ohlc = benchmark_ohlc_cache[benchmark]

    payload = {
        "meta":              _build_meta(u_row, benchmark),
        "hero":              _build_hero(u_row, m_row, s_row),
        "snapshot":          _build_snapshot(m_row, s_row),
        "cash_debt":         _build_cash_debt(m_row, s_row, h),
        "cashflow_quality":  _build_cashflow_quality(m_row, s_row, h),
        "profitability":     _build_profitability(m_row, s_row),
        "technical":         _build_technical(m_row),
        "price_series":      _build_price_series(ticker, ohlc, h),
        "diagnostics":       _build_diagnostics_series(ohlc, bench_ohlc),
        "seasonality":       compute_seasonality_for_ticker(ticker, years=10,
                                                              db_path=db_path),
        "scores":            _build_scores(m_row),
        "kq_score":          _build_kq_score(m_row),
        "ai_read":           _ai_read(u_row, m_row, s_row),
    }
    return payload


def build_all_ticker_pages(
    exchange_code: str | None = None,
    tickers: list[str] | None = None,
    max_tickers: int | None = None,
    output_dir: str | Path | None = None,
    pretty: bool | None = None,
    db_path=None,
) -> TickerPagesResult:
    """Orchestratore: produce uno o più file JSON in ``site/data/tickers/``."""
    result = TickerPagesResult()
    result.started_at = _utc_now_iso()

    settings = load_settings()
    if pretty is None:
        pretty = bool(settings.get("output", {}).get("pretty_json", False))

    out_dir = _output_dir(str(output_dir) if output_dir else None)
    result.output_dir = str(out_dir)

    # Universe → lista ticker da processare
    universe = get_universe(active_only=True, exchange_code=exchange_code,
                              db_path=db_path)
    if tickers is not None:
        universe = universe[universe["ticker"].isin(tickers)]
    if universe.empty:
        logger.warning("Nessun ticker da processare.")
        result.ended_at = _utc_now_iso()
        return result

    if max_tickers:
        universe = universe.head(max_tickers)
    result.tickers_in = len(universe)

    # Carico una volta sola i DF di riferimento
    data = _load_full_data(db_path=db_path)

    benchmark_cache: dict[str, pd.DataFrame] = {}

    indent = 2 if pretty else None

    for i, (_, u_row) in enumerate(universe.iterrows(), start=1):
        tk = u_row["ticker"]
        try:
            payload = build_one_ticker(tk, data, benchmark_cache, db_path=db_path)
            if payload is None:
                result.tickers_skipped += 1
                continue
            # Nome file: sostituisci caratteri non safe (ticker EODHD usa '.', ok su file)
            safe = tk.replace("/", "_")
            fp = out_dir / f"{safe}.json"
            # allow_nan=False: Python serializza NaN come `NaN` letterale, che
            # NON è JSON valido (JSON.parse lato browser fallisce). Forziamo
            # l'errore al build time così se un NaN sfugge a `_clean()`
            # vediamo subito il ticker colpevole nel log.
            text = json.dumps(payload, ensure_ascii=False, indent=indent,
                               separators=(",", ":"), allow_nan=False)
            fp.write_text(text, encoding="utf-8")
            result.total_size_bytes += len(text.encode("utf-8"))
            result.tickers_out += 1
        except Exception as e:
            logger.warning("Ticker %s · errore: %s", tk, e)
            result.errors.append(f"{tk}:{e!s}")
            result.tickers_skipped += 1

        if i % 100 == 0:
            logger.info("… %d/%d ticker processati", i, result.tickers_in)

    result.ended_at = _utc_now_iso()
    logger.info("── %s", result.summary())
    return result


# ------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------
def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Genera site/data/tickers/*.json (uno per ticker).",
    )
    parser.add_argument("--ticker", help="Solo un ticker specifico (es. AAPL.US).")
    parser.add_argument("--exchange", help="Limita a un solo exchange.")
    parser.add_argument("--max-tickers", type=int,
                          help="Limite numero ticker (per test).")
    parser.add_argument("--output-dir", help="Output directory custom.")
    parser.add_argument("--pretty", action="store_true",
                          help="Forza pretty-print.")
    parser.add_argument("--minify", action="store_true",
                          help="Forza output minificato.")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    pretty: bool | None = None
    if args.pretty:
        pretty = True
    elif args.minify:
        pretty = False

    tickers = [args.ticker] if args.ticker else None

    try:
        res = build_all_ticker_pages(
            exchange_code=args.exchange,
            tickers=tickers,
            max_tickers=args.max_tickers,
            output_dir=args.output_dir,
            pretty=pretty,
        )
    except KeyboardInterrupt:
        logger.warning("Interrotto.")
        return 130
    except Exception as e:
        logger.exception("Errore non gestito: %s", e)
        return 1

    print()
    print("=" * 70)
    print("  TICKER PAGES BUILD")
    print("=" * 70)
    print(f"  Output dir:      {res.output_dir}")
    print(f"  Tickers in:      {res.tickers_in}")
    print(f"  Tickers out:     {res.tickers_out}")
    print(f"  Tickers skipped: {res.tickers_skipped}")
    print(f"  Total size (MB): {res.total_size_bytes / (1024 * 1024):.1f}")
    print(f"  Errors:          {len(res.errors)}")
    print("=" * 70)
    return 0 if not res.errors else 2


if __name__ == "__main__":
    sys.exit(_main())
