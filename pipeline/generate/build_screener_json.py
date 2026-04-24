"""Genera il JSON principale dello screener per il frontend statico.

Output: ``site/data/screener.json`` (uno per tutto il sistema) — file
"magro" con UNA riga per ticker, contenente solo le colonne necessarie
a popolare la tabella DataTables della pagina index.

Flusso
------
1. Legge ``universe`` (solo ``is_active = 1``)
2. Legge ``computed_metrics`` (snapshot nightly)
3. Legge ``fundamentals_snapshot`` (valuation / size / growth)
4. Merge su ``ticker``
5. Estrae colonne pubbliche + normalizza (NaN → None, round)
6. Scrive ``site/data/screener.json`` (pretty o minificato secondo settings)

Uso CLI
-------
# Build standard (minificato secondo output.pretty_json)
python -m pipeline.generate.build_screener_json

# Forza pretty-print (debug umano)
python -m pipeline.generate.build_screener_json --pretty

# Output custom
python -m pipeline.generate.build_screener_json --output path/to/out.json
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

import pandas as pd

from pipeline.config import PROJECT_ROOT, load_settings
from pipeline.storage.db import get_connection, save_screener_snapshot

logger = logging.getLogger("kq.generate.screener_json")
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
class ScreenerJsonResult:
    tickers_out: int = 0
    size_bytes: int = 0
    output_path: str = ""
    cached: bool = False
    started_at: str = ""
    ended_at: str = ""
    warnings: list[str] = field(default_factory=list)

    def summary(self) -> str:
        kb = self.size_bytes / 1024.0
        return (
            f"ScreenerJsonResult(rows={self.tickers_out}, size={kb:.1f} KB, "
            f"cached={self.cached}, path={self.output_path})"
        )


# ------------------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------------------
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean(v: Any, ndigits: int | None = None) -> Any:
    """Pulisce valori per JSON: NaN/inf → None, numpy scalar → python scalar,
    arrotondamento opzionale a ``ndigits`` cifre decimali."""
    if v is None:
        return None
    # pandas NA / NaN
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    # numpy scalar
    if hasattr(v, "item") and not isinstance(v, (str, bytes)):
        try:
            v = v.item()
        except Exception:
            pass
    # infiniti
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        if ndigits is not None:
            return round(v, ndigits)
    return v


def _output_path(cli_override: str | None = None) -> Path:
    if cli_override:
        p = Path(cli_override)
    else:
        settings = load_settings()
        raw = settings["output"]["screener_json"]
        p = Path(raw)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


# ------------------------------------------------------------------------------
# Core
# ------------------------------------------------------------------------------
def _load_merged_df(db_path=None) -> pd.DataFrame:
    """Carica universe + computed_metrics + fundamentals_snapshot in un unico DF."""
    with get_connection(db_path) as conn:
        universe = pd.read_sql_query(
            """
            SELECT ticker, code, name, exchange_code, exchange_name,
                   country, currency, type, sector, industry,
                   market_capitalization, is_active
              FROM universe
             WHERE is_active = 1
            """,
            conn,
        )
        metrics = pd.read_sql_query("SELECT * FROM computed_metrics", conn)
        snap = pd.read_sql_query(
            """
            SELECT ticker, pe_ttm, forward_pe, peg, price_to_sales_ttm,
                   price_to_book, ev_to_ebitda, ev_to_revenue,
                   earnings_yield, dividend_yield, beta,
                   revenue_growth_yoy, eps_growth_yoy, earnings_growth_next_year,
                   market_cap_usd, shares_outstanding,
                   next_earnings_date, most_recent_quarter,
                   total_cash, total_debt, net_debt
              FROM fundamentals_snapshot
            """,
            conn,
        )

    if universe.empty:
        logger.warning("Universe vuoto → output vuoto.")
        return universe

    # Merge
    out = universe.merge(metrics, on="ticker", how="left")
    out = out.merge(snap, on="ticker", how="left")
    return out


def _row_to_record(row: pd.Series) -> dict[str, Any]:
    """Costruisce il record "magro" da una riga merged."""
    # market cap — preferisci snapshot (più aggiornato), fallback universe
    mcap = row.get("market_cap_usd")
    if mcap is None or (isinstance(mcap, float) and math.isnan(mcap)):
        mcap = row.get("market_capitalization")

    return {
        # Identity
        "ticker":        _clean(row.get("ticker")),
        "code":          _clean(row.get("code")),
        "name":          _clean(row.get("name")),
        "exchange":      _clean(row.get("exchange_code")),
        "exchange_name": _clean(row.get("exchange_name")),
        "country":       _clean(row.get("country")),
        "currency":      _clean(row.get("currency")),
        "sector":        _clean(row.get("sector")),
        "industry":      _clean(row.get("industry")),
        # Size
        "market_cap_usd": _clean(mcap, 0),
        # Last price
        "last_close":      _clean(row.get("last_close"), 4),
        "last_close_date": _clean(row.get("last_close_date")),
        # Valuation
        "pe_ttm":              _clean(row.get("pe_ttm"), 3),
        "forward_pe":          _clean(row.get("forward_pe"), 3),
        "peg":                 _clean(row.get("peg"), 3),
        "pe_percentile_5y":    _clean(row.get("pe_percentile_5y"), 4),
        "pe_vs_sector_median": _clean(row.get("pe_vs_sector_median"), 4),
        "ev_to_ebitda":        _clean(row.get("ev_to_ebitda"), 3),
        "ev_to_revenue":       _clean(row.get("ev_to_revenue"), 3),
        "price_to_book":       _clean(row.get("price_to_book"), 3),
        "price_to_sales_ttm":  _clean(row.get("price_to_sales_ttm"), 3),
        "earnings_yield":      _clean(row.get("earnings_yield"), 4),
        "dividend_yield":      _clean(row.get("dividend_yield"), 4),
        # Growth
        "revenue_growth_yoy":  _clean(row.get("revenue_growth_yoy"), 4),
        "eps_growth_yoy":      _clean(row.get("eps_growth_yoy"), 4),
        # Technical
        "pct_from_sma50":   _clean(row.get("pct_from_sma50"), 4),
        "pct_from_sma200":  _clean(row.get("pct_from_sma200"), 4),
        "zscore_90":        _clean(row.get("zscore_90"), 3),
        "hv_20":            _clean(row.get("hv_20_annualized"), 4),
        "drawdown_from_ath":_clean(row.get("drawdown_from_ath"), 4),
        "rs_126d":          _clean(row.get("rs_126d"), 4),
        "ytd_return":       _clean(row.get("ytd_return"), 4),
        "one_year_return":  _clean(row.get("one_year_return"), 4),
        # Cash & Debt
        "net_debt":          _clean(row.get("net_debt"), 0),
        "net_debt_ebitda":   _clean(row.get("net_debt_ebitda"), 3),
        "net_debt_fcf":      _clean(row.get("net_debt_fcf"), 3),
        "interest_coverage": _clean(row.get("interest_coverage"), 2),
        "current_ratio":     _clean(row.get("current_ratio"), 3),
        "quick_ratio":       _clean(row.get("quick_ratio"), 3),
        # Profitability
        "roic_ttm":     _clean(row.get("roic_ttm"), 4),
        "fcf_margin":   _clean(row.get("fcf_margin"), 4),
        "fcf_yield":    _clean(row.get("fcf_yield"), 4),
        # Scores
        "altman_z":     _clean(row.get("altman_z"), 2),
        "piotroski_f":  _clean(row.get("piotroski_f")),
        "beneish_m":    _clean(row.get("beneish_m"), 3),
        # Quality flags
        "flag_quality_ok":        _clean(row.get("flag_quality_ok")),
        "flag_fcf_positive":      _clean(row.get("flag_fcf_positive")),
        "flag_revenue_growth_ok": _clean(row.get("flag_revenue_growth_ok")),
        "flag_roic_ok":           _clean(row.get("flag_roic_ok")),
        # Composite score & ranking
        "kq_value_score": _clean(row.get("kq_value_score"), 2),
        "rank_global":    _clean(row.get("rank_global")),
        "rank_sector":    _clean(row.get("rank_sector")),
        # Schedule
        "next_earnings_date": _clean(row.get("next_earnings_date")),
    }


def _build_meta(df: pd.DataFrame) -> dict[str, Any]:
    """Header del file: versione schema, timestamp, liste distinte."""
    exchanges = sorted(df["exchange_code"].dropna().unique().tolist())
    sectors = sorted([s for s in df["sector"].dropna().unique().tolist() if s])
    countries = sorted([c for c in df["country"].dropna().unique().tolist() if c])
    return {
        "schema_version": SCHEMA_VERSION,
        "generator":      "pipeline.generate.build_screener_json",
        "build_ts_utc":   _utc_now_iso(),
        "ticker_count":   int(len(df)),
        "exchanges":      exchanges,
        "sectors":        sectors,
        "countries":      countries,
    }


def build_screener_json(
    output_path: str | Path | None = None,
    pretty: bool | None = None,
    cache_snapshot: bool = True,
    db_path=None,
) -> ScreenerJsonResult:
    """Genera ``site/data/screener.json`` dal DB.

    Parameters
    ----------
    output_path : str or Path, optional
        Override del percorso di uscita (default: ``output.screener_json``).
    pretty : bool, optional
        Forza pretty-print o minificazione. Se None, usa ``output.pretty_json``.
    cache_snapshot : bool
        Se True, salva una copia in ``screener_cache`` (versioning).
    """
    result = ScreenerJsonResult()
    result.started_at = _utc_now_iso()

    settings = load_settings()
    if pretty is None:
        pretty = bool(settings.get("output", {}).get("pretty_json", False))

    out_path = _output_path(str(output_path) if output_path else None)
    result.output_path = str(out_path)

    df = _load_merged_df(db_path=db_path)
    if df.empty:
        logger.warning("Nessun ticker in universe → file non scritto.")
        result.ended_at = _utc_now_iso()
        return result

    # Ordina per kq_value_score desc (o rank_global asc) — gli NaN vanno in fondo
    if "rank_global" in df.columns:
        df = df.sort_values(
            by=["rank_global", "kq_value_score"],
            ascending=[True, False],
            na_position="last",
        )
    else:
        df = df.sort_values(
            by="kq_value_score", ascending=False, na_position="last",
        )

    records = [_row_to_record(r) for _, r in df.iterrows()]
    meta = _build_meta(df)

    payload = {"meta": meta, "rows": records}
    indent = 2 if pretty else None
    # allow_nan=False: JSON.parse lato browser non accetta NaN/Infinity.
    # Se un NaN sfugge a `_clean()`, preferiamo fallire qui che produrre
    # JSON corrotto consumato dal frontend.
    text = json.dumps(
        payload, ensure_ascii=False, indent=indent,
        separators=(",", ":"), allow_nan=False,
    )

    out_path.write_text(text, encoding="utf-8")
    size = out_path.stat().st_size
    result.tickers_out = len(records)
    result.size_bytes = size

    logger.info("Scritto %s (%d ticker, %.1f KB)",
                out_path, result.tickers_out, size / 1024)

    # Cache snapshot su DB (facoltativo ma utile per versioning)
    if cache_snapshot:
        try:
            save_screener_snapshot(payload=text, db_path=db_path)
            result.cached = True
        except Exception as e:
            logger.warning("Cache snapshot fallita: %s", e)
            result.warnings.append(f"cache_snapshot_failed:{e!s}")

    result.ended_at = _utc_now_iso()
    logger.info("── %s", result.summary())
    return result


# ------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------
def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Genera site/data/screener.json dal DB.",
    )
    parser.add_argument("--output", help="Path di output custom.")
    parser.add_argument("--pretty", action="store_true",
                          help="Forza pretty-print.")
    parser.add_argument("--minify", action="store_true",
                          help="Forza output minificato.")
    parser.add_argument("--no-cache", action="store_true",
                          help="Non salvare copia in screener_cache.")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    pretty: bool | None = None
    if args.pretty:
        pretty = True
    elif args.minify:
        pretty = False

    try:
        res = build_screener_json(
            output_path=args.output,
            pretty=pretty,
            cache_snapshot=not args.no_cache,
        )
    except KeyboardInterrupt:
        logger.warning("Interrotto.")
        return 130
    except Exception as e:
        logger.exception("Errore non gestito: %s", e)
        return 1

    print()
    print("=" * 70)
    print("  SCREENER JSON BUILD")
    print("=" * 70)
    print(f"  Output:       {res.output_path}")
    print(f"  Tickers:      {res.tickers_out}")
    print(f"  Size (KB):    {res.size_bytes / 1024:.1f}")
    print(f"  Cached in DB: {'yes' if res.cached else 'no'}")
    print(f"  Warnings:     {len(res.warnings)}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(_main())
