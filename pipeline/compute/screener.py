"""Orchestratore compute — combina tutti i moduli e popola ``computed_metrics``.

Flusso:
1. Chiama ogni modulo compute (technical, cash_debt, profitability, valuation,
   scores) che ritorna lista di dict per ticker
2. Fa il merge orizzontale su ``ticker``
3. Aggiunge KQ Value Score + ranking (globale e settoriale)
4. Upsert su ``computed_metrics``

Uso CLI
-------
# Full compute su tutto l'universo
python -m pipeline.compute.screener

# Solo un exchange
python -m pipeline.compute.screener --exchange US

# Dry-run (niente scrittura)
python -m pipeline.compute.screener --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

from pipeline.compute.cash_debt import compute_cash_debt_all
from pipeline.compute.profitability import compute_profitability_all
from pipeline.compute.scores import compute_kq_value_score, compute_scores_all
from pipeline.compute.technical import compute_technical_all
from pipeline.compute.valuation import compute_valuation_all
from pipeline.storage.db import (
    get_connection,
    get_universe,
    upsert_computed_metrics,
)

logger = logging.getLogger("kq.compute.screener")
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
@dataclass
class ScreenerBuildResult:
    tickers_in: int = 0
    tickers_out: int = 0
    rows_written: int = 0
    started_at: str = ""
    ended_at: str = ""
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"ScreenerBuildResult(in={self.tickers_in}, out={self.tickers_out}, "
            f"rows_written={self.rows_written}, errors={len(self.errors)})"
        )


def _merge_on_ticker(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Merge outer sulla colonna ticker mantenendo tutte le colonne.

    Scarta i DataFrame vuoti o senza colonna ``ticker`` (robusto al caso in
    cui uno dei compute ritorna vuoto, es. technical quando prices_daily è
    vuoto).
    """
    valid = [f for f in frames if not f.empty and "ticker" in f.columns]
    if not valid:
        return pd.DataFrame()
    out = valid[0].copy()
    for f in valid[1:]:
        out = out.merge(f, on="ticker", how="outer")
    return out


def build_screener(
    exchange_code: str | None = None,
    db_path=None,
    dry_run: bool = False,
) -> ScreenerBuildResult:
    """Orchestratore principale — produce ``computed_metrics`` per tutti i ticker."""
    result = ScreenerBuildResult()
    result.started_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Universe
    universe_df = get_universe(active_only=True, exchange_code=exchange_code,
                                db_path=db_path)
    if universe_df.empty:
        logger.warning("Universe vuoto.")
        return result
    result.tickers_in = len(universe_df)

    # Chiamate moduli
    logger.info("┌─ Compute technical…")
    tech = pd.DataFrame(compute_technical_all(exchange_code=exchange_code,
                                                 db_path=db_path))
    logger.info("├─ Compute cash_debt…")
    cd = pd.DataFrame(compute_cash_debt_all(db_path=db_path))
    logger.info("├─ Compute profitability…")
    prof = pd.DataFrame(compute_profitability_all(db_path=db_path))
    logger.info("├─ Compute valuation…")
    val = pd.DataFrame(compute_valuation_all(db_path=db_path))
    logger.info("└─ Compute scores…")
    scores = pd.DataFrame(compute_scores_all(db_path=db_path))

    merged = _merge_on_ticker([tech, cd, prof, val, scores])
    if merged.empty:
        logger.warning("Merge risultante vuoto.")
        return result

    # Filtro per universo attivo (se chiamato con --exchange, limita al subset)
    merged = merged[merged["ticker"].isin(universe_df["ticker"])].copy()

    # KQ Value Score
    merged["kq_value_score"] = compute_kq_value_score(merged)

    # Ranking: globale e settoriale
    merged = merged.merge(
        universe_df[["ticker", "sector"]], on="ticker", how="left",
    )
    merged["rank_global"] = merged["kq_value_score"].rank(
        method="dense", ascending=False, na_option="bottom",
    ).astype("Int64")
    merged["rank_sector"] = merged.groupby("sector")["kq_value_score"].rank(
        method="dense", ascending=False, na_option="bottom",
    ).astype("Int64")

    # Seleziona solo le colonne della tabella computed_metrics
    target_cols = [
        "ticker", "last_close", "last_close_date", "sma_50", "sma_200",
        "pct_from_sma50", "pct_from_sma200", "zscore_90",
        "hv_20_annualized", "drawdown_from_ath", "ath_date",
        "rs_126d", "ytd_return", "one_year_return",
        "pe_percentile_5y", "pe_vs_sector_median",
        "net_debt_ebitda", "net_debt_fcf", "interest_coverage",
        "current_ratio", "quick_ratio", "cash_ratio",
        "roic_ttm", "fcf_margin", "fcf_yield",
        "altman_z", "piotroski_f", "beneish_m",
        "flag_quality_ok", "flag_fcf_positive",
        "flag_revenue_growth_ok", "flag_roic_ok",
        "kq_value_score", "rank_global", "rank_sector",
    ]
    for c in target_cols:
        if c not in merged.columns:
            merged[c] = None

    out_df = merged[target_cols].copy()
    # Clean NaN residui → None per SQLite
    records = out_df.where(pd.notna(out_df), None).to_dict(orient="records")

    result.tickers_out = len(records)

    if not dry_run:
        n = upsert_computed_metrics(records, db_path=db_path)
        result.rows_written = n
    else:
        logger.info("[DRY-RUN] Sarebbero stati scritti %d record", len(records))

    result.ended_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info("── %s", result.summary())
    return result


# ------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------
def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Orchestratore compute: popola computed_metrics.",
    )
    parser.add_argument("--exchange", help="Limita a un solo exchange.")
    parser.add_argument("--dry-run", action="store_true",
                         help="Non scrive su DB.")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        for name in ["kq.compute.technical", "kq.compute.cash_debt",
                      "kq.compute.profitability", "kq.compute.valuation",
                      "kq.compute.scores", "kq.compute.screener", "kq.db"]:
            logging.getLogger(name).setLevel(logging.DEBUG)

    try:
        res = build_screener(exchange_code=args.exchange, dry_run=args.dry_run)
    except KeyboardInterrupt:
        logger.warning("Interrotto.")
        return 130
    except Exception as e:
        logger.exception("Errore non gestito: %s", e)
        return 1

    print()
    print("=" * 70)
    print("  SCREENER BUILD RESULT")
    print("=" * 70)
    print(f"  Started:       {res.started_at}")
    print(f"  Ended:         {res.ended_at}")
    print(f"  Tickers in:    {res.tickers_in}")
    print(f"  Tickers out:   {res.tickers_out}")
    print(f"  Rows written:  {res.rows_written}")
    print(f"  Errors:        {len(res.errors)}")
    print("=" * 70)
    return 0 if not res.errors else 2


if __name__ == "__main__":
    sys.exit(_main())
