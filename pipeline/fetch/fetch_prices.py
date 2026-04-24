"""Fetch prezzi EOD — aggiornamento giornaliero (bulk) + backfill 10 anni.

Due modalità operative:

1. **Update incrementale** (nightly):
   Per ogni exchange configurato, chiama UNA volta l'endpoint
   ``eod-bulk-last-day/{exchange}`` (costo ~100 credits) per scaricare tutti
   i prezzi del giorno. Filtra ai soli ticker presenti nella tabella
   ``universe`` e fa upsert su ``prices_daily``.

2. **Backfill** (storico):
   Per ogni ticker senza storia nel DB, chiama ``get_eod(ticker, from_date)``
   per scaricare i 10 anni di storia (1 credit per chiamata).
   La scansione rispetta rate limit e budget configurati.

Uso CLI
-------
# Nightly update di tutti gli exchange con data odierna
python -m pipeline.fetch.fetch_prices

# Data specifica
python -m pipeline.fetch.fetch_prices --date 2026-04-22

# Backfill dei ticker senza storia
python -m pipeline.fetch.fetch_prices --backfill

# Backfill solo di un exchange
python -m pipeline.fetch.fetch_prices --backfill --exchange US

# Dry-run (nessuna scrittura su DB)
python -m pipeline.fetch.fetch_prices --backfill --exchange MI --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.config import get_all_exchanges, load_settings
from pipeline.fetch.eodhd_client import EODHDClient, EODHDError
from pipeline.storage.db import (
    get_connection,
    get_last_price_date,
    get_tickers_without_history,
    get_universe,
    init_schema,
    upsert_prices_bulk,
)

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logger = logging.getLogger("kq.fetch.prices")
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
# Risultati aggregati
# ------------------------------------------------------------------------------
@dataclass
class FetchPricesResult:
    """Statistiche del run."""
    mode: str                                       # "bulk" | "backfill"
    exchanges_processed: list[str] = field(default_factory=list)
    tickers_processed: int = 0
    rows_written: int = 0
    credits_estimated: int = 0
    errors: list[str] = field(default_factory=list)
    started_at: str = ""
    ended_at: str = ""

    def summary(self) -> str:
        return (
            f"FetchPricesResult(mode={self.mode}, "
            f"exchanges={len(self.exchanges_processed)}, "
            f"tickers={self.tickers_processed}, rows={self.rows_written}, "
            f"credits≈{self.credits_estimated}, errors={len(self.errors)})"
        )


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _normalize_bulk_df(df: pd.DataFrame, exchange_code: str) -> pd.DataFrame:
    """Normalizza output di ``get_bulk_eod`` nello schema di ``prices_daily``.

    EODHD bulk restituisce colonne: code, exchange_short_name, date, open, high,
    low, close, adjusted_close, volume. Ricostruiamo il ticker completo come
    ``{code}.{exchange_code}``.
    """
    if df.empty:
        return df

    out = df.copy()
    if "code" in out.columns:
        out["ticker"] = out["code"].astype(str) + "." + exchange_code
    elif "ticker" not in out.columns:
        raise ValueError("Bulk response senza colonna 'code' né 'ticker'")

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")

    keep = [
        "ticker", "date", "open", "high", "low",
        "close", "adjusted_close", "volume",
    ]
    for c in keep:
        if c not in out.columns:
            out[c] = None

    return out[keep]


def _filter_to_universe(df: pd.DataFrame, universe_tickers: set[str]) -> pd.DataFrame:
    """Mantiene solo le righe i cui ticker appartengono all'universo."""
    if df.empty:
        return df
    mask = df["ticker"].isin(universe_tickers)
    return df.loc[mask].reset_index(drop=True)


# ------------------------------------------------------------------------------
# MODALITÀ 1: Update incrementale bulk
# ------------------------------------------------------------------------------
def fetch_prices_bulk(
    target_date: str | None = None,
    exchange_codes: list[str] | None = None,
    client: EODHDClient | None = None,
    dry_run: bool = False,
) -> FetchPricesResult:
    """Aggiornamento bulk giornaliero (1 chiamata per exchange).

    Parameters
    ----------
    target_date : str, optional
        "YYYY-MM-DD". Default: ultima data disponibile da EODHD.
    exchange_codes : list of str, optional
        Lista codici EODHD da processare. Default: tutti da settings.yaml.
    client : EODHDClient, optional
        Istanza esistente (riusa rate limit/usage stats). Default: nuova.
    dry_run : bool
        Se True, scarica e logga ma non scrive nel DB.

    Returns
    -------
    FetchPricesResult
    """
    result = FetchPricesResult(mode="bulk")
    result.started_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Universo attivo — per filtrare le risposte bulk
    universe_df = get_universe(active_only=True)
    if universe_df.empty:
        logger.warning(
            "Universe vuoto: esegui prima `python -m pipeline.storage.db "
            "--init --load-universe` dopo aver generato universe.parquet"
        )
        return result
    universe_tickers = set(universe_df["ticker"].tolist())
    # Exchange realmente presenti nell'universo (evita di chiamare bulk per
    # exchange vuoti → risparmia crediti + evita 404 su exchange non esposti
    # dall'endpoint eod-bulk-last-day, es. MI sul nostro piano).
    exchanges_in_universe = sorted(universe_df["exchange_code"].dropna().unique().tolist())
    logger.info(
        "Universe attivo: %d ticker (%d exchange: %s)",
        len(universe_tickers),
        len(exchanges_in_universe),
        ", ".join(exchanges_in_universe),
    )

    if exchange_codes is None:
        # Default: solo gli exchange che hanno effettivamente ticker attivi.
        exchange_codes = exchanges_in_universe
    else:
        # Se l'utente ha passato --exchange esplicito, intersezione con
        # l'universo per non bruciare crediti su exchange vuoti.
        before = list(exchange_codes)
        exchange_codes = [xc for xc in exchange_codes if xc in set(exchanges_in_universe)]
        skipped = [xc for xc in before if xc not in exchange_codes]
        if skipped:
            logger.warning(
                "Exchange senza ticker nell'universo (skip): %s",
                ", ".join(skipped),
            )
        if not exchange_codes:
            logger.warning("Nessun exchange valido dopo il filtro universo.")
            return result

    owned_client = False
    if client is None:
        client = EODHDClient()
        owned_client = True

    try:
        for xc in exchange_codes:
            logger.info("── Bulk EOD exchange=%s date=%s ──", xc, target_date or "latest")
            try:
                df_raw = client.get_bulk_eod(xc, date=target_date)
            except EODHDError as e:
                msg = f"Bulk exchange={xc}: {e}"
                logger.error(msg)
                result.errors.append(msg)
                continue

            if df_raw.empty:
                logger.warning("Risposta vuota per exchange=%s", xc)
                continue

            df_norm = _normalize_bulk_df(df_raw, xc)
            before = len(df_norm)
            df_filt = _filter_to_universe(df_norm, universe_tickers)
            logger.info(
                "Bulk %s: %d righe totali → %d nell'universo",
                xc, before, len(df_filt),
            )

            if not df_filt.empty and not dry_run:
                n = upsert_prices_bulk(df_filt)
                result.rows_written += n

            result.exchanges_processed.append(xc)
            result.tickers_processed += len(df_filt)

    finally:
        if owned_client:
            stats = client.get_usage_stats()
            if isinstance(stats, dict):
                credits_total = stats.get('credits_total', 0)
                calls_total = stats.get('calls_total', 0)
                errors_total = stats.get('errors_total', 0)
            else:
                credits_total = getattr(stats, 'credits_total', 0)
                calls_total = getattr(stats, 'calls_total', 0)
                errors_total = getattr(stats, 'errors_total', 0)
            result.credits_estimated = credits_total
            logger.info(
                "Usage bulk: calls=%d, credits=%d, errors=%d",
                calls_total, credits_total, errors_total,
            )

    result.ended_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info("── %s", result.summary())
    return result


# ------------------------------------------------------------------------------
# MODALITÀ 2: Backfill storico
# ------------------------------------------------------------------------------
def fetch_prices_backfill(
    exchange_code: str | None = None,
    years: int = 10,
    client: EODHDClient | None = None,
    dry_run: bool = False,
    tickers: list[str] | None = None,
    max_tickers: int | None = None,
) -> FetchPricesResult:
    """Backfill storico per ticker senza dati in ``prices_daily``.

    Per ogni ticker nella lista target (default: chi non ha righe in
    ``prices_daily``), scarica gli ultimi ``years`` anni tramite
    ``client.get_eod()`` e fa upsert nel DB.

    Parameters
    ----------
    exchange_code : str, optional
        Limita il backfill ad un solo exchange.
    years : int
        Anni di storia da scaricare (default 10).
    client : EODHDClient, optional
        Istanza esistente.
    dry_run : bool
        Se True, non scrive nel DB.
    tickers : list of str, optional
        Override esplicito della lista ticker. Se None, usa
        ``get_tickers_without_history()``.
    max_tickers : int, optional
        Safety cap sul numero di ticker da processare (utile per test).

    Returns
    -------
    FetchPricesResult
    """
    result = FetchPricesResult(mode="backfill")
    result.started_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    if tickers is None:
        tickers = get_tickers_without_history(exchange_code=exchange_code)

    if max_tickers is not None:
        tickers = tickers[:max_tickers]

    if not tickers:
        logger.info("Nessun ticker da backfillare.")
        return result

    logger.info(
        "Backfill: %d ticker da processare (exchange=%s, years=%d)",
        len(tickers), exchange_code or "ALL", years,
    )

    from_date = (date.today() - timedelta(days=int(365.25 * years))).strftime("%Y-%m-%d")

    owned_client = False
    if client is None:
        client = EODHDClient()
        owned_client = True

    # Budget guardrail
    settings = load_settings()
    daily_budget = int(settings["eodhd"].get("daily_budget_credits", 100_000))
    alert_pct = float(settings["eodhd"].get("alert_budget_pct", 0.8))

    try:
        for i, tk in enumerate(tickers, start=1):
            try:
                df = client.get_eod(tk, from_date=from_date)
            except EODHDError as e:
                msg = f"Backfill {tk}: {e}"
                logger.warning(msg)
                result.errors.append(msg)
                continue

            if df.empty:
                logger.debug("Nessun dato per %s", tk)
                continue

            # Prepara per upsert
            out = df.reset_index().rename(columns={df.index.name or "date": "date"})
            out["ticker"] = tk
            out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
            cols = ["ticker", "date", "open", "high", "low",
                    "close", "adjusted_close", "volume"]
            for c in cols:
                if c not in out.columns:
                    out[c] = None
            out = out[cols]

            if not dry_run:
                n = upsert_prices_bulk(out)
                result.rows_written += n
            result.tickers_processed += 1

            # Check budget ogni 100 ticker
            if i % 100 == 0:
                stats = client.get_usage_stats()
                credits_used = stats.get('credits_total', 0) if isinstance(stats, dict) else getattr(stats, 'credits_total', 0)
                if credits_used >= daily_budget * alert_pct:
                    logger.warning(
                        "Budget alert: %d/%d credits usati (%.0f%%). "
                        "Interruzione backfill.",
                        credits_used, daily_budget,
                        100 * credits_used / daily_budget,
                    )
                    break
                logger.info(
                    "Progress backfill: %d/%d ticker, credits=%d",
                    i, len(tickers), credits_used,
                )

    finally:
        if owned_client:
            stats = client.get_usage_stats()
            if isinstance(stats, dict):
                credits_total = stats.get('credits_total', 0)
                calls_total = stats.get('calls_total', 0)
                errors_total = stats.get('errors_total', 0)
            else:
                credits_total = getattr(stats, 'credits_total', 0)
                calls_total = getattr(stats, 'calls_total', 0)
                errors_total = getattr(stats, 'errors_total', 0)
            result.credits_estimated = credits_total
            logger.info(
                "Usage backfill: calls=%d, credits=%d, errors=%d",
                calls_total, credits_total, errors_total,
            )

    result.ended_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info("── %s", result.summary())
    return result


# ------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------
def _main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch prezzi EOD: bulk update giornaliero (default) o backfill storico."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Esempi:\n"
            "  python -m pipeline.fetch.fetch_prices\n"
            "  python -m pipeline.fetch.fetch_prices --date 2026-04-22\n"
            "  python -m pipeline.fetch.fetch_prices --backfill\n"
            "  python -m pipeline.fetch.fetch_prices --backfill --exchange US --max-tickers 50\n"
            "  python -m pipeline.fetch.fetch_prices --backfill --dry-run\n"
        ),
    )
    parser.add_argument(
        "--date",
        help="Data bulk (YYYY-MM-DD). Ignorato se --backfill.",
    )
    parser.add_argument(
        "--exchange",
        help="Codice exchange EODHD (es. US, LSE). Limita il run.",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Esegue backfill storico su ticker senza dati.",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=10,
        help="Anni di storia da scaricare in backfill (default: 10).",
    )
    parser.add_argument(
        "--max-tickers",
        type=int,
        default=None,
        help="Cap numero ticker processati in backfill (utile per test).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Non scrive nel DB (solo fetch + log).",
    )
    parser.add_argument(
        "--init-schema",
        action="store_true",
        help="Crea lo schema DB prima di eseguire (idempotente).",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Log level DEBUG.",
    )
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logging.getLogger("kq.eodhd").setLevel(logging.DEBUG)
        logging.getLogger("kq.db").setLevel(logging.DEBUG)

    if args.init_schema:
        init_schema()

    exchanges = [args.exchange] if args.exchange else None

    try:
        if args.backfill:
            res = fetch_prices_backfill(
                exchange_code=args.exchange,
                years=args.years,
                dry_run=args.dry_run,
                max_tickers=args.max_tickers,
            )
        else:
            res = fetch_prices_bulk(
                target_date=args.date,
                exchange_codes=exchanges,
                dry_run=args.dry_run,
            )
    except KeyboardInterrupt:
        logger.warning("Interrotto dall'utente.")
        return 130
    except Exception as e:
        logger.exception("Errore non gestito: %s", e)
        return 1

    # Stato finale
    print()
    print("=" * 70)
    print(f"  FETCH PRICES RESULT ({res.mode.upper()})")
    print("=" * 70)
    print(f"  Started:           {res.started_at}")
    print(f"  Ended:             {res.ended_at}")
    print(f"  Exchanges:         {', '.join(res.exchanges_processed) or '-'}")
    print(f"  Tickers processed: {res.tickers_processed}")
    print(f"  Rows written:      {res.rows_written}")
    print(f"  Credits estimated: {res.credits_estimated}")
    print(f"  Errors:            {len(res.errors)}")
    if res.errors and args.verbose:
        for e in res.errors[:10]:
            print(f"    · {e}")
    print("=" * 70)

    return 0 if not res.errors else 2


if __name__ == "__main__":
    sys.exit(_main())
