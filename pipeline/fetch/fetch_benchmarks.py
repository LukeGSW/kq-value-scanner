"""Fetch prezzi EOD dei benchmark (indici/ETF) configurati per exchange.

I benchmark (SPY.US, VUKE.LSE, EXS1.XETRA, CAC.PA, AEX.AS, BEL20.BR,
FTSEMIB.MI, SMI.SW) non sono nell'universo delle azioni Large/Mid cap ma
servono al calcolo del Relative Strength (``rs_126d``) in
``pipeline.compute.technical``. Questo modulo li scarica separatamente e li
upserta in ``prices_daily``.

Costo: 1 chiamata EOD per benchmark → ~8 credits in modalità incremental (ultimi
30 giorni), ~8 credits anche in full (periodo lungo in una sola chiamata).

Uso CLI
-------
# Refresh incrementale (ultimi 30gg, copre gap)
python -m pipeline.fetch.fetch_benchmarks

# Backfill storico 10 anni
python -m pipeline.fetch.fetch_benchmarks --backfill --years 10
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta

import pandas as pd

from pipeline.config import get_all_exchanges
from pipeline.fetch.eodhd_client import EODHDClient, EODHDError, EODHDNotFoundError
from pipeline.storage.db import upsert_prices_bulk

logger = logging.getLogger("kq.fetch.benchmarks")
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


def _collect_benchmarks() -> list[str]:
    """Lista deduplicata dei benchmark configurati in settings.yaml."""
    seen: set[str] = set()
    out: list[str] = []
    for ex in get_all_exchanges():
        b = ex.get("benchmark")
        if b and b not in seen:
            seen.add(b)
            out.append(b)
    return out


def fetch_benchmarks(
    years: int = 10,
    incremental_days: int | None = 30,
    backfill: bool = False,
    dry_run: bool = False,
) -> dict:
    """Scarica prezzi EOD per tutti i benchmark configurati.

    Parameters
    ----------
    years : int
        Anni di storia in modalità backfill.
    incremental_days : int or None
        Giorni indietro dalla data corrente in modalità incrementale.
    backfill : bool
        Se True, scarica ``years`` anni; altrimenti ``incremental_days``.
    dry_run : bool
        Se True, non scrive nel DB.

    Returns
    -------
    dict
        Report con contatori per benchmark.
    """
    benches = _collect_benchmarks()
    if not benches:
        logger.warning("Nessun benchmark configurato in settings.yaml.")
        return {"processed": 0, "rows_written": 0, "errors": []}

    to_date = datetime.utcnow().strftime("%Y-%m-%d")
    if backfill:
        from_dt = datetime.utcnow() - timedelta(days=365 * years + 30)
    else:
        from_dt = datetime.utcnow() - timedelta(days=int(incremental_days or 30))
    from_date = from_dt.strftime("%Y-%m-%d")

    logger.info(
        "Benchmarks da scaricare: %d · periodo %s → %s (%s)",
        len(benches), from_date, to_date, "backfill" if backfill else "incremental",
    )

    client = EODHDClient()
    rows_written = 0
    errors: list[str] = []

    for bench in benches:
        try:
            df = client.get_eod(bench, from_date=from_date, to_date=to_date)
        except EODHDNotFoundError:
            msg = f"Benchmark {bench} non trovato"
            logger.warning(msg)
            errors.append(msg)
            continue
        except EODHDError as e:
            msg = f"Errore fetch benchmark {bench}: {e}"
            logger.error(msg)
            errors.append(msg)
            continue

        if df.empty:
            logger.warning("Benchmark %s: serie vuota.", bench)
            continue

        df = df.reset_index()
        df["ticker"] = bench

        # Normalizza nomi colonne alla convenzione di prices_daily
        rename_map = {
            "adjusted_close": "adjusted_close",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        if dry_run:
            logger.info("[dry-run] %s · %d righe", bench, len(df))
            continue

        n = upsert_prices_bulk(df)
        rows_written += n
        logger.info("  %s · %d righe upsertate", bench, n)

    stats = client.get_usage_stats()
    credits = (
        stats.get("credits_total", 0)
        if isinstance(stats, dict)
        else getattr(stats, "credits_total", 0)
    )
    logger.info(
        "── Benchmark fetch done: processed=%d, rows_written=%d, credits=%d, errors=%d",
        len(benches), rows_written, credits, len(errors),
    )

    return {
        "processed": len(benches),
        "rows_written": rows_written,
        "credits": credits,
        "errors": errors,
    }


def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch prezzi EOD dei benchmark (indici/ETF).",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Scarica storia completa (default: 10 anni).",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=10,
        help="Anni di storia in modalità backfill (default 10).",
    )
    parser.add_argument(
        "--incremental-days",
        type=int,
        default=30,
        help="Giorni indietro in modalità incrementale (default 30).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Non scrive nel DB.",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    try:
        res = fetch_benchmarks(
            years=args.years,
            incremental_days=args.incremental_days,
            backfill=args.backfill,
            dry_run=args.dry_run,
        )
    except KeyboardInterrupt:
        logger.warning("Interrotto.")
        return 130
    except Exception as e:
        logger.exception("Errore non gestito: %s", e)
        return 1

    print()
    print("=" * 70)
    print("  FETCH BENCHMARKS RESULT")
    print("=" * 70)
    print(f"  Benchmark processed: {res['processed']}")
    print(f"  Rows written:        {res['rows_written']}")
    print(f"  Credits estimated:   {res.get('credits', 0)}")
    print(f"  Errors:              {len(res['errors'])}")
    for e in res["errors"][:10]:
        print(f"    · {e}")
    print("=" * 70)
    return 0 if not res["errors"] else 2


if __name__ == "__main__":
    sys.exit(_main())
