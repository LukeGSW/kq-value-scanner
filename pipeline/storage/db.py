"""Wrapper SQLite per KQ Value Scanner.

Gestisce connessioni, inizializzazione schema e operazioni batch (upsert)
su tutte le tabelle definite in ``schema.sql``.

Funzionalità:
  - ``get_connection()`` — connessione con PRAGMA ottimizzate e row_factory dict-like
  - ``init_schema()`` — esegue DDL da ``schema.sql`` (idempotente)
  - ``upsert_universe()`` — scrive/aggiorna righe in ``universe``
  - ``upsert_prices_bulk()`` — insert batch in ``prices_daily``
  - ``upsert_fundamentals_snapshot()`` — upsert in ``fundamentals_snapshot``
  - ``upsert_financials_history()`` — batch in ``financials_history``
  - ``upsert_computed_metrics()`` — upsert in ``computed_metrics``
  - ``log_fetch()`` — audit chiamate API
  - ``get_universe()``, ``get_prices()``, ``get_last_price_date()`` — read helpers
  - ``vacuum()`` / ``analyze()`` — manutenzione

Esempio d'uso
-------------
>>> from pipeline.storage.db import get_connection, init_schema
>>> init_schema()
>>> with get_connection() as conn:
...     rows = conn.execute("SELECT COUNT(*) FROM universe").fetchone()
...     print(rows[0])
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import pandas as pd

from pipeline.config import PROJECT_ROOT, load_settings

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logger = logging.getLogger("kq.db")
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
# Path helpers
# ------------------------------------------------------------------------------
def get_db_path() -> Path:
    """Ritorna il path al file SQLite come configurato in settings.yaml."""
    settings = load_settings()
    raw = settings["storage"]["db_path"]
    p = Path(raw)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def get_schema_path() -> Path:
    """Path al file schema.sql (accanto a questo modulo)."""
    return Path(__file__).resolve().parent / "schema.sql"


# ------------------------------------------------------------------------------
# Connessione
# ------------------------------------------------------------------------------
@contextmanager
def get_connection(
    db_path: str | Path | None = None,
    row_factory: bool = True,
) -> Iterator[sqlite3.Connection]:
    """Apre una connessione SQLite con PRAGMA ottimizzate.

    Parameters
    ----------
    db_path : str or Path, optional
        Override esplicito del path DB. Default: configurazione.
    row_factory : bool
        Se True (default), abilita ``sqlite3.Row`` per accesso tipo dict.

    Yields
    ------
    sqlite3.Connection
        Connessione gestita via context manager; commit automatico all'uscita
        senza eccezioni, rollback in caso di errore.
    """
    path = Path(db_path) if db_path else get_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(
        str(path),
        timeout=30.0,
        isolation_level=None,  # autocommit — gestiamo transazioni esplicitamente
        detect_types=sqlite3.PARSE_DECLTYPES,
    )
    if row_factory:
        conn.row_factory = sqlite3.Row

    # PRAGMA run-time (schema.sql le setta comunque, ma utile a prima connessione)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA temp_store = MEMORY")

    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


# ------------------------------------------------------------------------------
# Init schema
# ------------------------------------------------------------------------------
def init_schema(db_path: str | Path | None = None) -> None:
    """Crea tutte le tabelle se mancano. Idempotente.

    Legge il DDL da ``schema.sql`` e lo esegue su `db_path`.
    """
    schema_sql = get_schema_path().read_text(encoding="utf-8")
    with get_connection(db_path, row_factory=False) as conn:
        conn.executescript(schema_sql)
    logger.info("Schema inizializzato su %s", db_path or get_db_path())


# ------------------------------------------------------------------------------
# Utility interne
# ------------------------------------------------------------------------------
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _chunked(seq: Sequence, n: int) -> Iterator[Sequence]:
    """Yield successive ``n``-sized chunks from ``seq``."""
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _normalize_value(v: Any) -> Any:
    """Converte NaN/pd.NA/None in None, numpy scalars in native python."""
    if v is None:
        return None
    # pandas/numpy scalar NaN
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    # numpy scalar → python
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            return v
    return v


# ------------------------------------------------------------------------------
# UPSERT — universe
# ------------------------------------------------------------------------------
UNIVERSE_COLS = [
    "ticker", "code", "name", "exchange_code", "exchange_name",
    "country", "currency", "type", "sector", "industry",
    "market_capitalization", "isin", "is_active", "last_refresh_utc",
]


def upsert_universe(
    rows: Iterable[Mapping[str, Any]],
    db_path: str | Path | None = None,
) -> int:
    """Insert-or-replace righe nella tabella ``universe``.

    Parameters
    ----------
    rows : iterable of mapping
        Ogni dict deve contenere almeno ``ticker``; le colonne mancanti vengono
        settate a NULL. ``last_refresh_utc`` viene auto-compilato se assente.
    db_path : str or Path, optional
        Override path DB.

    Returns
    -------
    int
        Numero di righe inserite/aggiornate.
    """
    rows = list(rows)
    if not rows:
        return 0

    now = _utc_now_iso()
    prepared = []
    for r in rows:
        prepared.append(
            tuple(
                _normalize_value(r.get(col) if col != "last_refresh_utc"
                                  else r.get(col, now))
                for col in UNIVERSE_COLS
            )
        )

    placeholders = ",".join(["?"] * len(UNIVERSE_COLS))
    cols_sql = ",".join(UNIVERSE_COLS)
    sql = f"""
        INSERT INTO universe ({cols_sql}) VALUES ({placeholders})
        ON CONFLICT(ticker) DO UPDATE SET
            {", ".join(f"{c}=excluded.{c}" for c in UNIVERSE_COLS if c != "ticker")}
    """

    with get_connection(db_path, row_factory=False) as conn:
        conn.execute("BEGIN")
        conn.executemany(sql, prepared)
        conn.execute("COMMIT")

    logger.info("Universe upsert: %d righe", len(prepared))
    return len(prepared)


def load_universe_from_parquet(
    parquet_path: str | Path | None = None,
    db_path: str | Path | None = None,
) -> int:
    """Importa l'universo da un file parquet nella tabella SQLite.

    Parameters
    ----------
    parquet_path : str or Path, optional
        Default: ``pipeline/storage/db/universe.parquet``.
    db_path : str or Path, optional
        Override DB.

    Returns
    -------
    int
        Numero di righe importate.
    """
    if parquet_path is None:
        parquet_path = PROJECT_ROOT / "pipeline" / "storage" / "db" / "universe.parquet"
    parquet_path = Path(parquet_path)

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Universo parquet non trovato: {parquet_path}\n"
            f"Eseguire prima: python -m pipeline.fetch.fetch_universe"
        )

    df = pd.read_parquet(parquet_path)
    logger.info("Parquet letto: %d righe da %s", len(df), parquet_path)

    # Mantiene solo le colonne conosciute, altre scartate
    keep = [c for c in UNIVERSE_COLS if c in df.columns]
    df = df[keep].copy()

    # is_active default = 1
    if "is_active" not in df.columns:
        df["is_active"] = 1

    return upsert_universe(df.to_dict(orient="records"), db_path=db_path)


# ------------------------------------------------------------------------------
# UPSERT — prices_daily
# ------------------------------------------------------------------------------
PRICES_COLS = [
    "ticker", "date", "open", "high", "low",
    "close", "adjusted_close", "volume",
]


def upsert_prices_bulk(
    rows: Iterable[Mapping[str, Any]] | pd.DataFrame,
    batch_size: int = 5000,
    db_path: str | Path | None = None,
) -> int:
    """Insert-or-replace batch di prezzi in ``prices_daily``.

    Parameters
    ----------
    rows : iterable of mapping OR pandas DataFrame
        Se DataFrame, deve contenere almeno ``ticker`` e ``date`` (nell'indice o
        come colonna). Le colonne mancanti sono NULL.
    batch_size : int
        Dimensione del chunk per executemany (default 5000).
    db_path : str or Path, optional
        Override path DB.

    Returns
    -------
    int
        Righe inserite/aggiornate.
    """
    # Normalizza a lista di tuple
    if isinstance(rows, pd.DataFrame):
        df = rows.copy()
        if "date" not in df.columns and df.index.name == "date":
            df = df.reset_index()
        # Converte date in stringa ISO
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        records = df.to_dict(orient="records")
    else:
        records = list(rows)

    if not records:
        return 0

    prepared = [
        tuple(_normalize_value(r.get(col)) for col in PRICES_COLS)
        for r in records
    ]

    placeholders = ",".join(["?"] * len(PRICES_COLS))
    cols_sql = ",".join(PRICES_COLS)
    sql = f"""
        INSERT INTO prices_daily ({cols_sql}) VALUES ({placeholders})
        ON CONFLICT(ticker, date) DO UPDATE SET
            open=excluded.open,
            high=excluded.high,
            low=excluded.low,
            close=excluded.close,
            adjusted_close=excluded.adjusted_close,
            volume=excluded.volume
    """

    total = 0
    with get_connection(db_path, row_factory=False) as conn:
        conn.execute("BEGIN")
        for chunk in _chunked(prepared, batch_size):
            conn.executemany(sql, chunk)
            total += len(chunk)
        conn.execute("COMMIT")

    logger.info("Prices upsert: %d righe (batch_size=%d)", total, batch_size)
    return total


# ------------------------------------------------------------------------------
# UPSERT — fundamentals_snapshot
# ------------------------------------------------------------------------------
FUNDAMENTALS_SNAPSHOT_COLS = [
    "ticker",
    # valuation
    "pe_ttm", "forward_pe", "peg", "price_to_sales_ttm", "price_to_book",
    "ev_to_ebitda", "ev_to_revenue", "earnings_yield", "dividend_yield",
    # profitability
    "roe", "roa", "roic", "gross_margin", "operating_margin", "profit_margin",
    # size
    "market_cap_usd", "shares_outstanding", "beta",
    # growth
    "revenue_growth_yoy", "eps_growth_yoy", "earnings_growth_next_year",
    # payout
    "dividend_per_share", "payout_ratio",
    # earnings / schedule
    "next_earnings_date", "most_recent_quarter",
    # balance sheet sintesi
    "total_cash", "total_debt", "net_debt",
    "total_revenue_ttm", "ebitda_ttm", "ebit_ttm",
    "free_cash_flow_ttm", "interest_expense_ttm",
    "total_current_assets", "total_current_liabilities",
    "inventory", "accounts_receivable",
    # metadata
    "currency_reporting", "last_refresh_utc",
]


def upsert_fundamentals_snapshot(
    rows: Iterable[Mapping[str, Any]],
    db_path: str | Path | None = None,
) -> int:
    """Upsert batch su ``fundamentals_snapshot`` (overwrite ultimo snapshot)."""
    rows = list(rows)
    if not rows:
        return 0

    now = _utc_now_iso()
    prepared = []
    for r in rows:
        prepared.append(
            tuple(
                _normalize_value(r.get(col) if col != "last_refresh_utc"
                                  else r.get(col, now))
                for col in FUNDAMENTALS_SNAPSHOT_COLS
            )
        )

    placeholders = ",".join(["?"] * len(FUNDAMENTALS_SNAPSHOT_COLS))
    cols_sql = ",".join(FUNDAMENTALS_SNAPSHOT_COLS)
    update_cols = [c for c in FUNDAMENTALS_SNAPSHOT_COLS if c != "ticker"]
    sql = f"""
        INSERT INTO fundamentals_snapshot ({cols_sql}) VALUES ({placeholders})
        ON CONFLICT(ticker) DO UPDATE SET
            {", ".join(f"{c}=excluded.{c}" for c in update_cols)}
    """

    with get_connection(db_path, row_factory=False) as conn:
        conn.execute("BEGIN")
        conn.executemany(sql, prepared)
        conn.execute("COMMIT")

    logger.info("Fundamentals snapshot upsert: %d righe", len(prepared))
    return len(prepared)


# ------------------------------------------------------------------------------
# UPSERT — financials_history
# ------------------------------------------------------------------------------
FINANCIALS_HISTORY_COLS = [
    "ticker", "period_end", "statement_type", "freq", "currency_symbol",
    # income
    "total_revenue", "gross_profit", "operating_income", "ebit", "ebitda",
    "net_income", "eps_basic", "eps_diluted", "interest_expense",
    "income_tax_expense",
    # balance sheet
    "cash_and_equivalents", "short_term_investments", "total_current_assets",
    "total_assets", "total_current_liabilities", "long_term_debt",
    "short_term_debt", "total_liabilities", "total_equity",
    "inventory", "accounts_receivable", "retained_earnings",
    # cashflow
    "operating_cashflow", "capital_expenditure", "free_cash_flow",
    "dividends_paid", "share_repurchases",
    # raw
    "raw_json", "last_refresh_utc",
]


def upsert_financials_history(
    rows: Iterable[Mapping[str, Any]],
    batch_size: int = 1000,
    db_path: str | Path | None = None,
) -> int:
    """Upsert batch su ``financials_history``."""
    rows = list(rows)
    if not rows:
        return 0

    now = _utc_now_iso()
    prepared = []
    for r in rows:
        prepared.append(
            tuple(
                _normalize_value(
                    r.get(col) if col != "last_refresh_utc"
                    else r.get(col, now)
                )
                for col in FINANCIALS_HISTORY_COLS
            )
        )

    placeholders = ",".join(["?"] * len(FINANCIALS_HISTORY_COLS))
    cols_sql = ",".join(FINANCIALS_HISTORY_COLS)
    pk_cols = {"ticker", "period_end", "statement_type", "freq"}
    update_cols = [c for c in FINANCIALS_HISTORY_COLS if c not in pk_cols]
    sql = f"""
        INSERT INTO financials_history ({cols_sql}) VALUES ({placeholders})
        ON CONFLICT(ticker, period_end, statement_type, freq) DO UPDATE SET
            {", ".join(f"{c}=excluded.{c}" for c in update_cols)}
    """

    total = 0
    with get_connection(db_path, row_factory=False) as conn:
        conn.execute("BEGIN")
        for chunk in _chunked(prepared, batch_size):
            conn.executemany(sql, chunk)
            total += len(chunk)
        conn.execute("COMMIT")

    logger.info("Financials history upsert: %d righe", total)
    return total


# ------------------------------------------------------------------------------
# UPSERT — computed_metrics
# ------------------------------------------------------------------------------
COMPUTED_METRICS_COLS = [
    "ticker",
    "last_close", "last_close_date", "sma_50", "sma_200",
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
    "last_compute_utc",
]


def upsert_computed_metrics(
    rows: Iterable[Mapping[str, Any]],
    db_path: str | Path | None = None,
) -> int:
    """Upsert batch su ``computed_metrics``."""
    rows = list(rows)
    if not rows:
        return 0

    now = _utc_now_iso()
    prepared = []
    for r in rows:
        prepared.append(
            tuple(
                _normalize_value(
                    r.get(col) if col != "last_compute_utc"
                    else r.get(col, now)
                )
                for col in COMPUTED_METRICS_COLS
            )
        )

    placeholders = ",".join(["?"] * len(COMPUTED_METRICS_COLS))
    cols_sql = ",".join(COMPUTED_METRICS_COLS)
    update_cols = [c for c in COMPUTED_METRICS_COLS if c != "ticker"]
    sql = f"""
        INSERT INTO computed_metrics ({cols_sql}) VALUES ({placeholders})
        ON CONFLICT(ticker) DO UPDATE SET
            {", ".join(f"{c}=excluded.{c}" for c in update_cols)}
    """

    with get_connection(db_path, row_factory=False) as conn:
        conn.execute("BEGIN")
        conn.executemany(sql, prepared)
        conn.execute("COMMIT")

    logger.info("Computed metrics upsert: %d righe", len(prepared))
    return len(prepared)


# ------------------------------------------------------------------------------
# Screener cache
# ------------------------------------------------------------------------------
def save_screener_snapshot(
    payload: dict | str,
    db_path: str | Path | None = None,
) -> int:
    """Salva uno snapshot del JSON servito al frontend in ``screener_cache``.

    Returns
    -------
    int
        build_id assegnato.
    """
    if isinstance(payload, dict):
        payload_str = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    else:
        payload_str = str(payload)

    # Conta ticker se la struttura lo consente (best-effort)
    try:
        obj = payload if isinstance(payload, dict) else json.loads(payload_str)
        tickers = obj.get("tickers", obj.get("data", []))
        ticker_count = len(tickers) if isinstance(tickers, list) else 0
    except Exception:
        ticker_count = 0

    size_bytes = len(payload_str.encode("utf-8"))
    now = _utc_now_iso()

    with get_connection(db_path, row_factory=False) as conn:
        cur = conn.execute(
            "INSERT INTO screener_cache (build_ts_utc, payload_json, ticker_count, size_bytes) "
            "VALUES (?, ?, ?, ?)",
            (now, payload_str, ticker_count, size_bytes),
        )
        build_id = cur.lastrowid

    logger.info(
        "Screener snapshot salvato: build_id=%d, ticker=%d, size=%.1f KB",
        build_id, ticker_count, size_bytes / 1024,
    )
    return build_id


# ------------------------------------------------------------------------------
# Fetch log
# ------------------------------------------------------------------------------
def log_fetch(
    endpoint: str,
    resource: str | None = None,
    status_code: int | None = None,
    credits_cost: int | None = None,
    duration_ms: int | None = None,
    error_msg: str | None = None,
    db_path: str | Path | None = None,
) -> None:
    """Scrive una riga di audit in ``fetch_log``."""
    with get_connection(db_path, row_factory=False) as conn:
        conn.execute(
            "INSERT INTO fetch_log "
            "(ts_utc, endpoint, resource, status_code, credits_cost, duration_ms, error_msg) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (_utc_now_iso(), endpoint, resource, status_code,
             credits_cost, duration_ms, error_msg),
        )


# ------------------------------------------------------------------------------
# Read helpers
# ------------------------------------------------------------------------------
def get_universe(
    active_only: bool = True,
    exchange_code: str | None = None,
    db_path: str | Path | None = None,
) -> pd.DataFrame:
    """Legge l'universo dal DB.

    Parameters
    ----------
    active_only : bool
        Se True (default), filtra ``is_active = 1``.
    exchange_code : str, optional
        Filtra per exchange (es. "US", "LSE").
    """
    sql = "SELECT * FROM universe WHERE 1=1"
    params: list[Any] = []
    if active_only:
        sql += " AND is_active = 1"
    if exchange_code:
        sql += " AND exchange_code = ?"
        params.append(exchange_code)

    with get_connection(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)


def get_prices(
    ticker: str,
    from_date: str | None = None,
    to_date: str | None = None,
    db_path: str | Path | None = None,
) -> pd.DataFrame:
    """Legge la serie prezzi per un ticker."""
    sql = "SELECT * FROM prices_daily WHERE ticker = ?"
    params: list[Any] = [ticker]
    if from_date:
        sql += " AND date >= ?"
        params.append(from_date)
    if to_date:
        sql += " AND date <= ?"
        params.append(to_date)
    sql += " ORDER BY date ASC"

    with get_connection(db_path) as conn:
        df = pd.read_sql_query(sql, conn, params=params)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
    return df


def get_last_price_date(
    ticker: str | None = None,
    exchange_code: str | None = None,
    db_path: str | Path | None = None,
) -> str | None:
    """Ritorna l'ultima data presente in ``prices_daily``.

    Se ``ticker`` è specificato, la query è globale sul ticker; se
    ``exchange_code`` è specificato, si filtra via join su universe; altrimenti
    ritorna il MAX(date) globale.
    """
    with get_connection(db_path) as conn:
        if ticker:
            row = conn.execute(
                "SELECT MAX(date) AS d FROM prices_daily WHERE ticker = ?",
                (ticker,),
            ).fetchone()
        elif exchange_code:
            row = conn.execute(
                """
                SELECT MAX(p.date) AS d
                FROM prices_daily p
                JOIN universe u ON u.ticker = p.ticker
                WHERE u.exchange_code = ?
                """,
                (exchange_code,),
            ).fetchone()
        else:
            row = conn.execute("SELECT MAX(date) AS d FROM prices_daily").fetchone()
    return row["d"] if row and row["d"] else None


def get_tickers_without_history(
    exchange_code: str | None = None,
    db_path: str | Path | None = None,
) -> list[str]:
    """Ritorna i ticker dell'universo per i quali manca completamente lo storico prezzi."""
    sql = """
        SELECT u.ticker
        FROM universe u
        LEFT JOIN (
            SELECT ticker, COUNT(*) AS n
            FROM prices_daily
            GROUP BY ticker
        ) p ON p.ticker = u.ticker
        WHERE u.is_active = 1
          AND (p.n IS NULL OR p.n = 0)
    """
    params: list[Any] = []
    if exchange_code:
        sql += " AND u.exchange_code = ?"
        params.append(exchange_code)

    with get_connection(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [r["ticker"] for r in rows]


# ------------------------------------------------------------------------------
# Manutenzione
# ------------------------------------------------------------------------------
def vacuum(db_path: str | Path | None = None) -> None:
    """Esegue VACUUM per compattare il DB."""
    with get_connection(db_path, row_factory=False) as conn:
        conn.execute("VACUUM")
    logger.info("VACUUM completato")


def analyze(db_path: str | Path | None = None) -> None:
    """Aggiorna le statistiche del query planner."""
    with get_connection(db_path, row_factory=False) as conn:
        conn.execute("ANALYZE")
    logger.info("ANALYZE completato")


def get_table_stats(db_path: str | Path | None = None) -> pd.DataFrame:
    """Ritorna conteggio righe per tutte le tabelle (diagnostica)."""
    tables = [
        "universe", "prices_daily", "fundamentals_snapshot",
        "financials_history", "computed_metrics", "screener_cache", "fetch_log",
    ]
    rows = []
    with get_connection(db_path) as conn:
        for t in tables:
            try:
                cnt = conn.execute(f"SELECT COUNT(*) AS n FROM {t}").fetchone()["n"]
                rows.append({"table": t, "rows": cnt})
            except sqlite3.OperationalError:
                rows.append({"table": t, "rows": None})
    return pd.DataFrame(rows)


# ------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------
def _main() -> None:
    """CLI minimale: inizializza lo schema e stampa le statistiche."""
    import argparse
    parser = argparse.ArgumentParser(
        description="Utility DB: init schema, import universe, stats."
    )
    parser.add_argument(
        "--init", action="store_true",
        help="Inizializza lo schema (idempotente).",
    )
    parser.add_argument(
        "--load-universe", action="store_true",
        help="Importa universe.parquet nella tabella universe.",
    )
    parser.add_argument(
        "--stats", action="store_true",
        help="Stampa conteggio righe per tabella.",
    )
    parser.add_argument(
        "--vacuum", action="store_true",
        help="Esegue VACUUM sul DB.",
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="Override path DB (default: da settings.yaml).",
    )
    args = parser.parse_args()

    db = args.db

    if args.init:
        init_schema(db)
        print(f"Schema inizializzato su {db or get_db_path()}")

    if args.load_universe:
        n = load_universe_from_parquet(db_path=db)
        print(f"Universo importato: {n} righe")

    if args.stats:
        df = get_table_stats(db)
        print(df.to_string(index=False))

    if args.vacuum:
        vacuum(db)

    if not any([args.init, args.load_universe, args.stats, args.vacuum]):
        parser.print_help()


if __name__ == "__main__":
    _main()
