"""Costruzione dell'universo investibile Large + Mid Cap USA + Europa.

Strategia
---------
Per ogni exchange configurato in `config/settings.yaml`:

  1. Scarica la lista completa dei ticker quotati via `exchange-symbol-list`
     (campo "Type" = Common Stock, ADR, Preferred Stock, ETF, Fund, ecc.).
  2. Esegue il screener EODHD con filtri:
       - exchange = <codice>
       - market_capitalization >= soglia (default $2B)
     La risposta è paginata (max 100 per call): paginiamo finché la pagina
     non torna parziale.
  3. Incrocia i due insiemi: tiene solo i ticker che soddisfano SIA la soglia
     di MarketCap SIA il tipo ammesso (Common Stock + ADR).

Output
------
Parquet file in `pipeline/storage/db/universe.parquet` con colonne:
  ticker, code, name, exchange, exchange_code, country, currency, type,
  sector, industry, market_capitalization, last_refresh_utc.

Stima costi
-----------
Per configurazione standard (8 exchange, ~2500 ticker totali):
  - exchange-symbol-list: 8 chiamate × 1 credit = 8 credits
  - screener paginato: ~30-40 chiamate × 5 credits = ~150-200 credits
  Totale: ~160-210 credits (molto sotto budget giornaliero 100k).

Uso CLI
-------
    python -m pipeline.fetch.fetch_universe                # full build
    python -m pipeline.fetch.fetch_universe --dry-run      # conta senza scrivere
    python -m pipeline.fetch.fetch_universe --exchange US  # solo un exchange
    python -m pipeline.fetch.fetch_universe --output /tmp/universe.parquet
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.config import PROJECT_ROOT, get_all_exchanges, load_settings
from pipeline.fetch.eodhd_client import (
    EODHDClient,
    EODHDError,
    EODHDNotFoundError,
)

logger = logging.getLogger("kq.universe")
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
# Mapping: codice EODHD exchange → valore atteso dal screener.
# Lo screener EODHD usa identificatori lowercase che non sempre coincidono
# con il codice dell'exchange composite. Questo mapping è derivato dalla
# documentazione e da test empirici.
# ------------------------------------------------------------------------------
SCREENER_EXCHANGE_MAP: dict[str, str] = {
    "US": "us",
    "LSE": "lse",
    "XETRA": "xetra",
    "PA": "paris",
    "AS": "amsterdam",
    "BR": "brussels",
    "MI": "milan",
    "SW": "switzerland",
}

# ------------------------------------------------------------------------------
# Tipi di strumento ammessi nell'universo finale.
# I tipi restituiti da exchange-symbol-list sono stringhe come:
# "Common Stock", "ADR", "Preferred Stock", "ETF", "Fund", "FUND", "REIT", ecc.
# ------------------------------------------------------------------------------
ALLOWED_TYPES_BASE = {"Common Stock"}
ALLOWED_TYPES_WITH_ADR = ALLOWED_TYPES_BASE | {"ADR"}


# ==============================================================================
# Fetchers atomici
# ==============================================================================
def fetch_symbols_with_types(
    client: EODHDClient, exchange_code: str
) -> pd.DataFrame:
    """Scarica la lista ticker con metadata Type da exchange-symbol-list.

    Returns
    -------
    pd.DataFrame con colonne: Code, Name, Country, Exchange, Currency, Type, Isin
    """
    logger.info("Exchange %s: scarico lista ticker completa", exchange_code)
    try:
        df = client.get_exchange_symbols(exchange_code)
    except EODHDError as exc:
        logger.error("Errore exchange-symbol-list per %s: %s", exchange_code, exc)
        return pd.DataFrame()

    if df.empty:
        logger.warning("Nessun simbolo restituito per exchange %s", exchange_code)
        return df

    # Normalizza a stringhe per robustezza (alcuni campi Type possono essere None)
    for col in ("Code", "Name", "Type", "Exchange", "Country", "Currency"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    logger.info("Exchange %s: %d ticker totali quotati", exchange_code, len(df))
    return df


def fetch_marketcap_candidates(
    client: EODHDClient,
    exchange_code: str,
    marketcap_min_usd: float,
    page_limit: int = 100,
    max_pages: int = 50,
) -> list[dict[str, Any]]:
    """Interroga il screener EODHD paginato per ottenere i candidati Large+Mid Cap.

    Parameters
    ----------
    client : EODHDClient
    exchange_code : str
        Codice EODHD dell'exchange (es. "US", "LSE").
    marketcap_min_usd : float
        Soglia minima di MarketCap in USD.
    page_limit : int
        Dimensione pagina (max 100 per EODHD screener).
    max_pages : int
        Limite di sicurezza per evitare loop infiniti (default 50 = 5000 ticker).

    Returns
    -------
    list of dict
        Ogni elemento contiene campi come: code, name, market_capitalization,
        sector, industry, exchange_short_name, country.
    """
    screener_exchange = SCREENER_EXCHANGE_MAP.get(exchange_code)
    if not screener_exchange:
        logger.warning(
            "Mapping screener mancante per %s, userò il codice raw in lowercase",
            exchange_code,
        )
        screener_exchange = exchange_code.lower()

    filters = [
        ["exchange", "=", screener_exchange],
        ["market_capitalization", ">=", marketcap_min_usd],
    ]

    all_results: list[dict[str, Any]] = []
    offset = 0
    for page_idx in range(max_pages):
        try:
            page = client.screener(
                filters=filters,
                sort="market_capitalization.desc",
                limit=page_limit,
                offset=offset,
            )
        except EODHDNotFoundError:
            logger.info(
                "Screener vuoto per exchange %s offset %d (fine dati)",
                exchange_code,
                offset,
            )
            break
        except EODHDError as exc:
            logger.error(
                "Errore screener per %s pagina %d: %s", exchange_code, page_idx, exc
            )
            break

        if not page:
            break

        all_results.extend(page)
        logger.info(
            "Exchange %s: pagina %d → +%d candidati (cumulato %d)",
            exchange_code,
            page_idx + 1,
            len(page),
            len(all_results),
        )

        if len(page) < page_limit:
            # Ultima pagina (parziale)
            break
        offset += page_limit

    return all_results


# ==============================================================================
# Merging
# ==============================================================================
def build_universe_for_exchange(
    client: EODHDClient,
    exchange_cfg: dict[str, Any],
    marketcap_min_usd: float,
    allowed_types: set[str],
) -> pd.DataFrame:
    """Costruisce l'universo filtrato per un singolo exchange.

    Intersezione di:
      - screener (MarketCap >= soglia)
      - exchange-symbol-list (Type in allowed_types)
    """
    code = exchange_cfg["code"]

    # 1. Symbols + Types
    symbols_df = fetch_symbols_with_types(client, code)
    if symbols_df.empty:
        return pd.DataFrame()

    types_lookup = (
        symbols_df.set_index("Code")[["Type", "Name", "Isin", "Currency"]]
        .to_dict(orient="index")
    )

    # 2. Candidates per MarketCap
    candidates = fetch_marketcap_candidates(
        client, code, marketcap_min_usd=marketcap_min_usd
    )
    if not candidates:
        logger.warning("Nessun candidato MarketCap per %s", code)
        return pd.DataFrame()

    # 3. Intersect
    rows: list[dict[str, Any]] = []
    missing_type_count = 0
    excluded_by_type: dict[str, int] = {}
    for cand in candidates:
        # Il campo 'code' del screener è il simbolo puro (senza suffisso exchange)
        cand_code = str(cand.get("code", "")).upper()
        if not cand_code:
            continue

        # Lookup Type — alcune listing potrebbero non essere nel symbol-list
        sym_info = types_lookup.get(cand_code)
        if not sym_info:
            missing_type_count += 1
            continue

        sym_type = sym_info.get("Type", "") or ""
        if sym_type not in allowed_types:
            excluded_by_type[sym_type] = excluded_by_type.get(sym_type, 0) + 1
            continue

        rows.append(
            {
                "ticker": f"{cand_code}.{code}",
                "code": cand_code,
                "name": cand.get("name") or sym_info.get("Name", ""),
                "exchange_code": code,
                "exchange_name": exchange_cfg.get("name", ""),
                "country": exchange_cfg.get("country", ""),
                "currency": exchange_cfg.get("currency")
                or sym_info.get("Currency", ""),
                "type": sym_type,
                "sector": cand.get("sector"),
                "industry": cand.get("industry"),
                "market_capitalization": cand.get("market_capitalization"),
                "isin": sym_info.get("Isin", ""),
            }
        )

    df = pd.DataFrame(rows)
    logger.info(
        "Exchange %s: %d ticker nell'universo "
        "(scartati %d per Type, %d assenti da symbol-list) "
        "· esclusioni per tipo: %s",
        code,
        len(df),
        sum(excluded_by_type.values()),
        missing_type_count,
        excluded_by_type,
    )
    return df


# ==============================================================================
# Orchestrator
# ==============================================================================
def build_universe(
    client: EODHDClient | None = None,
    exchange_filter: list[str] | None = None,
) -> pd.DataFrame:
    """Costruisce l'universo completo aggregando tutti gli exchange configurati.

    Parameters
    ----------
    client : EODHDClient, optional
        Se None, ne viene istanziato uno nuovo. Se si passa un client esistente,
        le sue stats di utilizzo rifletteranno anche le chiamate di questa funzione.
    exchange_filter : list of str, optional
        Se fornito, costruisce l'universo solo per questi exchange codes
        (utile per test o rebuild parziali).

    Returns
    -------
    pd.DataFrame
        Universo aggregato con tutte le colonne definite.
    """
    settings = load_settings()
    universe_cfg = settings["universe"]

    marketcap_min = float(universe_cfg["marketcap_min_usd"])
    include_adrs = bool(universe_cfg.get("include_adrs", True))
    allowed_types = ALLOWED_TYPES_WITH_ADR if include_adrs else ALLOWED_TYPES_BASE

    exchanges = get_all_exchanges()
    if exchange_filter:
        exchanges = [
            e for e in exchanges if e["code"] in set(exchange_filter)
        ]
        if not exchanges:
            logger.error(
                "Nessun exchange corrisponde al filtro: %s", exchange_filter
            )
            return pd.DataFrame()

    logger.info(
        "Build universo: %d exchange · MarketCap min $%.0fB · include ADR=%s",
        len(exchanges),
        marketcap_min / 1e9,
        include_adrs,
    )

    owns_client = client is None
    if client is None:
        client = EODHDClient()

    try:
        all_dfs: list[pd.DataFrame] = []
        for ex_cfg in exchanges:
            df_ex = build_universe_for_exchange(
                client=client,
                exchange_cfg=ex_cfg,
                marketcap_min_usd=marketcap_min,
                allowed_types=allowed_types,
            )
            if not df_ex.empty:
                all_dfs.append(df_ex)

        if not all_dfs:
            logger.error("Universo vuoto, nessun exchange ha prodotto risultati")
            return pd.DataFrame()

        universe = pd.concat(all_dfs, ignore_index=True)

        # Deduplicazione su ticker (stesso code.exchange)
        before = len(universe)
        universe = universe.drop_duplicates(subset=["ticker"], keep="first")
        if before != len(universe):
            logger.info("Deduplicati %d ticker", before - len(universe))

        # Timestamp di generazione
        universe["last_refresh_utc"] = datetime.now(timezone.utc).isoformat()

        # Ordinamento per MarketCap discendente
        universe = universe.sort_values(
            "market_capitalization", ascending=False, na_position="last"
        ).reset_index(drop=True)

        logger.info("Universo finale: %d ticker totali", len(universe))
        _log_breakdown(universe)
        return universe
    finally:
        if owns_client:
            client.log_usage()
            client.close()


def _log_breakdown(universe: pd.DataFrame) -> None:
    """Log riepilogativo per exchange e per MarketCap bucket."""
    if universe.empty:
        return
    logger.info("--- Breakdown per exchange ---")
    for code, grp in universe.groupby("exchange_code", sort=False):
        total_mcap = grp["market_capitalization"].sum()
        logger.info(
            "  %-6s  %4d ticker · MCap totale $%.1fB",
            code,
            len(grp),
            total_mcap / 1e9 if total_mcap else 0,
        )

    # Bucket dimensione
    mc = universe["market_capitalization"].dropna()
    if not mc.empty:
        mega = (mc >= 200e9).sum()
        large = ((mc >= 10e9) & (mc < 200e9)).sum()
        mid = ((mc >= 2e9) & (mc < 10e9)).sum()
        logger.info(
            "--- Breakdown size --- Mega(>$200B): %d · Large($10-200B): %d · Mid($2-10B): %d",
            mega,
            large,
            mid,
        )


# ==============================================================================
# Persistence
# ==============================================================================
def save_universe_parquet(
    df: pd.DataFrame, output_path: Path | str | None = None
) -> Path:
    """Salva l'universo in formato Parquet, creando le directory se necessario."""
    if output_path is None:
        output_path = PROJECT_ROOT / "pipeline" / "storage" / "db" / "universe.parquet"
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False, engine="pyarrow")
    logger.info("Universo salvato in %s (%d righe)", output_path, len(df))
    return output_path


# ==============================================================================
# CLI
# ==============================================================================
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Costruzione universo investibile KQ Value Scanner"
    )
    parser.add_argument(
        "--exchange",
        action="append",
        help="Codice exchange da processare (ripetibile). "
        "Default: tutti gli exchange configurati.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Path di output parquet. Default: pipeline/storage/db/universe.parquet",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Costruisce l'universo ma non salva su disco.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Log DEBUG verboso.",
    )
    args = parser.parse_args(argv)

    if args.verbose:
        logging.getLogger("kq").setLevel(logging.DEBUG)

    universe = build_universe(exchange_filter=args.exchange)

    if universe.empty:
        logger.error("Universo vuoto, nulla da salvare")
        return 1

    if args.dry_run:
        logger.info("Dry-run: universo NON salvato (%d righe pronte)", len(universe))
        # Print a tiny preview
        print("\n" + universe.head(10).to_string(index=False))
        return 0

    save_universe_parquet(universe, output_path=args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
