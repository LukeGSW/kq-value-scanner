"""EODHD API Client per KQ Value Scanner.

Client Python robusto per interagire con le API di EOD Historical Data
(https://eodhd.com). Progettato per funzionare in tre ambienti:

  1. Google Colab — legge la chiave dai Colab Secrets (`EODHD_API_KEY`)
  2. Sviluppo locale — legge da variabile d'ambiente o file `.env`
  3. GitHub Actions — legge dai repo secrets (esposti come env var)

Caratteristiche:
  - Rate limiting client-side (finestra scorrevole a 1 minuto)
  - Retry con backoff esponenziale su 429/5xx (via tenacity)
  - Tracking delle chiamate e stima dei crediti consumati
  - Session HTTP con connection pooling
  - Logging strutturato

Esempio d'uso
-------------
>>> from pipeline.fetch.eodhd_client import EODHDClient
>>> client = EODHDClient()
>>> df = client.get_eod("AAPL.US", from_date="2024-01-01")
>>> fundamentals = client.get_fundamentals("AAPL.US")
>>> stats = client.get_usage_stats()

Riferimenti API
---------------
https://eodhd.com/financial-apis/
"""

from __future__ import annotations

import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ------------------------------------------------------------------------------
# Logging setup
# ------------------------------------------------------------------------------
logger = logging.getLogger("kq.eodhd")
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
# Eccezioni custom
# ------------------------------------------------------------------------------
class EODHDError(Exception):
    """Eccezione base per errori del client EODHD."""


class EODHDAuthError(EODHDError):
    """Chiave API mancante o non valida."""


class EODHDRateLimitError(EODHDError):
    """Rate limit superato (HTTP 429)."""


class EODHDServerError(EODHDError):
    """Errore lato server EODHD (HTTP 5xx)."""


class EODHDNotFoundError(EODHDError):
    """Risorsa non trovata (HTTP 404)."""


# ------------------------------------------------------------------------------
# Stima costi API in crediti (documentazione EODHD)
# ------------------------------------------------------------------------------
# Fonte: https://eodhd.com/financial-apis/api-credits-usage/
# I valori sono il costo in "API calls" di ogni endpoint. Il piano All-World
# Extended ha budget di 100.000 API calls/giorno.
API_COST = {
    "eod": 1,                    # dati EOD singolo ticker
    "eod_bulk": 100,             # bulk EOD (tutti i ticker di un exchange)
    "fundamentals": 10,          # fundamentals full singolo ticker
    "fundamentals_bulk": 100,    # bulk fundamentals per exchange
    "intraday": 5,               # dati intraday
    "screener": 5,               # screener query
    "exchange_symbols": 1,       # lista ticker di un exchange
    "exchange_info": 1,          # info su un exchange
    "search": 1,                 # search ticker
    "live": 1,                   # quote real-time
    "dividends": 1,
    "splits": 1,
    "options": 10,               # catena opzioni (se disponibile)
    "news": 5,
    "insider_transactions": 10,
}


# ------------------------------------------------------------------------------
# Strutture dati per tracking
# ------------------------------------------------------------------------------
@dataclass
class UsageStats:
    """Statistiche di utilizzo accumulate durante la sessione client."""

    calls_total: int = 0
    credits_total: int = 0
    calls_by_endpoint: dict[str, int] = field(default_factory=dict)
    credits_by_endpoint: dict[str, int] = field(default_factory=dict)
    errors_total: int = 0
    started_at: datetime = field(default_factory=datetime.utcnow)

    def record(self, endpoint_key: str) -> None:
        cost = API_COST.get(endpoint_key, 1)
        self.calls_total += 1
        self.credits_total += cost
        self.calls_by_endpoint[endpoint_key] = (
            self.calls_by_endpoint.get(endpoint_key, 0) + 1
        )
        self.credits_by_endpoint[endpoint_key] = (
            self.credits_by_endpoint.get(endpoint_key, 0) + cost
        )

    def record_error(self) -> None:
        self.errors_total += 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "calls_total": self.calls_total,
            "credits_total": self.credits_total,
            "errors_total": self.errors_total,
            "session_started_at": self.started_at.isoformat() + "Z",
            "calls_by_endpoint": dict(self.calls_by_endpoint),
            "credits_by_endpoint": dict(self.credits_by_endpoint),
        }


# ==============================================================================
# API KEY RESOLUTION
# ==============================================================================
def resolve_api_key(explicit_key: str | None = None) -> str:
    """Risolve la chiave API EODHD cercando in ordine:

    1. Parametro esplicito passato al costruttore
    2. Colab Secrets (se in ambiente Colab)
    3. Variabile d'ambiente EODHD_API_KEY
    4. File .env nella cwd

    Raises
    ------
    EODHDAuthError
        Se nessuna fonte fornisce una chiave non vuota.
    """
    # 1. Parametro esplicito
    if explicit_key:
        logger.debug("API key: fornita esplicitamente")
        return explicit_key.strip()

    # 2. Colab Secrets
    try:
        from google.colab import userdata  # type: ignore

        key = userdata.get("EODHD_API_KEY")
        if key:
            logger.info("API key: letta da Colab Secrets")
            return key.strip()
    except ImportError:
        pass
    except Exception as exc:  # noqa: BLE001 - colab userdata può lanciare vari errori
        logger.debug("Colab userdata non utilizzabile: %s", exc)

    # 3. Env var
    env_key = os.environ.get("EODHD_API_KEY")
    if env_key:
        logger.info("API key: letta da variabile d'ambiente")
        return env_key.strip()

    # 4. File .env (senza dipendenza da python-dotenv per robustezza)
    dotenv_path = Path.cwd() / ".env"
    if dotenv_path.exists():
        for line in dotenv_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("EODHD_API_KEY="):
                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                if val:
                    logger.info("API key: letta da file .env")
                    return val

    raise EODHDAuthError(
        "EODHD API key non trovata. Imposta la chiave come:\n"
        "  - Colab Secret 'EODHD_API_KEY', oppure\n"
        "  - variabile d'ambiente EODHD_API_KEY, oppure\n"
        "  - riga 'EODHD_API_KEY=...' in un file .env nella cwd"
    )


# ==============================================================================
# EODHD CLIENT
# ==============================================================================
class EODHDClient:
    """Client sincrono per EOD Historical Data API.

    Parameters
    ----------
    api_key : str, optional
        Chiave API. Se None, viene risolta automaticamente (Colab / env / .env).
    base_url : str
        URL base delle API EODHD.
    rate_limit_per_min : int
        Numero massimo di chiamate al minuto (finestra scorrevole client-side).
    request_timeout : int
        Timeout in secondi per ogni chiamata HTTP.
    max_retries : int
        Numero massimo di retry su errori transitori (429, 5xx, timeout).
    """

    DEFAULT_BASE_URL = "https://eodhd.com/api"

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        rate_limit_per_min: int = 900,
        request_timeout: int = 30,
        max_retries: int = 4,
    ) -> None:
        self.api_key = resolve_api_key(api_key)
        self.base_url = base_url.rstrip("/")
        self.rate_limit_per_min = rate_limit_per_min
        self.request_timeout = request_timeout
        self.max_retries = max_retries

        # Stats tracking
        self.stats = UsageStats()

        # Rate limiting (sliding window)
        self._call_timestamps: deque[float] = deque(maxlen=rate_limit_per_min)

        # HTTP session con connection pooling
        self._session = requests.Session()
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=20)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)
        self._session.headers.update(
            {"User-Agent": "KriterionQuant-ValueScanner/0.1"}
        )

        logger.info(
            "EODHDClient inizializzato (rate_limit=%d/min, timeout=%ds)",
            rate_limit_per_min,
            request_timeout,
        )

    # --------------------------------------------------------------------------
    # Rate limiting interno
    # --------------------------------------------------------------------------
    def _wait_if_needed(self) -> None:
        """Blocca se siamo vicini al rate limit (sliding window)."""
        now = time.monotonic()
        # Rimuovi timestamp fuori dalla finestra di 60s
        while self._call_timestamps and now - self._call_timestamps[0] > 60:
            self._call_timestamps.popleft()

        if len(self._call_timestamps) >= self.rate_limit_per_min:
            # Aspetta che scorra via il timestamp più vecchio
            sleep_for = 60 - (now - self._call_timestamps[0]) + 0.05
            if sleep_for > 0:
                logger.warning(
                    "Rate limit raggiunto, pausa di %.2fs", sleep_for
                )
                time.sleep(sleep_for)

        self._call_timestamps.append(time.monotonic())

    # --------------------------------------------------------------------------
    # Metodo HTTP generico con retry
    # --------------------------------------------------------------------------
    @retry(
        retry=retry_if_exception_type(
            (EODHDRateLimitError, EODHDServerError, requests.Timeout, requests.ConnectionError)
        ),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1.5, min=1.5, max=30),
        reraise=True,
    )
    def _request(
        self,
        endpoint: str,
        endpoint_key: str,
        params: dict[str, Any] | None = None,
        as_json: bool = True,
    ) -> Any:
        """Esegue la chiamata HTTP con retry e tracking.

        Parameters
        ----------
        endpoint : str
            Path relativo dell'endpoint (es. "eod/AAPL.US").
        endpoint_key : str
            Chiave logica (in API_COST) per tracking crediti.
        params : dict, optional
            Query parameters.
        as_json : bool
            Se True, richiede JSON; altrimenti ritorna testo grezzo (CSV).
        """
        self._wait_if_needed()

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        params = dict(params or {})
        params.setdefault("api_token", self.api_key)
        if as_json:
            params.setdefault("fmt", "json")

        try:
            resp = self._session.get(
                url, params=params, timeout=self.request_timeout
            )
        except requests.Timeout:
            self.stats.record_error()
            logger.warning("Timeout su %s", endpoint)
            raise

        # Gestione status codes
        if resp.status_code == 200:
            self.stats.record(endpoint_key)
            return resp.json() if as_json else resp.text
        if resp.status_code == 401 or resp.status_code == 403:
            self.stats.record_error()
            raise EODHDAuthError(f"Autenticazione fallita: {resp.status_code}")
        if resp.status_code == 404:
            self.stats.record_error()
            raise EODHDNotFoundError(f"Risorsa non trovata: {endpoint}")
        if resp.status_code == 429:
            self.stats.record_error()
            raise EODHDRateLimitError(
                f"Rate limit EODHD superato (HTTP 429) su {endpoint}"
            )
        if 500 <= resp.status_code < 600:
            self.stats.record_error()
            raise EODHDServerError(
                f"Errore server EODHD {resp.status_code} su {endpoint}"
            )

        self.stats.record_error()
        raise EODHDError(
            f"Errore inatteso HTTP {resp.status_code} su {endpoint}: "
            f"{resp.text[:200]}"
        )

    # ==========================================================================
    # ENDPOINT: EOD Historical Prices
    # ==========================================================================
    def get_eod(
        self,
        ticker: str,
        from_date: str | None = None,
        to_date: str | None = None,
        period: str = "d",
    ) -> pd.DataFrame:
        """Scarica prezzi EOD storici per un singolo ticker.

        Parameters
        ----------
        ticker : str
            Ticker in formato EODHD (es. "AAPL.US", "VOW3.XETRA").
        from_date : str, optional
            Data inizio in formato "YYYY-MM-DD".
        to_date : str, optional
            Data fine in formato "YYYY-MM-DD".
        period : {"d", "w", "m"}
            Frequenza: giornaliera, settimanale, mensile.

        Returns
        -------
        pd.DataFrame
            Indicizzato per Date, colonne: Open, High, Low, Close, Adjusted_close, Volume.
        """
        params: dict[str, Any] = {"period": period}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        data = self._request(
            f"eod/{ticker}", endpoint_key="eod", params=params
        )
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
        df.columns = [c.lower() for c in df.columns]
        return df

    # ==========================================================================
    # ENDPOINT: Bulk EOD (tutti i ticker di un exchange per una data)
    # ==========================================================================
    def get_bulk_eod(
        self,
        exchange_code: str,
        date: str | None = None,
        symbols: Iterable[str] | None = None,
    ) -> pd.DataFrame:
        """Scarica tutti i prezzi EOD di un exchange per una data specifica.

        È la modalità più efficiente in crediti per aggiornare quotidianamente
        l'intero universo (costa ~100 credits per exchange).

        Parameters
        ----------
        exchange_code : str
            Codice EODHD dell'exchange (es. "US", "LSE", "XETRA").
        date : str, optional
            Data in formato "YYYY-MM-DD". Default: ultima disponibile.
        symbols : iterable of str, optional
            Se fornito, filtra la risposta ai soli ticker richiesti.

        Returns
        -------
        pd.DataFrame
            Colonne: code, exchange_short_name, date, open, high, low, close,
            adjusted_close, volume.
        """
        params: dict[str, Any] = {}
        if date:
            params["date"] = date
        if symbols:
            params["symbols"] = ",".join(symbols)

        data = self._request(
            f"eod-bulk-last-day/{exchange_code}",
            endpoint_key="eod_bulk",
            params=params,
        )
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        # Normalizza nomi colonne a lowercase: EODHD talvolta ritorna
        # `Adjusted_close` con la A maiuscola → `_normalize_bulk_df` cerca
        # `adjusted_close` lowercase e silenziosamente imposta NULL.
        # Questo causava metriche tecniche calcolate su `close` non-adjusted.
        df.columns = [c.lower() for c in df.columns]
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df

    # ==========================================================================
    # ENDPOINT: Fundamentals (completi per singolo ticker)
    # ==========================================================================
    def get_fundamentals(self, ticker: str) -> dict[str, Any]:
        """Scarica tutti i fundamentals completi per un ticker.

        Restituisce un dizionario JSON strutturato con:
          - General: info aziendali, settore, industria, descrizione
          - Highlights: KPI di sintesi (PE, EPS, MarketCap, ecc.)
          - Valuation: PE, EV, PEG, P/Sales, P/Book, ecc.
          - SharesStats: shares outstanding, float, insider/institutional ownership
          - Technicals: Beta, 52W high/low, SMA50/200
          - SplitsDividends: storico split e dividendi
          - AnalystRatings: consensus rating, target price, numero analisti
          - Holders: insider e institutional holders
          - InsiderTransactions: transazioni insider
          - ESGScores: scores ESG
          - outstandingShares: storico shares outstanding
          - Earnings: History, Trend, Annual
          - Financials: Balance_Sheet, Cash_Flow, Income_Statement (quarterly + yearly)

        Costo: 10 credits per chiamata.
        """
        return self._request(
            f"fundamentals/{ticker}", endpoint_key="fundamentals"
        )

    # ==========================================================================
    # ENDPOINT: Bulk Fundamentals (per exchange)
    # ==========================================================================
    def get_bulk_fundamentals(
        self,
        exchange_code: str,
        offset: int = 0,
        limit: int = 500,
        symbols: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        """Scarica fundamentals in bulk per un exchange.

        Utile per il refresh settimanale. La chiamata restituisce fino a `limit`
        ticker per volta; usare offset per paginare.

        Parameters
        ----------
        exchange_code : str
            Codice exchange EODHD.
        offset : int
            Offset di paginazione.
        limit : int
            Numero massimo di ticker per chiamata (max 500 per EODHD).
        symbols : iterable, optional
            Lista esplicita di ticker da recuperare (override di offset/limit).
        """
        params: dict[str, Any] = {"offset": offset, "limit": limit}
        if symbols:
            params["symbols"] = ",".join(symbols)

        return self._request(
            f"bulk-fundamentals/{exchange_code}",
            endpoint_key="fundamentals_bulk",
            params=params,
        )

    # ==========================================================================
    # ENDPOINT: Exchange Symbols
    # ==========================================================================
    def get_exchange_symbols(
        self,
        exchange_code: str,
        include_delisted: bool = False,
        types: Iterable[str] | None = None,
    ) -> pd.DataFrame:
        """Lista tutti i ticker quotati su un exchange.

        Parameters
        ----------
        exchange_code : str
            Es. "US", "LSE", "XETRA".
        include_delisted : bool
            Se True, include anche ticker delisted.
        types : iterable, optional
            Filtro per tipo (es. ["Common Stock", "ADR"]).

        Returns
        -------
        pd.DataFrame
            Colonne: Code, Name, Country, Exchange, Currency, Type, Isin.
        """
        params: dict[str, Any] = {}
        if include_delisted:
            params["delisted"] = 1

        data = self._request(
            f"exchange-symbol-list/{exchange_code}",
            endpoint_key="exchange_symbols",
            params=params,
        )
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        if types and "Type" in df.columns:
            df = df[df["Type"].isin(list(types))]
        return df.reset_index(drop=True)

    # ==========================================================================
    # ENDPOINT: Screener
    # ==========================================================================
    def screener(
        self,
        filters: list[list[Any]] | None = None,
        signals: list[str] | None = None,
        sort: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Esegue query sul screener EODHD.

        Parameters
        ----------
        filters : list of lists
            Filtri in formato [[field, operation, value], ...].
            Es: [["market_capitalization", ">", 2000000000], ["exchange", "=", "us"]]
        signals : list of str, optional
            Signal preset (es. "200d_new_hi", "bookvalue_neg").
        sort : str, optional
            Campo per ordinamento (es. "market_capitalization.desc").
        limit : int
            Numero risultati (max 100 per EODHD).
        offset : int
            Paginazione.

        Returns
        -------
        list of dict
            Lista ticker con metadati fondamentali.
        """
        import json as _json

        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if filters:
            params["filters"] = _json.dumps(filters)
        if signals:
            params["signals"] = ",".join(signals)
        if sort:
            params["sort"] = sort

        result = self._request(
            "screener", endpoint_key="screener", params=params
        )
        return result.get("data", []) if isinstance(result, dict) else result

    # ==========================================================================
    # ENDPOINT: Search
    # ==========================================================================
    def search(self, query: str, limit: int = 15) -> list[dict[str, Any]]:
        """Ricerca ticker per nome o simbolo."""
        return self._request(
            f"search/{query}",
            endpoint_key="search",
            params={"limit": limit},
        )

    # ==========================================================================
    # ENDPOINT: Live Quote (real-time o delayed)
    # ==========================================================================
    def get_live_quote(self, ticker: str) -> dict[str, Any]:
        """Quotazione real-time (o delayed a seconda del piano)."""
        return self._request(
            f"real-time/{ticker}", endpoint_key="live"
        )

    # ==========================================================================
    # ENDPOINT: Dividends
    # ==========================================================================
    def get_dividends(
        self, ticker: str, from_date: str | None = None
    ) -> pd.DataFrame:
        """Storico dividendi per un ticker."""
        params: dict[str, Any] = {}
        if from_date:
            params["from"] = from_date
        data = self._request(
            f"div/{ticker}", endpoint_key="dividends", params=params
        )
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df

    # ==========================================================================
    # ENDPOINT: Splits
    # ==========================================================================
    def get_splits(
        self, ticker: str, from_date: str | None = None
    ) -> pd.DataFrame:
        """Storico split azionari per un ticker."""
        params: dict[str, Any] = {}
        if from_date:
            params["from"] = from_date
        data = self._request(
            f"splits/{ticker}", endpoint_key="splits", params=params
        )
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df

    # ==========================================================================
    # UTILITIES
    # ==========================================================================
    def get_usage_stats(self) -> dict[str, Any]:
        """Ritorna lo snapshot delle statistiche di utilizzo della sessione."""
        return self.stats.as_dict()

    def log_usage(self) -> None:
        """Stampa nel log un riepilogo dell'utilizzo."""
        s = self.stats
        logger.info(
            "USAGE · calls=%d · credits=%d · errors=%d · breakdown=%s",
            s.calls_total,
            s.credits_total,
            s.errors_total,
            s.credits_by_endpoint,
        )

    def close(self) -> None:
        """Chiude la sessione HTTP sottostante."""
        self._session.close()

    def __enter__(self) -> EODHDClient:
        return self

    def __exit__(self, *_exc_info: Any) -> None:
        self.log_usage()
        self.close()
