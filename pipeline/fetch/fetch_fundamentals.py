"""Fetch fundamentals — refresh settimanale bulk + parsing in DB.

Due modalità:

1. **Bulk per exchange** (default, refresh settimanale):
   Per ciascun exchange configurato, chiama ``bulk-fundamentals/{exchange}``
   con paginazione (offset/limit 500). Costa ~10 credits per ticker coperto.

2. **Singolo ticker** (``--ticker TK.US``):
   Usa ``fundamentals/{ticker}`` per un aggiornamento puntuale.

Il payload JSON raw viene:
  - appiattito in `fundamentals_snapshot` (un record per ticker, overwrite)
  - esploso in `financials_history` (righe per Income/Balance/Cashflow ×
    annual/quarterly, con copia del JSON raw per futuri re-parse)

I ticker non presenti in ``universe`` vengono scartati (safety).

Uso CLI
-------
# Refresh settimanale tutti exchange (default)
python -m pipeline.fetch.fetch_fundamentals

# Solo un exchange
python -m pipeline.fetch.fetch_fundamentals --exchange LSE

# Un singolo ticker
python -m pipeline.fetch.fetch_fundamentals --ticker AAPL.US

# Dry-run (nessuna scrittura)
python -m pipeline.fetch.fetch_fundamentals --exchange MI --max-tickers 20 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

import pandas as pd

from pipeline.config import get_all_exchanges, load_settings
from pipeline.fetch.eodhd_client import EODHDClient, EODHDError, EODHDNotFoundError
from pipeline.storage.db import (
    get_universe,
    upsert_financials_history,
    upsert_fundamentals_snapshot,
)

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logger = logging.getLogger("kq.fetch.fundamentals")
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
# Risultato aggregato
# ------------------------------------------------------------------------------
@dataclass
class FetchFundamentalsResult:
    mode: str                                       # "bulk" | "single"
    exchanges_processed: list[str] = field(default_factory=list)
    tickers_processed: int = 0
    snapshots_written: int = 0
    history_rows_written: int = 0
    credits_estimated: int = 0
    errors: list[str] = field(default_factory=list)
    started_at: str = ""
    ended_at: str = ""

    def summary(self) -> str:
        return (
            f"FetchFundamentalsResult(mode={self.mode}, "
            f"exchanges={len(self.exchanges_processed)}, "
            f"tickers={self.tickers_processed}, "
            f"snapshots={self.snapshots_written}, "
            f"history_rows={self.history_rows_written}, "
            f"credits≈{self.credits_estimated}, errors={len(self.errors)})"
        )


# ------------------------------------------------------------------------------
# Helpers parsing — numeri da EODHD (spesso stringhe o null)
# ------------------------------------------------------------------------------
def _to_float(v: Any) -> float | None:
    """Convert EODHD value → float/None. Robusto a stringhe, None, '', numerici."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v: Any) -> int | None:
    """Convert EODHD value → int/None."""
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _to_date_str(v: Any) -> str | None:
    """Restituisce stringa YYYY-MM-DD, se v parse-able, altrimenti None."""
    if v is None or v == "":
        return None
    s = str(v)[:10]
    # pattern semplice di sanity check
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    return None


def _safe_get(d: dict | None, *keys: str, default: Any = None) -> Any:
    """d.get(k1, {}).get(k2, ...).get(kn, default) senza eccezioni."""
    cur: Any = d or {}
    for k in keys[:-1]:
        cur = cur.get(k, {}) if isinstance(cur, dict) else {}
    if isinstance(cur, dict):
        return cur.get(keys[-1], default)
    return default


# ------------------------------------------------------------------------------
# Parser: payload EODHD → snapshot row
# ------------------------------------------------------------------------------
def parse_snapshot(ticker: str, payload: dict) -> dict | None:
    """Estrae un record ``fundamentals_snapshot`` dal payload completo.

    Returns
    -------
    dict or None
        Dizionario pronto per ``upsert_fundamentals_snapshot``. ``None`` se il
        payload è vuoto/malformato.
    """
    if not isinstance(payload, dict) or not payload:
        return None

    general = payload.get("General", {}) or {}
    hl = payload.get("Highlights", {}) or {}
    val = payload.get("Valuation", {}) or {}
    shares = payload.get("SharesStats", {}) or {}
    tech = payload.get("Technicals", {}) or {}

    # Financials — ultimo quarter
    fin = payload.get("Financials", {}) or {}
    bs_q = fin.get("Balance_Sheet", {}).get("quarterly", {}) or {}
    is_q = fin.get("Income_Statement", {}).get("quarterly", {}) or {}
    cf_q = fin.get("Cash_Flow", {}).get("quarterly", {}) or {}

    last_bs = _latest_period(bs_q)
    last_is = _latest_period(is_q)
    last_cf = _latest_period(cf_q)

    # Somme per campi derivati
    total_cash = _sum_optional(
        _to_float(last_bs.get("cash")),
        _to_float(last_bs.get("shortTermInvestments")),
    )
    total_debt = _sum_optional(
        _to_float(last_bs.get("longTermDebt")),
        _to_float(last_bs.get("shortLongTermDebt")),
    )
    net_debt = None
    if total_cash is not None or total_debt is not None:
        net_debt = (total_debt or 0.0) - (total_cash or 0.0)

    # Earnings TTM aggregati dai 4 trimestri
    ebitda_ttm = _ttm_sum(is_q, "ebitda")
    ebit_ttm = _ttm_sum(is_q, "ebit")
    revenue_ttm = _ttm_sum(is_q, "totalRevenue")
    interest_ttm = _ttm_sum(is_q, "interestExpense")

    # FCF TTM = operating cashflow − capex (convenzione: capex in valore assoluto negativo già)
    fcf_ttm = _fcf_ttm(cf_q)

    row = {
        "ticker": ticker,
        # Valuation
        "pe_ttm": _to_float(hl.get("PERatio")),
        "forward_pe": _to_float(hl.get("ForwardPE") or val.get("ForwardPE")),
        "peg": _to_float(hl.get("PEGRatio")),
        "price_to_sales_ttm": _to_float(val.get("PriceSalesTTM")),
        "price_to_book": _to_float(val.get("PriceBookMRQ")),
        "ev_to_ebitda": _to_float(val.get("EnterpriseValueEbitda")),
        "ev_to_revenue": _to_float(val.get("EnterpriseValueRevenue")),
        "earnings_yield": _to_float(hl.get("EarningsYield")),
        "dividend_yield": _to_float(hl.get("DividendYield")),
        # Profitability
        "roe": _to_float(hl.get("ReturnOnEquityTTM")),
        "roa": _to_float(hl.get("ReturnOnAssetsTTM")),
        "roic": _to_float(hl.get("ReturnOnInvestedCapitalTTM")),
        "gross_margin": _to_float(hl.get("GrossProfitTTM")),
        "operating_margin": _to_float(hl.get("OperatingMarginTTM")),
        "profit_margin": _to_float(hl.get("ProfitMargin")),
        # Size
        "market_cap_usd": _to_float(hl.get("MarketCapitalization")),
        "shares_outstanding": _to_float(shares.get("SharesOutstanding")),
        "beta": _to_float(tech.get("Beta")),
        # Growth
        "revenue_growth_yoy": _to_float(hl.get("QuarterlyRevenueGrowthYOY")),
        "eps_growth_yoy": _to_float(hl.get("QuarterlyEarningsGrowthYOY")),
        "earnings_growth_next_year": _to_float(hl.get("EPSEstimateNextYear")),
        # Payout
        "dividend_per_share": _to_float(hl.get("DividendShare")),
        "payout_ratio": _to_float(hl.get("PayoutRatio")),
        # Schedule
        "next_earnings_date": _to_date_str(hl.get("NextEarningsDate")),
        "most_recent_quarter": _to_date_str(hl.get("MostRecentQuarter")),
        # Balance sheet sintesi
        "total_cash": total_cash,
        "total_debt": total_debt,
        "net_debt": net_debt,
        "total_revenue_ttm": revenue_ttm,
        "ebitda_ttm": ebitda_ttm,
        "ebit_ttm": ebit_ttm,
        "free_cash_flow_ttm": fcf_ttm,
        "interest_expense_ttm": interest_ttm,
        "total_current_assets": _to_float(last_bs.get("totalCurrentAssets")),
        "total_current_liabilities": _to_float(last_bs.get("totalCurrentLiabilities")),
        "inventory": _to_float(last_bs.get("inventory")),
        "accounts_receivable": _to_float(last_bs.get("netReceivables")),
        # Metadata
        "currency_reporting": general.get("CurrencyCode"),
    }
    return row


def _latest_period(periods_dict: dict) -> dict:
    """Ritorna il dict del periodo più recente in un blocco Financials/quarterly o /yearly."""
    if not isinstance(periods_dict, dict) or not periods_dict:
        return {}
    keys = sorted(periods_dict.keys(), reverse=True)
    return periods_dict.get(keys[0], {}) or {}


def _ttm_sum(quarterly_dict: dict, field_name: str) -> float | None:
    """Somma TTM (ultimi 4 trimestri) di un campo, con None se incompleto."""
    if not isinstance(quarterly_dict, dict) or not quarterly_dict:
        return None
    keys = sorted(quarterly_dict.keys(), reverse=True)[:4]
    if len(keys) < 4:
        return None
    vals = [_to_float(quarterly_dict[k].get(field_name)) for k in keys]
    if any(v is None for v in vals):
        return None
    return float(sum(vals))


def _fcf_ttm(cashflow_q: dict) -> float | None:
    """Free Cash Flow TTM = OCF TTM − CapEx TTM (capex è spesso negativo in EODHD)."""
    if not isinstance(cashflow_q, dict) or not cashflow_q:
        return None
    keys = sorted(cashflow_q.keys(), reverse=True)[:4]
    if len(keys) < 4:
        return None
    ocf = 0.0
    capex = 0.0
    for k in keys:
        row = cashflow_q[k]
        ocf_v = _to_float(row.get("totalCashFromOperatingActivities"))
        capex_v = _to_float(row.get("capitalExpenditures"))
        if ocf_v is None or capex_v is None:
            return None
        ocf += ocf_v
        capex += capex_v
    # capex in EODHD è già negativo (uscita); FCF = OCF + capex
    return ocf + capex


def _sum_optional(*vals: float | None) -> float | None:
    """Sum ignorando None, returning None se tutti None."""
    filtered = [v for v in vals if v is not None]
    if not filtered:
        return None
    return float(sum(filtered))


# ------------------------------------------------------------------------------
# Parser: payload → financials_history rows (income/balance/cashflow × freq)
# ------------------------------------------------------------------------------
_INCOME_MAP = {
    "totalRevenue": "total_revenue",
    "grossProfit": "gross_profit",
    "operatingIncome": "operating_income",
    "ebit": "ebit",
    "ebitda": "ebitda",
    "netIncome": "net_income",
    "basicEPS": "eps_basic",
    "dilutedEPS": "eps_diluted",
    "interestExpense": "interest_expense",
    "incomeTaxExpense": "income_tax_expense",
}

_BALANCE_MAP = {
    "cash": "cash_and_equivalents",
    "shortTermInvestments": "short_term_investments",
    "totalCurrentAssets": "total_current_assets",
    "totalAssets": "total_assets",
    "totalCurrentLiabilities": "total_current_liabilities",
    "longTermDebt": "long_term_debt",
    "shortLongTermDebt": "short_term_debt",
    "totalLiab": "total_liabilities",
    "totalStockholderEquity": "total_equity",
    "inventory": "inventory",
    "netReceivables": "accounts_receivable",
    "retainedEarnings": "retained_earnings",
}

_CASHFLOW_MAP = {
    "totalCashFromOperatingActivities": "operating_cashflow",
    "capitalExpenditures": "capital_expenditure",
    "freeCashFlow": "free_cash_flow",
    "dividendsPaid": "dividends_paid",
    "salePurchaseOfStock": "share_repurchases",
}


def parse_history_rows(
    ticker: str,
    payload: dict,
    retention_years: int = 10,
) -> list[dict]:
    """Esplode il payload in una lista di righe per ``financials_history``.

    Genera fino a 6 categorie:
    income/balance/cashflow × annual/quarterly.

    Parameters
    ----------
    retention_years : int
        Limita la storia agli ultimi N anni (default 10).
    """
    if not isinstance(payload, dict) or not payload:
        return []

    fin = payload.get("Financials", {}) or {}
    currency = _safe_get(fin, "Income_Statement", "currency_symbol")

    cutoff = f"{datetime.utcnow().year - retention_years}-01-01"
    rows: list[dict] = []

    # Per ciascun statement_type × freq
    blocks: list[tuple[str, str, dict, dict]] = [
        ("income", "annual",
         fin.get("Income_Statement", {}).get("yearly", {}) or {}, _INCOME_MAP),
        ("income", "quarterly",
         fin.get("Income_Statement", {}).get("quarterly", {}) or {}, _INCOME_MAP),
        ("balance", "annual",
         fin.get("Balance_Sheet", {}).get("yearly", {}) or {}, _BALANCE_MAP),
        ("balance", "quarterly",
         fin.get("Balance_Sheet", {}).get("quarterly", {}) or {}, _BALANCE_MAP),
        ("cashflow", "annual",
         fin.get("Cash_Flow", {}).get("yearly", {}) or {}, _CASHFLOW_MAP),
        ("cashflow", "quarterly",
         fin.get("Cash_Flow", {}).get("quarterly", {}) or {}, _CASHFLOW_MAP),
    ]

    for stmt_type, freq, data, colmap in blocks:
        if not data:
            continue
        for period_key, period_data in data.items():
            period_end = _to_date_str(period_key)
            if not period_end or period_end < cutoff:
                continue
            if not isinstance(period_data, dict):
                continue

            row = {
                "ticker": ticker,
                "period_end": period_end,
                "statement_type": stmt_type,
                "freq": freq,
                "currency_symbol": currency,
                "raw_json": json.dumps(period_data, separators=(",", ":"),
                                        ensure_ascii=False),
            }
            for src_key, dst_col in colmap.items():
                row[dst_col] = _to_float(period_data.get(src_key))
            rows.append(row)

    return rows


# ------------------------------------------------------------------------------
# Core pipelines
# ------------------------------------------------------------------------------
def _process_payloads(
    payloads: dict[str, dict],
    valid_tickers: set[str],
    dry_run: bool,
    retention_years: int,
) -> tuple[int, int, list[str]]:
    """Parsa e scrive su DB. Ritorna (n_snapshots, n_history, errors)."""
    snapshots: list[dict] = []
    history: list[dict] = []
    errors: list[str] = []

    for tk, payload in payloads.items():
        if tk not in valid_tickers:
            logger.debug("Skip %s: non in universe", tk)
            continue
        try:
            snap = parse_snapshot(tk, payload)
            if snap:
                snapshots.append(snap)
            rows = parse_history_rows(tk, payload, retention_years=retention_years)
            history.extend(rows)
        except Exception as e:
            msg = f"Parse {tk}: {type(e).__name__}: {e}"
            logger.warning(msg)
            errors.append(msg)

    if dry_run:
        logger.info(
            "[DRY-RUN] Snapshots parsed=%d, history rows parsed=%d",
            len(snapshots), len(history),
        )
        return len(snapshots), len(history), errors

    n_snap = upsert_fundamentals_snapshot(snapshots) if snapshots else 0
    n_hist = upsert_financials_history(history) if history else 0
    return n_snap, n_hist, errors


def fetch_fundamentals_bulk(
    exchange_codes: list[str] | None = None,
    max_tickers: int | None = None,
    page_limit: int = 500,
    client: EODHDClient | None = None,
    dry_run: bool = False,
) -> FetchFundamentalsResult:
    """Refresh fundamentals via endpoint bulk paginato per exchange.

    Parameters
    ----------
    exchange_codes : list of str, optional
        Default: tutti da settings.yaml.
    max_tickers : int, optional
        Cap totale (per exchange) di ticker da processare (test safety).
    page_limit : int
        Ticker per pagina (max 500 per EODHD).
    client : EODHDClient, optional
    dry_run : bool
    """
    result = FetchFundamentalsResult(mode="bulk")
    result.started_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    if exchange_codes is None:
        exchange_codes = [ex["code"] for ex in get_all_exchanges()]

    universe_df = get_universe(active_only=True)
    if universe_df.empty:
        logger.warning("Universe vuoto — esegui prima fetch_universe + load-universe.")
        return result
    by_exchange: dict[str, set[str]] = {
        xc: set(universe_df.loc[universe_df["exchange_code"] == xc, "ticker"])
        for xc in exchange_codes
    }

    # Budget guardrail
    settings = load_settings()
    daily_budget = int(settings["eodhd"].get("daily_budget_credits", 100_000))
    alert_pct = float(settings["eodhd"].get("alert_budget_pct", 0.8))
    retention = int(settings["storage"].get("retention_years", 10))

    owned_client = False
    if client is None:
        client = EODHDClient()
        owned_client = True

    try:
        for xc in exchange_codes:
            valid = by_exchange.get(xc, set())
            if not valid:
                logger.info("Exchange %s: nessun ticker nell'universo, skip.", xc)
                continue

            logger.info(
                "── Bulk fundamentals exchange=%s (universe=%d ticker) ──",
                xc, len(valid),
            )
            processed_this_exchange = 0
            offset = 0

            while True:
                try:
                    payload = client.get_bulk_fundamentals(
                        xc, offset=offset, limit=page_limit,
                    )
                except EODHDError as e:
                    msg = f"Bulk fundamentals {xc} offset={offset}: {e}"
                    logger.error(msg)
                    result.errors.append(msg)
                    break

                if not payload:
                    logger.info("Pagina vuota, fine exchange %s.", xc)
                    break

                # Normalizza risposta: alcuni exchange EODHD restituiscono
                # list anziché dict. Mappiamo entrambi.
                payloads_map: dict[str, dict] = {}
                if isinstance(payload, list):
                    for item in payload:
                        code = _safe_get(item, "General", "Code")
                        if code:
                            payloads_map[f"{code}.{xc}"] = item
                elif isinstance(payload, dict):
                    for k, v in payload.items():
                        # Key può essere "AAPL.US" o solo "AAPL"
                        tk = k if "." in k else f"{k}.{xc}"
                        payloads_map[tk] = v

                # Scarta i non-universo
                filtered = {
                    tk: p for tk, p in payloads_map.items() if tk in valid
                }
                logger.info(
                    "  Page offset=%d: ricevuti=%d, in universo=%d",
                    offset, len(payloads_map), len(filtered),
                )

                n_snap, n_hist, errs = _process_payloads(
                    filtered, valid, dry_run, retention,
                )
                result.snapshots_written += n_snap
                result.history_rows_written += n_hist
                result.tickers_processed += len(filtered)
                result.errors.extend(errs)
                processed_this_exchange += len(filtered)

                # Condizioni di uscita
                if len(payloads_map) < page_limit:
                    # Ultima pagina di questo exchange
                    break
                if max_tickers and processed_this_exchange >= max_tickers:
                    logger.info(
                        "max-tickers=%d raggiunto per %s, stop.",
                        max_tickers, xc,
                    )
                    break

                # Budget guard
                stats = client.get_usage_stats()
                if stats.credits_total >= daily_budget * alert_pct:
                    logger.warning(
                        "Budget alert: %d/%d crediti. Stop.",
                        stats.credits_total, daily_budget,
                    )
                    break

                offset += page_limit

            result.exchanges_processed.append(xc)

    finally:
        if owned_client:
            stats = client.get_usage_stats()
            result.credits_estimated = stats.credits_total
            logger.info(
                "Usage fundamentals: calls=%d, credits=%d, errors=%d",
                stats.calls_total, stats.credits_total, stats.errors_total,
            )

    result.ended_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info("── %s", result.summary())
    return result


def fetch_fundamentals_single(
    ticker: str,
    client: EODHDClient | None = None,
    dry_run: bool = False,
) -> FetchFundamentalsResult:
    """Refresh di un singolo ticker via endpoint ``fundamentals/{ticker}``."""
    result = FetchFundamentalsResult(mode="single")
    result.started_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    settings = load_settings()
    retention = int(settings["storage"].get("retention_years", 10))

    owned_client = False
    if client is None:
        client = EODHDClient()
        owned_client = True

    try:
        try:
            payload = client.get_fundamentals(ticker)
        except EODHDNotFoundError as e:
            msg = f"Ticker non trovato: {ticker}"
            logger.error(msg)
            result.errors.append(msg)
            return result
        except EODHDError as e:
            msg = f"Errore fetch {ticker}: {e}"
            logger.error(msg)
            result.errors.append(msg)
            return result

        # Scarto safety
        universe_df = get_universe(active_only=False)
        valid = set(universe_df["ticker"].tolist()) if not universe_df.empty else set()
        if valid and ticker not in valid:
            logger.warning(
                "Ticker %s non presente in universe (procedo comunque per test).",
                ticker,
            )
            valid = {ticker}
        else:
            valid = valid or {ticker}

        n_snap, n_hist, errs = _process_payloads(
            {ticker: payload}, valid, dry_run, retention,
        )
        result.snapshots_written = n_snap
        result.history_rows_written = n_hist
        result.errors.extend(errs)
        result.tickers_processed = 1

    finally:
        if owned_client:
            stats = client.get_usage_stats()
            result.credits_estimated = stats.credits_total

    result.ended_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info("── %s", result.summary())
    return result


# ------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------
def _main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch fundamentals: bulk per exchange (default) o singolo ticker."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Esempi:\n"
            "  python -m pipeline.fetch.fetch_fundamentals\n"
            "  python -m pipeline.fetch.fetch_fundamentals --exchange LSE\n"
            "  python -m pipeline.fetch.fetch_fundamentals --ticker AAPL.US\n"
            "  python -m pipeline.fetch.fetch_fundamentals --exchange MI --max-tickers 20 --dry-run\n"
        ),
    )
    parser.add_argument("--exchange", help="Codice exchange EODHD (es. US, LSE).")
    parser.add_argument("--ticker", help="Un singolo ticker in formato EODHD.")
    parser.add_argument(
        "--max-tickers",
        type=int,
        default=None,
        help="Cap numero ticker processati per exchange (safety).",
    )
    parser.add_argument(
        "--page-limit",
        type=int,
        default=500,
        help="Ticker per chiamata bulk (default 500, max EODHD).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Non scrive nel DB (solo fetch + parse + log).",
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

    try:
        if args.ticker:
            res = fetch_fundamentals_single(
                ticker=args.ticker, dry_run=args.dry_run,
            )
        else:
            exchanges = [args.exchange] if args.exchange else None
            res = fetch_fundamentals_bulk(
                exchange_codes=exchanges,
                max_tickers=args.max_tickers,
                page_limit=args.page_limit,
                dry_run=args.dry_run,
            )
    except KeyboardInterrupt:
        logger.warning("Interrotto dall'utente.")
        return 130
    except Exception as e:
        logger.exception("Errore non gestito: %s", e)
        return 1

    print()
    print("=" * 70)
    print(f"  FETCH FUNDAMENTALS RESULT ({res.mode.upper()})")
    print("=" * 70)
    print(f"  Started:            {res.started_at}")
    print(f"  Ended:              {res.ended_at}")
    print(f"  Exchanges:          {', '.join(res.exchanges_processed) or '-'}")
    print(f"  Tickers processed:  {res.tickers_processed}")
    print(f"  Snapshots written:  {res.snapshots_written}")
    print(f"  History rows:       {res.history_rows_written}")
    print(f"  Credits estimated:  {res.credits_estimated}")
    print(f"  Errors:             {len(res.errors)}")
    if res.errors and args.verbose:
        for e in res.errors[:10]:
            print(f"    · {e}")
    print("=" * 70)

    return 0 if not res.errors else 2


if __name__ == "__main__":
    sys.exit(_main())
