"""Composite scores: Altman Z, Piotroski F, Beneish M + KQ Value Score.

Input: ``fundamentals_snapshot`` + ``financials_history`` + metriche già
calcolate dai moduli valuation/cash_debt/profitability/technical.

Tutti gli score sono best-effort: se mancano componenti critiche, il valore
è ``None``. La pipeline screener_build userà questi valori per rank/filter.

Formule implementate
--------------------

**Altman Z-Score** (companies manufacturing — standard):
    Z = 1.2·A + 1.4·B + 3.3·C + 0.6·D + 1.0·E
dove
    A = (Current Assets − Current Liabilities) / Total Assets
    B = Retained Earnings / Total Assets
    C = EBIT / Total Assets
    D = Market Cap / Total Liabilities
    E = Revenue / Total Assets

**Piotroski F-Score** (0-9):
    Signal profitability (4):
      1. Net income positivo (ultimo annuale)
      2. ROA positivo
      3. OCF positivo
      4. OCF > Net income (quality of earnings)
    Leverage/liquidity (3):
      5. LT debt ratio in diminuzione YoY
      6. Current ratio in aumento YoY
      7. Shares outstanding non aumentate
    Efficiency (2):
      8. Gross margin in aumento YoY
      9. Asset turnover in aumento YoY

**Beneish M-Score** (detection di earnings manipulation):
    M = -4.84 + 0.92·DSRI + 0.528·GMI + 0.404·AQI + 0.892·SGI
        + 0.115·DEPI − 0.172·SGAI + 4.679·TATA − 0.327·LVGI
dove ogni indice compara il periodo corrente a quello precedente.
Se M > -2.22 → probabile manipolazione.

**KQ Value Score** (0-100):
    Composito proprietary che combina:
      - 40% valuation (PE, PEG, EV/EBITDA, PE percentile 5Y)
      - 25% qualità (ROIC, margini, Piotroski F)
      - 20% solidità (Altman Z, coverage, current ratio)
      - 15% momentum (RS 126d, distance da SMA200)
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from pipeline.config import load_settings
from pipeline.storage.db import get_connection

logger = logging.getLogger("kq.compute.scores")
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
def _f(v: Any) -> float | None:
    if v is None:
        return None
    try:
        import math
        x = float(v)
        if math.isnan(x):
            return None
        return x
    except (TypeError, ValueError):
        return None


def _safe_div(n: float | None, d: float | None) -> float | None:
    if n is None or d is None:
        return None
    try:
        if d == 0:
            return None
        return float(n) / float(d)
    except (TypeError, ValueError):
        return None


# ------------------------------------------------------------------------------
# Loaders
# ------------------------------------------------------------------------------
def _load_fin_annual(db_path=None) -> pd.DataFrame:
    """Carica gli ultimi 3 periodi annuali di income+balance+cashflow."""
    with get_connection(db_path) as conn:
        df = pd.read_sql_query(
            "SELECT * FROM financials_history WHERE freq='annual'", conn,
        )
    if df.empty:
        return df
    # per ciascun ticker × statement_type, ordina per period_end desc e prendi 3
    df = df.sort_values(["ticker", "statement_type", "period_end"],
                         ascending=[True, True, False])
    df = df.groupby(["ticker", "statement_type"]).head(3)
    return df


# ------------------------------------------------------------------------------
# Altman Z
# ------------------------------------------------------------------------------
def compute_altman_z(snap: dict[str, Any],
                      bal_latest: dict[str, Any] | None,
                      inc_latest: dict[str, Any] | None) -> float | None:
    """Altman Z per manufacturer (versione standard a 5 fattori)."""
    if not bal_latest:
        return None
    total_assets = _f(bal_latest.get("total_assets"))
    if total_assets is None or total_assets <= 0:
        return None

    wc = None
    ca = _f(bal_latest.get("total_current_assets"))
    cl = _f(bal_latest.get("total_current_liabilities"))
    if ca is not None and cl is not None:
        wc = ca - cl

    retained = _f(bal_latest.get("retained_earnings"))
    total_liab = _f(bal_latest.get("total_liabilities"))

    ebit = None
    revenue = None
    if inc_latest:
        ebit = _f(inc_latest.get("ebit"))
        revenue = _f(inc_latest.get("total_revenue"))

    mcap = _f(snap.get("market_cap_usd"))

    A = _safe_div(wc, total_assets)
    B = _safe_div(retained, total_assets)
    C = _safe_div(ebit, total_assets)
    D = _safe_div(mcap, total_liab)
    E = _safe_div(revenue, total_assets)

    comps = [A, B, C, D, E]
    if all(v is None for v in comps):
        return None
    # Tratta i None mancanti come 0 (soft) per non perdere tutti i ticker se
    # manca un singolo campo. Nota: riduce la robustezza del Z.
    A = A if A is not None else 0.0
    B = B if B is not None else 0.0
    C = C if C is not None else 0.0
    D = D if D is not None else 0.0
    E = E if E is not None else 0.0

    return float(1.2 * A + 1.4 * B + 3.3 * C + 0.6 * D + 1.0 * E)


# ------------------------------------------------------------------------------
# Piotroski F
# ------------------------------------------------------------------------------
def compute_piotroski_f(
    bal_annual: list[dict],
    inc_annual: list[dict],
    cf_annual: list[dict],
    snap: dict[str, Any],
) -> int | None:
    """Piotroski F Score 0-9. Richiede 2 anni fiscali completi."""
    if len(bal_annual) < 2 or len(inc_annual) < 2 or len(cf_annual) < 1:
        return None
    # Assume bal_annual[0] = anno più recente, [1] = precedente
    bal_cur, bal_prev = bal_annual[0], bal_annual[1]
    inc_cur, inc_prev = inc_annual[0], inc_annual[1]
    cf_cur = cf_annual[0]

    score = 0

    # 1. Net income positivo
    ni = _f(inc_cur.get("net_income"))
    if ni is not None and ni > 0:
        score += 1

    # 2. ROA positivo
    ta = _f(bal_cur.get("total_assets"))
    if ni is not None and ta is not None and ta > 0 and (ni / ta) > 0:
        score += 1

    # 3. OCF positivo
    ocf = _f(cf_cur.get("operating_cashflow"))
    if ocf is not None and ocf > 0:
        score += 1

    # 4. OCF > NI
    if ocf is not None and ni is not None and ocf > ni:
        score += 1

    # 5. LT debt / Total Assets in diminuzione YoY
    ltd_cur = _f(bal_cur.get("long_term_debt"))
    ltd_prev = _f(bal_prev.get("long_term_debt"))
    ta_prev = _f(bal_prev.get("total_assets"))
    if (ltd_cur is not None and ltd_prev is not None and
            ta is not None and ta_prev is not None and
            ta > 0 and ta_prev > 0):
        if (ltd_cur / ta) < (ltd_prev / ta_prev):
            score += 1

    # 6. Current ratio in aumento YoY
    ca_cur = _f(bal_cur.get("total_current_assets"))
    cl_cur = _f(bal_cur.get("total_current_liabilities"))
    ca_prev = _f(bal_prev.get("total_current_assets"))
    cl_prev = _f(bal_prev.get("total_current_liabilities"))
    if (ca_cur and cl_cur and ca_prev and cl_prev and cl_cur > 0 and cl_prev > 0):
        if (ca_cur / cl_cur) > (ca_prev / cl_prev):
            score += 1

    # 7. Shares outstanding non aumentate: usa snapshot vs placeholder
    # (EODHD non restituisce shares storiche affidabili dai financials). Skip safe.
    # Noi assegniamo il punto se non abbiamo evidenza di aumento (best-effort).
    # Per essere rigorosi lo saltiamo (no punto).
    # → skip

    # 8. Gross margin in aumento YoY
    gp_cur = _f(inc_cur.get("gross_profit"))
    rev_cur = _f(inc_cur.get("total_revenue"))
    gp_prev = _f(inc_prev.get("gross_profit"))
    rev_prev = _f(inc_prev.get("total_revenue"))
    if (gp_cur and rev_cur and gp_prev and rev_prev and rev_cur > 0 and rev_prev > 0):
        if (gp_cur / rev_cur) > (gp_prev / rev_prev):
            score += 1

    # 9. Asset turnover in aumento YoY (Revenue/TotalAssets)
    if (rev_cur and ta and rev_prev and ta_prev and ta > 0 and ta_prev > 0):
        if (rev_cur / ta) > (rev_prev / ta_prev):
            score += 1

    return int(score)


# ------------------------------------------------------------------------------
# Beneish M (semplificato)
# ------------------------------------------------------------------------------
def compute_beneish_m(
    bal_annual: list[dict],
    inc_annual: list[dict],
    cf_annual: list[dict],
) -> float | None:
    """Beneish M-Score. Richiede 2 anni fiscali."""
    if len(bal_annual) < 2 or len(inc_annual) < 2:
        return None
    b_cur, b_prev = bal_annual[0], bal_annual[1]
    i_cur, i_prev = inc_annual[0], inc_annual[1]
    cf_cur = cf_annual[0] if cf_annual else {}

    rev_c = _f(i_cur.get("total_revenue"))
    rev_p = _f(i_prev.get("total_revenue"))
    rec_c = _f(b_cur.get("accounts_receivable"))
    rec_p = _f(b_prev.get("accounts_receivable"))

    # DSRI: Days Sales in Receivables index
    if rec_c and rec_p and rev_c and rev_p and rev_c > 0 and rev_p > 0:
        dsri = (rec_c / rev_c) / (rec_p / rev_p)
    else:
        return None

    # GMI: Gross Margin index
    gp_c = _f(i_cur.get("gross_profit"))
    gp_p = _f(i_prev.get("gross_profit"))
    if gp_c and gp_p and rev_c and rev_p and rev_c > 0 and rev_p > 0:
        gmi_c = gp_c / rev_c
        gmi_p = gp_p / rev_p
        if gmi_c <= 0:
            return None
        gmi = gmi_p / gmi_c
    else:
        return None

    # AQI: Asset Quality Index
    ta_c = _f(b_cur.get("total_assets"))
    ta_p = _f(b_prev.get("total_assets"))
    ca_c = _f(b_cur.get("total_current_assets"))
    ca_p = _f(b_prev.get("total_current_assets"))
    # PPE non abbiamo: approx ppe = ta - ca
    if ta_c and ta_p and ca_c is not None and ca_p is not None and ta_c > 0 and ta_p > 0:
        nc_c = ta_c - ca_c  # non-current asset approx
        nc_p = ta_p - ca_p
        aqi_c = nc_c / ta_c
        aqi_p = nc_p / ta_p
        if aqi_p <= 0:
            aqi = 1.0
        else:
            aqi = aqi_c / aqi_p
    else:
        return None

    # SGI: Sales Growth Index
    sgi = rev_c / rev_p

    # DEPI: non calcolabile (no depreciation data separato) → usa 1.0 (neutro)
    depi = 1.0

    # SGAI: SG&A Index (non lo abbiamo nel nostro schema) → 1.0
    sgai = 1.0

    # TATA: Total Accruals / Total Assets
    ocf_c = _f(cf_cur.get("operating_cashflow")) if cf_cur else None
    ni_c = _f(i_cur.get("net_income"))
    if ocf_c is not None and ni_c is not None and ta_c and ta_c > 0:
        tata = (ni_c - ocf_c) / ta_c
    else:
        return None

    # LVGI: Leverage Index
    tl_c = _f(b_cur.get("total_liabilities"))
    tl_p = _f(b_prev.get("total_liabilities"))
    if tl_c and tl_p and ta_c and ta_p and ta_c > 0 and ta_p > 0:
        lvgi = (tl_c / ta_c) / (tl_p / ta_p)
    else:
        return None

    m = (-4.84 + 0.92 * dsri + 0.528 * gmi + 0.404 * aqi + 0.892 * sgi
         + 0.115 * depi - 0.172 * sgai + 4.679 * tata - 0.327 * lvgi)
    return float(m)


# ------------------------------------------------------------------------------
# KQ Value Score composito (0-100)
# ------------------------------------------------------------------------------
def _zscore_rank(series: pd.Series) -> pd.Series:
    """Rank percentile invertibile: ritorna [0,1] dove 1 = migliore.

    Qui input higher=worse (es. PE alto è peggio) — il caller inverte con
    ``1 - _zscore_rank``.
    """
    valid = series.dropna()
    if valid.empty:
        return pd.Series(np.nan, index=series.index)
    ranks = valid.rank(pct=True)
    out = pd.Series(np.nan, index=series.index)
    out.loc[ranks.index] = ranks
    return out


def compute_kq_value_score(metrics_df: pd.DataFrame) -> pd.Series:
    """Calcola KQ Value Score (0-100) su un DataFrame con colonne prodotte
    dai vari moduli compute.

    Ponderi:
    - 40% valuation (pe_ttm ↓, forward_pe ↓, peg ↓, pe_percentile_5y ↓)
    - 25% qualità (roic_ttm ↑, fcf_margin ↑, piotroski_f ↑)
    - 20% solidità (altman_z ↑, interest_coverage ↑, current_ratio ↑,
                    net_debt_ebitda ↓)
    - 15% momentum (rs_126d ↑, pct_from_sma200 ↑)
    """
    df = metrics_df.copy()

    # Per ogni indicatore "↑ è meglio", prendi rank pct. Per "↓ è meglio",
    # prendi 1 - rank pct.
    def up(col: str) -> pd.Series:
        return _zscore_rank(df[col]) if col in df.columns else pd.Series(np.nan, index=df.index)

    def dn(col: str) -> pd.Series:
        r = _zscore_rank(df[col]) if col in df.columns else pd.Series(np.nan, index=df.index)
        return 1 - r

    valuation_sub = pd.concat([
        dn("pe_ttm"), dn("forward_pe"), dn("peg"), dn("pe_percentile_5y"),
    ], axis=1).mean(axis=1, skipna=True)

    quality_sub = pd.concat([
        up("roic_ttm"), up("fcf_margin"), up("piotroski_f"),
    ], axis=1).mean(axis=1, skipna=True)

    solidity_sub = pd.concat([
        up("altman_z"), up("interest_coverage"), up("current_ratio"),
        dn("net_debt_ebitda"),
    ], axis=1).mean(axis=1, skipna=True)

    momentum_sub = pd.concat([
        up("rs_126d"), up("pct_from_sma200"),
    ], axis=1).mean(axis=1, skipna=True)

    # ------------------------------------------------------------------
    # Aggregazione finale — media pesata con rinormalizzazione.
    #
    # BUG storico corretto qui:
    # `w1*sub1 + w2*sub2 + …` in pandas produce NaN se anche UNA SOLA delle
    # sub è NaN (NaN*x = NaN). Conseguenza: ticker con buoni dati ma con
    # momentum_sub NaN (es. benchmark mancante o storia < 200gg) uscivano
    # con kq_value_score NULL, che si propagava sulle pagine ticker e
    # finiva a dominare le righe di testa dello screener (sort con NaN).
    #
    # Nuova logica: per ogni riga sommiamo peso × sub SOLO dove sub è
    # definita, poi normalizziamo dividendo per il peso totale effettivo.
    # Se il peso effettivo è < 0.50 (cioè meno della metà dei segnali è
    # disponibile), il ticker resta NaN — NON vogliamo punteggi inventati
    # con un'unica categoria attiva.
    # ------------------------------------------------------------------
    subs = pd.concat(
        [valuation_sub, quality_sub, solidity_sub, momentum_sub],
        axis=1, keys=["valuation", "quality", "solidity", "momentum"],
    )
    weights = pd.Series({"valuation": 0.40, "quality": 0.25,
                          "solidity": 0.20,  "momentum": 0.15})

    # mask dove la sub è valida
    valid_mask = subs.notna()
    # matrice dei pesi effettivi (0 se sub NaN)
    eff_weights = valid_mask.astype(float).multiply(weights, axis=1)
    total_w = eff_weights.sum(axis=1)  # peso totale per riga

    # contributo = peso * sub (trattando NaN come 0 nel prodotto)
    contrib = subs.fillna(0.0).multiply(weights, axis=1)
    # score rinormalizzato
    raw = contrib.sum(axis=1) / total_w  # NaN dove total_w == 0

    # Soglia di copertura: almeno metà del peso totale (0.50)
    MIN_COVERAGE = 0.50
    score = raw.where(total_w >= MIN_COVERAGE) * 100.0

    # Diagnostica utile nei log
    n_total = len(score)
    n_null = int(score.isna().sum())
    if n_null > 0:
        logger.info(
            "KQ score: %d/%d ticker con score NaN (copertura < %.0f%% "
            "o tutte le sub mancanti).",
            n_null, n_total, MIN_COVERAGE * 100,
        )
    return score


# ------------------------------------------------------------------------------
# Orchestrazione scores su tutto l'universo
# ------------------------------------------------------------------------------
def compute_scores_all(db_path=None) -> list[dict[str, Any]]:
    """Calcola Altman Z, Piotroski F, Beneish M per ciascun ticker."""
    with get_connection(db_path) as conn:
        snaps = pd.read_sql_query("SELECT * FROM fundamentals_snapshot", conn)
    if snaps.empty:
        logger.warning("fundamentals_snapshot vuoto — scores impossibili.")
        return []

    fin = _load_fin_annual(db_path)
    # Split per statement type
    by_ticker: dict[str, dict[str, list[dict]]] = {}
    for _, r in fin.iterrows():
        by_ticker.setdefault(r["ticker"], {}).setdefault(r["statement_type"], []).append(
            dict(r)
        )

    out: list[dict[str, Any]] = []
    for rec in snaps.to_dict(orient="records"):
        tk = rec["ticker"]
        tk_fin = by_ticker.get(tk, {})
        bal = tk_fin.get("balance", [])
        inc = tk_fin.get("income", [])
        cf = tk_fin.get("cashflow", [])

        altman = compute_altman_z(
            rec,
            bal[0] if bal else None,
            inc[0] if inc else None,
        )
        piot = compute_piotroski_f(bal, inc, cf, rec)
        ben = compute_beneish_m(bal, inc, cf)

        out.append({
            "ticker": tk,
            "altman_z": altman,
            "piotroski_f": piot,
            "beneish_m": ben,
        })

    logger.info("Scores: calcolati per %d ticker", len(out))
    return out
