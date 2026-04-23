"""Script di test rapido per validare il client EODHD.

Esegue chiamate di sanity check su un ticker noto (AAPL.US) e stampa
un report sui risultati e sul consumo di crediti.

Uso in Colab
------------
    !git clone https://github.com/kriterion-quant/kq-value-scanner.git
    %cd kq-value-scanner
    !pip install -q -r requirements.txt
    # Assicurati di avere EODHD_API_KEY nei Colab Secrets
    !python scripts/test_connection.py

Uso locale
----------
    export EODHD_API_KEY="la_tua_chiave"
    python scripts/test_connection.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Permette di eseguire lo script dalla root del repo senza installare il package
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.fetch.eodhd_client import EODHDClient, EODHDError  # noqa: E402


TEST_TICKER = "AAPL.US"


def section(title: str) -> None:
    """Stampa un divisore di sezione per leggibilità del log."""
    bar = "─" * 72
    print(f"\n{bar}\n  {title}\n{bar}")


def run() -> int:
    """Esegue la batteria di test e ritorna un exit code."""
    section("KQ Value Scanner · Test connessione EODHD")
    print(f"Ticker di test: {TEST_TICKER}")

    try:
        client = EODHDClient()
    except EODHDError as exc:
        print(f"✗ ERRORE INIZIALIZZAZIONE: {exc}")
        return 1

    failures = 0

    # --------------------------------------------------------------------------
    # TEST 1 — EOD prices (last 30 days)
    # --------------------------------------------------------------------------
    section("TEST 1 · EOD prices")
    try:
        import pandas as pd

        from_date = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).strftime(
            "%Y-%m-%d"
        )
        df = client.get_eod(TEST_TICKER, from_date=from_date)
        if df.empty:
            print("✗ DataFrame vuoto")
            failures += 1
        else:
            print(f"✓ Righe scaricate: {len(df)}")
            print(f"  Ultima data: {df.index[-1].date()}")
            print(f"  Ultima close: ${df['close'].iloc[-1]:.2f}")
            print(f"  Colonne: {list(df.columns)}")
    except Exception as exc:  # noqa: BLE001
        print(f"✗ ECCEZIONE: {exc}")
        failures += 1

    # --------------------------------------------------------------------------
    # TEST 2 — Fundamentals (struttura)
    # --------------------------------------------------------------------------
    section("TEST 2 · Fundamentals")
    try:
        fund = client.get_fundamentals(TEST_TICKER)
        if not isinstance(fund, dict) or not fund:
            print("✗ Risposta vuota o non valida")
            failures += 1
        else:
            keys_expected = {"General", "Highlights", "Valuation", "Financials"}
            keys_present = set(fund.keys())
            missing = keys_expected - keys_present
            print(f"✓ Sezioni presenti: {len(keys_present)}")
            print(f"  Chiavi top-level: {sorted(keys_present)[:8]}...")
            if missing:
                print(f"⚠ Sezioni attese mancanti: {missing}")
            else:
                gen = fund.get("General", {})
                hl = fund.get("Highlights", {})
                print(f"  Company: {gen.get('Name')}")
                print(f"  Sector: {gen.get('Sector')}")
                print(f"  Industry: {gen.get('Industry')}")
                print(f"  MarketCap: {hl.get('MarketCapitalization')}")
                print(f"  PE Ratio: {hl.get('PERatio')}")
                print(f"  Forward PE: {hl.get('ForwardPE')}")
                print(f"  EPS: {hl.get('EarningsShare')}")
    except Exception as exc:  # noqa: BLE001
        print(f"✗ ECCEZIONE: {exc}")
        failures += 1

    # --------------------------------------------------------------------------
    # TEST 3 — Dividends (storico compatto)
    # --------------------------------------------------------------------------
    section("TEST 3 · Dividends")
    try:
        div = client.get_dividends(TEST_TICKER, from_date="2020-01-01")
        print(f"✓ Dividendi scaricati: {len(div)} record")
        if not div.empty:
            print(f"  Ultimo dividendo: {div.iloc[-1].to_dict()}")
    except Exception as exc:  # noqa: BLE001
        print(f"✗ ECCEZIONE: {exc}")
        failures += 1

    # --------------------------------------------------------------------------
    # TEST 4 — Search
    # --------------------------------------------------------------------------
    section("TEST 4 · Search")
    try:
        results = client.search("Apple", limit=5)
        print(f"✓ Risultati ricerca 'Apple': {len(results)}")
        for r in results[:3]:
            print(f"  · {r.get('Code')} — {r.get('Name')} ({r.get('Exchange')})")
    except Exception as exc:  # noqa: BLE001
        print(f"✗ ECCEZIONE: {exc}")
        failures += 1

    # --------------------------------------------------------------------------
    # REPORT UTILIZZO
    # --------------------------------------------------------------------------
    section("Report utilizzo API")
    stats = client.get_usage_stats()
    print(f"Chiamate totali    : {stats['calls_total']}")
    print(f"Crediti consumati  : {stats['credits_total']}")
    print(f"Errori             : {stats['errors_total']}")
    print(f"Breakdown crediti  : {stats['credits_by_endpoint']}")

    client.close()

    section("Esito")
    if failures == 0:
        print("✓ TUTTI I TEST SUPERATI")
        print("\nIl client EODHD è operativo e pronto all'uso.")
        return 0
    print(f"✗ {failures} TEST FALLITI — controllare output sopra")
    return 1


if __name__ == "__main__":
    sys.exit(run())
