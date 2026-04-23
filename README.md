# Kriterion Quant · Value Scanner

Piattaforma screener di azioni Large + Mid Cap USA ed Europa basata su PE e Forward PE favorevoli all'acquisto, con pagina dettaglio per ogni ticker focalizzata su solidità finanziaria (cassa e debito) e analisi tecnica quantitativa.

Parte dell'ecosistema educativo/operativo [Kriterion Quant](https://kriterionquant.com).

## Caratteristiche

- Universo: Large Cap + Mid Cap USA ed Europa (~2.500 tickers)
- Soglia: MarketCap > $2B (con buffer anti-flickering a $1.6B)
- Fonte dati: [EODHD](https://eodhistoricaldata.com/) All-World Extended
- Refresh: notturno per prezzi, settimanale per fundamentals
- Output: sito statico su GitHub Pages

## Architettura

```
Backend Python (GitHub Actions cron)
    ├─ Fetch bulk EODHD (prezzi + fundamentals)
    ├─ Storage locale (SQLite / Parquet)
    ├─ Compute metriche (valuation, cash/debt, technical, scores)
    └─ Generate JSON precomputati
           ↓
Frontend HTML statico (GitHub Pages)
    ├─ Dashboard screener (DataTables)
    └─ Pagine ticker individuali
```

## Metriche principali

**Valuation**: PE TTM, Forward PE, PEG, EV/EBITDA, PE percentile 5Y, PE vs settore

**Cassa e debito**: Net Debt, Net Debt/EBITDA, Net Debt/FCF, Interest Coverage, D/E, Current/Quick/Cash Ratio

**Redditività**: ROE, ROA, ROIC, Gross/Operating/Net Margin

**Cash flow**: OCF/NI, FCF, FCF Margin, FCF Yield, CapEx/Revenue

**Tecnica**: SMA 50/200, zScore log returns SMA 90, HV 20 annualizzata, Drawdown from ATH, Relative Strength vs benchmark

**Composite scores**: Altman Z-Score, Piotroski F-Score, Beneish M-Score

## Setup

1. Clona il repository
2. Installa le dipendenze: `pip install -r requirements.txt`
3. Imposta la variabile di ambiente `EODHD_API_KEY` (in Colab: Secrets, in locale: `.env`, in GitHub Actions: Secret del repo)
4. Esegui lo script di test: `python scripts/test_connection.py`

## Struttura progetto

```
kq-value-scanner/
├── pipeline/
│   ├── fetch/          # Client EODHD e script di download
│   ├── compute/        # Calcolo metriche e score
│   ├── storage/        # Gestione database locale
│   └── generate/       # Generazione JSON/HTML output
├── site/               # Frontend statico (GitHub Pages)
├── tests/              # Unit test
├── config/             # Configurazione (settings.yaml)
├── scripts/            # Script utility e one-shot
└── .github/workflows/  # CI/CD GitHub Actions
```

## Stato sviluppo

Progetto in fase iniziale di sviluppo. Vedi `PROJECT_STATE.md` per lo stato corrente.

## Licenza

TBD

---

**Kriterion Quant** · Research-driven quantitative finance
