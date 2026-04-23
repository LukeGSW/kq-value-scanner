-- ============================================================================
-- KQ Value Scanner — Schema SQLite
-- ============================================================================
-- DDL completo del database locale. Eseguibile in modo idempotente
-- (tutte le tabelle usano `CREATE TABLE IF NOT EXISTS`).
--
-- Convenzioni:
--   - Tutti i ticker sono in formato EODHD (es. "AAPL.US", "VOW3.XETRA")
--   - Date in ISO 8601 "YYYY-MM-DD"
--   - Timestamp in ISO 8601 UTC "YYYY-MM-DDTHH:MM:SSZ"
--   - Valori monetari in USD (conversione applicata a monte per non-US)
--   - Valori percentuali come frazione (0.15 = 15%)
-- ============================================================================

-- PRAGMA ottimizzate per workload analitico single-writer / multi-reader
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -64000;  -- 64 MB

-- ----------------------------------------------------------------------------
-- universe: elenco ticker investibili (Large+Mid Cap USA+Europa)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS universe (
    ticker                  TEXT PRIMARY KEY,        -- es. "AAPL.US"
    code                    TEXT NOT NULL,           -- es. "AAPL"
    name                    TEXT,                    -- ragione sociale
    exchange_code           TEXT NOT NULL,           -- es. "US", "LSE"
    exchange_name           TEXT,                    -- "NYSE/NASDAQ/AMEX"
    country                 TEXT,
    currency                TEXT,                    -- valuta nativa exchange
    type                    TEXT,                    -- "Common Stock" | "ADR"
    sector                  TEXT,
    industry                TEXT,
    market_capitalization   REAL,                    -- USD
    isin                    TEXT,
    is_active               INTEGER DEFAULT 1,       -- 0 = escluso (sotto soglia exit)
    last_refresh_utc        TEXT NOT NULL            -- ISO timestamp UTC
);

CREATE INDEX IF NOT EXISTS idx_universe_exchange
    ON universe(exchange_code);
CREATE INDEX IF NOT EXISTS idx_universe_sector
    ON universe(sector);
CREATE INDEX IF NOT EXISTS idx_universe_active
    ON universe(is_active);

-- ----------------------------------------------------------------------------
-- prices_daily: serie storiche OHLCV giornaliere
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prices_daily (
    ticker          TEXT NOT NULL,
    date            TEXT NOT NULL,                   -- "YYYY-MM-DD"
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    adjusted_close  REAL,
    volume          INTEGER,
    PRIMARY KEY (ticker, date),
    FOREIGN KEY (ticker) REFERENCES universe(ticker)
);

CREATE INDEX IF NOT EXISTS idx_prices_date
    ON prices_daily(date);
CREATE INDEX IF NOT EXISTS idx_prices_ticker_date
    ON prices_daily(ticker, date DESC);

-- ----------------------------------------------------------------------------
-- fundamentals_snapshot: ultimi fundamentals per ticker (overwrite)
-- ----------------------------------------------------------------------------
-- Contiene un solo record per ticker (le Highlights/Valuation sezioni del
-- payload EODHD, appiattite per consumo diretto dalla pipeline di compute).
-- Storia completa in `financials_history`.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fundamentals_snapshot (
    ticker                      TEXT PRIMARY KEY,
    -- Valuation
    pe_ttm                      REAL,
    forward_pe                  REAL,
    peg                         REAL,
    price_to_sales_ttm          REAL,
    price_to_book                REAL,
    ev_to_ebitda                REAL,
    ev_to_revenue               REAL,
    earnings_yield              REAL,
    dividend_yield              REAL,
    -- Profitability
    roe                         REAL,
    roa                         REAL,
    roic                        REAL,
    gross_margin                REAL,
    operating_margin            REAL,
    profit_margin               REAL,
    -- Size / Stats
    market_cap_usd              REAL,
    shares_outstanding          REAL,
    beta                        REAL,
    -- Growth
    revenue_growth_yoy          REAL,
    eps_growth_yoy              REAL,
    earnings_growth_next_year   REAL,
    -- Payout
    dividend_per_share          REAL,
    payout_ratio                REAL,
    -- Earnings / schedule
    next_earnings_date          TEXT,                -- "YYYY-MM-DD"
    most_recent_quarter         TEXT,                -- "YYYY-MM-DD"
    -- Balance sheet sintesi (ultimo quarter disponibile)
    total_cash                  REAL,
    total_debt                  REAL,
    net_debt                    REAL,
    total_revenue_ttm           REAL,
    ebitda_ttm                  REAL,
    ebit_ttm                    REAL,
    free_cash_flow_ttm          REAL,
    interest_expense_ttm        REAL,
    total_current_assets        REAL,
    total_current_liabilities   REAL,
    inventory                   REAL,
    accounts_receivable         REAL,
    -- Metadata
    currency_reporting          TEXT,
    last_refresh_utc            TEXT NOT NULL,
    FOREIGN KEY (ticker) REFERENCES universe(ticker)
);

-- ----------------------------------------------------------------------------
-- financials_history: serie storiche annuali/quarterly dei bilanci
-- ----------------------------------------------------------------------------
-- Granularità: una riga per (ticker, period_end, statement_type, freq).
-- statement_type: "income" | "balance" | "cashflow"
-- freq:           "annual" | "quarterly"
-- I singoli campi (es. revenue, net_income) sono salvati come rows in `items`
-- per massima flessibilità, oppure appiattiti nelle colonne sottostanti per
-- i campi più comuni (velocità di accesso).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS financials_history (
    ticker              TEXT NOT NULL,
    period_end          TEXT NOT NULL,               -- "YYYY-MM-DD"
    statement_type      TEXT NOT NULL,               -- income|balance|cashflow
    freq                TEXT NOT NULL,               -- annual|quarterly
    currency_symbol     TEXT,
    -- Income statement
    total_revenue       REAL,
    gross_profit        REAL,
    operating_income    REAL,
    ebit                REAL,
    ebitda              REAL,
    net_income          REAL,
    eps_basic           REAL,
    eps_diluted         REAL,
    interest_expense    REAL,
    income_tax_expense  REAL,
    -- Balance sheet
    cash_and_equivalents REAL,
    short_term_investments REAL,
    total_current_assets REAL,
    total_assets        REAL,
    total_current_liabilities REAL,
    long_term_debt      REAL,
    short_term_debt     REAL,
    total_liabilities   REAL,
    total_equity        REAL,
    inventory           REAL,
    accounts_receivable REAL,
    retained_earnings   REAL,
    -- Cash flow
    operating_cashflow  REAL,
    capital_expenditure REAL,
    free_cash_flow      REAL,
    dividends_paid      REAL,
    share_repurchases   REAL,
    -- Payload raw (JSON serializzato) per chi vuole ricostruire
    raw_json            TEXT,
    last_refresh_utc    TEXT NOT NULL,
    PRIMARY KEY (ticker, period_end, statement_type, freq),
    FOREIGN KEY (ticker) REFERENCES universe(ticker)
);

CREATE INDEX IF NOT EXISTS idx_financials_ticker_period
    ON financials_history(ticker, period_end DESC);
CREATE INDEX IF NOT EXISTS idx_financials_freq
    ON financials_history(freq);

-- ----------------------------------------------------------------------------
-- computed_metrics: metriche derivate calcolate dalla pipeline compute
-- ----------------------------------------------------------------------------
-- Una riga per ticker con l'ultimo stato calcolato. Overwrite nightly.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS computed_metrics (
    ticker                      TEXT PRIMARY KEY,
    -- Prezzo e tecnica
    last_close                  REAL,
    last_close_date             TEXT,
    sma_50                      REAL,
    sma_200                     REAL,
    pct_from_sma50              REAL,
    pct_from_sma200             REAL,
    zscore_90                   REAL,                -- zScore log returns SMA 90
    hv_20_annualized            REAL,                -- Historical Volatility 20d
    drawdown_from_ath           REAL,                -- frazione negativa
    ath_date                    TEXT,
    rs_126d                     REAL,                -- Relative Strength vs benchmark
    ytd_return                  REAL,
    one_year_return             REAL,
    -- Valuation derivate
    pe_percentile_5y            REAL,                -- 0-1
    pe_vs_sector_median         REAL,                -- ratio
    -- Cash & Debt
    net_debt_ebitda             REAL,
    net_debt_fcf                REAL,
    interest_coverage           REAL,
    current_ratio               REAL,
    quick_ratio                 REAL,
    cash_ratio                  REAL,
    -- Profitability derivate
    roic_ttm                    REAL,
    fcf_margin                  REAL,
    fcf_yield                   REAL,
    -- Composite scores
    altman_z                    REAL,
    piotroski_f                 INTEGER,             -- 0-9
    beneish_m                   REAL,
    -- Quality flags (0/1) aggregate per anti-value-trap
    flag_quality_ok             INTEGER,
    flag_fcf_positive           INTEGER,
    flag_revenue_growth_ok      INTEGER,
    flag_roic_ok                INTEGER,
    -- Ranking
    kq_value_score              REAL,                -- score composito 0-100
    rank_global                 INTEGER,
    rank_sector                 INTEGER,
    -- Metadata
    last_compute_utc            TEXT NOT NULL,
    FOREIGN KEY (ticker) REFERENCES universe(ticker)
);

CREATE INDEX IF NOT EXISTS idx_metrics_score
    ON computed_metrics(kq_value_score DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_rank
    ON computed_metrics(rank_global);

-- ----------------------------------------------------------------------------
-- screener_cache: snapshot JSON compatto per il frontend
-- ----------------------------------------------------------------------------
-- Contiene il JSON esatto servito a `site/data/screener.json` (versioning).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS screener_cache (
    build_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    build_ts_utc        TEXT NOT NULL,
    payload_json        TEXT NOT NULL,
    ticker_count        INTEGER NOT NULL,
    size_bytes          INTEGER NOT NULL
);

-- ----------------------------------------------------------------------------
-- fetch_log: audit delle chiamate API (per debug e budget tracking)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fetch_log (
    log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc          TEXT NOT NULL,
    endpoint        TEXT NOT NULL,
    resource        TEXT,                            -- es. ticker/exchange
    status_code     INTEGER,
    credits_cost    INTEGER,
    duration_ms     INTEGER,
    error_msg       TEXT
);

CREATE INDEX IF NOT EXISTS idx_fetchlog_ts
    ON fetch_log(ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_fetchlog_endpoint
    ON fetch_log(endpoint);
