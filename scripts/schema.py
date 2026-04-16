"""
Database schema for the ASEAN Markets Dashboard.
Creates and initializes the SQLite database.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'dashboard.db')


def get_db_path():
    return os.path.abspath(DB_PATH)


def init_db():
    """Create the database and tables if they don't exist."""
    db_path = get_db_path()
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Main time-series table: one row per (date, indicator)
    c.execute('''
        CREATE TABLE IF NOT EXISTS daily_data (
            date        TEXT NOT NULL,       -- YYYY-MM-DD
            category    TEXT NOT NULL,       -- 'fx', 'bond', 'commodity'
            indicator   TEXT NOT NULL,       -- e.g. 'IDR', 'US_10Y', 'BRENT'
            value       REAL,               -- the numeric value
            unit        TEXT,               -- e.g. 'per_USD', 'percent', 'USD/bbl'
            source      TEXT,               -- e.g. 'yfinance:fx', 'yfinance:brent'
            ingested_at TEXT NOT NULL,       -- ISO timestamp of when we stored it
            PRIMARY KEY (date, indicator)
        )
    ''')

    # Reference table: metadata about each indicator
    c.execute('''
        CREATE TABLE IF NOT EXISTS indicators (
            indicator   TEXT PRIMARY KEY,
            category    TEXT NOT NULL,
            label       TEXT NOT NULL,       -- human-readable name
            unit        TEXT,
            source      TEXT,               -- default source
            tier        INTEGER DEFAULT 1   -- 1=API, 2=scrape, 3=manual
        )
    ''')

    # Ingestion log: track each run
    c.execute('''
        CREATE TABLE IF NOT EXISTS ingestion_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at      TEXT NOT NULL,
            source      TEXT NOT NULL,
            status      TEXT NOT NULL,       -- 'success', 'partial', 'error'
            records     INTEGER DEFAULT 0,
            message     TEXT
        )
    ''')

    # Data sources reference table: full attribution for dashboard display
    c.execute('''
        CREATE TABLE IF NOT EXISTS data_sources (
            source_key      TEXT PRIMARY KEY,    -- short key used in daily_data.source
            provider        TEXT NOT NULL,        -- e.g. "Yahoo Finance", "Asian Development Bank"
            provider_url    TEXT,                 -- link to provider homepage
            dataset         TEXT,                 -- specific dataset or product name
            ticker_or_id    TEXT,                 -- ticker symbol or series ID used
            data_url        TEXT,                 -- direct link to the data or API endpoint
            frequency       TEXT DEFAULT 'daily', -- how often data is published
            lag             TEXT,                 -- e.g. "~1 day", "real-time", "T+1"
            license_info    TEXT,                 -- free, freemium, etc.
            notes           TEXT                  -- caveats, limitations
        )
    ''')

    # Seed data sources
    data_sources = [
        # FX source (yfinance)
        ('yfinance:fx', 'Yahoo Finance', 'https://finance.yahoo.com',
         'FX Rates (USD pairs)', 'IDR=X, MYR=X, PHP=X, THB=X, VND=X',
         'https://finance.yahoo.com/quote/IDR=X',
         'daily', 'near real-time', 'free (yfinance library)',
         'Mid-market USD exchange rates via Yahoo Finance. Updated throughout trading hours.'),

        # yfinance sources — one entry per underlying dataset
        ('yfinance:us10y', 'Yahoo Finance', 'https://finance.yahoo.com',
         'CBOE 10-Year Treasury Note Yield Index', '^TNX',
         'https://finance.yahoo.com/quote/%5ETNX/',
         'daily', 'end-of-day (US market close)', 'free (yfinance library)',
         'Yield in percentage points. Updated at US market close ~16:00 ET.'),

        ('yfinance:brent', 'Yahoo Finance', 'https://finance.yahoo.com',
         'Brent Crude Oil Last Day Financial Futures (ICE)', 'BZ=F',
         'https://finance.yahoo.com/quote/BZ=F/',
         'daily', 'end-of-day', 'free (yfinance library)',
         'ICE Brent crude front-month futures settlement price in USD/barrel.'),

        ('yfinance:gold', 'Yahoo Finance', 'https://finance.yahoo.com',
         'Gold Futures (COMEX)', 'GC=F',
         'https://finance.yahoo.com/quote/GC=F/',
         'daily', 'end-of-day', 'free (yfinance library)',
         'COMEX gold front-month futures settlement price in USD/troy oz.'),

        ('yfinance:jkm', 'Yahoo Finance', 'https://finance.yahoo.com',
         'JKM LNG Futures', 'JKM=F',
         'https://finance.yahoo.com/quote/JKM=F/',
         'daily', 'end-of-day', 'free (yfinance library)',
         'S&P Global Platts JKM LNG futures in USD/MMBtu.'),

        # Tier 2: Asian Bonds Online (ADB)
        ('adb:asianbondsonline', 'Asian Development Bank', 'https://asianbondsonline.adb.org',
         'AsianBondsOnline Market Indicators', None, 'https://asianbondsonline.adb.org/{country}/',
         'daily', '~1 day', 'free (public website)',
         'ADB-maintained portal with government bond yields for ASEAN+3 economies. '
         'Yields scraped from country overview pages. No official API; HTML parsing required.'),

        # Tier 2: Investing.com commodity pages
        ('investing.com:nickel', 'Investing.com', 'https://www.investing.com',
         'Nickel Futures', None, 'https://www.investing.com/commodities/nickel',
         'daily', 'near real-time', 'free (public website)',
         'LME nickel futures price in USD/tonne. Scraped from embedded JSON on page.'),

        ('investing.com:cpo', 'Investing.com', 'https://www.investing.com',
         'Crude Palm Oil Futures (Bursa Malaysia FCPO)', None,
         'https://www.investing.com/commodities/palm-oil',
         'daily', 'near real-time', 'free (public website)',
         'Bursa Malaysia FCPO front-month futures in MYR/tonne.'),

        ('investing.com:rubber', 'Investing.com', 'https://www.investing.com',
         'Rubber TSR20 Futures (SGX)', None,
         'https://www.investing.com/commodities/rubber-tsr20-futures',
         'daily', 'near real-time', 'free (public website)',
         'SGX TSR20 rubber futures in USc/kg.'),

        ('investing.com:coal', 'Investing.com', 'https://www.investing.com',
         'Newcastle Coal Futures', None,
         'https://www.investing.com/commodities/newcastle-coal-futures',
         'daily', 'near real-time', 'free (public website)',
         'Newcastle (globalCOAL) thermal coal futures in USD/tonne.'),

        # Tier 2 backfill: Investing.com historical pages
        ('investing.com:backfill', 'Investing.com', 'https://www.investing.com',
         'Historical Data Pages', None, 'https://www.investing.com/.../...-historical-data',
         'one-off backfill', 'end-of-day', 'free (public website)',
         'Backfill source only. Historical data scraped from Investing.com historical data pages. '
         'Typically provides ~20-25 trading days of history. Used for bonds and VND FX backfill.'),

        # Tier 3: Manual / Bloomberg fallback
        ('manual:bloomberg', 'Bloomberg Terminal', 'https://www.bloomberg.com/professional/',
         'Bloomberg Professional', None, None,
         'daily', 'real-time', 'commercial subscription',
         'Manual CSV export from Bloomberg Terminal. Used as fallback if scraping fails.'),

        ('manual:csv', 'Manual CSV Upload', None,
         'User-provided CSV', None, None,
         'ad-hoc', 'varies', 'n/a',
         'Manually provided data files dropped into the import folder.'),
    ]

    c.executemany('''
        INSERT OR REPLACE INTO data_sources
        (source_key, provider, provider_url, dataset, ticker_or_id, data_url,
         frequency, lag, license_info, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', data_sources)

    # Seed indicator metadata
    indicators = [
        # FX rates (yfinance)
        ('IDR', 'fx', 'Indonesian Rupiah', 'per USD', 'yfinance:fx', 1),
        ('MYR', 'fx', 'Malaysian Ringgit', 'per USD', 'yfinance:fx', 1),
        ('PHP', 'fx', 'Philippine Peso', 'per USD', 'yfinance:fx', 1),
        ('THB', 'fx', 'Thai Baht', 'per USD', 'yfinance:fx', 1),
        ('VND', 'fx', 'Vietnamese Dong', 'per USD', 'yfinance:fx', 1),

        # Bond yields
        ('US_10Y', 'bond', 'US 10Y Treasury Yield', 'percent', 'yfinance:us10y', 1),
        ('ID_10Y', 'bond', 'Indonesia 10Y Govt Bond Yield', 'percent', 'adb:asianbondsonline', 2),
        ('MY_10Y', 'bond', 'Malaysia 10Y Govt Bond Yield', 'percent', 'adb:asianbondsonline', 2),
        ('PH_10Y', 'bond', 'Philippines 10Y Govt Bond Yield', 'percent', 'adb:asianbondsonline', 2),
        ('TH_10Y', 'bond', 'Thailand 10Y Govt Bond Yield', 'percent', 'adb:asianbondsonline', 2),

        # Commodities
        ('BRENT', 'commodity', 'Brent Crude Oil (ICE Futures)', 'USD/bbl', 'yfinance:brent', 1),
        ('JKM_LNG', 'commodity', 'JKM LNG Futures (Platts)', 'USD/MMBtu', 'yfinance:jkm', 1),
        ('COAL_NEWC', 'commodity', 'Thermal Coal (Newcastle FOB)', 'USD/tonne', 'investing.com:coal', 2),
        ('CPO', 'commodity', 'Crude Palm Oil (Bursa Malaysia FCPO)', 'MYR/tonne', 'investing.com:cpo', 2),
        ('RUBBER_TSR20', 'commodity', 'Rubber TSR20 Futures (SGX)', 'USc/kg', 'investing.com:rubber', 2),
        ('NICKEL', 'commodity', 'Nickel Futures (LME)', 'USD/tonne', 'investing.com:nickel', 2),
        ('GOLD', 'commodity', 'Gold Futures (COMEX)', 'USD/oz', 'yfinance:gold', 1),
    ]

    c.executemany('''
        INSERT OR REPLACE INTO indicators (indicator, category, label, unit, source, tier)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', indicators)

    conn.commit()
    conn.close()
    print(f"Database initialized at: {db_path}")
    return db_path


if __name__ == '__main__':
    init_db()
