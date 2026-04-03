"""
Tier 1 Data Ingestion: Free API sources
========================================
Pulls data from:
  - ExchangeRate-API (open.er-api.com) for FX rates
  - yfinance for US 10Y yield, Brent crude, Gold spot

Usage:
  python ingest_tier1.py              # ingest latest data
  python ingest_tier1.py --date 2026-03-28  # ingest specific date (FX only; yfinance always gets latest)
  python ingest_tier1.py --backfill 30      # backfill last N days of FX data
"""

import argparse
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError

# Optional: yfinance for market data
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False
    print("WARNING: yfinance not installed. Run: pip install yfinance")

# ---------- Config ----------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.abspath(os.path.join(SCRIPT_DIR, '..', 'data', 'dashboard.db'))
LOG_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', 'logs'))

FX_CURRENCIES = ['IDR', 'MYR', 'PHP', 'THB', 'VND']
FX_API_URL = "https://open.er-api.com/v6/latest/USD"

YFINANCE_TICKERS = {
    'US_10Y':  ('^TNX',  'bond',      'percent',  'yfinance:us10y'),
    'BRENT':   ('BZ=F',  'commodity', 'USD/bbl',  'yfinance:brent'),
    'GOLD':    ('GC=F',  'commodity', 'USD/oz',   'yfinance:gold'),
}

# ---------- Logging ----------

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'ingestion.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------- Database helpers ----------

def get_conn():
    """Get a database connection, creating the DB if needed."""
    if not os.path.exists(DB_PATH):
        # Initialize DB via schema module
        import schema
        schema.init_db()
    return sqlite3.connect(DB_PATH)


def upsert_record(conn, date_str, category, indicator, value, unit, source):
    """Insert or update a single data record."""
    now = datetime.utcnow().isoformat()
    conn.execute('''
        INSERT INTO daily_data (date, category, indicator, value, unit, source, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (date, indicator)
        DO UPDATE SET value=excluded.value, unit=excluded.unit,
                      source=excluded.source, ingested_at=excluded.ingested_at
    ''', (date_str, category, indicator, value, unit, source, now))


def log_ingestion(conn, source, status, records, message=''):
    """Write an entry to the ingestion log."""
    now = datetime.utcnow().isoformat()
    conn.execute('''
        INSERT INTO ingestion_log (run_at, source, status, records, message)
        VALUES (?, ?, ?, ?, ?)
    ''', (now, source, status, records, message))


# ---------- FX ingestion ----------

def fetch_fx_latest():
    """Fetch latest FX rates from open.er-api.com."""
    logger.info("Fetching FX rates from ExchangeRate-API...")
    req = Request(FX_API_URL, headers={'User-Agent': 'ASEAN-Dashboard/1.0'})
    try:
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except (URLError, Exception) as e:
        logger.error(f"FX API request failed: {e}")
        return None

    if data.get('result') != 'success':
        logger.error(f"FX API returned error: {data}")
        return None

    # Extract date and rates
    # The API returns time_last_update_utc like "Wed, 01 Apr 2026 00:02:31 +0000"
    # Parse just the date portion
    date_str = data.get('time_last_update_utc', '')
    try:
        dt = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
        date_str = dt.strftime('%Y-%m-%d')
    except ValueError:
        date_str = datetime.utcnow().strftime('%Y-%m-%d')

    rates = data.get('rates', {})
    result = {}
    for ccy in FX_CURRENCIES:
        if ccy in rates:
            result[ccy] = rates[ccy]
        else:
            logger.warning(f"Currency {ccy} not found in API response")

    logger.info(f"FX rates for {date_str}: {result}")
    return date_str, result


def ingest_fx(conn, date_override=None):
    """Ingest FX rates into the database."""
    data = fetch_fx_latest()
    if data is None:
        log_ingestion(conn, 'exchangerate-api', 'error', 0, 'API request failed')
        return 0

    date_str, rates = data
    if date_override:
        date_str = date_override

    count = 0
    for ccy, rate in rates.items():
        upsert_record(conn, date_str, 'fx', ccy, rate, 'per USD', 'exchangerate-api')
        count += 1

    log_ingestion(conn, 'exchangerate-api', 'success', count,
                  f'Ingested {count} FX rates for {date_str}')
    logger.info(f"Stored {count} FX rates for {date_str}")
    return count


# ---------- yfinance ingestion ----------

def ingest_yfinance(conn):
    """Ingest US 10Y, Brent, Gold from yfinance."""
    if not HAS_YFINANCE:
        logger.error("yfinance not available, skipping market data")
        log_ingestion(conn, 'yfinance', 'error', 0, 'yfinance not installed')
        return 0

    logger.info("Fetching market data from yfinance...")
    count = 0
    errors = []

    for indicator, (ticker, category, unit, source) in YFINANCE_TICKERS.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period='5d')

            if hist.empty:
                logger.warning(f"No data for {indicator} ({ticker})")
                errors.append(f"{indicator}: no data")
                continue

            # Get the most recent row
            latest = hist.iloc[-1]
            date_str = hist.index[-1].strftime('%Y-%m-%d')
            value = float(latest['Close'])

            # US 10Y from yfinance is already in percentage points
            upsert_record(conn, date_str, category, indicator, value, unit, source)
            count += 1
            logger.info(f"  {indicator}: {value:.4f} {unit} ({date_str})")

        except Exception as e:
            logger.error(f"yfinance error for {indicator}: {e}")
            errors.append(f"{indicator}: {e}")

    status = 'success' if not errors else ('partial' if count > 0 else 'error')
    msg = f"Ingested {count}/{len(YFINANCE_TICKERS)} tickers"
    if errors:
        msg += f" | Errors: {'; '.join(errors)}"

    log_ingestion(conn, 'yfinance', status, count, msg)
    return count


# ---------- Backfill helper (FX only) ----------

def backfill_fx(conn, days=30):
    """Backfill FX rates for the last N days using Frankfurter API (supports historical)."""
    logger.info(f"Backfilling FX rates for last {days} days via Frankfurter API...")
    count = 0
    today = datetime.utcnow().date()

    for i in range(days, 0, -1):
        target_date = today - timedelta(days=i)
        date_str = target_date.strftime('%Y-%m-%d')

        # Skip weekends
        if target_date.weekday() >= 5:
            continue

        url = f"https://api.frankfurter.dev/v1/{date_str}?base=USD&symbols=IDR,MYR,PHP,THB"
        try:
            req = Request(url, headers={'User-Agent': 'ASEAN-Dashboard/1.0'})
            with urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())

            rates = data.get('rates', {})
            actual_date = data.get('date', date_str)

            for ccy, rate in rates.items():
                upsert_record(conn, actual_date, 'fx', ccy, rate, 'per USD', 'frankfurter')
                count += 1

        except Exception as e:
            logger.warning(f"Backfill failed for {date_str}: {e}")
            continue

    # VND is not available on Frankfurter, so we note that
    logger.info(f"Backfilled {count} FX records (note: VND not available on Frankfurter historical)")
    log_ingestion(conn, 'frankfurter-backfill', 'success', count,
                  f'Backfilled {count} records over {days} days (excl. VND)')
    return count


# ---------- yfinance backfill ----------

def backfill_yfinance(conn, days=30):
    """Backfill yfinance tickers for the last N days."""
    if not HAS_YFINANCE:
        logger.error("yfinance not available")
        return 0

    logger.info(f"Backfilling yfinance data for last {days} days...")
    count = 0
    period = f'{days}d'

    for indicator, (ticker, category, unit, source) in YFINANCE_TICKERS.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period=period)

            for idx, row in hist.iterrows():
                date_str = idx.strftime('%Y-%m-%d')
                value = float(row['Close'])
                upsert_record(conn, date_str, category, indicator, value, unit, source)
                count += 1

            logger.info(f"  {indicator}: {len(hist)} days backfilled")
        except Exception as e:
            logger.error(f"Backfill error for {indicator}: {e}")

    log_ingestion(conn, 'yfinance-backfill', 'success', count,
                  f'Backfilled {count} records over {days} days')
    return count


# ---------- Main ----------

def run_daily():
    """Run the standard daily ingestion."""
    logger.info("=" * 60)
    logger.info("STARTING DAILY INGESTION")
    logger.info("=" * 60)

    conn = get_conn()
    total = 0

    try:
        total += ingest_fx(conn)
        total += ingest_yfinance(conn)
        conn.commit()
        logger.info(f"Daily ingestion complete: {total} records upserted")
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        conn.rollback()
    finally:
        conn.close()

    return total


def main():
    parser = argparse.ArgumentParser(description='ASEAN Dashboard - Tier 1 Data Ingestion')
    parser.add_argument('--backfill', type=int, metavar='DAYS',
                        help='Backfill the last N days of data')
    parser.add_argument('--date', type=str, metavar='YYYY-MM-DD',
                        help='Override date for FX ingestion')
    parser.add_argument('--fx-only', action='store_true',
                        help='Only ingest FX rates')
    parser.add_argument('--market-only', action='store_true',
                        help='Only ingest yfinance data (US 10Y, Brent, Gold)')
    args = parser.parse_args()

    # Ensure DB exists
    from schema import init_db
    init_db()

    conn = get_conn()

    try:
        if args.backfill:
            backfill_fx(conn, args.backfill)
            backfill_yfinance(conn, args.backfill)
            conn.commit()
        elif args.fx_only:
            ingest_fx(conn, date_override=args.date)
            conn.commit()
        elif args.market_only:
            ingest_yfinance(conn)
            conn.commit()
        else:
            # Default: run everything
            ingest_fx(conn, date_override=args.date)
            ingest_yfinance(conn)
            conn.commit()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
