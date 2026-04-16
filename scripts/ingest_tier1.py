"""
Tier 1 Data Ingestion: yfinance sources
========================================
Pulls data from yfinance for:
  - ASEAN FX rates (USD/IDR, USD/MYR, USD/PHP, USD/THB, USD/VND)
  - US 10Y Treasury yield
  - Brent crude oil
  - Gold spot

Source URLs (per indicator) live in build_dashboard.py DISPLAY_URLS and
all point to the matching Yahoo Finance quote page so the dashboard's
displayed value can be cross-checked against the same source.

Usage:
  python ingest_tier1.py              # ingest latest data
  python ingest_tier1.py --backfill 30  # backfill last N days
  python ingest_tier1.py --fx-only       # FX only
  python ingest_tier1.py --market-only   # bonds + commodities only
"""

import argparse
import logging
import os
import sqlite3
from datetime import datetime

# yfinance is required (FX, bonds, and commodities all use it)
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

# FX tickers on Yahoo Finance: "{CCY}=X" returns the USD/{CCY} mid rate
# (units: per USD, same as what we previously stored from ExchangeRate-API).
FX_TICKERS = {
    'IDR': ('IDR=X', 'fx', 'per USD', 'yfinance:fx'),
    'MYR': ('MYR=X', 'fx', 'per USD', 'yfinance:fx'),
    'PHP': ('PHP=X', 'fx', 'per USD', 'yfinance:fx'),
    'THB': ('THB=X', 'fx', 'per USD', 'yfinance:fx'),
    'VND': ('VND=X', 'fx', 'per USD', 'yfinance:fx'),
}

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


# ---------- Generic yfinance fetch ----------

def _fetch_yf_latest(tickers_dict, label):
    """
    For a {indicator: (ticker, category, unit, source)} dict, fetch the
    most recent close for each and return a list of records to upsert.
    Returns (records_list, errors_list).
    """
    records = []
    errors = []
    for indicator, (ticker, category, unit, source) in tickers_dict.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period='5d')
            if hist.empty:
                logger.warning(f"  {indicator} ({ticker}): no data")
                errors.append(f"{indicator}: no data")
                continue
            latest = hist.iloc[-1]
            date_str = hist.index[-1].strftime('%Y-%m-%d')
            value = float(latest['Close'])
            records.append((date_str, category, indicator, value, unit, source))
            logger.info(f"  {indicator}: {value:.6f} {unit} ({date_str})")
        except Exception as e:
            logger.error(f"yfinance error for {indicator}: {e}")
            errors.append(f"{indicator}: {e}")
    logger.info(f"{label}: fetched {len(records)}/{len(tickers_dict)} tickers")
    return records, errors


def _backfill_yf(tickers_dict, days, label):
    """Backfill tickers_dict for the last N days. Returns list of records."""
    records = []
    period = f'{days}d'
    for indicator, (ticker, category, unit, source) in tickers_dict.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period=period)
            for idx, row in hist.iterrows():
                date_str = idx.strftime('%Y-%m-%d')
                value = float(row['Close'])
                records.append((date_str, category, indicator, value, unit, source))
            logger.info(f"  {indicator}: {len(hist)} days backfilled")
        except Exception as e:
            logger.error(f"Backfill error for {indicator}: {e}")
    logger.info(f"{label} backfill: {len(records)} records over {days} days")
    return records


# ---------- FX ingestion (yfinance) ----------

def ingest_fx(conn, date_override=None):
    """Fetch latest FX rates from yfinance and upsert into DB."""
    if not HAS_YFINANCE:
        logger.error("yfinance not available, skipping FX")
        log_ingestion(conn, 'yfinance:fx', 'error', 0, 'yfinance not installed')
        return 0

    logger.info("Fetching FX rates from yfinance...")
    records, errors = _fetch_yf_latest(FX_TICKERS, 'FX')

    count = 0
    for date_str, category, indicator, value, unit, source in records:
        if date_override:
            date_str = date_override
        upsert_record(conn, date_str, category, indicator, value, unit, source)
        count += 1

    status = 'success' if not errors else ('partial' if count > 0 else 'error')
    msg = f"Ingested {count}/{len(FX_TICKERS)} FX rates"
    if errors:
        msg += f" | Errors: {'; '.join(errors)}"
    log_ingestion(conn, 'yfinance:fx', status, count, msg)
    return count


def backfill_fx(conn, days=30):
    """Backfill FX rates from yfinance for the last N days."""
    if not HAS_YFINANCE:
        logger.error("yfinance not available")
        return 0

    logger.info(f"Backfilling FX rates for last {days} days from yfinance...")
    records = _backfill_yf(FX_TICKERS, days, 'FX')

    for date_str, category, indicator, value, unit, source in records:
        upsert_record(conn, date_str, category, indicator, value, unit, source)

    log_ingestion(conn, 'yfinance:fx-backfill', 'success', len(records),
                  f'Backfilled {len(records)} FX records over {days} days')
    return len(records)


# ---------- Bonds + commodities (yfinance) ----------

def ingest_yfinance(conn):
    """Ingest US 10Y, Brent, Gold from yfinance."""
    if not HAS_YFINANCE:
        logger.error("yfinance not available, skipping market data")
        log_ingestion(conn, 'yfinance', 'error', 0, 'yfinance not installed')
        return 0

    logger.info("Fetching market data from yfinance...")
    records, errors = _fetch_yf_latest(YFINANCE_TICKERS, 'Market data')

    count = 0
    for date_str, category, indicator, value, unit, source in records:
        upsert_record(conn, date_str, category, indicator, value, unit, source)
        count += 1

    status = 'success' if not errors else ('partial' if count > 0 else 'error')
    msg = f"Ingested {count}/{len(YFINANCE_TICKERS)} tickers"
    if errors:
        msg += f" | Errors: {'; '.join(errors)}"
    log_ingestion(conn, 'yfinance', status, count, msg)
    return count


def backfill_yfinance(conn, days=30):
    """Backfill yfinance market tickers for the last N days."""
    if not HAS_YFINANCE:
        logger.error("yfinance not available")
        return 0

    logger.info(f"Backfilling yfinance market data for last {days} days...")
    records = _backfill_yf(YFINANCE_TICKERS, days, 'Market data')

    for date_str, category, indicator, value, unit, source in records:
        upsert_record(conn, date_str, category, indicator, value, unit, source)

    log_ingestion(conn, 'yfinance-backfill', 'success', len(records),
                  f'Backfilled {len(records)} records over {days} days')
    return len(records)


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
                        help='Only ingest yfinance bond and commodity data')
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
