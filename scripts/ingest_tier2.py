"""
Tier 2 Data Ingestion: Web scraping sources
=============================================
Pulls data from:
  - Asian Bonds Online (ADB) for ASEAN 10Y government bond yields
  - Investing.com for commodities (Nickel, CPO, Rubber, JKM LNG, Coal Newcastle)

Usage:
  python ingest_tier2.py                # ingest all Tier 2 data
  python ingest_tier2.py --bonds-only   # only scrape bond yields
  python ingest_tier2.py --commod-only  # only scrape commodities

Note: These are web scraping sources and may break if site layouts change.
      The script includes error handling and logs all failures.
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    print("WARNING: beautifulsoup4 not installed. Run: pip install beautifulsoup4")

# ---------- Config ----------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.abspath(os.path.join(SCRIPT_DIR, '..', 'data', 'dashboard.db'))
LOG_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', 'logs'))

# Bond yield sources: Asian Bonds Online (ADB)
ADB_BONDS = {
    'ID_10Y': ('indonesia',   'Indonesia 10Y'),
    'MY_10Y': ('malaysia',    'Malaysia 10Y'),
    'PH_10Y': ('philippines', 'Philippines 10Y'),
    'TH_10Y': ('thailand',    'Thailand 10Y'),
}
ADB_BASE_URL = 'https://asianbondsonline.adb.org'

# Commodity sources: Investing.com
INVESTING_COMMODITIES = {
    'NICKEL':       ('https://www.investing.com/commodities/nickel',
                     'commodity', 'USD/tonne', 'investing.com:nickel'),
    'CPO':          ('https://www.investing.com/commodities/palm-oil',
                     'commodity', 'MYR/tonne', 'investing.com:cpo'),
    'RUBBER_TSR20': ('https://www.investing.com/commodities/rubber-tsr20-futures',
                     'commodity', 'USc/kg',    'investing.com:rubber'),
    'JKM_LNG':      ('https://www.investing.com/commodities/lng-japan-korea-marker-platts-futures',
                     'commodity', 'USD/MMBtu', 'investing.com:jkm'),
    'COAL_NEWC':    ('https://www.investing.com/commodities/newcastle-coal-futures',
                     'commodity', 'USD/tonne', 'investing.com:coal'),
}

BROWSER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

# Polite delay between requests (seconds)
REQUEST_DELAY = 2

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
    if not os.path.exists(DB_PATH):
        import schema
        schema.init_db()
    return sqlite3.connect(DB_PATH)


def upsert_record(conn, date_str, category, indicator, value, unit, source):
    now = datetime.utcnow().isoformat()
    conn.execute('''
        INSERT INTO daily_data (date, category, indicator, value, unit, source, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (date, indicator)
        DO UPDATE SET value=excluded.value, unit=excluded.unit,
                      source=excluded.source, ingested_at=excluded.ingested_at
    ''', (date_str, category, indicator, value, unit, source, now))


def log_ingestion(conn, source, status, records, message=''):
    now = datetime.utcnow().isoformat()
    conn.execute('''
        INSERT INTO ingestion_log (run_at, source, status, records, message)
        VALUES (?, ?, ?, ?, ?)
    ''', (now, source, status, records, message))


# ---------- Bond yield scraping (ADB) ----------

def scrape_adb_yield(slug):
    """
    Scrape 10-year government bond yield from Asian Bonds Online.
    Returns (yield_value, date_str) or (None, None) on failure.
    """
    url = f'{ADB_BASE_URL}/{slug}/'
    req = Request(url, headers=BROWSER_HEADERS)

    try:
        resp = urlopen(req, timeout=15)
        html = resp.read().decode('utf-8')
    except (URLError, HTTPError) as e:
        logger.error(f"ADB request failed for {slug}: {e}")
        return None, None

    if not HAS_BS4:
        logger.error("BeautifulSoup not available")
        return None, None

    soup = BeautifulSoup(html, 'html.parser')

    # Find the "10 Year" row in any table
    for table in soup.find_all('table'):
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                if label == '10 Year':
                    value_text = cells[1].get_text(strip=True)
                    match = re.match(r'([\d.]+)', value_text)
                    if match:
                        return float(match.group(1)), None

    logger.warning(f"Could not find 10Y yield for {slug}")
    return None, None


def ingest_bonds(conn):
    """Scrape and store ASEAN 10Y bond yields from ADB."""
    logger.info("Scraping ASEAN bond yields from Asian Bonds Online...")
    today = datetime.utcnow().strftime('%Y-%m-%d')
    count = 0
    errors = []

    for indicator, (slug, label) in ADB_BONDS.items():
        try:
            value, _ = scrape_adb_yield(slug)
            if value is not None:
                upsert_record(conn, today, 'bond', indicator, value,
                              'percent', 'adb:asianbondsonline')
                count += 1
                logger.info(f"  {indicator} ({label}): {value:.3f}%")
            else:
                errors.append(f"{indicator}: parse failed")
                logger.warning(f"  {indicator}: could not extract yield")

            time.sleep(REQUEST_DELAY)  # be polite

        except Exception as e:
            errors.append(f"{indicator}: {e}")
            logger.error(f"  {indicator}: {e}")

    status = 'success' if not errors else ('partial' if count > 0 else 'error')
    msg = f"Ingested {count}/{len(ADB_BONDS)} bond yields"
    if errors:
        msg += f" | Errors: {'; '.join(errors)}"
    log_ingestion(conn, 'adb:asianbondsonline', status, count, msg)

    return count


# ---------- Commodity scraping (Investing.com) ----------

def scrape_investing_price(url):
    """
    Scrape commodity price from Investing.com page.
    Extracts the 'last' price from embedded JSON data.
    Returns (last_price, prev_close, currency) or (None, None, None).
    """
    req = Request(url, headers=BROWSER_HEADERS)

    try:
        resp = urlopen(req, timeout=20)
        html = resp.read().decode('utf-8', errors='replace')
    except (URLError, HTTPError) as e:
        logger.error(f"Investing.com request failed for {url}: {e}")
        return None, None, None

    # Extract price from JSON blobs embedded in HTML
    # Pattern: {"lastClose": NNN, ..., "last": NNN, ..., "currency": "XXX"}
    # or: {"last": NNN, ..., "lastClose": NNN, ...}
    last_match = re.search(r'"last"\s*:\s*([\d.]+)', html)
    prev_match = re.search(r'"lastClose"\s*:\s*([\d.]+)', html)
    curr_match = re.search(r'"currency"\s*:\s*"([^"]+)"', html)

    if last_match:
        last = float(last_match.group(1))
        prev = float(prev_match.group(1)) if prev_match else None
        currency = curr_match.group(1) if curr_match else None
        return last, prev, currency

    return None, None, None


def ingest_commodities(conn):
    """Scrape and store commodity prices from Investing.com."""
    logger.info("Scraping commodity prices from Investing.com...")
    today = datetime.utcnow().strftime('%Y-%m-%d')
    count = 0
    errors = []

    for indicator, (url, category, unit, source_key) in INVESTING_COMMODITIES.items():
        try:
            last, prev, currency = scrape_investing_price(url)

            if last is not None:
                upsert_record(conn, today, category, indicator, last, unit, source_key)
                count += 1
                prev_str = f"  prev={prev:.2f}" if prev else ""
                ccy_str = f"  ({currency})" if currency else ""
                logger.info(f"  {indicator}: {last:.2f} {unit}{prev_str}{ccy_str}")
            else:
                errors.append(f"{indicator}: parse failed")
                logger.warning(f"  {indicator}: could not extract price from {url}")

            time.sleep(REQUEST_DELAY)  # be polite

        except Exception as e:
            errors.append(f"{indicator}: {e}")
            logger.error(f"  {indicator}: {e}")

    status = 'success' if not errors else ('partial' if count > 0 else 'error')
    msg = f"Ingested {count}/{len(INVESTING_COMMODITIES)} commodities"
    if errors:
        msg += f" | Errors: {'; '.join(errors)}"
    log_ingestion(conn, 'investing.com', status, count, msg)

    return count


# ---------- Backfill via Investing.com historical pages ----------

# Investing.com historical data page slugs (append "-historical-data" to base path)
INVESTING_HISTORY_SLUGS = {
    # Bonds
    'ID_10Y': ('rates-bonds/indonesia-10-year-bond-yield', 'bond', 'percent', 'investing.com:backfill'),
    'MY_10Y': ('rates-bonds/malaysia-10-year-bond-yield',  'bond', 'percent', 'investing.com:backfill'),
    'PH_10Y': ('rates-bonds/philippines-10-year-bond-yield','bond', 'percent', 'investing.com:backfill'),
    'TH_10Y': ('rates-bonds/thailand-10-year-bond-yield',  'bond', 'percent', 'investing.com:backfill'),
    # Commodities
    'NICKEL':       ('commodities/nickel',                          'commodity', 'USD/tonne', 'investing.com:nickel'),
    'CPO':          ('commodities/palm-oil',                        'commodity', 'MYR/tonne', 'investing.com:cpo'),
    'RUBBER_TSR20': ('commodities/rubber-tsr20-futures',            'commodity', 'USc/kg',    'investing.com:rubber'),
    'JKM_LNG':      ('commodities/lng-japan-korea-marker-platts-futures', 'commodity', 'USD/MMBtu', 'investing.com:jkm'),
    'COAL_NEWC':    ('commodities/newcastle-coal-futures',          'commodity', 'USD/tonne', 'investing.com:coal'),
    # FX
    'VND':          ('currencies/usd-vnd',                          'fx', 'per USD', 'investing.com:backfill'),
}


def scrape_investing_historical(url_path):
    """
    Scrape the historical data table from an Investing.com *-historical-data page.
    Returns list of (date_str, price) tuples, newest first.
    """
    url = f'https://www.investing.com/{url_path}-historical-data'
    req = Request(url, headers=BROWSER_HEADERS)

    try:
        resp = urlopen(req, timeout=20)
        html = resp.read().decode('utf-8', errors='replace')
    except (URLError, HTTPError) as e:
        logger.error(f"Historical page request failed: {url}: {e}")
        return []

    if not HAS_BS4:
        logger.error("BeautifulSoup not available for historical scraping")
        return []

    soup = BeautifulSoup(html, 'html.parser')
    results = []

    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if len(rows) < 5:
            continue
        header_cells = [th.get_text(strip=True) for th in rows[0].find_all(['td', 'th'])]
        if 'Date' not in header_cells or 'Price' not in header_cells:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all('td')]
            if len(cells) >= 2:
                try:
                    from datetime import datetime as dt_cls
                    date_parsed = dt_cls.strptime(cells[0], '%b %d, %Y')
                    price = float(cells[1].replace(',', ''))
                    results.append((date_parsed.strftime('%Y-%m-%d'), price))
                except (ValueError, IndexError):
                    continue
        break  # use the first matching table only

    return results


def backfill_tier2(conn):
    """Backfill all Tier 2 indicators using Investing.com historical pages."""
    logger.info("=" * 60)
    logger.info("BACKFILLING TIER 2 DATA (Investing.com historical pages)")
    logger.info("=" * 60)

    total_count = 0
    errors = []

    for indicator, (slug, category, unit, source_key) in INVESTING_HISTORY_SLUGS.items():
        try:
            data = scrape_investing_historical(slug)
            if data:
                count = 0
                for date_str, price in data:
                    upsert_record(conn, date_str, category, indicator, price, unit, source_key)
                    count += 1
                total_count += count
                logger.info(f"  {indicator:14s}: {count} days backfilled "
                            f"({data[-1][0]} to {data[0][0]})")
            else:
                errors.append(f"{indicator}: no historical data found")
                logger.warning(f"  {indicator}: no historical data from {slug}")

            time.sleep(REQUEST_DELAY)

        except Exception as e:
            errors.append(f"{indicator}: {e}")
            logger.error(f"  {indicator}: backfill error - {e}")

    status = 'success' if not errors else ('partial' if total_count > 0 else 'error')
    msg = f"Backfilled {total_count} records across {len(INVESTING_HISTORY_SLUGS)} indicators"
    if errors:
        msg += f" | Errors: {'; '.join(errors)}"
    log_ingestion(conn, 'investing.com:backfill', status, total_count, msg)

    logger.info(f"Tier 2 backfill complete: {total_count} total records")
    return total_count


# ---------- Main ----------

def run_daily():
    """Run the standard daily Tier 2 ingestion."""
    logger.info("=" * 60)
    logger.info("STARTING TIER 2 INGESTION (web scraping)")
    logger.info("=" * 60)

    conn = get_conn()
    total = 0

    try:
        total += ingest_bonds(conn)
        total += ingest_commodities(conn)
        conn.commit()
        logger.info(f"Tier 2 ingestion complete: {total} records upserted")
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        conn.rollback()
    finally:
        conn.close()

    return total


def main():
    parser = argparse.ArgumentParser(description='ASEAN Dashboard - Tier 2 Data Ingestion (Web Scraping)')
    parser.add_argument('--bonds-only', action='store_true',
                        help='Only scrape bond yields')
    parser.add_argument('--commod-only', action='store_true',
                        help='Only scrape commodities')
    parser.add_argument('--backfill', action='store_true',
                        help='Backfill historical data from Investing.com (~20-25 days)')
    args = parser.parse_args()

    # Ensure DB exists
    from schema import init_db
    init_db()

    conn = get_conn()

    try:
        if args.backfill:
            backfill_tier2(conn)
        elif args.bonds_only:
            ingest_bonds(conn)
        elif args.commod_only:
            ingest_commodities(conn)
        else:
            ingest_bonds(conn)
            ingest_commodities(conn)
        conn.commit()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
