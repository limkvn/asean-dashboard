#!/usr/bin/env python3
"""
Daily Runner — ASEAN Markets Dashboard
========================================
Runs Tier 1 + Tier 2 ingestion, then regenerates the dashboard HTML.

Usage:
  python run_daily.py              # standard daily run
  python run_daily.py --init       # first-time setup: init DB, backfill, build dashboard
  python run_daily.py --dry-run    # show what would run without executing

Designed for cron:
  0 10 * * 1-5 cd /opt/asean-dashboard && /opt/asean-dashboard/venv/bin/python scripts/run_daily.py >> /var/log/asean-dashboard.log 2>&1
"""

import argparse
import os
import sys
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
SG_TZ = ZoneInfo("Asia/Singapore")


def log(msg):
    now = datetime.now(SG_TZ).strftime('%Y-%m-%d %H:%M:%S SGT')
    print(f"[{now}] {msg}", flush=True)


def run_script(name, args=None):
    """Run a sibling Python script. Returns True on success."""
    script_path = os.path.join(SCRIPT_DIR, name)
    cmd = [sys.executable, script_path]
    if args:
        cmd.extend(args)

    log(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)

    if result.returncode != 0:
        log(f"ERROR: {name} exited with code {result.returncode}")
        return False
    return True


def push_to_github():
    """Commit and push dashboard.html to GitHub Pages."""
    dashboard_path = os.path.join(PROJECT_DIR, 'dashboard.html')
    if not os.path.exists(dashboard_path):
        log("No dashboard.html to push")
        return False

    # Check if this is a git repo
    git_dir = os.path.join(PROJECT_DIR, '.git')
    if not os.path.exists(git_dir):
        log("Not a git repo — skipping push (run deploy.sh to set up)")
        return False

    now = datetime.now(SG_TZ).strftime('%Y-%m-%d %H:%M SGT')

    cmds = [
        ['git', '-C', PROJECT_DIR, 'add', 'dashboard.html'],
        ['git', '-C', PROJECT_DIR, 'diff', '--cached', '--quiet'],  # exits 1 if there are changes
    ]

    # Check if there are changes to commit
    result = subprocess.run(
        ['git', '-C', PROJECT_DIR, 'diff', '--cached', '--quiet', 'dashboard.html'],
        capture_output=True
    )

    # Also check unstaged changes
    result2 = subprocess.run(
        ['git', '-C', PROJECT_DIR, 'diff', '--quiet', 'dashboard.html'],
        capture_output=True
    )

    if result.returncode == 0 and result2.returncode == 0:
        log("No changes to dashboard.html — skipping push")
        return True

    # Stage, commit, push
    subprocess.run(['git', '-C', PROJECT_DIR, 'add', 'dashboard.html'], check=True)
    subprocess.run(
        ['git', '-C', PROJECT_DIR, 'commit', '-m', f'Update dashboard ({now})'],
        check=True
    )
    result = subprocess.run(
        ['git', '-C', PROJECT_DIR, 'push'],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        log(f"Git push failed: {result.stderr.strip()}")
        return False

    log("Dashboard pushed to GitHub Pages")
    return True


def run_daily():
    """Standard daily ingestion + dashboard rebuild."""
    log("=" * 60)
    log("DAILY RUN START")
    log("=" * 60)

    success = True

    # Tier 1: API-based (FX, US 10Y, Brent, Gold)
    if not run_script('ingest_tier1.py'):
        log("WARNING: Tier 1 ingestion had errors (continuing)")
        success = False

    # Tier 2: Web scraping (ASEAN bonds, commodities)
    if not run_script('ingest_tier2.py'):
        log("WARNING: Tier 2 ingestion had errors (continuing)")
        success = False

    # Rebuild dashboard HTML
    if not run_script('build_dashboard.py'):
        log("ERROR: Dashboard build failed")
        success = False

    # Push to GitHub Pages
    if not push_to_github():
        log("WARNING: Git push failed (dashboard still updated locally)")

    log("=" * 60)
    log(f"DAILY RUN {'COMPLETE' if success else 'COMPLETE WITH ERRORS'}")
    log("=" * 60)

    return success


def run_init():
    """First-time setup: init DB, backfill all history, build dashboard."""
    log("=" * 60)
    log("INITIALIZATION START")
    log("=" * 60)

    # Init DB schema
    if not run_script('schema.py'):
        log("ERROR: Schema init failed")
        return False

    # Tier 1 backfill (30 days)
    if not run_script('ingest_tier1.py', ['--backfill', '30']):
        log("WARNING: Tier 1 backfill had errors")

    # Tier 1 daily (get today's data incl. VND)
    if not run_script('ingest_tier1.py'):
        log("WARNING: Tier 1 daily had errors")

    # Tier 2 daily (get today's bond yields + commodities)
    if not run_script('ingest_tier2.py'):
        log("WARNING: Tier 2 daily had errors")

    # Tier 2 backfill (historical from Investing.com)
    if not run_script('ingest_tier2.py', ['--backfill']):
        log("WARNING: Tier 2 backfill had errors")

    # Build dashboard
    if not run_script('build_dashboard.py'):
        log("ERROR: Dashboard build failed")
        return False

    log("=" * 60)
    log("INITIALIZATION COMPLETE")
    log("=" * 60)
    return True


def main():
    parser = argparse.ArgumentParser(description='ASEAN Dashboard - Daily Runner')
    parser.add_argument('--init', action='store_true',
                        help='First-time setup: init DB, backfill, build dashboard')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would run without executing')
    args = parser.parse_args()

    if args.dry_run:
        log("DRY RUN — would execute:")
        if args.init:
            print("  1. schema.py")
            print("  2. ingest_tier1.py --backfill 30")
            print("  3. ingest_tier1.py")
            print("  4. ingest_tier2.py")
            print("  5. ingest_tier2.py --backfill")
            print("  6. build_dashboard.py")
        else:
            print("  1. ingest_tier1.py")
            print("  2. ingest_tier2.py")
            print("  3. build_dashboard.py")
        return

    if args.init:
        ok = run_init()
    else:
        ok = run_daily()

    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
