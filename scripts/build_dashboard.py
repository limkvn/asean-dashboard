"""
Dashboard Generator
====================
Reads the SQLite database and generates a self-contained HTML dashboard.

Usage:
  python build_dashboard.py                    # output to ../dashboard.html
  python build_dashboard.py -o /path/out.html  # custom output path
"""

import argparse
import json
import os
import sqlite3
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.abspath(os.path.join(SCRIPT_DIR, '..', 'data', 'dashboard.db'))
DEFAULT_OUTPUT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', 'dashboard.html'))


def load_data():
    """Load all dashboard data from the database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Latest value per indicator + previous value for change calc
    latest = {}
    for row in conn.execute('''
        SELECT d1.indicator, d1.date, d1.value, d1.unit, d1.source,
               d2.date AS prev_date, d2.value AS prev_value
        FROM daily_data d1
        LEFT JOIN daily_data d2 ON d1.indicator = d2.indicator
            AND d2.date = (SELECT MAX(date) FROM daily_data
                           WHERE indicator = d1.indicator AND date < d1.date)
        WHERE d1.date = (SELECT MAX(date) FROM daily_data WHERE indicator = d1.indicator)
        ORDER BY d1.indicator
    '''):
        latest[row['indicator']] = dict(row)

    # Indicator metadata
    indicators = {}
    for row in conn.execute('''
        SELECT i.indicator, i.category, i.label, i.unit, i.tier,
               ds.provider, ds.dataset, ds.ticker_or_id, ds.data_url,
               ds.frequency, ds.lag, ds.license_info, ds.notes
        FROM indicators i
        LEFT JOIN data_sources ds ON i.source = ds.source_key
        ORDER BY i.category, i.indicator
    '''):
        indicators[row['indicator']] = dict(row)

    # Time series for sparklines (all data, sorted by date)
    series = {}
    for row in conn.execute('''
        SELECT indicator, date, value FROM daily_data
        ORDER BY indicator, date
    '''):
        ind = row['indicator']
        if ind not in series:
            series[ind] = {'dates': [], 'values': []}
        series[ind]['dates'].append(row['date'])
        series[ind]['values'].append(row['value'])

    # Last ingestion info
    last_run = conn.execute('''
        SELECT run_at, source, status, records, message
        FROM ingestion_log ORDER BY id DESC LIMIT 5
    ''').fetchall()
    last_run = [dict(r) for r in last_run]

    # Data sources for attribution
    sources = []
    for row in conn.execute('SELECT * FROM data_sources ORDER BY source_key'):
        sources.append(dict(row))

    conn.close()

    return {
        'latest': latest,
        'indicators': indicators,
        'series': series,
        'last_run': last_run,
        'sources': sources,
        'generated_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
    }


def build_html(data):
    """Generate the full HTML dashboard string."""
    d = data
    generated_at = d['generated_at']

    # Pre-compute display data for each section
    def fmt_value(indicator, value):
        """Format a value for display based on indicator type."""
        if value is None:
            return '—'
        meta = d['indicators'].get(indicator, {})
        cat = meta.get('category', '')
        if indicator == 'VND' or indicator == 'IDR':
            return f'{value:,.0f}'
        elif cat == 'fx':
            return f'{value:,.3f}'
        elif cat == 'bond':
            return f'{value:.3f}'
        elif indicator in ('NICKEL', 'CPO'):
            return f'{value:,.0f}'
        elif indicator == 'GOLD':
            return f'{value:,.1f}'
        else:
            return f'{value:,.2f}'

    def calc_change(rec):
        """Calculate change and percentage change."""
        if rec.get('value') is not None and rec.get('prev_value') is not None:
            chg = rec['value'] - rec['prev_value']
            pct = (chg / rec['prev_value']) * 100 if rec['prev_value'] != 0 else 0
            return chg, pct
        return None, None

    # Build JSON data blob for Chart.js
    chart_data = {}
    for ind, s in d['series'].items():
        if len(s['values']) > 1:
            chart_data[ind] = {
                'labels': s['dates'],
                'values': s['values'],
            }

    chart_data_json = json.dumps(chart_data)

    # Build card data for each section
    fx_order = ['IDR', 'MYR', 'PHP', 'THB', 'VND']
    bond_order = ['US_10Y', 'ID_10Y', 'MY_10Y', 'PH_10Y', 'TH_10Y']
    commodity_order = ['BRENT', 'JKM_LNG', 'COAL_NEWC', 'CPO', 'RUBBER_TSR20', 'NICKEL', 'GOLD']

    def make_card_data(ind_list):
        cards = []
        for ind in ind_list:
            meta = d['indicators'].get(ind, {})
            rec = d['latest'].get(ind, {})
            value = rec.get('value')
            chg, pct = calc_change(rec)
            cards.append({
                'indicator': ind,
                'label': meta.get('label', ind),
                'value': fmt_value(ind, value),
                'raw_value': value,
                'unit': meta.get('unit', ''),
                'date': rec.get('date', '—'),
                'prev_date': rec.get('prev_date', ''),
                'change': chg,
                'change_pct': pct,
                'has_chart': ind in chart_data,
                'provider': meta.get('provider', ''),
                'dataset': meta.get('dataset', ''),
                'tier': meta.get('tier', ''),
            })
        return cards

    fx_cards = make_card_data(fx_order)
    bond_cards = make_card_data(bond_order)
    commodity_cards = make_card_data(commodity_order)

    # Display-URL overrides — the URLs stored in data_sources.data_url are
    # scraping endpoints (JSON APIs, template URLs, etc.) not always useful
    # for a human reader. Map each indicator to a friendly landing page.
    DISPLAY_URLS = {
        # FX — Yahoo Finance quote pages (USD/{ccy})
        'IDR': 'https://finance.yahoo.com/quote/IDR=X',
        'MYR': 'https://finance.yahoo.com/quote/MYR=X',
        'PHP': 'https://finance.yahoo.com/quote/PHP=X',
        'THB': 'https://finance.yahoo.com/quote/THB=X',
        'VND': 'https://finance.yahoo.com/quote/VND=X',
        # Bonds — ADB per-country pages
        'US_10Y': 'https://finance.yahoo.com/quote/%5ETNX',
        'ID_10Y': 'https://asianbondsonline.adb.org/economy/?economy=ID',
        'MY_10Y': 'https://asianbondsonline.adb.org/economy/?economy=MY',
        'PH_10Y': 'https://asianbondsonline.adb.org/economy/?economy=PH',
        'TH_10Y': 'https://asianbondsonline.adb.org/economy/?economy=TH',
        # Commodities — Yahoo Finance quote pages where applicable
        'BRENT':        'https://finance.yahoo.com/quote/BZ%3DF',
        'GOLD':         'https://finance.yahoo.com/quote/GC%3DF',
        'JKM_LNG':      'https://finance.yahoo.com/quote/JKM%3DF',
        'NICKEL':       'https://www.investing.com/commodities/nickel',
        'CPO':          'https://www.investing.com/commodities/palm-oil',
        'RUBBER_TSR20': 'https://www.investing.com/commodities/rubber-tsr20-futures',
        'COAL_NEWC':    'https://www.investing.com/commodities/newcastle-coal-futures',
    }

    # Source attribution rows
    source_rows = []
    for ind_key in fx_order + bond_order + commodity_order:
        meta = d['indicators'].get(ind_key, {})
        prov = meta.get('provider', '—')
        ds = meta.get('dataset', '—')
        ticker = meta.get('ticker_or_id', '')
        # Prefer a friendly display URL; fall back to the raw scraping URL
        url = DISPLAY_URLS.get(ind_key) or meta.get('data_url', '')
        freq = meta.get('frequency', '')
        lag = meta.get('lag', '')
        lic = meta.get('license_info', '')
        notes = meta.get('notes', '')
        source_rows.append({
            'indicator': ind_key,
            'label': meta.get('label', ind_key),
            'provider': prov,
            'dataset': ds,
            'ticker': ticker or '',
            'url': url or '',
            'frequency': freq,
            'lag': lag,
            'license': lic,
            'notes': notes or '',
        })

    source_rows_json = json.dumps(source_rows)

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ASEAN Markets Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root {{
    --bg: #0f1117;
    --surface: #1a1d27;
    --surface2: #242836;
    --border: #2e3348;
    --text: #e4e4e7;
    --text-dim: #9194a1;
    --accent: #6c8cff;
    --green: #34d399;
    --red: #f87171;
    --amber: #fbbf24;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
    padding: 24px;
    max-width: 1400px;
    margin: 0 auto;
  }}
  h1 {{
    font-size: 1.6rem;
    font-weight: 600;
    margin-bottom: 4px;
  }}
  .subtitle {{
    color: var(--text-dim);
    font-size: 0.85rem;
    margin-bottom: 28px;
  }}
  .section-title {{
    font-size: 1.05rem;
    font-weight: 600;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin: 32px 0 14px 0;
    padding-bottom: 8px;
    border-bottom: 1px solid var(--border);
  }}
  .section-title:first-of-type {{ margin-top: 0; }}
  .grid {{
    display: grid;
    gap: 14px;
  }}
  .grid-5 {{ grid-template-columns: repeat(5, 1fr); }}
  .grid-7 {{ grid-template-columns: repeat(4, 1fr); }}
  @media (max-width: 900px) {{
    .grid-7 {{ grid-template-columns: repeat(3, 1fr); }}
  }}
  @media (max-width: 768px) {{
    .grid-5, .grid-7 {{ grid-template-columns: repeat(2, 1fr); }}
  }}

  .card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 6px;
    transition: border-color 0.15s;
  }}
  .card:hover {{ border-color: var(--accent); }}
  .card-header {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
  }}
  .card-label {{
    font-size: 0.78rem;
    color: var(--text-dim);
    font-weight: 500;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex: 1;
  }}
  .card-download {{
    background: none;
    border: none;
    color: var(--text-dim);
    cursor: pointer;
    padding: 2px 4px;
    border-radius: 4px;
    font-size: 0.75rem;
    line-height: 1;
    opacity: 0.4;
    transition: all 0.15s;
  }}
  .card:hover .card-download {{ opacity: 1; }}
  .card-download:hover {{ color: var(--accent); background: var(--surface2); }}
  .card-value {{
    font-size: 1.45rem;
    font-weight: 700;
    font-variant-numeric: tabular-nums;
    letter-spacing: -0.02em;
  }}
  .card-unit {{
    font-size: 0.72rem;
    color: var(--text-dim);
    font-weight: 400;
  }}
  .card-change {{
    font-size: 0.78rem;
    font-weight: 500;
    font-variant-numeric: tabular-nums;
  }}
  .card-change.up {{ color: var(--green); }}
  .card-change.down {{ color: var(--red); }}
  .card-change.flat {{ color: var(--text-dim); }}
  .card-date {{
    font-size: 0.68rem;
    color: var(--text-dim);
  }}
  .spark-container {{
    height: 40px;
    margin-top: 4px;
  }}
  .spark-container canvas {{ width: 100% !important; height: 100% !important; }}

  /* Source attribution table */
  .sources-section {{
    margin-top: 48px;
    padding-top: 24px;
    border-top: 1px solid var(--border);
  }}
  .sources-toggle {{
    background: none;
    border: 1px solid var(--border);
    color: var(--text-dim);
    padding: 8px 16px;
    border-radius: 6px;
    cursor: pointer;
    font-size: 0.82rem;
    margin-bottom: 14px;
    transition: all 0.15s;
  }}
  .sources-toggle:hover {{ color: var(--text); border-color: var(--accent); }}
  .sources-table-wrap {{ display: none; overflow-x: auto; }}
  .sources-table-wrap.open {{ display: block; }}
  .sources-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.75rem;
  }}
  .sources-table th {{
    text-align: left;
    padding: 8px 10px;
    background: var(--surface2);
    color: var(--text-dim);
    font-weight: 600;
    white-space: nowrap;
    border-bottom: 1px solid var(--border);
  }}
  .sources-table td {{
    padding: 7px 10px;
    border-bottom: 1px solid var(--border);
    color: var(--text);
    vertical-align: top;
  }}
  .sources-table tr:hover td {{ background: var(--surface2); }}
  .sources-table a {{ color: var(--accent); text-decoration: none; }}
  .sources-table a:hover {{ text-decoration: underline; }}
  .tag {{
    display: inline-block;
    padding: 2px 7px;
    border-radius: 4px;
    font-size: 0.68rem;
    font-weight: 600;
  }}
  .tag-api {{ background: #1e3a2f; color: var(--green); }}
  .tag-scrape {{ background: #3a2f1e; color: var(--amber); }}
  .tag-manual {{ background: #2e2030; color: #c084fc; }}

  .footer {{
    margin-top: 32px;
    padding-top: 16px;
    border-top: 1px solid var(--border);
    color: var(--text-dim);
    font-size: 0.72rem;
    display: flex;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 8px;
  }}
</style>
</head>
<body>

<h1>ASEAN Markets Dashboard</h1>
<p class="subtitle">Last updated: {generated_at} &nbsp;·&nbsp; <a href="#" onclick="downloadAllCSV(); return false;" style="color:var(--accent);text-decoration:none;">Download all data (CSV)</a></p>

<!-- FX Section -->
<div class="section-title">Currency Performance vs USD</div>
<div class="grid grid-5" id="fx-grid"></div>

<!-- Bonds Section -->
<div class="section-title">10-Year Government Bond Yields</div>
<div class="grid grid-5" id="bond-grid"></div>

<!-- Commodities Section -->
<div class="section-title">Key Commodities</div>
<div class="grid grid-7" id="commodity-grid"></div>

<!-- Data Sources -->
<div class="sources-section">
  <button class="sources-toggle" onclick="toggleSources()">Show Data Sources &amp; Attribution</button>
  <div class="sources-table-wrap" id="sources-wrap">
    <table class="sources-table">
      <thead>
        <tr>
          <th>Indicator</th>
          <th>Provider</th>
          <th>Dataset</th>
          <th>Ticker / ID</th>
          <th>Frequency</th>
          <th>Lag</th>
          <th>License</th>
          <th>Notes</th>
        </tr>
      </thead>
      <tbody id="sources-body"></tbody>
    </table>
  </div>
</div>

<div class="footer">
  <span>ASEAN Markets Dashboard &mdash; Data from ExchangeRate-API, Yahoo Finance, Asian Bonds Online (ADB), Investing.com</span>
  <span>Generated: {generated_at}</span>
</div>

<script>
// === Embedded data ===
const CHART_DATA = {chart_data_json};
const SOURCE_ROWS = {source_rows_json};

const FX_CARDS = {json.dumps(fx_cards)};
const BOND_CARDS = {json.dumps(bond_cards)};
const COMMODITY_CARDS = {json.dumps(commodity_cards)};

// === Render cards ===
function changeClass(chg, indicator) {{
  // For FX: higher number = currency weakened vs USD, so flip the color
  const fxIndicators = ['IDR','MYR','PHP','THB','VND'];
  if (chg === null || chg === undefined) return 'flat';
  if (fxIndicators.includes(indicator)) {{
    return chg > 0.0001 ? 'down' : chg < -0.0001 ? 'up' : 'flat';
  }}
  return chg > 0.0001 ? 'up' : chg < -0.0001 ? 'down' : 'flat';
}}

function fmtChange(chg, pct) {{
  if (chg === null || chg === undefined) return '—';
  const sign = chg >= 0 ? '+' : '';
  const absChg = Math.abs(chg);
  let chgStr;
  if (absChg >= 100) chgStr = sign + chg.toFixed(0);
  else if (absChg >= 1) chgStr = sign + chg.toFixed(2);
  else chgStr = sign + chg.toFixed(4);
  const pctStr = pct !== null ? ` (${{pct >= 0 ? '+' : ''}}${{pct.toFixed(2)}}%)` : '';
  return chgStr + pctStr;
}}

function renderCards(containerId, cards) {{
  const grid = document.getElementById(containerId);
  cards.forEach((c, idx) => {{
    const cls = changeClass(c.change, c.indicator);
    const chgText = fmtChange(c.change, c.change_pct);
    const canvasId = `spark-${{c.indicator}}`;

    const card = document.createElement('div');
    card.className = 'card';
    card.innerHTML = `
      <div class="card-header">
        <div class="card-label">${{c.label}}</div>
        ${{c.has_chart ? `<button class="card-download" title="Download ${{c.indicator}} time series as CSV" onclick="downloadCSV('${{c.indicator}}', '${{c.label.replace(/'/g, "\\\\'")}}', '${{c.unit}}')">&#x2B07;</button>` : ''}}
      </div>
      <div>
        <span class="card-value">${{c.value}}</span>
        <span class="card-unit">${{c.unit}}</span>
      </div>
      <div class="card-change ${{cls}}" title="${{c.prev_date ? 'vs ' + c.prev_date : ''}}">${{chgText}}${{chgText !== '—' ? ' <span style="opacity:0.5;font-size:0.68rem">1d</span>' : ''}}</div>
      ${{c.has_chart ? `<div class="spark-container"><canvas id="${{canvasId}}"></canvas></div>` : ''}}
      <div class="card-date">as of ${{c.date}}</div>
    `;
    grid.appendChild(card);
  }});
}}

// === CSV download ===
function downloadAllCSV() {{
  const header = `date,value,indicator,unit\\n`;
  const allRows = [];
  for (const ind in CHART_DATA) {{
    const data = CHART_DATA[ind];
    const card = [...FX_CARDS, ...BOND_CARDS, ...COMMODITY_CARDS].find(c => c.indicator === ind);
    const unit = card ? card.unit : '';
    data.labels.forEach((date, i) => {{
      allRows.push(`${{date}},${{data.values[i]}},${{ind}},${{unit}}`);
    }});
  }}
  const csv = header + allRows.join('\\n') + '\\n';
  const blob = new Blob([csv], {{ type: 'text/csv;charset=utf-8;' }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `asean_dashboard_${{new Date().toISOString().slice(0,10)}}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}}

function downloadCSV(indicator, label, unit) {{
  const data = CHART_DATA[indicator];
  if (!data) return;
  const header = `date,value,indicator,unit\\n`;
  const rows = data.labels.map((date, i) => {{
    return `${{date}},${{data.values[i]}},${{indicator}},${{unit || ''}}`;
  }}).join('\\n');
  const csv = header + rows + '\\n';
  const blob = new Blob([csv], {{ type: 'text/csv;charset=utf-8;' }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `${{indicator}}_timeseries.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}}

renderCards('fx-grid', FX_CARDS);
renderCards('bond-grid', BOND_CARDS);
renderCards('commodity-grid', COMMODITY_CARDS);

// === Sparkline charts ===
function drawSparkline(canvasId, indicator) {{
  const el = document.getElementById(canvasId);
  if (!el || !CHART_DATA[indicator]) return;
  const d = CHART_DATA[indicator];
  const vals = d.values;
  const first = vals[0], last = vals[vals.length - 1];
  const color = last >= first ? '#34d399' : '#f87171';

  new Chart(el, {{
    type: 'line',
    data: {{
      labels: d.labels,
      datasets: [{{
        data: vals,
        borderColor: color,
        backgroundColor: color + '18',
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        borderWidth: 1.5,
      }}]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{ legend: {{ display: false }}, tooltip: {{ enabled: false }} }},
      scales: {{
        x: {{ display: false }},
        y: {{ display: false }}
      }},
      animation: false,
    }}
  }});
}}

const allIndicators = [...FX_CARDS, ...BOND_CARDS, ...COMMODITY_CARDS];
allIndicators.forEach(c => {{
  if (c.has_chart) drawSparkline(`spark-${{c.indicator}}`, c.indicator);
}});

// === Source attribution table ===
function toggleSources() {{
  const wrap = document.getElementById('sources-wrap');
  const btn = document.querySelector('.sources-toggle');
  wrap.classList.toggle('open');
  btn.textContent = wrap.classList.contains('open')
    ? 'Hide Data Sources & Attribution'
    : 'Show Data Sources & Attribution';
}}

(function renderSources() {{
  const tbody = document.getElementById('sources-body');
  SOURCE_ROWS.forEach(r => {{
    const tierTag = r.frequency === 'ad-hoc'
      ? '<span class="tag tag-manual">manual</span>'
      : r.url && r.url.includes('investing.com')
        ? '<span class="tag tag-scrape">scrape</span>'
        : r.url && r.url.includes('asianbondsonline')
          ? '<span class="tag tag-scrape">scrape</span>'
          : '<span class="tag tag-api">API</span>';

    const urlLink = r.url
      ? `<a href="${{r.url}}" target="_blank" rel="noopener">${{r.dataset}}</a>`
      : r.dataset;

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td><strong>${{r.indicator}}</strong><br><span style="color:var(--text-dim)">${{r.label}}</span></td>
      <td>${{r.provider}} ${{tierTag}}</td>
      <td>${{urlLink}}</td>
      <td>${{r.ticker || '—'}}</td>
      <td>${{r.frequency}}</td>
      <td>${{r.lag}}</td>
      <td>${{r.license}}</td>
      <td style="max-width:220px;font-size:0.7rem;color:var(--text-dim)">${{r.notes}}</td>
    `;
    tbody.appendChild(tr);
  }});
}})();
</script>
</body>
</html>'''

    return html


def main():
    parser = argparse.ArgumentParser(description='Generate ASEAN Markets Dashboard HTML')
    parser.add_argument('-o', '--output', default=DEFAULT_OUTPUT,
                        help='Output HTML file path')
    args = parser.parse_args()

    print(f"Reading database: {DB_PATH}")
    data = load_data()

    print(f"Building dashboard...")
    html = build_html(data)

    output_path = os.path.abspath(args.output)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"Dashboard written to: {output_path}")
    return output_path


if __name__ == '__main__':
    main()
