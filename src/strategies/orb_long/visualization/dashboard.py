"""
Local HTTP dashboard for the ORB long strategy.

Starts an aiohttp server on ``localhost:DASHBOARD_PORT`` as a background
task on streamer startup. Two endpoints:

    GET /               -- inline HTML page (TradingView Lightweight Charts
                           candlestick chart per symbol, with the breakout
                           level and stop level drawn as price lines and
                           fires shown as arrow markers)
    GET /api/state      -- JSON snapshot of the in-memory ORB state

The page polls ``/api/state`` every 1000 ms. Nothing here touches the
database -- the dashboard reads only the in-process state maintained by
``orb_state``, so it's cheap and stays perfectly in sync with what the
strategy actually sees.

If ``aiohttp`` isn't installed (unlikely, since ib_async depends on it),
the startup routine logs a warning and returns; the streamer continues
without a dashboard.
"""

from __future__ import annotations

import logging
from typing import Optional

from . import state as orb_state

logger = logging.getLogger(__name__)


DASHBOARD_PORT: int = 8790
DASHBOARD_HOST: str = "127.0.0.1"


_DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ORB long dashboard</title>
<style>
  :root {
    --bg: #f6f5f0; --card: #ffffff; --text: #1c1c1a; --muted: #6b6a63;
    --border: #d3d1c7; --blue: #185fa5; --coral: #d85a30; --green: #3b6d11;
    --amber: #ba7517; --red: #a32d2d;
    --amber-bg: #faeeda; --blue-bg: #e6f1fb; --green-bg: #eaf3de; --gray-bg: #f1efe8;
  }
  * { box-sizing: border-box; }
  body { margin: 0; padding: 20px; background: var(--bg); color: var(--text);
         font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
  header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }
  h1 { font-size: 18px; font-weight: 500; margin: 0; }
  .meta { font-size: 12px; color: var(--muted); }
  .grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; }
  .head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
  .sym { font-weight: 500; font-size: 15px; letter-spacing: 0.5px; }
  .info { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
          gap: 8px 10px; font-size: 11px; margin: 6px 0 10px; }
  .info .k { color: var(--muted); margin-bottom: 2px; }
  .info .v { font-weight: 500; font-size: 12px; }
  .chart-container { width: 100%; height: 280px; }
  .empty { padding: 40px; text-align: center; color: var(--muted); font-size: 13px; grid-column: 1 / -1; }
  .checks { display: flex; flex-direction: column; gap: 4px; margin-top: 12px;
            padding-top: 10px; border-top: 1px solid var(--border); font-size: 12px; }
  .check { display: flex; align-items: center; gap: 10px;
           padding: 6px 10px; border-radius: 4px; }
  .check.ok { background: #eaf3de; }
  .check.fail { background: #fcebeb; }
  .box { display: inline-flex; align-items: center; justify-content: center;
         width: 16px; height: 16px; border-radius: 3px; border: 1.5px solid transparent;
         font-size: 12px; font-weight: 500; line-height: 1; flex-shrink: 0; }
  .box.ok { background: #3b6d11; border-color: #3b6d11; color: #ffffff; }
  .box.fail { background: #a32d2d; border-color: #a32d2d; color: #ffffff; }
  .check .label { color: var(--text); }
  .check.ok .label { color: #173404; }
  .check.fail .label { color: #501313; }
  .check .detail { color: var(--muted); font-size: 11px; margin-left: auto; }
  .check.ok .detail { color: #3b6d11; }
  .check.fail .detail { color: #a32d2d; }
</style>
</head>
<body>
<header>
  <h1>ORB long dashboard</h1>
  <div class="meta"><span id="stamp">--</span> &nbsp;polling every 1000 ms</div>
</header>
<div id="grid" class="grid">
  <div class="empty">Waiting for historical 2-min candles to seed...</div>
</div>

<script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
<script>
const cards = {};
const STOP_OFFSET = 0.02;

async function poll() {
  try {
    const r = await fetch('/api/state', {cache: 'no-store'});
    const data = await r.json();
    document.getElementById('stamp').textContent = 'as of ' + data.generated_at;
    render(data);
  } catch (e) {
    document.getElementById('stamp').textContent = 'connection lost';
  }
  setTimeout(poll, 1000);
}

function createCard(sym) {
  const el = document.createElement('div');
  el.className = 'card';
  el.innerHTML = ''
    + '<div class="head"><div class="sym">' + sym + '</div></div>'
    + '<div class="info">'
      + '<div><div class="k">last 5s tick</div><div class="v last-t">--</div></div>'
      + '<div><div class="k">last 5s price</div><div class="v last-c">--</div></div>'
      + '<div><div class="k">ref 2m candle</div><div class="v ref-t">--</div></div>'
      + '<div><div class="k">level to watch</div><div class="v ref-c">--</div></div>'
      + '<div><div class="k">stop level</div><div class="v stop">--</div></div>'
    + '</div>'
    + '<div class="chart-container"></div>'
    + '<div class="checks">'
      + '<div class="check chk-rvol"><span class="box fail">&#10007;</span>'
        + '<span class="label">Rvol &gt; 3</span> <span class="detail">--</span></div>'
      + '<div class="check chk-yhi"><span class="box fail">&#10007;</span>'
        + '<span class="label">price &gt; yesterday high</span> <span class="detail">--</span></div>'
      + '<div class="check chk-ycl"><span class="box fail">&#10007;</span>'
        + '<span class="label">price &gt; yesterday close</span> <span class="detail">--</span></div>'
    + '</div>';

  const chartContainer = el.querySelector('.chart-container');
  const chart = LightweightCharts.createChart(chartContainer, {
    width: chartContainer.clientWidth,
    height: 280,
    layout: { background: {type: 'solid', color: '#ffffff'}, textColor: '#1c1c1a', fontSize: 10 },
    grid: { vertLines: {color: '#eee'}, horzLines: {color: '#eee'} },
    timeScale: { timeVisible: true, secondsVisible: true, borderColor: '#d3d1c7' },
    rightPriceScale: { borderColor: '#d3d1c7' },
    crosshair: { mode: 1 },
    autoSize: true,
  });
  const series = chart.addCandlestickSeries({
    upColor: '#3b6d11',
    downColor: '#d85a30',
    borderVisible: false,
    wickUpColor: '#3b6d11',
    wickDownColor: '#d85a30',
    priceFormat: {type: 'price', precision: 2, minMove: 0.01},
  });

  return {
    symbol: sym,
    element: el,
    chart: chart,
    series: series,
    breakoutLine: null,
    stopLine: null,
    lastRefClose: null,
    lastRefLow: null,
    lastRefTime: null,
    lastRefField: null,
    lastStopDrawn: null,
    lastFireCount: -1,
    lastBarTs: 0,
  };
}

function updateInfo(card, sym) {
  const q = (sel) => card.element.querySelector(sel);
  q('.last-t').textContent = sym.last_bar_time || '--';
  q('.last-c').textContent = sym.last_bar_close != null ? sym.last_bar_close.toFixed(2) : '--';
  q('.ref-t').textContent = sym.ref_time || '--';
  q('.ref-c').textContent = sym.ref_close != null ? sym.ref_close.toFixed(2) : '--';
  const lf = (sym.fires && sym.fires.length) ? sym.fires[sym.fires.length - 1] : null;
  q('.stop').textContent = lf ? lf.stop.toFixed(2) : '--';
  updateChecks(card, sym);
}

function setCheck(card, cls, ok, detail) {
  // Two states only. Anything that isn't explicitly true (including missing
  // data) renders red. Tint applies to the whole row.
  const row = card.element.querySelector('.' + cls);
  const box = row.querySelector('.box');
  if (ok === true) {
    row.classList.remove('fail');
    row.classList.add('ok');
    box.className = 'box ok';
    box.innerHTML = '&#10003;';   // check mark
  } else {
    row.classList.remove('ok');
    row.classList.add('fail');
    box.className = 'box fail';
    box.innerHTML = '&#10007;';   // cross
  }
  row.querySelector('.detail').textContent = detail;
}

function updateChecks(card, sym) {
  const price = sym.last_bar_close;
  // Rvol > 3
  const rvolText = sym.latest_rvol != null
    ? 'Rvol = ' + sym.latest_rvol.toFixed(2)
    : 'Rvol = -- (no live 2m candle yet)';
  setCheck(card, 'chk-rvol', sym.latest_rvol != null && sym.latest_rvol > 3, rvolText);
  // price > yesterday high (RTH)
  const yhiText = (price != null && sym.yesterday_high != null)
    ? ('price ' + price.toFixed(2) + '  vs  yhi ' + sym.yesterday_high.toFixed(2))
    : 'waiting for data';
  setCheck(card, 'chk-yhi',
    price != null && sym.yesterday_high != null && price > sym.yesterday_high,
    yhiText);
  // price > yesterday close (RTH)
  const yclText = (price != null && sym.yesterday_close != null)
    ? ('price ' + price.toFixed(2) + '  vs  yclose ' + sym.yesterday_close.toFixed(2))
    : 'waiting for data';
  setCheck(card, 'chk-ycl',
    price != null && sym.yesterday_close != null && price > sym.yesterday_close,
    yclText);
}

function updateChart(card, sym) {
  const candles = sym.candles || [];
  if (!candles.length) return;

  // First render: bulk-load the whole 2-min OHLC series (historical seed
  // + current in-progress candle). Subsequent polls only push candles
  // at or after the last known timestamp, so the in-progress candle
  // animates in place via series.update() with the same time value.
  if (card.lastBarTs === 0) {
    card.series.setData(candles.map(k => ({
      time: k.ts, open: k.o, high: k.h, low: k.l, close: k.c
    })));
    card.lastBarTs = candles[candles.length - 1].ts;
  } else {
    for (const k of candles) {
      if (k.ts >= card.lastBarTs) {
        card.series.update({time: k.ts, open: k.o, high: k.h, low: k.l, close: k.c});
        card.lastBarTs = k.ts;
      }
    }
  }

  // Level to watch = ref_close. In test mode this is max(High) of the last
  // two 2-min candles; in production it's the 16:32 close. Line is
  // recreated every poll so it can never go stale, and it appears the
  // instant the reference becomes available.
  if (sym.ref_close != null) {
    if (sym.ref_close !== card.lastRefClose || sym.ref_time !== card.lastRefTime || sym.ref_field !== card.lastRefField) {
      if (card.breakoutLine) card.series.removePriceLine(card.breakoutLine);
      // Compact HH:MM form for the label; ref_time comes in as ISO "HH:MM:SS".
      const t = sym.ref_time ? sym.ref_time.slice(0, 5) : '';
      const field = sym.ref_field || 'ref';
      card.breakoutLine = card.series.createPriceLine({
        price: sym.ref_close,
        color: '#d85a30',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Solid,
        axisLabelVisible: true,
        title: field + ' ' + t + ' ',
      });
      card.lastRefClose = sym.ref_close;
      card.lastRefTime = sym.ref_time;
      card.lastRefField = sym.ref_field;
    }
  } else if (card.breakoutLine) {
    // Reference disappeared (e.g. livestream table truncated). Clear the
    // stale line so the chart doesn't lie.
    card.series.removePriceLine(card.breakoutLine);
    card.breakoutLine = null;
    card.lastRefClose = null;
    card.lastRefTime = null;
    card.lastRefField = null;
  }

  // Stop price line -- only drawn AFTER a breakout has fired. Anchored to
  // the stop that came with the most recent fire, so it reflects the
  // reference candle whose low was used at fire time (which may differ
  // from the current reference if it has since rolled forward). The line
  // vanishes if no fires exist yet.
  const lastFire = (sym.fires && sym.fires.length) ? sym.fires[sym.fires.length - 1] : null;
  if (lastFire) {
    if (lastFire.stop !== card.lastStopDrawn) {
      if (card.stopLine) card.series.removePriceLine(card.stopLine);
      card.stopLine = card.series.createPriceLine({
        price: lastFire.stop,
        color: '#a32d2d',
        lineWidth: 2,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true,
        title: 'stop',
      });
      card.lastStopDrawn = lastFire.stop;
    }
  } else if (card.stopLine) {
    card.series.removePriceLine(card.stopLine);
    card.stopLine = null;
    card.lastStopDrawn = null;
  }

  // Fire markers
  const fires = sym.fires || [];
  if (fires.length !== card.lastFireCount) {
    const markers = fires.map(f => ({
      time: f.ts,
      position: 'belowBar',
      color: '#3b6d11',
      shape: 'arrowUp',
      text: 'FIRE @ ' + f.c.toFixed(2) + '  stop ' + f.stop.toFixed(2),
    }));
    card.series.setMarkers(markers);
    card.lastFireCount = fires.length;
  }
}

function render(data) {
  const grid = document.getElementById('grid');
  const empty = grid.querySelector('.empty');
  if (!data.symbols || !data.symbols.length) {
    if (!empty) grid.innerHTML = '<div class="empty">Waiting for historical 2-min candles to seed...</div>';
    return;
  }
  if (empty) grid.innerHTML = '';

  const seen = new Set();
  data.symbols.forEach(sym => {
    seen.add(sym.symbol);
    let card = cards[sym.symbol];
    if (!card) {
      card = createCard(sym.symbol);
      grid.appendChild(card.element);
      cards[sym.symbol] = card;
    }
    updateInfo(card, sym);
    updateChart(card, sym);
  });

  Object.keys(cards).forEach(sym => {
    if (!seen.has(sym)) {
      cards[sym].chart.remove();
      cards[sym].element.remove();
      delete cards[sym];
    }
  });
}

poll();
</script>
</body>
</html>"""


_runner = None


async def start_dashboard(port: int = DASHBOARD_PORT) -> Optional[object]:
    """
    Start the aiohttp server as a background task. Returns the AppRunner
    on success, or ``None`` if aiohttp isn't installed (in which case we
    log a warning and let the streamer continue).
    """
    global _runner
    try:
        from aiohttp import web
    except Exception:
        logger.warning(
            "ORB dashboard: aiohttp not installed -- dashboard disabled. "
            "Run `pip install aiohttp` to enable, then restart the streamer."
        )
        return None

    async def _index(_request):
        return web.Response(text=_DASHBOARD_HTML, content_type="text/html")

    async def _state(_request):
        return web.json_response(orb_state.snapshot())

    app = web.Application()
    app.router.add_get("/", _index)
    app.router.add_get("/api/state", _state)

    # Silence aiohttp's per-request access log entirely; the request
    # volume is high and the info isn't useful for this workflow.
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, DASHBOARD_HOST, port)
    await site.start()
    _runner = runner
    logger.info("ORB dashboard live at http://%s:%d", DASHBOARD_HOST, port)
    return runner
