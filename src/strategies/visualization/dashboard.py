"""
Unified strategy dashboard.

One aiohttp server serving one page at ``localhost:DASHBOARD_PORT``.
Every symbol appears once per active strategy as a separate card in a
single flat grid; each card's header shows ``<SYMBOL>`` + the strategy
name, and the card body only carries that strategy's fields, filters,
and chart overlays.

Merged snapshot shape returned by ``/api/state``:

    {
      "generated_at": "...",
      "meta": {
        "orb":      {},
        "reversal": {"capitulation_threshold": 3.0, "recent_relatr_window": 3}
      },
      "symbols": [
        {
          "symbol": "DFNS",
          "candles": [...],           # shared: same 2m bars for both strategies
          "last_bar_time": "...",
          "last_bar_close": 6.20,
          "orb":      { ref_close, ref_low, ref_time, ref_field,
                        yesterday_high, yesterday_close, latest_rvol, fires },
          "reversal": { ref_close, ref_low, ref_time, ref_field,
                        recent_max_relatr, fires }
        },
        ...
      ]
    }

Adding a new strategy's data pipe:
    1. Import its viz ``state`` module here; add its symbol-level fields
       under a new sub-key in ``_merged_snapshot``, and any global meta
       under ``meta.<strategy_key>``.
    2. In the JS, add a ``createXCard`` / ``updateXCard`` pair and one
       line in ``render()`` to emit those cards into the grid.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from src.strategies.orb_long.visualization import state as orb_viz
from src.strategies.reversal_long.visualization import state as reversal_viz
from src.strategies.state import get_watchlist_strategies_for

logger = logging.getLogger(__name__)


DASHBOARD_PORT: int = 8790
DASHBOARD_HOST: str = "127.0.0.1"


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _merged_snapshot() -> dict:
    """Merge the per-strategy snapshots into a single flat symbol list."""
    orb_snap = orb_viz.snapshot()
    rev_snap = reversal_viz.snapshot()

    orb_by_sym = {s["symbol"]: s for s in orb_snap.get("symbols", [])}
    rev_by_sym = {s["symbol"]: s for s in rev_snap.get("symbols", [])}
    all_syms = sorted(set(orb_by_sym) | set(rev_by_sym))

    merged = []
    for sym in all_syms:
        o = orb_by_sym.get(sym, {})
        r = rev_by_sym.get(sym, {})
        candles = o.get("candles") or r.get("candles") or []
        last_bar_time = o.get("last_bar_time") or r.get("last_bar_time")
        last_bar_close = (
            o.get("last_bar_close")
            if o.get("last_bar_close") is not None
            else r.get("last_bar_close")
        )
        merged.append({
            "symbol": sym,
            # The set of entry-strategy names the user armed for this symbol
            # via the watchlist. Rendered on the client to decide which cards
            # to emit -- a symbol with only ``reversal_long_breakout`` armed
            # will NOT get an ORB card, and vice versa.
            "strategies": sorted(get_watchlist_strategies_for(sym)),
            "candles": candles,
            "last_bar_time": last_bar_time,
            "last_bar_close": last_bar_close,
            "orb": {
                "ref_time":        o.get("ref_time"),
                "ref_close":       o.get("ref_close"),
                "ref_low":         o.get("ref_low"),
                "ref_field":       o.get("ref_field"),
                "yesterday_high":  o.get("yesterday_high"),
                "yesterday_close": o.get("yesterday_close"),
                "latest_rvol":     o.get("latest_rvol"),
                "fires":           o.get("fires", []),
            },
            "reversal": {
                "ref_time":          r.get("ref_time"),
                "ref_close":         r.get("ref_close"),
                "ref_low":           r.get("ref_low"),
                "ref_field":         r.get("ref_field"),
                "recent_max_relatr": r.get("recent_max_relatr"),
                "fires":             r.get("fires", []),
            },
        })

    return {
        "generated_at": _now_iso(),
        "meta": {
            "orb": {},
            "reversal": {
                "capitulation_threshold": rev_snap.get("capitulation_threshold"),
                "recent_relatr_window":   rev_snap.get("recent_relatr_window"),
            },
        },
        "symbols": merged,
    }


_DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Strategy dashboard</title>
<style>
  :root {
    --bg: #f6f5f0; --card: #ffffff; --text: #1c1c1a; --muted: #6b6a63;
    --border: #d3d1c7; --blue: #185fa5; --coral: #d85a30; --green: #3b6d11;
    --amber: #ba7517; --red: #a32d2d;
    /* Shared overlay palette -- both strategies use the same colours so
       "breakout level", "stop", and "fire" mean the same thing on any
       chart. The strategy is identified via the header tag below. */
    --breakout-line: #d85a30;
    --stop-line:     #a32d2d;
    --fire-color:    #3b6d11;
    /* Header tag identifier colours (per strategy). */
    --tag-orb: #d85a30;
    --tag-rev: #185fa5;
  }
  * { box-sizing: border-box; }
  body { margin: 0; padding: 20px; background: var(--bg); color: var(--text);
         font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
  header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }
  h1 { font-size: 18px; font-weight: 500; margin: 0; }
  .meta { font-size: 12px; color: var(--muted); }
  .legend { display: flex; gap: 14px; align-items: center; font-size: 11px; color: var(--muted); }
  .legend span.sw { display: inline-block; width: 10px; height: 10px; border-radius: 2px;
                    margin-right: 4px; vertical-align: middle; }

  .grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; }
  .head { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 8px; gap: 10px; }
  .sym { font-weight: 500; font-size: 15px; letter-spacing: 0.5px; }
  .strategy-tag { font-size: 11px; color: var(--muted); text-transform: uppercase;
                  letter-spacing: 0.6px; padding: 2px 6px; border: 1px solid var(--border);
                  border-radius: 3px; background: var(--bg); }
  .strategy-tag.orb-tag { color: var(--tag-orb); border-color: var(--tag-orb); }
  .strategy-tag.rev-tag { color: var(--tag-rev); border-color: var(--tag-rev); }

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
  <h1>Strategy dashboard</h1>
  <div class="legend">
    <span><span class="sw" style="background: var(--breakout-line);"></span>breakout level</span>
    <span><span class="sw" style="background: var(--stop-line);"></span>stop</span>
    <span><span class="sw" style="background: var(--fire-color); border-radius: 50%;"></span>fire</span>
    <span class="meta"><span id="stamp">--</span> &nbsp;polling every 1000 ms</span>
  </div>
</header>
<div id="grid" class="grid">
  <div class="empty">Waiting for historical 2-min candles to seed...</div>
</div>

<script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
<script>
// Keyed by "<strategy>:<symbol>" so ORB and reversal cards for the same
// symbol coexist in the single grid without stomping each other.
const cards = {};
let META = { orb: {}, reversal: {} };

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

async function poll() {
  try {
    const r = await fetch('/api/state', {cache: 'no-store'});
    const data = await r.json();
    if (data.meta) META = data.meta;
    document.getElementById('stamp').textContent = 'as of ' + data.generated_at;
    render(data);
  } catch (e) {
    document.getElementById('stamp').textContent = 'connection lost';
  }
  setTimeout(poll, 1000);
}

function setCheck(card, cls, ok, detail, label) {
  const row = card.element.querySelector('.' + cls);
  if (!row) return;
  const box = row.querySelector('.box');
  if (ok === true) {
    row.classList.remove('fail'); row.classList.add('ok');
    box.className = 'box ok'; box.innerHTML = '&#10003;';
  } else {
    row.classList.remove('ok'); row.classList.add('fail');
    box.className = 'box fail'; box.innerHTML = '&#10007;';
  }
  row.querySelector('.detail').textContent = detail;
  if (label) row.querySelector('.label').textContent = label;
}

function upsertLine(card, key, current, colour, title) {
  const stateKey = key + 'Key';
  const price = current && current.price;
  if (price == null) {
    if (card[key]) { card.series.removePriceLine(card[key]); card[key] = null; card[stateKey] = null; }
    return;
  }
  const sig = price + '|' + current.time + '|' + current.field;
  if (sig === card[stateKey]) return;
  if (card[key]) card.series.removePriceLine(card[key]);
  card[key] = card.series.createPriceLine({
    price: price, color: colour, lineWidth: 1,
    lineStyle: LightweightCharts.LineStyle.Solid,
    axisLabelVisible: true, title: title,
  });
  card[stateKey] = sig;
}

function baseCard(sym, strategyLabel, tagClass, infoHtml, checkHtml) {
  const el = document.createElement('div');
  el.className = 'card';
  el.innerHTML = '<div class="head">'
      + '<div class="sym">' + sym + '</div>'
      + '<div class="strategy-tag ' + tagClass + '">' + strategyLabel + '</div>'
    + '</div>'
    + '<div class="info">' + infoHtml + '</div>'
    + '<div class="chart-container"></div>'
    + '<div class="checks">' + checkHtml + '</div>';
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
    upColor: '#3b6d11', downColor: '#d85a30',
    borderVisible: false,
    wickUpColor: '#3b6d11', wickDownColor: '#d85a30',
    priceFormat: {type: 'price', precision: 2, minMove: 0.01},
  });
  return {
    symbol: sym, element: el, chart: chart, series: series,
    refLine: null, refLineKey: null,
    stopLine: null, stopLineKey: null,
    lastFireSig: null,
    lastBarTs: 0,
  };
}

function updateChartCandles(card, sym) {
  const candles = sym.candles || [];
  if (!candles.length) return;
  if (card.lastBarTs === 0) {
    card.series.setData(candles.map(k => ({time: k.ts, open: k.o, high: k.h, low: k.l, close: k.c})));
    card.lastBarTs = candles[candles.length - 1].ts;
  } else {
    for (const k of candles) {
      if (k.ts >= card.lastBarTs) {
        card.series.update({time: k.ts, open: k.o, high: k.h, low: k.l, close: k.c});
        card.lastBarTs = k.ts;
      }
    }
  }
}

// -----------------------------------------------------------------------
// ORB long card
// -----------------------------------------------------------------------

function createOrbCard(sym) {
  return baseCard(sym, 'ORB long', 'orb-tag',
    '<div><div class="k">last 5s tick</div><div class="v last-t">--</div></div>'
    + '<div><div class="k">last 5s price</div><div class="v last-c">--</div></div>'
    + '<div><div class="k">ORB level</div><div class="v ref-c">--</div></div>'
    + '<div><div class="k">stop level</div><div class="v stop-v">--</div></div>'
    + '<div><div class="k">latest Rvol</div><div class="v rvol">--</div></div>',
    '<div class="check chk-rvol"><span class="box fail">&#10007;</span>'
      + '<span class="label">Rvol &gt; 3</span> <span class="detail">--</span></div>'
    + '<div class="check chk-yhi"><span class="box fail">&#10007;</span>'
      + '<span class="label">price &gt; yesterday high</span> <span class="detail">--</span></div>'
    + '<div class="check chk-ycl"><span class="box fail">&#10007;</span>'
      + '<span class="label">price &gt; yesterday close</span> <span class="detail">--</span></div>');
}

function updateOrbCard(card, sym) {
  const o = sym.orb || {};
  const price = sym.last_bar_close;
  const q = (sel) => card.element.querySelector(sel);
  q('.last-t').textContent = sym.last_bar_time || '--';
  q('.last-c').textContent = price != null ? price.toFixed(2) : '--';
  q('.ref-c').textContent = o.ref_close != null ? o.ref_close.toFixed(2) : '--';
  const lf = (o.fires && o.fires.length) ? o.fires[o.fires.length - 1] : null;
  q('.stop-v').textContent = (lf && lf.stop != null) ? lf.stop.toFixed(2) : '--';
  q('.rvol').textContent = o.latest_rvol != null ? o.latest_rvol.toFixed(2) : '--';

  // --- ORB filter checks ---
  setCheck(card, 'chk-rvol',
    o.latest_rvol != null && o.latest_rvol > 3,
    o.latest_rvol != null ? 'Rvol = ' + o.latest_rvol.toFixed(2) : 'Rvol = -- (no live 2m candle yet)');
  setCheck(card, 'chk-yhi',
    price != null && o.yesterday_high != null && price > o.yesterday_high,
    (price != null && o.yesterday_high != null)
      ? ('price ' + price.toFixed(2) + '  vs  yhi ' + o.yesterday_high.toFixed(2))
      : 'waiting for data');
  setCheck(card, 'chk-ycl',
    price != null && o.yesterday_close != null && price > o.yesterday_close,
    (price != null && o.yesterday_close != null)
      ? ('price ' + price.toFixed(2) + '  vs  yclose ' + o.yesterday_close.toFixed(2))
      : 'waiting for data');

  // --- Chart ---
  updateChartCandles(card, sym);
  upsertLine(card, 'refLine',
    o.ref_close != null ? {price: o.ref_close, time: o.ref_time, field: o.ref_field || 'ref'} : null,
    css('--breakout-line') || '#d85a30',
    (o.ref_field || 'ref') + ' ' + (o.ref_time ? o.ref_time.slice(0, 5) : '') + ' ');

  const stop = (lf && lf.stop != null) ? lf.stop : null;
  const stopSig = stop != null ? (stop + '|orbstop') : null;
  if (stopSig !== card.stopLineKey) {
    if (card.stopLine) card.series.removePriceLine(card.stopLine);
    card.stopLine = null; card.stopLineKey = null;
    if (stop != null) {
      card.stopLine = card.series.createPriceLine({
        price: stop, color: css('--stop-line') || '#a32d2d',
        lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true, title: 'stop',
      });
      card.stopLineKey = stopSig;
    }
  }

  const fires = o.fires || [];
  const sig = fires.length + '_' + (fires.length ? fires[fires.length - 1].ts : 0);
  if (sig !== card.lastFireSig) {
    card.series.setMarkers(fires.map(f => ({
      time: f.ts, position: 'belowBar',
      color: css('--fire-color') || '#3b6d11', shape: 'arrowUp',
      text: 'FIRE @ ' + f.c.toFixed(2) + (f.stop != null ? '  stop ' + f.stop.toFixed(2) : ''),
    })));
    card.lastFireSig = sig;
  }
}

// -----------------------------------------------------------------------
// reversal_long card
// -----------------------------------------------------------------------

function createReversalCard(sym) {
  return baseCard(sym, 'reversal_long', 'rev-tag',
    '<div><div class="k">last 5s tick</div><div class="v last-t">--</div></div>'
    + '<div><div class="k">last 5s price</div><div class="v last-c">--</div></div>'
    + '<div><div class="k">2-bar high</div><div class="v ref-c">--</div></div>'
    + '<div><div class="k">ref 2m candle</div><div class="v ref-t">--</div></div>'
    + '<div><div class="k">recent max Relatr</div><div class="v relatr">--</div></div>',
    '<div class="check chk-cap"><span class="box fail">&#10007;</span>'
      + '<span class="label">Relatr &gt;= capitulation threshold</span> <span class="detail">--</span></div>');
}

function updateReversalCard(card, sym) {
  const r = sym.reversal || {};
  const price = sym.last_bar_close;
  const q = (sel) => card.element.querySelector(sel);
  q('.last-t').textContent = sym.last_bar_time || '--';
  q('.last-c').textContent = price != null ? price.toFixed(2) : '--';
  q('.ref-c').textContent = r.ref_close != null ? r.ref_close.toFixed(2) : '--';
  q('.ref-t').textContent = r.ref_time || '--';
  q('.relatr').textContent = r.recent_max_relatr != null ? r.recent_max_relatr.toFixed(2) : '--';

  const cap = META.reversal && META.reversal.capitulation_threshold;
  const win = META.reversal && META.reversal.recent_relatr_window;
  const maxR = r.recent_max_relatr;
  const capLabel = (cap != null)
    ? ('Relatr >= ' + cap.toFixed(2))
    : 'Relatr >= capitulation threshold';
  setCheck(card, 'chk-cap',
    maxR != null && cap != null && maxR >= cap,
    (maxR != null && cap != null) ? ('max Relatr = ' + maxR.toFixed(2)) : 'waiting for finalized 2m candles',
    capLabel);

  updateChartCandles(card, sym);
  upsertLine(card, 'refLine',
    r.ref_close != null ? {price: r.ref_close, time: r.ref_time, field: r.ref_field || 'ref'} : null,
    css('--breakout-line') || '#d85a30',
    (r.ref_field || 'ref') + ' ' + (r.ref_time ? r.ref_time.slice(0, 5) : '') + ' ');

  const fires = r.fires || [];
  const sig = fires.length + '_' + (fires.length ? fires[fires.length - 1].ts : 0);
  if (sig !== card.lastFireSig) {
    card.series.setMarkers(fires.map(f => ({
      time: f.ts, position: 'belowBar',
      color: css('--fire-color') || '#3b6d11', shape: 'arrowUp',
      text: 'FIRE @ ' + f.c.toFixed(2),
    })));
    card.lastFireSig = sig;
  }
}

// -----------------------------------------------------------------------
// Grid renderer -- one card per (strategy, symbol). Cards for the same
// symbol land next to each other via the 2-column CSS grid because we
// emit them consecutively in the DOM.
// -----------------------------------------------------------------------

function render(data) {
  const grid = document.getElementById('grid');
  const empty = grid.querySelector('.empty');
  if (!data.symbols || !data.symbols.length) {
    if (!empty) grid.innerHTML = '<div class="empty">Waiting for historical 2-min candles to seed...</div>';
    return;
  }
  if (empty) grid.innerHTML = '';

  const seen = new Set();

  function upsert(strategyKey, sym, createFn, updateFn) {
    const cardKey = strategyKey + ':' + sym.symbol;
    seen.add(cardKey);
    let card = cards[cardKey];
    if (!card) {
      card = createFn(sym.symbol);
      grid.appendChild(card.element);
      cards[cardKey] = card;
    }
    updateFn(card, sym);
  }

  data.symbols.forEach(sym => {
    // Only emit cards for strategies the user actually armed on the
    // watchlist for this symbol. Registry keys ("orb_breakout_long",
    // "reversal_long_breakout") are the source of truth.
    const armed = new Set(sym.strategies || []);
    if (armed.has('orb_breakout_long')) {
      upsert('orb', sym, createOrbCard, updateOrbCard);
    }
    if (armed.has('reversal_long_breakout')) {
      upsert('reversal', sym, createReversalCard, updateReversalCard);
    }
  });

  Object.keys(cards).forEach(key => {
    if (!seen.has(key)) {
      cards[key].chart.remove();
      cards[key].element.remove();
      delete cards[key];
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
    on success, or ``None`` if aiohttp isn't installed.
    """
    global _runner
    try:
        from aiohttp import web
    except Exception:
        logger.warning(
            "Strategy dashboard: aiohttp not installed -- dashboard disabled. "
            "Run `pip install aiohttp` to enable, then restart the streamer."
        )
        return None

    async def _index(_request):
        return web.Response(text=_DASHBOARD_HTML, content_type="text/html")

    async def _state(_request):
        return web.json_response(_merged_snapshot())

    app = web.Application()
    app.router.add_get("/", _index)
    app.router.add_get("/api/state", _state)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, DASHBOARD_HOST, port)
    await site.start()
    _runner = runner
    logger.info("Strategy dashboard live at http://%s:%d", DASHBOARD_HOST, port)
    return runner
