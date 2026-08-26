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
      "symbols": [
        {
          "symbol": "DFNS",
          "candles": [...],           # shared: same 2m bars for all strategies
          "last_bar_time": "...",
          "last_bar_close": 6.20,
          "orb":       { ref_close, ref_low, ref_time, ref_field,
                         yesterday_high, yesterday_close, filters, fires },
          "reversal":  { ref_close, ref_low, ref_time, ref_field,
                         filters, fires },
          "vwap_cont": { ref_close, ref_low, ref_time, ref_field,
                         filters, fires }
        },
        ...
      ]
    }

    Every ``<strategy>.filters`` is a list of
    ``{id, label, passed, detail}`` dicts -- the JS renders one check
    row per entry, so add/remove/rethreshold a filter server-side and
    the UI follows on the next poll with no JS edit.

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
from src.strategies.orb_short.visualization import state as orb_short_viz
from src.strategies.reversal_long.visualization import state as reversal_viz
from src.strategies.vwap_continuation_long.visualization import state as vwap_cont_viz
from src.strategies.dispatcher_state import get_watchlist_strategies_for

logger = logging.getLogger(__name__)


DASHBOARD_PORT: int = 8790
DASHBOARD_HOST: str = "127.0.0.1"


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _merged_snapshot() -> dict:
    """Merge the per-strategy snapshots into a single flat symbol list."""
    orb_snap = orb_viz.snapshot()
    orb_short_snap = orb_short_viz.snapshot()
    rev_snap = reversal_viz.snapshot()
    vwap_snap = vwap_cont_viz.snapshot()

    orb_by_sym       = {s["symbol"]: s for s in orb_snap.get("symbols", [])}
    orb_short_by_sym = {s["symbol"]: s for s in orb_short_snap.get("symbols", [])}
    rev_by_sym       = {s["symbol"]: s for s in rev_snap.get("symbols", [])}
    vwap_by_sym      = {s["symbol"]: s for s in vwap_snap.get("symbols", [])}
    all_syms = sorted(
        set(orb_by_sym) | set(orb_short_by_sym) | set(rev_by_sym) | set(vwap_by_sym),
    )

    merged = []
    for sym in all_syms:
        o   = orb_by_sym.get(sym, {})
        os_ = orb_short_by_sym.get(sym, {})
        r   = rev_by_sym.get(sym, {})
        v   = vwap_by_sym.get(sym, {})
        # Candles + last-bar snapshot come from whichever strategy has
        # them; the shared candle_timeline populates all four the same
        # way, so any non-empty source works.
        candles = o.get("candles") or os_.get("candles") or r.get("candles") or v.get("candles") or []
        last_bar_time = (
            o.get("last_bar_time")
            or os_.get("last_bar_time")
            or r.get("last_bar_time")
            or v.get("last_bar_time")
        )
        last_bar_close = (
            o.get("last_bar_close")
            if o.get("last_bar_close") is not None
            else os_.get("last_bar_close")
            if os_.get("last_bar_close") is not None
            else r.get("last_bar_close")
            if r.get("last_bar_close") is not None
            else v.get("last_bar_close")
        )
        merged.append({
            "symbol": sym,
            # The set of entry-strategy names the user armed for this symbol
            # via the watchlist. Rendered on the client to decide which cards
            # to emit -- a symbol with only ``reversal_long_breakout`` armed
            # will NOT get an ORB or VWAP-continuation card, and so on.
            "strategies": sorted(get_watchlist_strategies_for(sym)),
            "candles": candles,
            "last_bar_time": last_bar_time,
            "last_bar_close": last_bar_close,
            "orb": {
                "ref_time":        o.get("ref_time"),
                "ref_close":       o.get("ref_close"),
                "ref_low":         o.get("ref_low"),
                "ref_field":       o.get("ref_field"),
                # ``filters`` is the full list of active filter results
                # (id, label, passed, detail) that filters.py produced on
                # the last tick. The JS renders one row per entry -- add
                # a filter server-side and it shows up here with no JS
                # edit. ``yesterday_high`` / ``yesterday_close`` are kept
                # in the snapshot for anything else that wants them, but
                # the ORB card itself no longer renders info tiles for
                # filter values -- the check rows carry them.
                "yesterday_high":  o.get("yesterday_high"),
                "yesterday_close": o.get("yesterday_close"),
                "filters":         o.get("filters", []),
                "fires":           o.get("fires", []),
            },
            "orb_short": {
                # Same shape as ``orb``. Because the shared ORBStrategy
                # class remaps the reference for short (breakdown level
                # -> ``ref_close``, stop anchor -> ``ref_low``), the JS
                # overlay code stays direction-agnostic: refLine shows
                # the breakdown level, stop line shows the session-high
                # anchored stop. Yesterday tile carries ``yesterday_low``
                # instead of ``yesterday_high``.
                "ref_time":        os_.get("ref_time"),
                "ref_close":       os_.get("ref_close"),
                "ref_low":         os_.get("ref_low"),
                "ref_field":       os_.get("ref_field"),
                "yesterday_low":   os_.get("yesterday_low"),
                "yesterday_close": os_.get("yesterday_close"),
                "filters":         os_.get("filters", []),
                "fires":           os_.get("fires", []),
            },
            "reversal": {
                "ref_time":          r.get("ref_time"),
                "ref_close":         r.get("ref_close"),
                "ref_low":           r.get("ref_low"),
                "ref_field":         r.get("ref_field"),
                # Same shape as ``orb.filters`` -- see snapshot docstring
                # at the top of this file.
                "filters":           r.get("filters", []),
                "fires":             r.get("fires", []),
            },
            "vwap_cont": {
                "ref_time":          v.get("ref_time"),
                "ref_close":         v.get("ref_close"),
                "ref_low":           v.get("ref_low"),
                "ref_field":         v.get("ref_field"),
                # Same shape as ``orb.filters`` -- see snapshot docstring
                # at the top of this file.
                "filters":           v.get("filters", []),
                "fires":             v.get("fires", []),
            },
        })

    return {
        "generated_at": _now_iso(),
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
    /* Shared overlay palette -- every strategy uses the same colours so
       "breakout level", "stop", and "fire" mean the same thing on any
       chart. The strategy is identified via the header tag below. */
    --breakout-line: #d85a30;
    --stop-line:     #a32d2d;
    --fire-color:    #3b6d11;
    /* Indicator overlay lines. VWAP = red, EMA9 = blue, both 1px so
       they read as thin references against the candles. */
    --vwap-line:     #a32d2d;
    --ema9-line:     #185fa5;
    /* Header tag identifier colours (per strategy). */
    --tag-orb:       #d85a30;
    --tag-orb-short: #185fa5;
    --tag-rev:       #185fa5;
    --tag-vwap:      #ba7517;
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
  .strategy-tag.orb-tag       { color: var(--tag-orb);       border-color: var(--tag-orb); }
  .strategy-tag.orb-short-tag { color: var(--tag-orb-short); border-color: var(--tag-orb-short); }
  .strategy-tag.rev-tag       { color: var(--tag-rev);       border-color: var(--tag-rev); }
  .strategy-tag.vwap-tag      { color: var(--tag-vwap);      border-color: var(--tag-vwap); }

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
    <span><span class="sw" style="background: var(--vwap-line);"></span>VWAP</span>
    <span><span class="sw" style="background: var(--ema9-line);"></span>EMA9</span>
    <span class="meta"><span id="stamp">--</span> &nbsp;polling every 1000 ms</span>
  </div>
</header>
<div id="grid" class="grid">
  <div class="empty">Waiting for historical 2-min candles to seed...</div>
</div>

<script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
<script>
// Keyed by "<strategy>:<symbol>" so cards for the same symbol coexist
// in the single grid without stomping each other.
const cards = {};

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

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
  // Volume histogram anchored to its own price scale so the bars sit
  // as a compact strip at the bottom of the chart without stealing
  // vertical space from the candles. Per-bar colour is set on each
  // datapoint (green for up-close, red for down-close) so the volume
  // history reads the same trend at a glance as the candles above.
  const volumeSeries = chart.addHistogramSeries({
    priceFormat: {type: 'volume'},
    priceScaleId: 'volume',
    lastValueVisible: false,
    priceLineVisible: false,
  });
  chart.priceScale('volume').applyOptions({
    scaleMargins: {top: 0.8, bottom: 0},
  });
  // Indicator overlay series -- one line each for session VWAP and
  // EMA9. Both share the candles' right-hand price scale so scale
  // context stays 1:1. Values arrive per finalized 2-min candle; the
  // in-progress candle carries no vwap/ema9 (5-sec bars don't have
  // them), so the lines naturally trail one bar behind the wick.
  const vwapSeries = chart.addLineSeries({
    color: css('--vwap-line') || '#a32d2d',
    lineWidth: 1,
    priceLineVisible: false,
    lastValueVisible: false,
    crosshairMarkerVisible: true,
    priceFormat: {type: 'price', precision: 2, minMove: 0.01},
  });
  const ema9Series = chart.addLineSeries({
    color: css('--ema9-line') || '#185fa5',
    lineWidth: 1,
    priceLineVisible: false,
    lastValueVisible: false,
    crosshairMarkerVisible: true,
    priceFormat: {type: 'price', precision: 2, minMove: 0.01},
  });
  return {
    symbol: sym, element: el, chart: chart, series: series,
    vwapSeries: vwapSeries, ema9Series: ema9Series,
    volumeSeries: volumeSeries,
    refLine: null, refLineKey: null,
    stopLine: null, stopLineKey: null,
    lastFireSig: null,
    lastBarTs: 0,
    lastVwapTs: 0, lastEma9Ts: 0,
    lastVolTs: 0,
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
  updateIndicatorSeries(card, candles, 'vwap',  card.vwapSeries,  'lastVwapTs');
  updateIndicatorSeries(card, candles, 'ema9',  card.ema9Series,  'lastEma9Ts');
  updateVolumeSeries(card, candles);
}

// Volume histogram sync: mirrors ``updateChartCandles``' diff logic
// (bulk setData on first paint, per-bar update after that) but colours
// each bar by the candle's up/down direction so trend context matches
// what's happening in the candles above. Candles with ``null`` volume
// are skipped -- the in-progress candle only accumulates volume when
// the streamer forwards it, and finalize replaces it with the DB value.
function updateVolumeSeries(card, candles) {
  if (!card.volumeSeries) return;
  const upColor   = '#3b6d11';
  const downColor = '#d85a30';
  const point = (k) => ({
    time: k.ts,
    value: k.v,
    color: (k.c >= k.o) ? upColor : downColor,
  });
  if (card.lastVolTs === 0) {
    const seed = [];
    for (const k of candles) {
      if (k.v == null) continue;
      seed.push(point(k));
    }
    if (seed.length) {
      card.volumeSeries.setData(seed);
      card.lastVolTs = seed[seed.length - 1].time;
    }
    return;
  }
  for (const k of candles) {
    if (k.ts < card.lastVolTs) continue;
    if (k.v == null) continue;
    card.volumeSeries.update(point(k));
    card.lastVolTs = k.ts;
  }
}

// Push VWAP / EMA9 (or any nullable per-candle scalar) into a Lightweight
// Charts line series. Mirrors the candle diff logic in
// ``updateChartCandles``: bulk setData on the first paint, per-bar
// ``series.update`` after that. Candles with a ``null`` value for the
// field are simply skipped -- the line naturally trails one bar behind
// the wick because the in-progress 2-min candle carries no vwap / ema9.
function updateIndicatorSeries(card, candles, field, series, lastTsKey) {
  if (!series) return;
  if (card[lastTsKey] === 0) {
    const seed = [];
    for (const k of candles) {
      const v = k[field];
      if (v == null) continue;
      seed.push({time: k.ts, value: v});
    }
    if (seed.length) {
      series.setData(seed);
      card[lastTsKey] = seed[seed.length - 1].time;
    }
    return;
  }
  for (const k of candles) {
    if (k.ts < card[lastTsKey]) continue;
    const v = k[field];
    if (v == null) continue;
    series.update({time: k.ts, value: v});
    card[lastTsKey] = k.ts;
  }
}

// -----------------------------------------------------------------------
// Shared: filter check rows (identical DOM contract across strategies)
// -----------------------------------------------------------------------

function renderFilterChecks(card, filters) {
  // Rebuild the row skeleton only when the set of filter ids changes.
  // Every poll after that only mutates the class/text of existing rows,
  // which keeps the DOM stable and avoids reflow. Signature is prefixed
  // with "-" for the empty state so it doesn't collide with a real
  // (empty-string) join on some future filter list.
  const container = card.element.querySelector('.checks');
  if (!filters.length) {
    const sig = '__empty__';
    if (sig !== card.filtersSig) {
      container.innerHTML = '<div class="check fail">'
        + '<span class="box fail">&#10007;</span>'
        + '<span class="label">filters</span>'
        + '<span class="detail">waiting for first tick</span>'
        + '</div>';
      card.filtersSig = sig;
    }
    return;
  }
  const sig = filters.map(f => f.id).join('|');
  if (sig !== card.filtersSig) {
    container.innerHTML = filters.map(f =>
      '<div class="check" data-id="' + f.id + '">'
        + '<span class="box"></span>'
        + '<span class="label"></span>'
        + '<span class="detail"></span>'
      + '</div>').join('');
    card.filtersSig = sig;
  }
  filters.forEach(f => {
    const row = container.querySelector('[data-id="' + f.id + '"]');
    if (!row) return;
    const ok = !!f.passed;
    row.className = 'check ' + (ok ? 'ok' : 'fail');
    const box = row.querySelector('.box');
    box.className = 'box ' + (ok ? 'ok' : 'fail');
    box.innerHTML = ok ? '&#10003;' : '&#10007;';
    row.querySelector('.label').textContent = f.label;
    row.querySelector('.detail').textContent = f.detail;
  });
}

// -----------------------------------------------------------------------
// Shared: reference + stop + fire overlays. Every strategy card uses the
// same overlay logic; the only differences are the strategy sub-object
// on the payload and the stop signature suffix (so cards for the same
// symbol don't stomp each other's stop line).
// -----------------------------------------------------------------------

function renderOverlays(card, strat, stopSuffix, direction) {
  // ``direction`` is 'long' (default) or 'short'. It controls only the
  // fire marker: long entries sit BELOW the trigger candle with an
  // up-arrow; short entries sit ABOVE the trigger candle with a
  // down-arrow -- the visual reads the same way the trade does.
  // Every other overlay (reference line, stop line) is direction-agnostic
  // because ORBStrategy already remaps the reference for short.
  const isShort = direction === 'short';

  upsertLine(card, 'refLine',
    strat.ref_close != null
      ? {price: strat.ref_close, time: strat.ref_time, field: strat.ref_field || 'ref'}
      : null,
    css('--breakout-line') || '#d85a30',
    (strat.ref_field || 'ref') + ' ' + (strat.ref_time ? strat.ref_time.slice(0, 5) : '') + ' ');

  const lf = (strat.fires && strat.fires.length) ? strat.fires[strat.fires.length - 1] : null;
  const stop = (lf && lf.stop != null) ? lf.stop : null;
  const stopSig = stop != null ? (stop + '|' + stopSuffix) : null;
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

  const fires = strat.fires || [];
  // Bump the signature when direction changes so a card that swapped
  // sides (would only happen in dev) redraws its markers.
  const sig = fires.length + '_' + (fires.length ? fires[fires.length - 1].ts : 0) + '_' + (isShort ? 's' : 'l');
  if (sig !== card.lastFireSig) {
    const markerPos   = isShort ? 'aboveBar' : 'belowBar';
    const markerShape = isShort ? 'arrowDown' : 'arrowUp';
    // Lightweight Charts v4 uses ONE ``color`` for both the arrow and
    // the text label -- there's no separate ``textColor`` on markers.
    // Black keeps the "FIRE @ ..." message legible against every
    // chart background (light theme + red / green candles). The arrow
    // shape still points the trade direction, so the visual cue does
    // not depend on the arrow being green.
    card.series.setMarkers(fires.map(f => ({
      time: f.ts, position: markerPos,
      color: '#000000', shape: markerShape,
      text: 'FIRE @ ' + f.c.toFixed(2) + (f.stop != null ? '  stop ' + f.stop.toFixed(2) : ''),
    })));
    card.lastFireSig = sig;
  }
}

// -----------------------------------------------------------------------
// ORB long card
// -----------------------------------------------------------------------

function createOrbCard(sym) {
  // Info grid keeps only the strategy-level facts (tick + reference +
  // stop). Filter values now live in the check rows below and no longer
  // need duplicate tiles up top. The .checks container starts empty --
  // rows are built on the first poll from ``sym.orb.filters``.
  return baseCard(sym, 'ORB long', 'orb-tag',
    '<div><div class="k">last 5s tick</div><div class="v last-t">--</div></div>'
    + '<div><div class="k">last 5s price</div><div class="v last-c">--</div></div>'
    + '<div><div class="k">ORB level</div><div class="v ref-c">--</div></div>'
    + '<div><div class="k">stop level</div><div class="v stop-v">--</div></div>',
    '');
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

  renderFilterChecks(card, o.filters || []);
  updateChartCandles(card, sym);
  renderOverlays(card, o, 'orbstop', 'long');
}

// -----------------------------------------------------------------------
// ORB short card
//
// Mirror of the ORB long card. The server-side ORBStrategy class remaps
// the reference for short direction (breakdown level -> ``ref_close``,
// stop anchor -> ``ref_low``), so the shared overlay helper draws
// exactly the right two lines with no direction-specific JS. The info
// tile says "ORB level (short)" so a glance at the tag + tile makes it
// clear which side is armed.
// -----------------------------------------------------------------------

function createOrbShortCard(sym) {
  return baseCard(sym, 'ORB short', 'orb-short-tag',
    '<div><div class="k">last 5s tick</div><div class="v last-t">--</div></div>'
    + '<div><div class="k">last 5s price</div><div class="v last-c">--</div></div>'
    + '<div><div class="k">ORB level (short)</div><div class="v ref-c">--</div></div>'
    + '<div><div class="k">stop level</div><div class="v stop-v">--</div></div>',
    '');
}

function updateOrbShortCard(card, sym) {
  const o = sym.orb_short || {};
  const price = sym.last_bar_close;
  const q = (sel) => card.element.querySelector(sel);
  q('.last-t').textContent = sym.last_bar_time || '--';
  q('.last-c').textContent = price != null ? price.toFixed(2) : '--';
  q('.ref-c').textContent = o.ref_close != null ? o.ref_close.toFixed(2) : '--';
  const lf = (o.fires && o.fires.length) ? o.fires[o.fires.length - 1] : null;
  q('.stop-v').textContent = (lf && lf.stop != null) ? lf.stop.toFixed(2) : '--';

  renderFilterChecks(card, o.filters || []);
  updateChartCandles(card, sym);
  renderOverlays(card, o, 'orbshortstop', 'short');
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
    + '<div><div class="k">stop level</div><div class="v stop-v">--</div></div>',
    '');
}

function updateReversalCard(card, sym) {
  const r = sym.reversal || {};
  const price = sym.last_bar_close;
  const q = (sel) => card.element.querySelector(sel);
  q('.last-t').textContent = sym.last_bar_time || '--';
  q('.last-c').textContent = price != null ? price.toFixed(2) : '--';
  q('.ref-c').textContent = r.ref_close != null ? r.ref_close.toFixed(2) : '--';
  q('.ref-t').textContent = r.ref_time || '--';
  const lf = (r.fires && r.fires.length) ? r.fires[r.fires.length - 1] : null;
  q('.stop-v').textContent = (lf && lf.stop != null) ? lf.stop.toFixed(2) : '--';

  renderFilterChecks(card, r.filters || []);
  updateChartCandles(card, sym);
  renderOverlays(card, r, 'revstop', 'long');
}

// -----------------------------------------------------------------------
// vwap_continuation_long card
// -----------------------------------------------------------------------

function createVwapContCard(sym) {
  // Candle-driven card. "Level to watch" IS the VWAP itself, already
  // drawn as a chart overlay, so no breakout-reference line. A stop
  // level DOES get computed on fire (long-side detect_stoplevel), so
  // we render it both as an info tile and as a dashed price line
  // via the shared overlay helper below.
  return baseCard(sym, 'VWAP continuation long', 'vwap-tag',
    '<div><div class="k">last 2m close time</div><div class="v last-t">--</div></div>'
    + '<div><div class="k">last 2m close</div><div class="v last-c">--</div></div>'
    + '<div><div class="k">latest VWAP</div><div class="v last-vwap">--</div></div>'
    + '<div><div class="k">latest EMA9</div><div class="v last-ema9">--</div></div>'
    + '<div><div class="k">stop level</div><div class="v stop-v">--</div></div>'
    + '<div><div class="k">state</div><div class="v alarm-v">armed</div></div>',
    '');
}

// Walk the candles list backwards and return the newest non-null value
// for ``field``. Used by the VWAP-continuation card to echo the last
// finalized VWAP / EMA9 into an info tile without a second store on
// the server.
function latestFromCandles(candles, field) {
  if (!candles || !candles.length) return null;
  for (let i = candles.length - 1; i >= 0; i--) {
    const v = candles[i][field];
    if (v != null) return v;
  }
  return null;
}

function updateVwapContCard(card, sym) {
  const v = sym.vwap_cont || {};
  const price = sym.last_bar_close;
  const q = (sel) => card.element.querySelector(sel);
  q('.last-t').textContent = sym.last_bar_time || '--';
  q('.last-c').textContent = price != null ? price.toFixed(2) : '--';
  const vwapV = latestFromCandles(sym.candles, 'vwap');
  const ema9V = latestFromCandles(sym.candles, 'ema9');
  q('.last-vwap').textContent = vwapV != null ? vwapV.toFixed(2) : '--';
  q('.last-ema9').textContent = ema9V != null ? ema9V.toFixed(2) : '--';

  const fires = v.fires || [];
  const lf = fires.length ? fires[fires.length - 1] : null;
  q('.stop-v').textContent = (lf && lf.stop != null) ? lf.stop.toFixed(2) : '--';
  q('.alarm-v').textContent = lf
    ? ('fired at ' + (lf.t ? lf.t.slice(11, 19) : '--'))
    : 'armed';

  renderFilterChecks(card, v.filters || []);
  updateChartCandles(card, sym);

  // Reference line stays skipped (VWAP is the reference and is already
  // drawn as its own overlay). On fire we render THREE overlays:
  //   * stop line   -- dashed red, same style as ORB / reversal
  //   * entry line  -- solid green at the exact fire close, so the
  //                    price axis prints the entry level and the level
  //                    is unambiguous at a glance (markers alone can
  //                    only anchor above/in/below the bar, not at an
  //                    exact price -- this pins it)
  //   * fire marker -- arrow at the fire bar, positioned INSIDE the
  //                    bar so it sits at the price zone rather than
  //                    floating below the wick
  const stop = (lf && lf.stop != null) ? lf.stop : null;
  const stopSig = stop != null ? (stop + '|vwapstop') : null;
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

  const entryPrice = lf ? lf.c : null;
  const entrySig = entryPrice != null ? (entryPrice + '|vwapentry') : null;
  if (entrySig !== card.entryLineKey) {
    if (card.entryLine) card.series.removePriceLine(card.entryLine);
    card.entryLine = null; card.entryLineKey = null;
    if (entryPrice != null) {
      card.entryLine = card.series.createPriceLine({
        price: entryPrice, color: css('--fire-color') || '#3b6d11',
        lineWidth: 1, lineStyle: LightweightCharts.LineStyle.Solid,
        axisLabelVisible: true, title: 'entry',
      });
      card.entryLineKey = entrySig;
    }
  }

  const sig = fires.length + '_' + (fires.length ? fires[fires.length - 1].ts : 0);
  if (sig !== card.lastFireSig) {
    // Same black-marker rationale as ``renderOverlays`` above -- v4
    // marker text and arrow share one colour; black keeps the message
    // readable regardless of chart theme or candle colour underneath.
    card.series.setMarkers(fires.map(f => ({
      time: f.ts, position: 'belowBar',
      color: '#000000', shape: 'arrowUp',
      text: 'ENTRY @ ' + f.c.toFixed(2) + (f.stop != null ? '  stop ' + f.stop.toFixed(2) : ''),
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
    // watchlist for this symbol. Registry keys are the source of truth:
    //   "orb_breakout"           -> ORB long card
    //   "orb_breakdown"          -> ORB short card
    //   "reversal_long_breakout" -> reversal_long card
    //   "vwap_continuation_long" -> VWAP continuation long card
    const armed = new Set(sym.strategies || []);
    if (armed.has('orb_breakout')) {
      upsert('orb', sym, createOrbCard, updateOrbCard);
    }
    if (armed.has('orb_breakdown')) {
      upsert('orb_short', sym, createOrbShortCard, updateOrbShortCard);
    }
    if (armed.has('reversal_long_breakout')) {
      upsert('reversal', sym, createReversalCard, updateReversalCard);
    }
    if (armed.has('vwap_continuation_long')) {
      upsert('vwap_cont', sym, createVwapContCard, updateVwapContCard);
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
