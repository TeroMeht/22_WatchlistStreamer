# Polygon warmup source for `22_WatchlistStreamer` replay

## Scope

5-sec bars stay on IB via `replay/fetch_bars5s.py` — unchanged.

What Polygon replaces: the **warmup fetches** in `datastreamer._fetch_history_data`, which today call `ibclient.fetch_history_daily` (14-day daily bars for ATR14) and `ibclient.fetch_intraday_volume_history` (5-day 2-min bars for the RVOL model). Both require an IB connection right now, and both run unconditionally in `MODE=replay` too. Take those off IB and you can run replay end-to-end without gateway/TWS, using CSVs that were fetched from IB on some earlier day.

## Where Polygon fits

`32_smsystem/backend/datapipe/sources/datasource.py` already has the two calls you need, one-to-one with the IB warmup:

- `fetch_daily_bars_range(rest, symbol, start_day, end_day)` → daily OHLCV for the ATR14 window.
- `fetch_intraday_bars_range(rest, symbol, start_day, end_day)` with `BAR_MINUTES=2` → the 5-day 2-min history the RVOL model consumes.

Both hit `/v2/aggs/ticker/{sym}/range/{n}/{unit}/{from}/{to}` and return `RestAggregateBar` (short keys `t/o/h/l/c/v/vw/n`, `t` in unix ms UTC).

## Concrete changes

### 1. `src/helpers/polygon_client.py` (new, ~100 lines)

Standalone async wrapper — do **not** import from `32_smsystem`; the two projects are separate deployables and you don't want a cross-repo import path. Copy the pattern:

```python
class PolygonClient:
    def __init__(self, api_key: str, base_url: str, session: aiohttp.ClientSession):
        self.api_key = api_key
        self.base_url = base_url
        self.session = session

async def _get_bars(client, url, params) -> list[dict]:
    # one GET, apiKey in params, raise on next_url, return results[]

async def fetch_daily_bars(client, symbol, start_day, end_day) -> list[dict]:
    # /v2/aggs/ticker/{sym}/range/1/day/{from}/{to}?adjusted=true&sort=asc&limit=5000

async def fetch_intraday_bars(client, symbol, start_day, end_day, minutes=2) -> list[dict]:
    # /v2/aggs/ticker/{sym}/range/{minutes}/minute/{from}/{to}?adjusted=true&sort=asc&limit=50000
```

Keep the "fail loud on `next_url`" rule from smsystem — with `limit=50000` and a 5-day 2-min window (~1,950 rows) or 14-day daily window (14 rows) pagination should never fire.

### 2. `src/helpers/polygon_history.py` (new, ~120 lines)

The shim that turns Polygon results into the shapes `handle_dataframes` expects. Two entry points that mirror the IB signatures exactly, so the swap in `datastreamer._fetch_history_data` is one branch:

```python
async def fetch_history_daily(client, symbol) -> pd.DataFrame | None:
    # end_day = get_replay_start_datetime().date() - 1 in replay mode,
    #           else date.today() - 1
    # start_day = end_day - 14 (calendar; buffer for weekends is fine, 14 D is what IB uses)
    # bars = await polygon_client.fetch_daily_bars(...)
    # adapt into the SimpleNamespace(date, open, high, low, close, volume) shape
    #   handle_incoming_dataframe_daily reads today, then call it.

async def fetch_intraday_volume_history(client, symbol) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    # end_day = get_replay_start_datetime().date() in replay, else date.today()
    # start_day = end_day - 5 (calendar; matches IB's "5 D" call)
    # bars = await polygon_client.fetch_intraday_bars(..., minutes=2)
    # adapt each bar into SimpleNamespace(date=datetime_from_t, open, high, low, close, volume)
    #   then call handle_incoming_dataframe_intradays_volume(bars, symbol)
```

**The one thing to verify before writing this file:** grep `handle_dataframes.py` for every attribute it reads off `bars[i]` — currently `.date`, `.open`, `.high`, `.low`, `.close`, `.volume` — and the `.date` type it expects (naive datetime? tz-aware? date only?). Build the `SimpleNamespace` to match exactly, so `handle_incoming_dataframe_daily` and `handle_incoming_dataframe_intradays_volume` don't know they're being fed Polygon.

**Timezone:** Polygon `t` is unix ms **UTC**. IB `BarData.date` for daily bars is a `datetime.date`; for 2-min bars with `formatDate=2` it's a tz-aware UTC datetime. Match whichever your handlers are consuming today — I'd bet on tz-aware UTC for the intraday call and a bare date for the daily call; confirm by reading the two handlers.

**Session anchor:** the existing `ibclient.py` already reads `get_replay_start_datetime()` to anchor `endDateTime` in replay mode — carry the same logic verbatim so Polygon fetches end at the replay's session date, not wall-clock now.

### 3. `src/streamer/datastreamer.py` — dispatch in `_fetch_history_data`

Two-line branch:

```python
if settings.HISTORY_SOURCE == "polygon":
    from src.helpers import polygon_history as hist
    # need a PolygonClient too — create once here and pass in, or make hist
    # own its aiohttp session with a lazy singleton.
    daily_fn = hist.fetch_history_daily
    intra_fn = hist.fetch_intraday_volume_history
else:
    daily_fn = fetch_history_daily        # from ibclient
    intra_fn = fetch_intraday_volume_history
```

Then use `daily_fn`/`intra_fn` in the existing `asyncio.gather` calls. `ib` gets passed to the IB versions, `client` (a `PolygonClient`) to the Polygon versions — keep the signatures parallel: `daily_fn(ib_or_client, symbol)`.

### 4. `src/streamer/startup.py` — make IB connection optional

In `initialize_app`, when `settings.MODE == "replay" and settings.HISTORY_SOURCE == "polygon"`, skip `ib.connectAsync` and return `None`. `main.py`'s `finally` already guards `if ib is not None: ib.disconnect()`, so the shutdown path is fine.

`run_streamer` never touches `ib` in replay mode (it hands off to `replay.run_replay` before subscribing), so `data_pipe(None, monitor_set)` runs cleanly as long as `_fetch_history_data` doesn't pass `None` into the IB path.

### 5. Config + env

`src/core/config.py`:

```python
POLYGON_API_KEY: str
POLYGON_BASE_URL: str      # https://api.polygon.io
HISTORY_SOURCE: str        # "ib" | "polygon"

@field_validator("HISTORY_SOURCE")
def validate_history_source(cls, v):
    v = (v or "ib").lower()
    if v not in ("ib", "polygon"):
        raise ValueError(f"HISTORY_SOURCE must be 'ib' or 'polygon', got {v!r}")
    return v
```

`C:/codebase/env-repo/22_WatchlistStreamer.env`:

```
POLYGON_API_KEY=<your key>
POLYGON_BASE_URL=https://api.polygon.io
HISTORY_SOURCE=polygon
```

Keep `IB_HOST` / `IB_PORT` / `IB_CLIENT_ID` populated — they're required fields on the settings model — they're just unread when `HISTORY_SOURCE=polygon` in replay mode. In live mode leave `HISTORY_SOURCE=ib` (or make it default to `"ib"` in the validator).

## Order of implementation

1. Ship `polygon_client.py` with a tiny `if __name__ == "__main__"` harness that prints one day of MRNA daily + 2-min bars. Confirms your key/base URL work.
2. Ship `polygon_history.py`. Diff its `fetch_history_daily(MRNA)` output DataFrame against `ibclient.fetch_history_daily(MRNA)` on the same replay-anchor date — columns and values should be within cent-level tolerance. Same for `fetch_intraday_volume_history`. This is the load-bearing correctness check; if these two DataFrames don't match, ATR and RVOL warmup will silently be wrong and every strategy filter downstream degrades.
3. Wire the branch in `datastreamer._fetch_history_data` and add the config keys. Run a full replay with `HISTORY_SOURCE=polygon` while IB is still connected — nothing calls IB anymore on that path, but keeping the connection alive lets you flip back to `HISTORY_SOURCE=ib` for A/B comparisons.
4. Only then flip the switch in `startup.initialize_app` to skip `ib.connectAsync` when replay+polygon.

## Estimated effort

- `polygon_client.py`: ~1 hour.
- `polygon_history.py` + DataFrame-parity diff against IB: ~2–3 hours (the diff work is where surprises live — column types, tz-awareness, the ET-vs-Helsinki session-boundary spill).
- Wiring + config + startup skip: ~30 min.

Total: half a day, dominated by verifying the DataFrames coming out of Polygon match what `handle_incoming_dataframe_daily` and `handle_incoming_dataframe_intradays_volume` produce today from IB, since that's what the entire warmup chain (avg-volume tables, RVOL model, ATR seed) is built on.

## The one gotcha to test first

Polygon aggregate `from`/`to` are **ET session dates**. `handle_incoming_dataframe_intradays_volume` splits its 5-day frame into today-vs-past — and "today" there is `get_effective_today()` from `replay.py`, which is the replay date in the streamer's `TIMEZONE` (Helsinki). A 2-min bar with ET timestamp 19:58 (post-market) becomes Helsinki 02:58 the next calendar day. When you convert Polygon `t` to the datetime your handlers consume, make sure the today/past split lands on the same set of bars IB puts there — otherwise a whole afternoon of session data could shift into "past" or vice versa. One-line check: for the same replay date, `sorted(today_df.date.unique())` from IB vs Polygon should be identical.
