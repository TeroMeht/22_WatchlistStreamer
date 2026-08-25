import httpx
import logging
from src.core.config import settings
from src.helpers.handle_candles import CandleRow
from datetime import date,time

logger = logging.getLogger(__name__)

async def send_exit_request_to_fastapi(candle: CandleRow, alarm_name: str,fastapi_url:str):
    """
    Sends an alarm to the Flask /api/portfoliomanager endpoint using date and time from CandleRow.
    Converts date/time to string to be JSON serializable.

    In replay mode this is a no-op: payload is logged, no HTTP call.
    """
    # Convert date/time to strings
    date_str = candle.date.isoformat() if hasattr(candle, "date") else str(candle.date)
    time_str = candle.time.isoformat() if hasattr(candle, "time") else str(candle.time)

    # Payload matching your ExitRequest model
    payload = {
        "date": date_str,
        "time": time_str,
        "alarm": alarm_name,
        "symbol": candle.symbol
    }

    # if settings.MODE == "replay":
    #     logger.info("[replay] would POST exit request to %s: %s", fastapi_url, payload)
    #     return {"ok": True, "replay": True}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(fastapi_url, json=payload, timeout=10.0)
            response.raise_for_status()
            json_response = response.json()
            logger.info(f"FastAPI response for {candle.symbol}: {json_response}")
            return json_response
    except Exception as e:
        logger.error(f"Failed to send alarm for {candle.symbol} to Fast: {e}")
        return None


async def send_streamer_status(fastapi_url: str, label: str = "status", payload: dict | None = None):
    """
    Fire one POST at a streamer-status endpoint (start or stop). Short
    timeout + swallowed errors so shutdown can't hang on a flaky network.
    On /start we send {"pid": os.getpid()} so the backend can watch the
    process for hard kills (closing the cmd window, kill -9, etc.).

    In replay mode this is a no-op: we don't want to notify the prod
    backend that a streamer started/stopped when we're only replaying.
    """
    # if settings.MODE == "replay":
    #     logger.info("[replay] would POST streamer %s to %s: %s", label, fastapi_url, payload or {})
    #     return None

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(fastapi_url, json=payload or {}, timeout=3.0)
            response.raise_for_status()
            return response.json()
    except Exception as e:
        logger.warning(f"Streamer {label} signal to {fastapi_url} failed: {e}")
        return None


async def send_entry_request_to_fastapi(
    candle: CandleRow,
    stop_level: float,
    fastapi_url: str,
    contract_type: str = "stock",
):
    """
    POST an *automatic* entry request into the trade backend's
    /api/portfolio/entry-request endpoint.

    Payload matches the backend's ``EntryRequest`` schema:
        symbol, contract_type, entry_price, stop_price, position_size,
        request_type

    Notes on the numeric fields
    ---------------------------
    The backend re-derives the authoritative ``entry_price`` from IB
    bid/ask and computes ``position_size`` from the configured risk. It
    still requires all fields to be present, so we send:
      * ``entry_price``   = candle.close (streamer's last known price)
      * ``position_size`` = 0 (placeholder — backend recomputes)
      * ``stop_price``    = the strategy-derived stop_level, which the
        backend does trust and uses for both entry pricing and sizing.

    ``request_type="automatic"`` is what makes the backend park the
    order for user acceptance instead of placing it immediately.

    In replay mode this is a no-op (payload logged, no HTTP call) so
    historical replays don't fire live entries at the prod backend.
    """
    payload = {
        "symbol": candle.symbol,
        "contract_type": contract_type,
        "entry_price": float(candle.close),
        "stop_price": float(stop_level),
        "position_size": 0,
        "request_type": "automatic",
    }

    # if settings.MODE == "replay":
    #     logger.info(
    #         "[replay] would POST automatic entry to %s: %s", fastapi_url, payload
    #     )
    #     return {"ok": True, "replay": True}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(fastapi_url, json=payload, timeout=10.0)
            response.raise_for_status()
            json_response = response.json()
            logger.info(
                "Automatic entry POSTed for %s -> %s", candle.symbol, json_response
            )
            return json_response
    except Exception as e:
        # Don't propagate: an automatic entry that fails to reach the
        # backend must not tear down the streaming loop. It's already
        # recorded in the local orders table via generate_entry_order.
        logger.error(
            "Failed to POST automatic entry for %s to %s: %s",
            candle.symbol, fastapi_url, e,
        )
        return None


async def send_alarm_to_fastapi(candle: CandleRow, alarm_message: str, fastapi_url: str):
    """
    Sends an alarm to FastAPI.

    :param candle: CandleRow object containing candle info
    :param alarm_message: The alarm message to send
    :param fastapi_url: URL of the FastAPI endpoint
    :return: JSON response from FastAPI or None if failed

    In replay mode this is a no-op: payload is logged, no HTTP call.
    """
    payload = {
        "id": 0,
        "symbol": candle.symbol,
        "time": candle.time.isoformat(),
        "alarm": alarm_message,
        "date": candle.date.isoformat(),
    }

    # if settings.MODE == "replay":
    #     logger.info("[replay] would POST alarm to %s: %s", fastapi_url, payload)
    #     return {"ok": True, "replay": True}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(fastapi_url, json=payload, timeout=10.0)
            response.raise_for_status()
            json_response = response.json()
            logger.info(f"Sent alarm for {candle.symbol}: {json_response}")
            return json_response
    except Exception as e:
        logger.error(f"Failed to send alarm for {candle.symbol}: {e}")
        return None
