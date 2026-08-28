"""
Regime-detection primitives for the reversal strategies.

Two stateless dataframe predicates that identify the setup a reversal
trade fires against:

    * ``detect_capitulation(df, threshold)`` -- ``True`` when any row in
      ``df`` has ``Relatr >= threshold`` (positive threshold; downside
      spike). Used by reversal_long's filter to confirm recent panic
      selling before waiting for the EMA9 crossover UP.

    * ``detect_euforia(df, threshold)`` -- ``True`` when any row in
      ``df`` has ``Relatr <= threshold`` (SIGNED threshold, configure
      it as a negative float in the env; strong upward move). Used by
      reversal_short's filter to confirm a recent rip before waiting
      for the EMA9 crossover DOWN.

Both are pure: they only inspect the ``relatr`` column and return a
bool. Both log a one-line info summary of the last matching row when
the check fires; this landed the wrong module historically
(``src.alarms.alarm_logics``) and has now moved next to the reversal
strategy that owns the concept -- no other module in the tree uses
these two predicates.
"""

from __future__ import annotations

import json
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def detect_capitulation(df: pd.DataFrame, threshold: float) -> bool:
    """True when any row in ``df`` has ``Relatr >= threshold``."""
    try:
        capitulated_rows = df[df["relatr"] >= threshold]
        if capitulated_rows.empty:
            logging.debug("No capitulation rows found (Relatr >= %.3f).", threshold)
            return False
        last_row = capitulated_rows.iloc[-1]
        selected = {
            "symbol": last_row["symbol"],
            "time":   last_row["time"],
            "relatr": last_row["relatr"],
        }
        logging.info(
            "Capitulation detected:\n" + json.dumps(selected, indent=4, default=str)
        )
        return True
    except Exception as e:
        logging.error(f"Error in detect_capitulation: {e}")
    return False


def detect_euforia(df: pd.DataFrame, threshold: float) -> bool:
    """
    Detect euforia: opposite of capitulation.

    ``threshold`` is SIGNED (configure a negative float in the env);
    passes when any row's ``Relatr`` is at or below it (a strong
    upward move).
    """
    try:
        euforia_rows = df[df["relatr"] <= threshold]
        if euforia_rows.empty:
            logging.debug("No euforia rows found (Relatr <= %.3f).", threshold)
            return False
        last_row = euforia_rows.iloc[-1]
        selected = {
            "symbol": last_row["symbol"],
            "time":   last_row["time"],
            "relatr": last_row["relatr"],
        }
        logging.info(
            "Euforia detected:\n" + json.dumps(selected, indent=4, default=str)
        )
        return True
    except Exception as e:
        logging.error(f"Error in detect_euforia: {e}")
    return False
