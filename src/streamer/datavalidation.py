"""
Ticker validation for the startup pipeline.

One public function: ``validate_tickers``. Takes the three symbol-keyed
dicts produced by ``_fetch_history_data`` and returns the tickers that
show up in all three, alongside those dicts narrowed to only the
surviving tickers.

Position-alignment / None-guarding are unnecessary here because
``_fetch_history_data`` already drops failed fetches at the fetch
boundary -- everything reaching this module is a real, non-empty
DataFrame keyed by its Symbol.
"""

from __future__ import annotations

import logging
from typing import Tuple


def validate_tickers(
    daily: dict,
    today_intra: dict,
    past_intra: dict,
    tickers: list,
) -> Tuple[list, dict, dict, dict]:
    """
    Intersect the three fetch-result dicts. Returns:
        ``(valid_tickers, daily, today_intra, past_intra)``

    * ``valid_tickers`` -- tickers present as keys in ALL THREE dicts,
      ordered as they appear in the input ``tickers`` list.
    * The three returned dicts are the input dicts narrowed to only the
      valid tickers. Downstream calculators can iterate them without any
      ``None`` / empty guards.

    Dropped tickers are logged once as a single aggregate warning.
    """
    valid = [t for t in tickers if t in daily and t in today_intra and t in past_intra]

    dropped = [t for t in tickers if t not in valid]
    if dropped:
        logging.warning(
            "Dropped %d tickers due to missing datasets: %s",
            len(dropped), ", ".join(dropped),
        )

    return (
        valid,
        {t: daily[t]       for t in valid},
        {t: today_intra[t] for t in valid},
        {t: past_intra[t]  for t in valid},
    )
