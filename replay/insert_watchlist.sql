-- Single symbol + one strategy
WITH w AS (
    INSERT INTO watchlist (symbol)
    VALUES ('PLTR')
    ON CONFLICT (symbol) DO UPDATE SET symbol = EXCLUDED.symbol
    RETURNING id
)
INSERT INTO watchlist_strategies (watchlist_id, strategy_name)
SELECT id, 'orb_breakout_long' FROM w
ON CONFLICT (watchlist_id, strategy_name) DO NOTHING;