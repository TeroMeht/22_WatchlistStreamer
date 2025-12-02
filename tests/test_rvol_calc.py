import pandas as pd
import math
from src.common.calculate import calculate_next_rvol,calculate_rvol
from src.helpers.handle_candles import CandleRow



# Helper to create CandleRow with defaults
def make_candle(symbol="TEST", volume=0):
    return CandleRow(
        symbol=symbol, open=0, high=0, low=0, close=0, volume=volume,
        date="2025-11-24", time="11:00",
        vwap=0.0, ema9=0.0, avg_volume=0.0, rvol=0.0, relatR=0.0
    )

# -------------------------------
# Helper to run a test
def run_test(name, func):
    print(f"\nRunning test: {name}")
    try:
        func()
        print(f"[PASS] {name}")
    except AssertionError as e:
        print(f"[FAIL] {name}")
        print("Assertion Error:", e)
    except Exception as e:
        print(f"[ERROR] {name}")
        print("Exception:", e)

# -------------------------------
# Tests for calculate_next_rvol
def test_next_rvol_basic():
    historical_df = pd.DataFrame({
        "Volume": [100, 200, 300],
        "Avg_volume": [80, 90, 100]
    })

    candle = make_candle(symbol="BTC", volume=400)
    avg_volume = 120

    result = calculate_next_rvol(candle, historical_df, avg_volume)
    expected = round((100+200+300+400)/(80+90+100+120), 4)

    print(f"Computed RVOL: {result.rvol}, Expected: {expected}")
    assert math.isclose(result.rvol, expected, rel_tol=1e-4)

def test_next_rvol_zero_avg():
    historical_df = pd.DataFrame({"Volume": [100], "Avg_volume": [50]})
    candle = make_candle(symbol="ETH", volume=100)
    result = calculate_next_rvol(candle, historical_df, avg_volume=0)
    print(f"Computed RVOL: {result.rvol}, Expected: 0.0")
    assert result.rvol == 0.0

def test_next_rvol_missing_columns():
    historical_df = pd.DataFrame({"Volume": [100]})
    candle = make_candle(symbol="ETH", volume=100)
    result = calculate_next_rvol(candle, historical_df, avg_volume=50)
    print(f"Computed RVOL (missing columns): {result.rvol}, Expected: 0.0")
    assert result.rvol == 0.0

# -------------------------------
# Tests for calculate_rvol (vectorized)
def test_rvol_dataframe_basic():
    df = pd.DataFrame({
        "Volume": [100, 200, 300],
        "Avg_volume": [80, 90, 100]
    })
    result = calculate_rvol(df)
    expected = [
        100/80,
        (100+200)/(80+90),
        (100+200+300)/(80+90+100)
    ]
    print("Computed RVOL column:", result['Rvol'].tolist())
    print("Expected RVOL column:", expected)
    for r, e in zip(result['Rvol'], expected):
        assert math.isclose(r, e, rel_tol=1e-4)

def test_rvol_dataframe_zero_avg():
    df = pd.DataFrame({
        "Volume": [100, 200],
        "Avg_volume": [0, 0]
    })
    result = calculate_rvol(df)
    print("Computed RVOL column with zero avg:", result['Rvol'].tolist())
    for r in result['Rvol']:
        assert r == 0.0

# -------------------------------
# Run all tests
if __name__ == "__main__":
    tests = [
        test_next_rvol_basic,
        test_next_rvol_zero_avg,
        test_next_rvol_missing_columns,
        test_rvol_dataframe_basic,
        test_rvol_dataframe_zero_avg
    ]

    print("=== Running RVOL Tests ===")
    for t in tests:
        run_test(t.__name__, t)
    print("\n=== RVOL Tests Complete ===")