import aiohttp, asyncio, csv, os
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE = "https://api.exchange.coinbase.com"
PRODUCT = "ETH-USD"
GRANULARITY = 86400
DAYS = 1800 # Tiemframe
CHUNK = 300 # API limit

async def chunk(session, start, end):
    url = f"{BASE}/products/{PRODUCT}/candles"
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "granularity": GRANULARITY,
    }
    async with session.get(url, params=params) as r:
        return await r.json() if r.status == 200 else []

async def main():
    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=DAYS)
    filename = os.getenv("DATA")
    if not filename:
        raise Exception("Environment failure.")

    # Split into chunks
    chunks = []
    current = start
    while current < end:
        close = min(current + timedelta(days=CHUNK), end)
        chunks.append((current, close))
        current = close

    # Run fetches concurrently
    async with aiohttp.ClientSession() as session:
        tasks = [chunk(session, s, e) for s, e in chunks]
        results = await asyncio.gather(*tasks)

    seen, rows = set(), []
    for data in results:
        for entry in data:
            ts = datetime.fromtimestamp(entry[0], tz=timezone.utc).date().isoformat()
            if entry[0] not in seen:
                seen.add(entry[0])
                values = [round(entry[i], 4) for i in [3, 2, 1, 4, 5]] # open, high, low, close, volume
                rows.append([ts] + values)

    rows.sort()
    with Path(filename).open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "open", "high", "low", "close", "volume"])
        w.writerows(rows)

    print("Database configured.")

asyncio.run(main())

# RSI (Relative Strength Index): Measures the magnitude of recent price changes to evaluate overbought or oversold conditions.
# Formula: RSI = 100 - (100 / (1 + RS)), where RS is the average gain divided by the average loss over a specified period.
# Period: 14 days (14 is the default period in the typical RSI calculation).
# Interpretation: Values above 70 indicate overbought conditions; values below 30 indicate oversold conditions.

# SMA (Simple Moving Average): A simple average of the closing prices over a specified period.
# Formula: SMA = Sum of closing prices over a period divided by the period length.
# Period: 20 days (default period for the SMA).
# Interpretation: Used to smooth price data and identify trends over time.

# Bollinger Bands: Used to measure the volatility of an asset’s price and to identify potential overbought or oversold conditions.
# Formula: Upper Band = SMA + (Standard Deviation × Multiplier), Lower Band = SMA - (Standard Deviation × Multiplier).
# Period: 20 days (same as the SMA).
# Multiplier: 2 (commonly used multiplier for Bollinger Bands).
# Interpretation: Prices tend to bounce within the bands. Prices outside the bands may indicate an extreme condition (overbought or oversold).

# EMA (Exponential Moving Average): Gives more weight to recent prices, making it more responsive to new information.
# Formula: EMA = (Close - Previous EMA) × (2 / (Period + 1)) + Previous EMA.
# Period: 9 days (default period used for the EMA in this script).
# Interpretation: EMA reacts faster to price changes than the SMA, providing more current trend information.

# SMMA (Smoothed Moving Average): A type of moving average similar to the EMA but uses a different smoothing factor.
# Formula: SMMA = (Previous SMMA × (Period - 1) + Current Close) divided by the period.
# Period: 7 days (default period used for the SMMA in this script).
# Interpretation: This moving average provides a smoother curve and is less reactive to price changes than the EMA but more than the SMA.

# MACD (Moving Average Convergence Divergence): Measures the difference between two exponential moving averages (EMAs), typically used to indicate changes in momentum.
# Formula: MACD = EMA (fast) - EMA (slow).
# Fast EMA: 12-day period.
# Slow EMA: 26-day period.
# Signal Line: The 9-day EMA of the MACD value.
# Histogram: The difference between the MACD line and the signal line (MACD - Signal).
# Interpretation: A MACD cross above the signal line is considered a bullish signal, and a cross below is considered a bearish signal.

# Adjustments for Customization:
# Period adjustments: You can modify the period for indicators like SMA, EMA, RSI, and SMMA based on your strategy or timeframe.
# For instance, if you are working with minute-level data, you may want a shorter period such as 5 or 10.
# Multiplier in Bollinger Bands: The standard multiplier is 2, but for more conservative strategies, a multiplier of 1.5 or 2.5 may be used.
# MACD Signal Line: The standard signal line period is 9, but it can be adjusted depending on how sensitive you want the system to be to price changes.
# Histograms and Divergences: Some traders use MACD histogram and divergence patterns as indicators for price action. If the MACD histogram shows increasing divergence from price action, this may indicate a potential reversal.
