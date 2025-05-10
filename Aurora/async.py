import pandas as pd
import talib as ta
import aiohttp
import asyncio
import csv
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from asyncio import Semaphore

# Configuration constants
BASE = "https://api.exchange.coinbase.com"
PRODUCT = "ETH-USD"
GRANULARITY = 3600 # Frequency
DAYS = 945
CHUNK = 300 # API limit
CONCURRENT = 10 # Concurrent requests

# Pull request configuration
async def pull(session, start, end, semaphore):
    url = f"{BASE}/products/{PRODUCT}/candles"
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "granularity": GRANULARITY,
    }

    # API control
    async with semaphore:
        try:
            async with session.get(url, params=params) as response:
                return await response.json() if response.status == 200 else []
        except Exception as e:
            print(f"Error fetching data: {e}")
            return []

# Initialization
async def generate(start, end):
    return [
        (start + timedelta(days=i), min(start + timedelta(days=i+1), end))
        for i in range((end - start).days)
    ]

# Interpretation
async def process(session, chunks, semaphore):
    return await asyncio.gather(*(pull(session, s, e, semaphore) for s, e in chunks))

# Writing configuration
async def write(filename, data):
    rows = [
        [
            datetime.fromtimestamp(entry[0], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        ] + [round(entry[i], 4) for i in [3, 2, 1, 4, 5]]
        for chunk in data for entry in chunk
    ]
    rows.sort()

    # Parsing
    if rows:
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "open", "high", "low", "close", "volume"])
            writer.writerows(rows)
            
    print("Data parsed.")

async def main():
    filename = os.getenv("DATA")
    if not filename:
        raise Exception("Environment variable 'DATA' is not set.")

    # Time range setup
    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=DAYS)

    # Semaphore for limiting concurrent requests
    semaphore = Semaphore(CONCURRENT)

    # Generate chunks and fetch data concurrently
    chunks = await generate(start, end)
    async with aiohttp.ClientSession() as session:
        results = await process(session, chunks, semaphore)

    # Save the results to a CSV file
    await write(filename, results)

asyncio.run(main())

try:
    import talib as ta
except Exception as e:
    raise ImportError("TA-Lib is not properly installed. Please install the C library and Python wrapper.") from e

filepath = os.getenv('DATA')
if not filepath:
    raise RuntimeError("Please set the DATA environment variable to the CSV file path.")

df = pd.read_csv(filepath, parse_dates=['date'])
df.sort_values('date', inplace=True)
close = df['close']

# Technical indicators
df['RSI'] = ta.RSI(close, timeperiod=14)
df['SMA'] = ta.SMA(close, timeperiod=20)
upper, _, lower = ta.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
df['upper'] = upper
df['lower'] = lower
df['EMA'] = ta.EMA(close, timeperiod=9)
macd, signal, hist = ta.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
df['MACD'] = macd
df['signal'] = signal
df['histogram'] = hist

df.dropna(inplace=True)
for col in df.select_dtypes(include='number'):
    df[col] = df[col].round(4)

df.to_csv(filepath, index=False)
print("Data analyzed.")
