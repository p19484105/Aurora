import pandas as pd
import talib
import aiohttp
import asyncio
import csv
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from asyncio import Semaphore
import logging

# Configuration constants
BASE = "https://api.exchange.coinbase.com"
PRODUCT = "ETH-USD"
GRANULARITY = 3600 # Frequency (1 hour granularity)
DAYS = 945
CHUNK = 300 # API limit (300 minutes = 5 hours)
CONCURRENT = 10 # Concurrent requests
RETRIES = 5 # Max retry attempts
MIN = 1 # Minimum backoff time in seconds
MAX = 60 # Maximum backoff time in seconds
LOG = os.getenv('LOG') # Logging

# Basic logging settings
logging.basicConfig(filename=LOG, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def timebase():
    # Returns time to the current hour
    now = datetime.now(timezone.utc)
    return now.replace(minute=0, second=0, microsecond=0)

# Pull request with retry logic and backoff
async def pull(session, start, end, semaphore):
    url = f"{BASE}/products/{PRODUCT}/candles"
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "granularity": GRANULARITY,
    }

    # API control with retry and backoff mechanism
    async with semaphore:
        retries = 0
        backoff = MIN
        while retries < MAX:
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 400: # Bad Request
                        logging.error(f"Bad request for {start} to {end}.")
                        return [] # Continuity
                    elif response.status == 429: # Rate limit exceeded
                        logging.error(f"Rate limit exceeded for {start} to {end}. Retrying in {backoff} seconds...")
                        await asyncio.sleep(backoff)
                        retries += 1
                        backoff = min(backoff * 2, MAX)
                    else:
                        logging.error(f"Error: Received status {response.status} for {start} to {end}")
                        return []
            except Exception as e:
                retries += 1
                logging.error(f"Error fetching data for {start} to {end}: {e}. Retrying ({retries}/{MAX})...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
        logging.error(f"Failed to fetch data after {MAX} attempts for {start} to {end}.")
        print(f"Failed to fetch partial data after {MAX} attempts.")
        return []

# Generate chunks based on time range
async def generate(start, end):
    chunks = []
    current = start
    while current < end:
        next = min(current + timedelta(days=1), end) # Break into daily chunks
        chunks.append((current, next))
        current = next
    return chunks

# Process the data concurrently
async def process(session, chunks, semaphore):
    return await asyncio.gather(*(pull(session, s, e, semaphore) for s, e in chunks))

# Write data to CSV
async def write(filename, data):
    rows = [
        [datetime.fromtimestamp(entry[0], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        ] + [round(entry[i], 4) for i in [3, 2, 1, 4, 5]]
        for chunk in data for entry in chunk
    ]
    rows.sort()

    # Parsing and writing to CSV
    if rows:
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "open", "high", "low", "close", "volume"])
            writer.writerows(rows)
        print("Data parsed.")
    else:
        logging.error("Failed to parse data.")
        print("Failed to parse data.")

async def main():
    filename = os.getenv("DATA")
    if not filename:
        raise Exception("Environment variable not set.")

    # Time range setup
    end = timebase()
    start = end - timedelta(days=DAYS)

    # Semaphore for limiting concurrent requests
    semaphore = Semaphore(CONCURRENT)

    # Generate chunks and fetch data concurrently
    chunks = await generate(start, end)
    async with aiohttp.ClientSession() as session:
        results = await process(session, chunks, semaphore)

    # Save the results to a CSV file
    await write(filename, results)

# Run the main async function
asyncio.run(main())

# Indicator system requirements
try:
    import talib as ta
except Exception as e:
    raise ImportError("TA-Lib is not properly installed. Please install the C library and Python wrapper.") from e

# Reload environment
filepath = os.getenv('DATA')
if not filepath:
    raise RuntimeError("Environment variable not set.")

# Initialization
df = pd.read_csv(filepath, parse_dates=['date'])
df.sort_values('date', inplace=True)
close = df['close']

# Calculate technical indicators
df['lower'], _, df['upper'] = ta.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
df['SMA'] = ta.SMA(close, timeperiod=20)
df['EMA'] = ta.EMA(close, timeperiod=9)
macd, signal, hist = ta.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
df['MACD'] = macd
df['signal'] = signal
df['histogram'] = hist
df['RSI'] = ta.RSI(close, timeperiod=14)

df.dropna(inplace=True)

# Round numeric columns
for col in df.select_dtypes(include='number'):
    df[col] = df[col].round(2)

# Define original columns and desired indicator order
column = ['date', 'open', 'high', 'low', 'close', 'volume']
indicator = ['SMA', 'upper', 'lower', 'EMA', 'RSI', 'histogram', 'MACD', 'signal',]

# Ensure we only reorder available indicator columns
available = [col for col in indicator if col in df.columns]

# Rebuild DataFrame with original + ordered indicators
df = pd.concat([df[column], df[available]], axis=1)

df.to_csv(filepath, index=False)
print("Technical indicators calculated.")
