import json
import time
from kafka import KafkaConsumer
from trading_strategy import TradingStrategy
import pandas as pd
from datetime import datetime, timedelta


def loafd_config():
    with open("kafka-config.json", 'r') as f:
        config = json.load(f)
    return config

def parse_timestamp(ts_str):
    return datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")

topic = "HNXDS.VN30F1M"  
config = loafd_config()  
consumer = KafkaConsumer(
    topic,
    **config,
    group_id="21125146",  
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
)

strategy = TradingStrategy()
tick_buffer = []
current_bar_start = None
ohlcv_data = []

for msg in consumer:
    tick = msg.value

    price_info = tick.get('latest_matched_price', {})
    try:
        price = float(price_info['value'])
        tick_time = datetime.strptime(price_info['last_updated_str'], "%d/%m/%Y %H:%M:%S")
    except (KeyError, ValueError, TypeError) as e:
        print(f"Error parsing tick: {e}")  # Debug: Print parsing errors
        continue

    symbol = tick.get('instrument', 'UNKNOWN')
    volume = 0.0  # Default volume if not provided
    print(f"Parsed tick - Price: {price}, Time: {tick_time}, Symbol: {symbol}, Volume: {volume}")  # Debug: Parsed tick details

    bar_time = tick_time.replace(second=0, microsecond=0)
    bar_time = bar_time.replace(minute=bar_time.minute - bar_time.minute % 5)

    if current_bar_start is None:
        current_bar_start = bar_time
        print(f"Initialized current_bar_start: {current_bar_start}")  # Debug: Initial bar start time

    if tick_time >= current_bar_start + timedelta(minutes=5):
        print(f"New bar detected. Current bar start: {current_bar_start}, Tick time: {tick_time}")  # Debug: New bar condition

        if tick_buffer:
            prices = [t['price'] for t in tick_buffer]
            volumes = [t['volume'] for t in tick_buffer]
            print(f"Tick buffer - Prices: {prices}, Volumes: {volumes}")  # Debug: Tick buffer contents

            bar = {
                'Date': current_bar_start.strftime('%Y-%m-%d %H:%M:%S'),
                'Open': prices[0],
                'High': max(prices),
                'Low': min(prices),
                'Close': prices[-1],
                'Volume': sum(volumes),
                'tickersymbol': symbol
            }


            ohlcv_data.append(bar)
            df = pd.DataFrame(ohlcv_data)

            strategy.process_tick(bar, df)

        tick_buffer = []
        current_bar_start = bar_time

    tick_buffer.append({
        'timestamp': tick_time,
        'price': price,
        'volume': volume,
        'symbol': symbol
    })
    print(f"Appended to tick buffer: {tick_buffer[-1]}")  # Debug: Appended tick details