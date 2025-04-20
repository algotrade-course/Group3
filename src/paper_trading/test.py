import pandas as pd
from trading_strategy import TradingStrategy
import pandas as pd
from datetime import datetime
from collections import defaultdict
import ast
import pandas as pd
import ast
from datetime import datetime, timedelta


class SnapshotToBarSimulator:
    def __init__(self, strategy):
        self.strategy = strategy
        self.current_tick_data = []
        self.bars = []
        self.current_bar_start_time = None
        self.last_processed_time = None

    def process_tick(self, tick_data):
        tick_data['Date'] = datetime.strptime(tick_data['datetime_str'], '%d/%m/%Y %H:%M:%S')
        self.current_tick_data.append(tick_data)
        if len(self.current_tick_data) < 30:
            return
        if self.last_processed_time is None:
            self.last_processed_time = tick_data['Date']
            self.current_bar_start_time = tick_data['Date']
        self.current_tick_data.append(tick_data)
        if (tick_data['Date'] - self.last_processed_time).total_seconds() >= 300:
            self.generate_5min_bar()
            self.last_processed_time = tick_data['Date']

    def generate_5min_bar(self):
        if not self.current_tick_data:
            return
        df = pd.DataFrame(self.current_tick_data)
        open_price = df['Close'].iloc[0]
        close_price = df['Close'].iloc[-1]
        high_price = df['High'].max()
        low_price = df['Low'].min()
        volume = df['Volume'].sum()
        bar = {
            'Date': self.current_bar_start_time,
            'Open': open_price,
            'High': high_price,
            'Low': low_price,
            'Close': close_price,
            'Volume': volume
        }
        self.bars.append(bar)
        self.strategy.process_tick(bar, df)
        self.current_tick_data = []
        self.current_bar_start_time = self.last_processed_time

    def get_bars(self):
        return self.bars


strategy = TradingStrategy()
simulator = SnapshotToBarSimulator(strategy)
tick_data = pd.read_csv("kafka_output (1).csv")
for index, row in tick_data.iterrows():
    simulator.process_tick(row)
bars = simulator.get_bars()
print(bars)
