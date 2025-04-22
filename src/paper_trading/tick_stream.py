import pandas as pd
import ast
from datetime import datetime, timedelta

from preprocess_pt import create_connection, get_data_from_db, process_data, aggregate_to_interval
from trading_strategy import TradingStrategy

class TickStreamSimulator:
    def __init__(self):
        self.tick_buffer = []
        self.current_window_start = None
        self.strategy = TradingStrategy()
        self.data= pd.DataFrame()

    def get_data_from_last_date(self, current_tick_data):
        current_date = pd.to_datetime(current_tick_data['datetime'].iloc[0])
        yesterday = current_date - timedelta(days=1)

        if yesterday.weekday() == 5:
            yesterday -= timedelta(days=1)
        elif yesterday.weekday() == 6:
            yesterday -= timedelta(days=2)

        if yesterday.day == 1:
            return None

        connection = create_connection()
        last_date_data = process_data(get_data_from_db(connection, yesterday.date(),current_date.date()))
        last_date_data_5T = aggregate_to_interval(last_date_data, "5T")
        return last_date_data_5T

    def parse_row(self, row):
        row_dict = {}

        for col in ['datetime_str', 'latest_matched_price', 'latest_matched_quantity']:
            val_raw = row[col]

            # Only try literal_eval on fields that are supposed to be dictionaries
            if col in ['latest_matched_price', 'latest_matched_quantity']:
                if isinstance(val_raw, str):
                    try:
                        val = ast.literal_eval(val_raw)
                    except Exception as e:
                        raise ValueError(f"Failed to parse {col} with value {val_raw}\nError: {e}")
                else:
                    val = val_raw

                row_dict[col + '_value'] = val.get('value') if isinstance(val, dict) else None
            else:
                row_dict[col] = val_raw  # for datetime_str, just assign directly

        # Safely parse datetime
        try:
            parsed_datetime = pd.to_datetime(row_dict['datetime_str'], format="%d/%m/%Y %H:%M:%S")
            row_dict['datetime'] = parsed_datetime
            row_dict['datetime_str'] = parsed_datetime.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            raise ValueError(f"Failed to parse datetime_str: {row_dict['datetime_str']}\nError: {e}")

        return row_dict


    def preprocess_tick(self, row_raw_data):
        clean = self.parse_row(row_raw_data)
        clean_df = pd.DataFrame([clean])
        return clean_df

    def convert_to_5min_bars(self, tick_data, ticker_symbol='VN30F1M'):
        if not tick_data:
            return pd.DataFrame()

        df = pd.DataFrame(tick_data)
        df['datetime'] = pd.to_datetime(df['datetime'], dayfirst=True)
        df.sort_values('datetime', inplace=True)

        df['latest_matched_price_value'] = df['latest_matched_price_value'].astype(float)
        df['latest_matched_quantity_value'] = df['latest_matched_quantity_value'].astype(float)

        grouped = df.groupby(pd.Grouper(key='datetime', freq='5min'))
        ohlc = grouped['latest_matched_price_value'].ohlc()
        volume = grouped['latest_matched_quantity_value'].sum()

        result = pd.concat([ohlc, volume], axis=1).reset_index()
        result.columns = ['datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
        result['Date'] = result['datetime'].dt.strftime('%Y-%m-%d')
        result['Time'] = result['datetime'].dt.strftime('%H:%M:%S')
        result['tickersymbol'] = ticker_symbol

        for col in ['High', 'Low', 'Close', 'Open']:
            result[col] = result[col].astype(float)

        result = result[['Date', 'Time', 'tickersymbol', 'Open', 'Close', 'High', 'Low', 'Volume']]
        result.index = result.index + 1
        result.reset_index(inplace=True)
        result.columns = [''] + list(result.columns[1:])
        return result

    def process_tick_streaming(self, processed_row, ticker_symbol='VN30F1M'):
        tick_time = processed_row['datetime'].iloc[0]
        if self.current_window_start is None:
            self.current_window_start = tick_time

        self.tick_buffer.append(processed_row.iloc[0])

        new_bar_df = None
        if tick_time - self.current_window_start >= timedelta(minutes=5):
            new_bar_df = self.convert_to_5min_bars(self.tick_buffer, ticker_symbol)
            print(f"New bar created: {new_bar_df}")
            self.tick_buffer.clear()
            self.current_window_start = None

        return new_bar_df
    
    def get_trading_log(self):
        return self.strategy.get_trade_log() 


    def run(self, tick_data):
        new_bar_df = None
        processed_data = self.preprocess_tick(tick_data.iloc[0])
        #print(f"Processed data: {processed_data}")
        if len(self.data) == 0:
            last_data = self.get_data_from_last_date(processed_data)
            if last_data is not None:
                self.data = pd.concat([self.data, last_data], ignore_index=True)

        new_bar_df = self.process_tick_streaming(processed_data)

        if new_bar_df is not None and not new_bar_df.empty:
            print(f"new_bar: {new_bar_df}")
            self.data= pd.concat([self.data, new_bar_df], ignore_index=True)
            latest_bar = new_bar_df.iloc[-1]
            print(f'Lastest bar: {latest_bar}')
            self.strategy.process_tick(latest_bar, self.data)
