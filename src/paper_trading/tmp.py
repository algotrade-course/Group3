# import pandas as pd
# from trading_strategy import TradingStrategy
# import pandas as pd
# from collections import defaultdict
# import ast
# import sys
# import os

# from datetime import datetime, timedelta
# from preprocess_pt import load_data, create_connection, get_data_from_db, process_data, aggregate_to_interval
# from trading_strategy import TradingStrategy


# data= pd.DataFrame()
# current_tick_data=[]
# tick_buffer = []
# current_window_start = None


# import pandas as pd
# from datetime import timedelta

# def get_data_from_last_date(current_tick_data):
#     current_date = pd.to_datetime(current_tick_data['datetime'].iloc[0])  
#     yesterday = current_date - timedelta(days=1)

#     if yesterday.weekday() == 5:  
#         yesterday -= timedelta(days=1)
#     elif yesterday.weekday() == 6: 
#         yesterday -= timedelta(days=2)


#     if yesterday.day == 1:
#         return None

#     db_info = load_data()
#     connection = create_connection(db_info)
    
#     last_date_data = process_data(get_data_from_db(connection, yesterday.date()))
#     last_date_data_5T = aggregate_to_interval(last_date_data, "5T")
#     return last_date_data_5T


    
# def preprocess_data(row_raw_data):
#     clean = parse_row(row_raw_data)
#     clean_df = pd.DataFrame([clean])

#     return clean_df

# def parse_row(row):
#     row_dict = {}
#     for col in ['datetime_str', 'latest_matched_price', 'latest_matched_quantity']:
#         if 'price' in col or 'quantity' in col:
#             val = ast.literal_eval(row[col])
#             row_dict[col + '_value'] = val.get('value') if isinstance(val, dict) else None
#         else:
#             row_dict[col] = row[col]


#     parsed_datetime = pd.to_datetime(row_dict['datetime_str'], format="%d/%m/%Y %H:%M:%S")
#     row_dict['datetime'] = parsed_datetime
    
#     row_dict['datetime_str'] = parsed_datetime.strftime("%Y-%m-%d %H:%M:%S")

#     return row_dict


# def convert_to_5min_bars(tick_data, ticker_symbol='VN30F1M'):
#     if not tick_data:
#         return pd.DataFrame()

#     df = pd.DataFrame(tick_data)

#     df['datetime'] = pd.to_datetime(df['datetime'], dayfirst=True)
#     df.sort_values('datetime', inplace=True)


#     df['latest_matched_price_value'] = df['latest_matched_price_value'].astype(float)
#     df['latest_matched_quantity_value'] = df['latest_matched_quantity_value'].astype(float)


#     grouped = df.groupby(pd.Grouper(key='datetime', freq='5min'))
#     ohlc = grouped['latest_matched_price_value'].ohlc()
#     volume = grouped['latest_matched_quantity_value'].sum()

#     result = pd.concat([ohlc, volume], axis=1).reset_index()
#     result.columns = ['datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
#     result['Date'] = result['datetime'].dt.strftime('%Y-%m-%d')
#     result['Time'] = result['datetime'].dt.strftime('%H:%M:%S')

#     result['tickersymbol'] = ticker_symbol

#     for col in ['High', 'Low', 'Close', 'Open']:
#         result[col] = result[col].astype(float)


#     result = result[['Date', 'Time', 'tickersymbol', 'Open', 'Close', 'High', 'Low', 'Volume']]
#     result.index = result.index + 1
#     result.reset_index(inplace=True)
#     result.columns = [''] + list(result.columns[1:])  # Empty header for row number column
#     return result

# def process_streaming_tick(processed_row, ticker_symbol='VN30F1M'):
#     global tick_buffer, five_minute_bars, current_window_start, data
#     tick_time = processed_row['datetime'].iloc[0]

#     if current_window_start is None:
#         current_window_start = tick_time

#     tick_buffer.append(processed_row.iloc[0])


#     if tick_time - current_window_start >= timedelta(minutes=5):
#         bar_df = convert_to_5min_bars(tick_buffer, ticker_symbol)
#         if not bar_df.empty:
#             data = pd.concat([data, bar_df], ignore_index=True)
#             latest_bar = bar_df.iloc[-1] 
#             TradingStrategy.process_tick(latest_bar, data)

#         tick_buffer = []    
#         current_window_start = None  
        

# raw_data = pd.read_csv("kafka_data.csv")
# raw_data.drop(['bid_price_1',
# 'bid_quantity_1', 'ask_price_1', 'ask_quantity_1', 'bid_price_2',
# 'bid_quantity_2', 'ask_price_2', 'ask_quantity_2', 'bid_price_3',
# 'bid_quantity_3', 'ask_price_3', 'ask_quantity_3', 'bid_price_4',
# 'bid_quantity_4', 'ask_price_4', 'ask_quantity_4', 'bid_price_5',
# 'bid_quantity_5', 'ask_price_5', 'ask_quantity_5', 'bid_price_6',
# 'bid_quantity_6', 'ask_price_6', 'ask_quantity_6', 'bid_price_7',
# 'bid_quantity_7', 'ask_price_7', 'ask_quantity_7', 'bid_price_8',
# 'bid_quantity_8', 'ask_price_8', 'ask_quantity_8', 'bid_price_9',
# 'bid_quantity_9', 'ask_price_9', 'ask_quantity_9', 'bid_price_10',
# 'bid_quantity_10', 'ask_price_10', 'ask_quantity_10'], axis=1, inplace=True)

# TradingStrategy=TradingStrategy()


# for index, row in raw_data.iterrows():
#     processed_row = preprocess_data(row)
    
#     if len(data) == 0:
#         last_data = get_data_from_last_date(processed_row)
#         data = pd.concat([data, last_data], ignore_index=True)
#     process_streaming_tick(processed_row)

   
import pandas as pd
import ast
from datetime import datetime, timedelta
from collections import defaultdict
import sys, os

from preprocess_pt import load_data, create_connection, get_data_from_db, process_data, aggregate_to_interval
from trading_strategy import TradingStrategy

# Global state
data = pd.DataFrame()
current_tick_data = []
tick_buffer = []
current_window_start = None

# Initialize trading strategy instance
TradingStrategy = TradingStrategy()


def get_data_from_last_date(current_tick_data):
    current_date = pd.to_datetime(current_tick_data['datetime'].iloc[0])
    yesterday = current_date - timedelta(days=1)

    if yesterday.weekday() == 5:  # Saturday
        yesterday -= timedelta(days=1)
    elif yesterday.weekday() == 6:  # Sunday
        yesterday -= timedelta(days=2)

    if yesterday.day == 1:
        return None

    db_info = load_data()
    connection = create_connection(db_info)
    
    last_date_data = process_data(get_data_from_db(connection, yesterday.date()))
    last_date_data_5T = aggregate_to_interval(last_date_data, "5T")
    return last_date_data_5T


def parse_row(row):
    row_dict = {}
    for col in ['datetime_str', 'latest_matched_price', 'latest_matched_quantity']:
        if 'price' in col or 'quantity' in col:
            val = ast.literal_eval(row[col])
            row_dict[col + '_value'] = val.get('value') if isinstance(val, dict) else None
        else:
            row_dict[col] = row[col]

    parsed_datetime = pd.to_datetime(row_dict['datetime_str'], format="%d/%m/%Y %H:%M:%S")
    row_dict['datetime'] = parsed_datetime
    row_dict['datetime_str'] = parsed_datetime.strftime("%Y-%m-%d %H:%M:%S")
    return row_dict


def preprocess_data(row_raw_data):
    clean = parse_row(row_raw_data)
    clean_df = pd.DataFrame([clean])
    return clean_df


def convert_to_5min_bars(tick_data, ticker_symbol='VN30F1M'):
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
    result.columns = [''] + list(result.columns[1:])  # Empty header for row number column
    return result


def process_streaming_tick(processed_row, ticker_symbol='VN30F1M'):
    global tick_buffer, current_window_start

    tick_time = processed_row['datetime'].iloc[0]
    if current_window_start is None:
        current_window_start = tick_time

    tick_buffer.append(processed_row.iloc[0])

    new_bar_df = None
    if tick_time - current_window_start >= timedelta(minutes=5):
        new_bar_df = convert_to_5min_bars(tick_buffer, ticker_symbol)
        tick_buffer.clear()
        current_window_start = None

    return new_bar_df  # Return new bar for later processing


# Read Kafka-style raw data
raw_data = pd.read_csv("kafka_data.csv")

# Drop unused columns
raw_data.drop([
    'bid_price_1', 'bid_quantity_1', 'ask_price_1', 'ask_quantity_1',
    'bid_price_2', 'bid_quantity_2', 'ask_price_2', 'ask_quantity_2',
    'bid_price_3', 'bid_quantity_3', 'ask_price_3', 'ask_quantity_3',
    'bid_price_4', 'bid_quantity_4', 'ask_price_4', 'ask_quantity_4',
    'bid_price_5', 'bid_quantity_5', 'ask_price_5', 'ask_quantity_5',
    'bid_price_6', 'bid_quantity_6', 'ask_price_6', 'ask_quantity_6',
    'bid_price_7', 'bid_quantity_7', 'ask_price_7', 'ask_quantity_7',
    'bid_price_8', 'bid_quantity_8', 'ask_price_8', 'ask_quantity_8',
    'bid_price_9', 'bid_quantity_9', 'ask_price_9', 'ask_quantity_9',
    'bid_price_10', 'bid_quantity_10', 'ask_price_10', 'ask_quantity_10'
], axis=1, inplace=True)

# Stream each row
for index, row in raw_data.iterrows():
    processed_row = preprocess_data(row)

    if len(data) == 0:
        last_data = get_data_from_last_date(processed_row)
        if last_data is not None:
            data = pd.concat([data, last_data], ignore_index=True)

    new_bar_df = process_streaming_tick(processed_row)

    if new_bar_df is not None and not new_bar_df.empty:
        data = pd.concat([data, new_bar_df], ignore_index=True)
        latest_bar = new_bar_df.iloc[-1]
        TradingStrategy.process_tick(latest_bar, data)


print(TradingStrategy.get_trade_log())
