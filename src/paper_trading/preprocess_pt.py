import json
import psycopg2 as psycopg
import pprint
import pandas as pd
from datetime import datetime, timedelta
import argparse
import os



def load_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(base_dir, "../database.json")
    print(f"Loading database configuration from: {json_path}")  
    with open(json_path) as f:
        return json.load(f)
    
def create_connection (db_info):
    con= psycopg.connect(
      host=db_info['host'],
      port=db_info['port'],
      dbname=db_info['database'],
      user=db_info['user'],
      password=db_info['password']
    )
    return con

def get_data_from_db(connection, start_date, end_date):
    query=f"""
            SELECT 
            m.datetime::DATE AS date,
            m.datetime::TIME AS time,
            m.tickersymbol,
            m.price,
            tb_open.price AS open_price,
            tb_close.price AS close_price,
            tb_max.price AS high_price,
            tb_min.price AS low_price,
            tb.quantity AS total_quantity
        FROM "quote"."matched" m
        INNER JOIN "quote"."open" tb_open 
            ON m.tickersymbol = tb_open.tickersymbol 
            AND m.datetime::DATE = tb_open.datetime::DATE
        INNER JOIN "quote"."close" tb_close 
            ON m.tickersymbol = tb_close.tickersymbol 
            AND m.datetime::DATE = tb_close.datetime::DATE
        INNER JOIN "quote"."max" tb_max 
            ON m.tickersymbol = tb_max.tickersymbol 
            AND m.datetime::DATE = tb_max.datetime::DATE
        INNER JOIN "quote"."min" tb_min 
            ON m.tickersymbol = tb_min.tickersymbol 
            AND m.datetime::DATE = tb_min.datetime::DATE
        INNER JOIN "quote"."matchedvolume" tb 
            ON m.tickersymbol = tb.tickersymbol 
            AND m.datetime = tb.datetime
        WHERE m.datetime::DATE BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        AND m.tickersymbol LIKE 'VN30F%'
        ORDER BY m.datetime, m.tickersymbol;

  
           """
    with connection.cursor() as cursor:
        cursor.execute(query)
        data=cursor.fetchall()
        return data
  

def get_expiry_date(year, month):
    first_day = datetime(year, month, 1)
    first_thursday = first_day + timedelta(days=(3 - first_day.weekday() + 7) % 7)
    third_thursday = first_thursday + timedelta(weeks=2)
    return third_thursday

def process_data(data):
    columns = ["date", "time", "tickersymbol", "price", "open_price", "close_price", "high_price", "Low_price", "quantity"]
    df = pd.DataFrame(data, columns=columns)

    df = df.rename(columns={
        "date": "Date",
        "time": "Time",
        "tickersymbol": "tickersymbol",
        "high_price": "High",
        "Low_price": "Low",
        "close_price": "Close",
        "open_price": "Open",
        "quantity": "Volume"
    })

    df["Date"] = pd.to_datetime(df["Date"])



    df["date_year"] = df["Date"].dt.year
    df["date_month"] = df["Date"].dt.month



    return df


def get_processed_data(from_date, to_date):
    db_info = load_data()
    connection = create_connection(db_info)
    
    if connection is None:
        raise ValueError("Database connection failed.")
    
    try:
        all_data = []
        current_date = datetime.strptime(from_date, "%Y-%m-%d")
        end_date = datetime.strptime(to_date, "%Y-%m-%d")
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            data = get_data_from_db(connection, date_str) 
            processed_data = process_data(data)  
            all_data.append(processed_data)  
            current_date += timedelta(days=1)  
        
        merged_series = pd.concat(all_data) if all_data else pd.Series(dtype='float64')

    finally:
        connection.close() 
    
    return merged_series

def aggregate_to_interval(input_data, interval='5T'):
    df = input_data.copy()

    # Fix: Ensure Date and Time are strings before combining
    df["datetime"] = pd.to_datetime(df["Date"].astype(str) + " " + df["Time"].astype(str))
    df.set_index("datetime", inplace=True)

    # Filter only valid trading times
    valid_trading_times = (
        (df.index.time >= pd.to_datetime("09:00:00").time()) & 
        (df.index.time <= pd.to_datetime("11:30:00").time())
    ) | (
        (df.index.time >= pd.to_datetime("13:00:00").time()) & 
        (df.index.time <= pd.to_datetime("14:45:00").time())
    )
    df = df[valid_trading_times]

    # Resample to given interval
    df_resampled = df.resample(interval).agg({
        'tickersymbol': 'first',
        'price': ['first', 'last', 'max', 'min'],
        'Volume': 'sum'
    }).dropna()

    # Flatten multi-level columns
    df_resampled.columns = ['tickersymbol', 'Open', 'Close', 'High', 'Low', 'Volume']

    # Add datetime back as columns
    df_resampled.reset_index(inplace=True)
    df_resampled["Date"] = df_resampled["datetime"].dt.date
    df_resampled["Time"] = df_resampled["datetime"].dt.time

    # Reorder columns
    df_resampled = df_resampled[["Date", "Time", "tickersymbol", "Open", "Close", "High", "Low", "Volume"]]

    # Add index column starting from 1
    df_resampled.insert(0, "", range(1, len(df_resampled) + 1))

    return df_resampled
