import os
import psycopg2 as psycopg
import pprint
import pandas as pd
from datetime import datetime, timedelta
import argparse
from dotenv import load_dotenv
import sys
from tqdm import tqdm
import pandas as pd
import pprint
import time
from tqdm import tqdm
from datetime import datetime, timedelta
load_dotenv()
data_path = os.getenv('DATAPATH')
def create_connection():
    load_dotenv()

    con = psycopg.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    return con
  


def split_date_range(start_date, end_date, num_chunks=10):
    total_days = (end_date - start_date).days + 1
    chunk_size = max(1, total_days // num_chunks)
    chunks = []

    current = start_date
    while current <= end_date:
        chunk_start = current
        chunk_end = min(current + timedelta(days=chunk_size - 1), end_date)
        chunks.append((chunk_start, chunk_end))
        current = chunk_end + timedelta(days=1)

    return chunks

def get_data_from_db(connection, start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

    all_data = []
    chunks = split_date_range(start_date, end_date, num_chunks=4)

    for chunk_start, chunk_end in tqdm(chunks, desc="Querying by date chunks"):
        query = f"""
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
            WHERE m.datetime::DATE BETWEEN DATE '{chunk_start}' AND DATE '{chunk_end}'
            AND m.tickersymbol LIKE 'VN30F%'
            ORDER BY m.datetime, m.tickersymbol;
        """
        with connection.cursor() as cursor:
            cursor.execute(query)
            chunk_data = cursor.fetchall()
            all_data.extend(chunk_data)

        time.sleep(1)  # ? Add 1-second delay for smoother real-time progress

    return all_data


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

    df["year"] = df["tickersymbol"].str.extract(r'VN30F(\d{2})(\d{2})')[0].astype(int) + 2000
    df["month"] = df["tickersymbol"].str.extract(r'VN30F(\d{2})(\d{2})')[1].astype(int)

    df["date_year"] = df["Date"].dt.year
    df["date_month"] = df["Date"].dt.month

    # Enable tqdm for pandas apply
    tqdm.pandas(desc="Processing data")

    # Show progress bar while applying get_expiry_date
    df["expiry_date"] = df.progress_apply(
        lambda row: get_expiry_date(row["date_year"], row["date_month"]),
        axis=1
    )

    df_filtered = df[
        ((df["Date"] <= df["expiry_date"]) & (df["month"] == df["date_month"]) & (df["year"] == df["date_year"])) |
        ((df["Date"] > df["expiry_date"]) & (df["month"] == df["date_month"] + 1) & (df["year"] == df["date_year"]))
    ]

    df_filtered = df_filtered.drop(columns=["year", "month", "date_year", "date_month", "expiry_date"])
    pprint.pprint(df_filtered.head(10))
    return df_filtered


def get_processed_data(from_date, to_date):
    connection = create_connection()
    
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


def aggregate_to_interval(input_csv, output_csv, interval='5T'):
    df = pd.read_csv(input_csv)

    df["datetime"] = pd.to_datetime(df["Date"] + " " + df["Time"])
    df.set_index("datetime", inplace=True)

    valid_trading_times = (
        (df.index.time >= pd.to_datetime("09:00:00").time()) & 
        (df.index.time <= pd.to_datetime("11:30:00").time())
    ) | (
        (df.index.time >= pd.to_datetime("13:00:00").time()) & 
        (df.index.time <= pd.to_datetime("14:45:00").time())
    )
    df = df[valid_trading_times]

    df_resampled = df.resample(interval).agg({
        'tickersymbol': 'first',  
        'price': ['first', 'last', 'max', 'min'], 
        'Volume': 'sum'  
    }).dropna()

    df_resampled.columns = ['tickersymbol', 'Open', 'Close', 'High', 'Low', 'Volume']

    df_resampled.reset_index(inplace=True)

    df_resampled["Date"] = df_resampled["datetime"].dt.date
    df_resampled["Time"] = df_resampled["datetime"].dt.time

    df_resampled = df_resampled[["Date", "Time", "tickersymbol", "Open", "Close", "High", "Low", "Volume"]]
    df_resampled.insert(0, "", range(1, len(df_resampled) + 1))
    df_resampled.to_csv(output_csv, index=False)

    print(f"Processed {interval} interval OHLC data saved to: {output_csv}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process and aggregate trading data.")
    parser.add_argument("--start_date", type=str, required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end_date", type=str, required=True, help="End date in YYYY-MM-DD format")
    parser.add_argument("--interval", type=str, default="5T", help="Pandas resample interval (default: 5T for 5 minutes)")
    
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1) 
    args = parser.parse_args()

    connection = create_connection()
    data = process_data(get_data_from_db(connection, args.start_date, args.end_date))

    os.makedirs(data_path, exist_ok=True)


    # File paths based on resolved data_path
    raw_output_path = f"{data_path}/{args.start_date}_to_{args.end_date}.csv"
    agg_output_path = f"{data_path}/{args.start_date}_to_{args.end_date}_by_{args.interval}.csv"

    # Save raw data
    data.to_csv(raw_output_path, index=False)

    # Run aggregation
    aggregate_to_interval(raw_output_path, agg_output_path, args.interval)

# python preprocess.py --start_date 2024-01-01 --end_date 2025-01-01 --interval 5T 