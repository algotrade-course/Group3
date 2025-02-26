import json
import psycopg2 as psycopg
import pprint
import pandas as pd
from datetime import datetime, timedelta


def load_data():
    with open('database.json') as f:
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


    df["year"] = df["tickersymbol"].str.extract(r'VN30F(\d{2})(\d{2})')[0].astype(int) + 2000
    df["month"] = df["tickersymbol"].str.extract(r'VN30F(\d{2})(\d{2})')[1].astype(int)

    df["date_year"] = df["Date"].dt.year
    df["date_month"] = df["Date"].dt.month

    df["expiry_date"] = df.apply(lambda row: get_expiry_date(row["date_year"], row["date_month"]), axis=1)


    df_filtered = df[
        ((df["Date"] <= df["expiry_date"]) & (df["month"] == df["date_month"]) & (df["year"] == df["date_year"])) |
        ((df["Date"] > df["expiry_date"]) & (df["month"] == df["date_month"] + 1) & (df["year"] == df["date_year"]))
    ]

    df_filtered = df_filtered.drop(columns=["year", "month", "date_year", "date_month", "expiry_date"])
    pprint.pprint(df_filtered.head(10))
    return df_filtered


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


if __name__ == "__main__":
    db_info = load_data()
    connection = create_connection(db_info)
    data=process_data(get_data_from_db(connection, "2023-01-01", '2023-03-31'))
    data.to_csv('data.csv')