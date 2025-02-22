import json
import psycopg2 as psycopg
import pandas as pd
import mplfinance as mpf
import pprint
from datetime import datetime


def load_data():
    with open('database.json') as f:
        return json.load(f)
    
def create_connection(db_info):
    con= psycopg.connect(
      host=db_info['host'],
      port=db_info['port'],
      dbname=db_info['database'],
      user=db_info['user'],
      password=db_info['password']
    )
    return con

def get_data_from_db(connection, date):
    query=f"""
            SELECT tb_max.datetime AS date,  -- Keep as DATE
                tb_total.datetime::TIME AS time,  -- Extract TIME from total table
                tb_max.tickersymbol AS symbol,
                tb_max.price AS max,
                tb_min.price AS min,
                tb_close.price AS "close",
                tb_open.price AS "open",
                tb_total.quantity AS total_quantity
            FROM quote.max tb_max
            INNER JOIN quote.min tb_min
                ON tb_max.tickersymbol = tb_min.tickersymbol
                AND tb_max.datetime = tb_min.datetime
            INNER JOIN quote.close tb_close
                ON tb_min.tickersymbol = tb_close.tickersymbol
                AND tb_min.datetime = tb_close.datetime::DATE  -- Ensure correct date matching
            INNER JOIN quote.open tb_open
                ON tb_open.tickersymbol = tb_close.tickersymbol
                AND tb_open.datetime = tb_close.datetime
            INNER JOIN (
                SELECT m.tickersymbol, m.datetime,  -- Include datetime here
                    MAX(m.quantity) AS quantity
                FROM quote.total m
                WHERE m.tickersymbol LIKE 'VN30F%' 
                AND m.datetime::DATE = '{date}'
                GROUP BY m.tickersymbol, m.datetime
            ) tb_total
            ON tb_open.tickersymbol = tb_total.tickersymbol
            AND tb_open.datetime::DATE = tb_total.datetime::DATE -- Ensure matching
            WHERE tb_max.tickersymbol LIKE 'VN30F%' 
            AND tb_max.datetime = '{date}';

            """
    with connection.cursor() as cursor:
        cursor.execute(query)
        data=cursor.fetchall()
        return data
  
def process_data(data):
    columns = ["date", "time","tickersymbol", "max_price", "min_price", "close_price", "open_price", "total_quantity"]
    df = pd.DataFrame(data, columns=columns)
    df = df.rename(columns={
    "date": "Date",
    "time": "Time",
    "tickersymbol": "tickersymbol",
    "max_price": "High",
    "min_price": "Low",
    "close_price": "Close",
    "open_price": "Open",
    "total_quantity": "Volume"
    })
    df = df.sort_values(by=["Date", "tickersymbol"]).reset_index(drop=True)
    pprint.pprint(df.head(100))
    return data

if __name__ == "__main__":
    db_info = load_data()
    connection = create_connection(db_info)
    data=get_data_from_db(connection,'2022-12-28')
    process_data(data)


