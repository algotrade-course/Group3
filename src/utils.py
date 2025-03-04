import pandas as pd
import numpy as np
import pprint
from typing import List, Tuple
from datetime import datetime
from dotenv import load_dotenv
import os
load_dotenv()

from data import get_expiry_date

TAKE_PROFIT_THRES = float(os.getenv("TAKE_PROFIT_THRES")) 
CUT_LOSS_THRES = int(os.getenv("CUT_LOSS_THRES"))


def calculate_ema(df, column, period):
    return df[column].ewm(span=period, adjust=False).mean()


def calculate_rsi(df, column='Close', period=14):
    delta = df[column].diff().to_numpy()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).ewm(span=period, adjust=False).mean()
    avg_loss = pd.Series(loss).ewm(span=period, adjust=False).mean()
    rs = np.where(avg_loss == 0, np.inf, avg_gain / avg_loss)
    rsi = 100 - (100 / (1 + rs))
    return pd.Series(rsi, index=df.index)


# def calculate_vwap(df, window_size=5):
#     df = df.dropna().copy()
#     df = df[df['Volume'] > 0].copy()
#     typical_price = (df['High'] + df['Low'] + df['Close']) / 3
#     vwap_series = (typical_price * df['Volume']).rolling(window=window_size).sum() / df['Volume'].rolling(window=window_size).sum()
#     return vwap_series.iloc[-1]

def calculate_vwap(df):
    df = df.dropna().copy()
    df = df[df['Volume'] > 0].copy()
    typical_price = (df['High'] + df['Low'] + df['Close']) / 3
    vwap_series = (typical_price * df['Volume']).cumsum() / df['Volume'].cumsum()
    return vwap_series.iloc[-1]



def calculate_atr(data, period=14):
    tr = pd.concat([
        data["High"] - data["Low"],
        (data["High"] - data["Close"].shift()).abs(),
        (data["Low"] - data["Close"].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


# def check_volume_trend(data):
#     avg_volume = data['Volume'].rolling(15).mean()
#     return data['Volume'].iloc[-1] > avg_volume.iloc[-1] * 1.0

def check_volume_trend(data, direction="LONG"):
    avg_volume = data['Volume'].rolling(15).mean() 
    if direction == "LONG":
        return data['Volume'].iloc[-1] > avg_volume.iloc[-1] * 1.05
    return data['Volume'].iloc[-1] < avg_volume.iloc[-1] * 0.95


def detect_trend(data, short_period=10, long_period=50, column='Close'):
    ema_short = calculate_ema(data, column, short_period)
    ema_long = calculate_ema(data, column, long_period)
    atr = calculate_atr(data)
    sideway = (ema_short - ema_long).abs().rolling(window=10).mean() < atr * 0.2
    return [sideway.iloc[-1], check_volume_trend(data)]

def calculate_adx(data, period=14):
    raw_plus_dm = data['High'].diff()
    raw_minus_dm = -data['Low'].diff()

    plus_dm = raw_plus_dm.copy()
    minus_dm = raw_minus_dm.copy()

    plus_condition = (raw_plus_dm > 0) & (raw_plus_dm > raw_minus_dm)
    minus_condition = (raw_minus_dm > 0) & (raw_minus_dm > raw_plus_dm)
    
    plus_dm[~plus_condition] = 0
    minus_dm[~minus_condition] = 0

    tr = pd.concat([
        data['High'] - data['Low'],
        (data['High'] - data['Close'].shift()).abs(),
        (data['Low'] - data['Close'].shift()).abs()
    ], axis=1).max(axis=1)

    plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / tr.ewm(alpha=1/period, adjust=False).mean())
    minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / tr.ewm(alpha=1/period, adjust=False).mean())
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = dx.ewm(alpha=1/period, adjust=False).mean()
    return adx



#================================================================================================#



def open_position(position_type, entry_point, holdings, data, cash):
    if position_type not in {"LONG", "SHORT"}:
        raise ValueError("Invalid position type. Must be 'LONG' or 'SHORT'.")
    holdings = (data['Date'], entry_point, "OPEN", position_type, entry_point,cash, data['tickersymbol'])
    return holdings



def close_positions(data, cur_price, holdings):
    total_realized_pnl = 0.0
    _, entry_price, _, position_type, _, _ , ticketsymbol= holdings 
    pnl = (cur_price - entry_price) if position_type == "LONG" else (entry_price - cur_price)
    # print(f"Closing position {position_type} at {cur_price} (Entry: {entry_price}, PnL: {pnl})")
    atr = calculate_atr(data).iloc[-1]
    # # if pnl >= 1.5*atr or pnl <= -1*atr:
    # #     return [], pnl
    # if pnl >= 1.5*atr or pnl <= CUT_LOSS_THRES:
    #     return [], pnl
    return [], pnl

def open_position_type(data, cur_price):
    ema8 = calculate_ema(data, 'Close', 10)
    ema21 = calculate_ema(data, 'Close', 30)
    vwap = calculate_vwap(data)
    rsi = calculate_rsi(data, 'Close', 14)
    atr = calculate_atr(data)
    adx = calculate_adx(data)
    trend_info = detect_trend(data, 10, 30, 'Close')

    rsi_lower = 30 + (10 * (atr.iloc[-1] / data['Close'].iloc[-1]))
    rsi_upper = 70 - (10 * (atr.iloc[-1] / data['Close'].iloc[-1]))

    if (trend_info[0]  or pd.isna(rsi.iloc[-1]) or not (rsi_lower < rsi.iloc[-1] < rsi_upper) or adx.iloc[-1] < 30):
        return 0  

    long_criteria = sum([
        ema8.iloc[-1] > ema21.iloc[-1],  
        cur_price > vwap,  
        rsi_lower < rsi.iloc[-1] < rsi_upper,  
        trend_info[1], 
        adx.iloc[-1] > 30  
    ])

    short_criteria = sum([
        ema8.iloc[-1] < ema21.iloc[-1],  
        cur_price < vwap,  
        rsi_lower < rsi.iloc[-1] < rsi_upper, 
        not trend_info[1],
        adx.iloc[-1] > 30 
    ])

    return 1 if long_criteria >= 3 else 2 if short_criteria >= 3 else 0


def close_position_type(data, cur_price, holdings):
    ema8 = calculate_ema(data, 'Close', 10)
    ema21 = calculate_ema(data, 'Close', 30)
    rsi = calculate_rsi(data, 'Close', 14)
    has_long = holdings[3] == "LONG" if holdings else False
    has_short = holdings[3] == "SHORT" if holdings else False

    close_long = sum([ema8.iloc[-1] < ema21.iloc[-1], rsi.iloc[-1] > 70, cur_price > ema21.iloc[-1]])
    close_short = sum([ema8.iloc[-1] > ema21.iloc[-1], rsi.iloc[-1] < 30, cur_price < ema8.iloc[-1]])
    atr = calculate_atr(data).iloc[-1]
    position_type = holdings[3] 
    entry_point = holdings[1] 
    pnl = (cur_price - entry_point) if position_type == "LONG" else (entry_point - cur_price)
    if pnl <= -5:
        return 3
    if has_long and close_long >= 2:
        return 1  
    elif has_short and close_short >= 2:
        return 2  
    return 0 


def holding_future_contract_expired(holding, next_contract):
    if (holding[6] != next_contract["tickersymbol"]):
        return True
    return False

def future_contract_expired(data, next_contract):
    if (data["tickersymbol"] != next_contract["tickersymbol"]):
        return True
    return False