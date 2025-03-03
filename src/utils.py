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

    plus_dm = data['High'].diff()
    minus_dm = -data['Low'].diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm < 0] = 0

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

def open_position(position_type: str, entry_point: float, holdings: List[Tuple[str, float]]) -> List[Tuple[str, float]]:
    if position_type not in {"LONG", "SHORT"}:
        raise ValueError("Invalid position type. Must be 'LONG' or 'SHORT'.")
    holdings.append((position_type, entry_point))
    return holdings



def close_positions(cur_price: float, holdings: List[Tuple[str, float]]) -> Tuple[List[Tuple[str, float]], float, float]:
    total_realized_pnl = 0.0
    for position_type, entry_point in holdings:
        pnl = (cur_price - entry_point) if position_type == "LONG" else (entry_point - cur_price)
        total_realized_pnl += pnl
    return [], total_realized_pnl



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
    has_long = any(pos[0] == "LONG" for pos in holdings)
    has_short = any(pos[0] == "SHORT" for pos in holdings)

    atr = calculate_atr(data).iloc[-1]

    close_long = sum([ema8.iloc[-1] < ema21.iloc[-1], rsi.iloc[-1] > 70, cur_price > ema21.iloc[-1]])
    close_short = sum([ema8.iloc[-1] > ema21.iloc[-1], rsi.iloc[-1] < 30, cur_price < ema8.iloc[-1]])


    for position_type, entry_point in holdings:
        pnl = (cur_price - entry_point) if position_type == "LONG" else (entry_point - cur_price)
        # print(f"Checking position {position_type}: Entry {entry_point}, Current {cur_price}, PnL {pnl}")
        if pnl >= 1.5*atr or pnl <= -1*atr:
            #print(f"Closing due to threshold: {position_type} {entry_point} -> {cur_price} (PnL: {pnl})")
            return 3  # Close immediately if any position hits take profit or cut loss


    if has_long and close_long >= 2:
        return 1  
    elif has_short and close_short >= 2:
        return 2  
    return 0 


def future_contract_expired(curent_contract, next_contract):
    curent_date = datetime.strptime(curent_contract["Date"], "%Y-%m-%d")
    next_date = datetime.strptime(next_contract["Date"], "%Y-%m-%d")
    if (curent_contract["tickersymbol"] != next_contract["tickersymbol"]):
        return True
    return False