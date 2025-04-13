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


def calculate_rsi(df, column='Close', period=9):
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
    holdings = (data['Date'], entry_point, "OPEN", position_type, entry_point, cash, data['tickersymbol'])
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
    return [], round(pnl,3)

def calculate_pnl_after_fee(pnl, contract_size=100):
    profit_after_fee = pnl - 0.47
    profit_in_cash=profit_after_fee*contract_size*1000
    return profit_in_cash

# def open_position_type(data, cur_price, ema_periods, rsi_period):
#     """
#     Determines whether to open a long (1), short (2), or no position (0) based on weighted criteria.
    
#     Assumes the following helper functions are defined:
#       - calculate_ema(data, column, period)
#       - calculate_vwap(data)
#       - calculate_rsi(data, column, period)
#       - calculate_atr(data)
#       - calculate_adx(data)
#       - detect_trend(data, fast_period, slow_period, column)
      
#     Returns:
#       1: Long signal
#       2: Short signal
#       0: No signal / Hold
#     """
#     # Calculate technical indicators.
#     ema8   = calculate_ema(data, 'Close', ema_periods[0])
#     ema21  = calculate_ema(data, 'Close', ema_periods[1])
#     vwap   = calculate_vwap(data)
#     rsi    = calculate_rsi(data, 'Close', rsi_period)
#     atr    = calculate_atr(data)
#     adx    = calculate_adx(data)
#     trend_info = detect_trend(data, 10, 30, 'Close')  # trend_info[0]: avoid signal flag, trend_info[1]: bullish flag

#     rsi_lower = 30 + (10 * (atr.iloc[-1] / data['Close'].iloc[-1]))
#     rsi_upper = 70 - (10 * (atr.iloc[-1] / data['Close'].iloc[-1]))

#     if pd.isna(rsi.iloc[-1]) or not (rsi_lower < rsi.iloc[-1] < rsi_upper) or adx.iloc[-1] < 30 or trend_info[0]:
#         return 0

#     weights = {
#         'ema':   1.0,   
#         'vwap':  0.5,   
#         'rsi':   0.7,  
#         'trend': 1.0,   
#     }
    
#     long_score = 0.0
#     short_score = 0.0

#     if ema8.iloc[-1] > ema21.iloc[-1]:
#         long_score += weights['ema']
#     else:
#         short_score += weights['ema']

#     if cur_price > vwap:
#         long_score += weights['vwap']
#     else:
#         short_score += weights['vwap']

#     mid_rsi = (rsi_lower + rsi_upper) / 2
#     if rsi.iloc[-1] < mid_rsi:
#         long_score += weights['rsi']
#     else:
#         short_score += weights['rsi']

#     if trend_info[1]:
#         long_score += weights['trend']
#     else:
#         short_score += weights['trend']
        
#     threshold = 1.5

#     if long_score > short_score and long_score >= 1.5:
#         return 1 
#     elif short_score > long_score and short_score >= 1.5:
#         return 2  
#     else:
#         return 0 

def open_position_type(data, cur_price, ema_periods, rsi_period):
    ema10 = calculate_ema(data, 'Close', ema_periods[0])
    ema30 = calculate_ema(data, 'Close', ema_periods[1])
    rsi = calculate_rsi(data, 'Close', rsi_period)

    if pd.isna(rsi.iloc[-1]) or pd.isna(ema10.iloc[-1]) or pd.isna(ema30.iloc[-1]):
        return 0  # not enough data

    current_vol = data['Volume'].iloc[-1]
    avg_vol = data['Volume'].rolling(window=10).mean().iloc[-1]

    # Trend and momentum confirmation
    long_trend = ema10.iloc[-1] > ema30.iloc[-1] and cur_price > ema10.iloc[-1] and rsi.iloc[-1] > 55
    short_trend = ema10.iloc[-1] < ema30.iloc[-1] and cur_price < ema10.iloc[-1] and rsi.iloc[-1] < 45
    strong_volume = current_vol > 1.2 * avg_vol

    if long_trend and strong_volume:
        return 1  # Long
    elif short_trend and strong_volume:
        return 2  # Short
    else:
        return 0  # Do nthing


# def close_position_type(data, cur_price, holdings, ema_periods, rsi_period):
#     ema8 = calculate_ema(data, 'Close', ema_periods[0])
#     ema21 = calculate_ema(data, 'Close', ema_periods[1])
#     rsi = calculate_rsi(data, 'Close', rsi_period)
#     atr = calculate_atr(data).iloc[-1]

#     if not holdings or len(holdings) < 4:
#         return 0

#     has_long = holdings[3] == "LONG"
#     has_short = holdings[3] == "SHORT"

#     weights = {
#         'ema_rev': 1.0,   
#         'rsi': 0.8,       
#         'price_ema': 0.6   
#     }
    
#     exit_score = 0.0

#     if has_long:
#         if ema8.iloc[-1] < ema21.iloc[-1]:  
#             exit_score += weights['ema_rev']
#         if rsi.iloc[-1] > 70:                
#             exit_score += weights['rsi']
#         if cur_price > ema21.iloc[-1]:        
#             exit_score += weights['price_ema']
    
#     if has_short:
#         if ema8.iloc[-1] > ema21.iloc[-1]:
#             exit_score += weights['ema_rev']
#         if rsi.iloc[-1] < 30:
#             exit_score += weights['rsi']
#         if cur_price < ema8.iloc[-1]:
#             exit_score += weights['price_ema']

#     position_type = holdings[3]
#     entry_point = holdings[1]
#     pnl = (cur_price - entry_point) if position_type == "LONG" else (entry_point - cur_price)
#     atr_stop_loss = 1.5 * atr

#     if has_long and cur_price < ema21.iloc[-1] * 0.98:
#         return 3
#     if has_short and cur_price > ema8.iloc[-1] * 1.02:
#         return 3
#     if pnl <= -atr_stop_loss or pnl >= 5:
#         return 3
#     threshold = 1.5 

#     if exit_score >= threshold and pnl >= 0.47:
#         # Return 1 for long exit, 2 for short exit based on position type
#         return 1 if has_long else 2
    
#     return 0
def close_position_type(data, cur_price, holdings, ema_periods, rsi_period):
    ema10 = calculate_ema(data, 'Close', ema_periods[0])
    ema30 = calculate_ema(data, 'Close', ema_periods[1])
    rsi = calculate_rsi(data, 'Close', rsi_period)
    atr = calculate_atr(data).iloc[-1]
    
    if pd.isna(rsi.iloc[-1]) or pd.isna(ema10.iloc[-1]) or pd.isna(ema30.iloc[-1]):
        return 0
    
    if not holdings or len(holdings) < 4:
        return 0
    
    entry_price = holdings[1]
    position_type = holdings[3].upper()
    
    # Configurable thresholds
    max_loss = 3.0
    min_profit = 0.5
    atr_multiplier = 1.5

    if position_type == "LONG":
        pnl = cur_price - entry_price

        # CUt loss
        if pnl <= -max_loss:
            return 1
        
        # Strong revErsal signal: price below EMA30 and RSI < 50
        if cur_price < ema30.iloc[-1] and rsi.iloc[-1] < 50:
            return 1

        # Trailing stop if profitable
        if pnl > min_profit:
            trailing_stop = max(entry_price + min_profit, data['Close'].iloc[-2] - atr * atr_multiplier)
            if cur_price < trailing_stop:
                return 1

        return 0

    elif position_type == "SHORT":
        pnl = entry_price - cur_price

        if pnl <= -max_loss:
            return 2

        # Strong reversal signal: price above EMA30 and RSI > 50
        if cur_price > ema30.iloc[-1] and rsi.iloc[-1] > 50:
            return 2

        if pnl > min_profit:
            trailing_stop = min(entry_price - min_profit, data['Close'].iloc[-2] + atr * atr_multiplier)
            if cur_price > trailing_stop:
                return 2

        return 0

    return 0


def holding_future_contract_expired(holding, next_contract):
    if (holding[6] != next_contract["tickersymbol"]):
        return True
    return False

def future_contract_expired(data, next_contract):
    if (data["tickersymbol"] != next_contract["tickersymbol"]):
        return True
    return False

def check_margin_ratio(cash, cur_price, margin_requierment, contract_size,volume=1):
    price_with_margin = margin_requierment * cur_price * 1000 * volume * contract_size
    account_ration=price_with_margin/0.8
    return cash >= account_ration

def is_next_day(data, index):
    return (data.iloc[index]["Time"] == "14:27:00") or (data.iloc[index+1]["Time"] == "09:00:00")

# def is_next_session(data, index):
#     return (data.iloc[index]["Time"] == "11:27:00") or (data.iloc[index+1]["Time"] == "13:00:00")
