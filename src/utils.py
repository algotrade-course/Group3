import pandas as pd
import numpy as np
import pprint
from typing import List, Tuple
from datetime import datetime
from dotenv import load_dotenv
import os
load_dotenv()

from preprocess import get_expiry_date

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

def calculate_atr(data, period=14):
    tr = pd.concat([
        data["High"] - data["Low"],
        (data["High"] - data["Close"].shift()).abs(),
        (data["Low"] - data["Close"].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()

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

    return [], round(pnl,3)

def calculate_pnl_after_fee(pnl, contract_size=100):
    profit_after_fee = pnl - 0.47
    profit_in_cash=profit_after_fee*contract_size*1000
    return profit_in_cash


def open_position_type(data, cur_price, ema_periods, rsi_period, vol_window, vol_thres, rsi_upper_threshold, rsi_lower_threshold):
    ema10 = calculate_ema(data, 'Close', ema_periods[0])
    ema30 = calculate_ema(data, 'Close', ema_periods[1])
    rsi = calculate_rsi(data, 'Close', rsi_period)

    if pd.isna(rsi.iloc[-1]) or pd.isna(ema10.iloc[-1]) or pd.isna(ema30.iloc[-1]):
        return 0  # not enough data

    current_vol = data['Volume'].iloc[-1]
    avg_vol = data['Volume'].rolling(window=vol_window).mean().iloc[-1]

    # Trend and momentum confirmation
    long_trend = ema10.iloc[-1] > ema30.iloc[-1] and cur_price > ema10.iloc[-1] and rsi.iloc[-1] > rsi_upper_threshold
    short_trend = ema10.iloc[-1] < ema30.iloc[-1] and cur_price < ema10.iloc[-1] and rsi.iloc[-1] < rsi_lower_threshold
    strong_volume = current_vol > vol_thres * avg_vol

    if long_trend and strong_volume:
        return 1  # Long
    elif short_trend and strong_volume:
        return 2  # Short
    else:
        return 0  # Do nthing


def close_position_type(data, cur_price, holdings, ema_periods, rsi_period, atr_period, max_loss, min_profit, atr_multiplier, rsi_exit_threshold_range):
    ema10 = calculate_ema(data, 'Close', ema_periods[0])
    ema30 = calculate_ema(data, 'Close', ema_periods[1])
    rsi = calculate_rsi(data, 'Close', rsi_period)
    atr = calculate_atr(data,atr_period).iloc[-1]
    
    if pd.isna(rsi.iloc[-1]) or pd.isna(ema10.iloc[-1]) or pd.isna(ema30.iloc[-1]):
        return 0
    
    if not holdings or len(holdings) < 4:
        return 0
    
    entry_price = holdings[1]
    position_type = holdings[3].upper()
    
    # Configurable thresholds
    # max_loss = 3.0
    # min_profit = 0.5
    # atr_multiplier = 1.5

    if position_type == "LONG":
        pnl = cur_price - entry_price

        # CUt loss
        if pnl <= -max_loss:
            return 1
        
        if cur_price < ema30.iloc[-1] and rsi.iloc[-1] < rsi_exit_threshold_range:
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

        if cur_price > ema30.iloc[-1] and rsi.iloc[-1] > rsi_exit_threshold_range:
            return 2

        if pnl > min_profit:
            trailing_stop = max(entry_price - min_profit, data['Close'].iloc[-2] - atr * atr_multiplier)
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

