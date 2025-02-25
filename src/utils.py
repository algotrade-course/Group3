import pandas as pd
import numpy as np
import pprint
from typing import List, Tuple
from dotenv import load_dotenv
import os
load_dotenv()

TAKE_PROFIT_THRES = float(os.getenv("TAKE_PROFIT_THRES")) 
CUT_LOSS_THRES = int(os.getenv("CUT_LOSS_THRES"))


def calculate_ema(df, column, period):
    return df[column].ewm(span=period, adjust=False).mean()

def calculate_rsi(df, column='Close', period=14):
    delta = df[column].diff().to_numpy()
    gain = np.zeros_like(delta)
    loss = np.zeros_like(delta)
    mask = delta > 0
    gain[mask] = delta[mask]
    loss[~mask] = -delta[~mask]
    avg_gain = pd.Series(gain).ewm(span=period, adjust=False).mean()
    avg_loss = pd.Series(loss).ewm(span=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return np.minimum(rsi, 100)
    

def calculate_vwap(df):
    df = df.dropna(subset=['High', 'Low', 'Close', 'Volume'])
    df = df[df['Volume'] > 0]
    typical_price = (df['High'] + df['Low'] + df['Close']) / 3
    cumulative_vol = df['Volume'].cumsum()
    return (typical_price * df['Volume']).cumsum() / cumulative_vol


def calculate_atr(data, period=14):
    high_low = data["High"] - data["Low"]
    high_close = (data["High"] - data["Close"].shift()).abs()
    low_close = (data["Low"] - data["Close"].shift()).abs()

    high_close.fillna(high_low, inplace=True)
    low_close.fillna(high_low, inplace=True)

    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()

    return atr

def check_volume_trend(data):
    avg_volume = data['Volume'].rolling(10).mean()
    increasing_volume = data['Volume'].iloc[-1] > avg_volume.iloc[-1] * 1.1 # 10% higherr than avg. True/False
    return increasing_volume
    
def detect_trend(data, short_period=5, long_period=20,column='Close'):
    ema_short = calculate_ema(data,column, short_period)
    ema_long = calculate_ema(data,column, long_period)
    
    atr = calculate_atr(data)
    
    sideway = (ema_short - ema_long).abs().rolling(window=10).mean() < atr * 0.1
    
    is_sideway = sideway.iloc[-1] if isinstance(sideway, pd.Series) else sideway

    return [is_sideway,check_volume_trend(data)]


def open_position(position_type: str, entry_point: float, holdings: List[Tuple[str, float]]) -> List[Tuple[str, float]]:
    if position_type not in {"LONG", "SHORT"}:
        raise ValueError("Invalid position type. Must be 'LONG' or 'SHORT'.")

    holdings.append((position_type, entry_point))
    return holdings


def close_positions(
    cur_price: float,
    holdings: List[Tuple[str, float]]
) -> Tuple[List[Tuple[str, float]], float, float]:
    total_realized_pnl = 0.0
    total_unrealized_pnl = 0.0
    new_holdings = []  

    for position_type, entry_point in holdings:
        if position_type == "LONG":
            pnl = cur_price - entry_point  # Profit if price goes up
        elif position_type == "SHORT":
            pnl = entry_point - cur_price  # Profit if price goes down
        else:
            raise ValueError(f"Unknown position type: {position_type}")
        if pnl >= TAKE_PROFIT_THRES or pnl <= CUT_LOSS_THRES:
            total_realized_pnl += pnl  
        else:
            total_unrealized_pnl += pnl
            new_holdings.append((position_type, entry_point))  # Keep position open

    return new_holdings, total_realized_pnl, total_unrealized_pnl


def open_position_type(data, cur_price):
    ema5 = calculate_ema(data,'Close', 5)
    ema20 = calculate_ema(data,'Close', 20)
    vwap = calculate_vwap(data)
    rsi = calculate_rsi(data,'Close', 14)
    # print(f'RSI: {rsi}')
    # print(f'EMA5: {ema5}')
    # print(f'EMA20: {ema20}')
    # print(f'vwap: {vwap}')
    trend_info = detect_trend(data, 5, 20, 'Close')  
    
    if (trend_info[0] == 1) or pd.isna(rsi.iloc[-1]) or (rsi[-1] < 30 or rsi[-1] > 70):
        return 0  
    
    long_criteria = int(ema5.iloc[-1] > ema20.iloc[-1]) + int(cur_price > vwap) + int(50 <= rsi.iloc[-1] < 70) + int(trend_info[1])
    short_criteria = int(ema5.iloc[-1] < ema20.iloc[-1]) + int(cur_price < vwap) + int(30 < rsi.iloc[-1] < 50) + int(not trend_info[1])

    if long_criteria >= 3:
        return 1  
    if short_criteria >= 3:
        return 2  

    return 0  

def close_position_type(data, cur_price, holdings):
    ema5 = calculate_ema(data,'Close', 5)
    ema20 = calculate_ema(data,'Close', 20)
    rsi = calculate_rsi(data,'Close', 14)

    has_long_position = any(pos[0] == "LONG" for pos in holdings)
    has_short_position = any(pos[0] == "SHORT" for pos in holdings)
    
    print(ema5)
    close_long_criteria = int(ema5.iloc[-1] < ema20.iloc[-1]) + int(rsi.iloc[-1] > 70) + int(cur_price > ema20.iloc[-1])
    close_short_criteria = int(ema5.iloc[-1] > ema20.iloc[-1]) + int(rsi.iloc[-1] < 30) + int(cur_price < ema5.iloc[-1])

    if has_long_position and close_long_criteria >= 1:
        return 1  # Close LONG
    elif has_short_position and close_short_criteria >= 1:
        return 2  # Close SHORT
    else:
        return 0  # No action  

if __name__ == "__main__":
    data = pd.read_csv('data.csv')
    values=open_position_type(data.iloc[:10], 1000)
    pprint.pprint(values)


