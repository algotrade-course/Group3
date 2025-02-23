import pandas as pd
import numpy as np
import pprint

# Note: infput is the closes prices of each candle withifn the window. type is pandas series.
def calculate_ema(data, period):
    print(data)
    return pd.Series(data).ewm(span=period, adjust=False).mean()


def calculate_rsi(data, period=14):
    delta = pd.Series(data).diff()
    
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = pd.Series(gain).ewm(span=period, adjust=False).mean()
    avg_loss = pd.Series(loss).ewm(span=period, adjust=False).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    rsi = np.where(avg_loss == 0, 100, rsi)
    return pd.Series(rsi, index=data.index)
    

def calculate_vwap(data):
    data = data.dropna(subset=['High', 'Low', 'Close', 'Volume'])
    data = data[data['Volume'] > 0]
    
    typical_price = (data['High'] + data['Low'] + data['Close'])/3


    cumulative_vol = np.cumsum(data['Volume'])
    cumulative_vwap = np.cumsum(typical_price * data['Volume'])/cumulative_vol
    
    return pd.Series(cumulative_vwap, index=data.index)
    
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
    
def detect_trend(data, short_period=5, long_period=20):

    ema_short = calculate_ema(data['Close'], short_period)
    ema_long = calculate_ema(data['Close'], long_period)
    atr = calculate_atr(data)
    sideway = (ema_short - ema_long).abs().rolling(window=10).mean() < atr * 0.1
    return [sideway,check_volume_trend(data)]


    