import pandas as pd
import numpy as np

def calculate_ema(data, period):
    print("Not implemented")
    
def calculate_rsi(data, period=14):
    print("Not calculated")
    
def calculate_vwap(data):
    print("Not calculated")
    
def calculate_atr(data, period=14):
    print("Not calculated")

def detect_trend(data, short_period=5, long_period=20):

    ema_short = calculate_ema(data, short_period)
    ema_long = calculate_ema(data, long_period)
    
    sideway = (ema_short - ema_long).abs().rolling(window=10).mean() < data['ATR'] * 0.1
    return sideway 

    