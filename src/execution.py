import pandas as pd
from utils import calculate_ema, calculate_rsi, calculate_vwap, calculate_atr, detect_trend

def load_data():
    print("Not implemented")

def is_valid_trade(data, index):
    
    if index < 20:
        print("Please provide more data")
        return None
    
    ## Ghi chu: ema5 va ema20, 5 vaf 20 co the update o optimizatino sau backtesting
    
    ema5 = data['EMA5'].iloc[index]
    ema20 = data['EMA20'].iloc[index]
    prev_ema5 = data['EMA5'].iloc[index - 1]
    prev_ema20 = data['EMA20'].iloc[index - 1]
    rsi = data['RSI'].iloc[index]
    vwap = data['VWAP'].iloc[index]
    atr = data['ATR'].iloc[index]
    sideway = data['Sideway'].iloc[index] ## Khong thuc hienj trading neu cos sideway
    
    avg_volume = data['Volume'].rolling(10).mean()
    increasing_volume = data['Volume'].iloc[index] > avg_volume.iloc[index] * 1.1 # 10% higherr than avg. True/False
    decreasing_volume = data['Volume'].iloc[index] < avg_volume.iloc[index] * 0.9 # 10% lower than avg. True/False

    buy_signal = (
        prev_ema5 < prev_ema20 and ema5 > ema20 and  
        data['Close'].iloc[index] > vwap and  
        50 < rsi < 70 and  
        increasing_volume and  
        not sideway  
    )
    
    sell_signal = (
        prev_ema5 > prev_ema20 and ema5 < ema20 and  
        data['Close'].iloc[index] < vwap and  
        30 < rsi < 50 and  
        decreasing_volume and  
        not sideway  
    )
    
    if buy_signal:
        return "BUY -- Long"
    elif sell_signal:
        return "SELL -- Short"
    return None
    
def should_close_position(entry_price, current_price, stop_loss=-3, take_profit=5):
    if current_price >= entry_price + take_profit:
        return "CLOSE_LONG" 
    elif current_price <= entry_price + stop_loss:
        return "CLOSE_SHORT" 
    return "HOLD" 


if __name__ == "__main__":
    data = load_data()

    data['EMA5'] = calculate_ema(data, 5)
    data['EMA20'] = calculate_ema(data, 20)
    data['RSI'] = calculate_rsi(data, 14)
    data['VWAP'] = calculate_vwap(data)
    data['ATR'] = calculate_atr(data, 14)
    data['Sideway'] = detect_trend(data)

    data['Signal'] = [is_valid_trade(data, i) for i in range(len(data))]

    print(data[['Close', 'EMA5', 'EMA20', 'RSI', 'VWAP', 'ATR', 'Sideway', 'Signal']])