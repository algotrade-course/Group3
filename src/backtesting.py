from helper import open_position, close_positions, position_type
from data import get_processed_data
from utils import calculate_ema, calculate_rsi


holdings = []
data = get_processed_data("2022-12-11", "2022-12-30")

def backtesting(data, sma_period=14, initial_capital=100000):
    prev_min = None
    trading_data = data.copy()
    asset_value = initial_capital
    holdings = []
    
    for minute in data.index:
        if prev_min is None:
            prev_min = minute
            continue

        cur_price = data.loc[minute, 'Close']

        holdings, total_realized_pnf, total_unrealized_pnf = close_positions(cur_price, holdings)

        asset_value = asset_value + total_realized_pnf

        if total_realized_pnf == 0:
            trading_data.loc[minute, 'Asset'] = asset_value + total_unrealized_pnf
        else:
            trading_data.loc[minute, 'Asset'] = asset_value

        if holdings:
            continue

        prev_price = data.loc[prev_min, 'Close']
        prev_sma = data.loc[prev_min, f'SMA_{sma_period}']
        cur_sma = data.loc[minute, f'SMA_{sma_period}']

        # Open a LONG position if price crosses above SMA
        if prev_price < prev_sma and cur_price > cur_sma:
            holdings.append({'type': 'LONG', 'entry_price': cur_price})
        
        # Open a SHORT position if price crosses below SMA
        elif prev_price > prev_sma and cur_price < cur_sma:
            holdings.append({'type': 'SHORT', 'entry_price': cur_price})

        prev_min = minute  # Update previous minute

    trading_data = trading_data.dropna()
    data = trading_data.copy()
    
    return trading_data


