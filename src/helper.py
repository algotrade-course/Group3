from typing import List, Tuple
from utils import calculate_ema, calculate_rsi, calculate_vwap, calculate_atr, detect_trend

def open_position(
    position_type: str,
    entry_point: float,
    holdings: List[Tuple[str, float]]
) -> List[Tuple[str, float]]:
    """Opens position and adds to the holdings.

    Args:
        position_type (str): The position type. The algorithm has only two positions: `LONG` or `SHORT`.
        entry_point (float): The entry point of the position. The entry point is the price point at which the position is opened.
        holdings (List[Tuple[str, float]]): The holdings that contain the positions (represented by position type and entry point).

    Returns:
        List[Tuple[str, float]]: The updated holdings containing the new position.
    """
    if position_type not in {"LONG", "SHORT"}:
        raise ValueError("Invalid position type. Must be 'LONG' or 'SHORT'.")

    holdings.append((position_type, entry_point))
    return holdings


def close_positions(
    cur_price: float,
    holdings: List[Tuple[str, float]]
) -> Tuple[List[Tuple[str, float]], float, float]:
    """Closes the position and removes it from the holdings while computing realized and unrealized PnL.
    
    Args:
        cur_price (float): Current price point.
        holdings (List[Tuple[str, float]]): The holdings that contain the positions 
        (represented by position type and entry point).

    Returns:
        Tuple containing updated holdings, total realized profit/loss, and total unrealized profit/loss.
    """
    total_realized_pnl = 0
    total_unrealized_pnl = 0
    new_holdings = [] 

    for position_type, entry_point in holdings:
        pnl = cur_price - entry_point  # Absolute point-based PnL

        if pnl >= TAKE_PROFIT_THRES or pnl <= CUT_LOSS_THRES:
            total_realized_pnl += pnl  
        else:
            total_unrealized_pnl += pnl
            new_holdings.append((position_type, entry_point))
    
    return new_holdings, total_realized_pnl, total_unrealized_pnl

#Output: 
# 0: No action
# 1: open long position
# 2: open short position

def open_position_type(data, stock, cur_price):
    ema5 = calculate_ema(data['Close'], 5)
    ema20 = calculate_ema(data['Close'],20)
    vwap = calculate_vwap(data)
    rsi = calculate_rsi(data, 14)
    
    if (detect_trend[0] == True or rsi<30 or rsi>70):
        return 0
    
    long_criteria = int(ema5>ema20) + int(cur_price > vwap) + int(rsi >= 50) + int(rsi < 70) + int(detect_trend[1] == True)
    short_criteria =  int(ema5<ema20) + int(cur_price < vwap) + int(rsi<50) + int(rsi>30) + int(detect_trend[1]==False)
    
    if (long_criteria > 3):
        return 1
    if (short_criteria > 3):
        return 2

# Output:
## 0: Noo action
## 1: Close Long position
## 2: Close short position
## Note: Check if the holding is close before acting close long position, and otherwise.
def close_position_type(data, stock, cur_price):
    ema5 = calculate_ema(data['Close'], 5)
    ema20 = calculate_ema(data['Close'],20)
    rsi = calculate_rsi(data, 14)

    close_long_criteria = int(ema5 < ema20) + int(rsi>70) + int(cur_price>ema20)
    close_short_criteria = int(ema5 > ema20) + int(rsi<30) + int(cur_price<ema5)
    
    if (close_long_criteria >= 1):
        return 1
    elif (close_short_criteria >= 1):
        return 2
    else:
        return 0