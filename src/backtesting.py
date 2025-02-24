from data import get_processed_data
from utils import close_positions, open_position, open_position_type, close_position_type
import numpy as np
import matplotlib.pyplot as plt
import pprint
import pandas as pd
from dotenv import load_dotenv
import os
load_dotenv()


INITAL_CAPITAL = float(os.getenv("INITAL_CAPITAL"))  # Default to 100M if not found
CONTRACT_SIZE = int(os.getenv("CONTRACT_SIZE"))
MARGIN_REQUIREMENT = float(os.getenv("MARGIN_REQUIREMENT"))
                           
holdings = []

def backtesting(data, holdings=holdings):
    cash = INITAL_CAPITAL # Available cash for trading
    portfolio_values = []  
    total_realized_pnl = 0.0  

    for i in range(len(data)):
        row = data.iloc[i]
        cur_price = row["Close"]

        # Step 1: Close position if needed
        close_action = close_position_type(data.iloc[:i+1], cur_price, holdings)
        if close_action in [1, 2]:
            print("Closing position")  
            new_holdings, realized_pnl, _ = close_positions(cur_price, holdings)
            total_realized_pnl += realized_pnl * CONTRACT_SIZE* 1000
            cash += realized_pnl * CONTRACT_SIZE* 1000  
            holdings = new_holdings  

        open_action = open_position_type(data.iloc[:i+1],cur_price)
        margin_needed = cur_price * CONTRACT_SIZE * MARGIN_REQUIREMENT*1000

        if open_action == 1 and cash >= margin_needed:
            print("Open Long")  # Open LONG
            holdings = open_position("LONG", cur_price, holdings)
            cash -= margin_needed  

        elif open_action == 2 and cash >= margin_needed:
            print("Open SHORT")  # Open SHORT
            holdings = open_position("SHORT", cur_price, holdings)
            cash -= margin_needed  

        _, _, unrealized_pnl = close_positions(cur_price, holdings)
        portfolio_value = cash + unrealized_pnl * CONTRACT_SIZE*1000
        portfolio_values.append({"Date": row["Date"], "Portfolio Value": portfolio_value})

    return portfolio_values


if __name__ == "__main__":
    data = pd.read_csv('data.csv')
    portfolio_values=backtesting(data)
    pprint.pprint(portfolio_values)

