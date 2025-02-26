from data import get_processed_data
from utils import close_positions, open_position, open_position_type, close_position_type
import numpy as np
import matplotlib.pyplot as plt
import pprint
import pandas as pd
from dotenv import load_dotenv
from evaluation2 import plot_backtesting_results
import os
load_dotenv()

INITAL_CAPITAL = float(os.getenv("INITAL_CAPITAL"))  # Default to 100M if not found
CONTRACT_SIZE = int(os.getenv("CONTRACT_SIZE"))
MARGIN_REQUIREMENT = float(os.getenv("MARGIN_REQUIREMENT"))
                           
holdings = []

import pandas as pd

def backtesting(data, holdings=[]):
    cash = INITAL_CAPITAL  
    portfolio_values = []
    total_realized_pnl = 0.0
    trade_log = [] 

    for i in range(len(data)):
        cur_price = data.iloc[i]['Close']
        trade_entry = {"Date": data.iloc[i]["Date"], "Time": data.iloc[i]["Time"], "Price": cur_price}

        # Step 1: Close position if needed
        if holdings: 
            close_action = close_position_type(data.iloc[:i+1], cur_price, holdings)
            if close_action in [1, 2]:
                new_holdings, realized_pnl, closed_position_type = close_positions(cur_price, holdings)
                total_realized_pnl += realized_pnl * CONTRACT_SIZE * 1000
                cash += realized_pnl * CONTRACT_SIZE * 1000

                trade_entry.update({
                    "Action": "Close",
                    "Position Type": closed_position_type,
                    "Trade Price": cur_price,
                    "Total Money": cash
                })
                trade_log.append(trade_entry) 
                holdings = new_holdings 

            continue  

        # Step 2: Check if we can open a position
        open_action = open_position_type(data.iloc[:i+1], cur_price)
        margin_needed = 0  # chua cap nhat margin

        if open_action == 1 and cash >= margin_needed:
            holdings = open_position("LONG", cur_price, holdings)
            cash -= margin_needed
            trade_entry.update({
                "Action": "Open",
                "Position Type": "LONG",
                "Trade Price": cur_price,
                "Total Money": cash
            })
            trade_log.append(trade_entry) 

        elif open_action == 2 and cash >= margin_needed:
            holdings = open_position("SHORT", cur_price, holdings)
            cash -= margin_needed
            trade_entry.update({
                "Action": "Open",
                "Position Type": "SHORT",
                "Trade Price": cur_price,
                "Total Money": cash
            })
            trade_log.append(trade_entry)

        unrealized_pnl = sum(
            (cur_price - pos[1]) * CONTRACT_SIZE * 1000 if pos[0] == "LONG" else
            (pos[1] - cur_price) * CONTRACT_SIZE * 1000
            for pos in holdings
        )

        portfolio_value = cash + unrealized_pnl
        portfolio_values.append({"Date": data.iloc[i]["Date"], "Portfolio Value": portfolio_value})

    trade_log_df = pd.DataFrame(trade_log)
    trade_log_df.to_csv("trade_log.csv", index=False)

    return portfolio_values

if __name__ == "__main__":
    data = pd.read_csv('dataByMinute.csv')
    portfolio_values=backtesting(data)
    pprint.pprint(portfolio_values)
    plot_backtesting_results(portfolio_values)

