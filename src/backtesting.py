from data import get_processed_data
from utils import close_positions, open_position, open_position_type, is_next_day, close_position_type, holding_future_contract_expired, calculate_pnl_after_fee, check_margin_ratio
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

def future_contract_expired_close(holdings, cur_price, i, cash=INITAL_CAPITAL):
    total_realized_pnl = 0.0
    _, entry_price, _, position_type, _, _ , ticketsymbol= holdings 
    pnl = (cur_price - entry_price) if position_type == "LONG" else (entry_price - cur_price)
    value_in_cash = calculate_pnl_after_fee(pnl)
    total_realized_pnl += pnl * CONTRACT_SIZE * 1000
    cash += value_in_cash
    holdings = []
    return i + 1, [], total_realized_pnl, cash
                      
def backtesting(data):
    cash = INITAL_CAPITAL
    portfolio_values = []
    total_realized_pnl = 0.0
    holdings = []
    trade_log = []
    k=0
    
    for i in range(len(data)):
        cur_price=data.iloc[i]['Close']
        trade_entry= {"Date": data.iloc[i]["Date"], 
                      "Time": data.iloc[i]["Time"], 
                      "Price": cur_price}    
      
        if holdings:
            close_action = close_position_type(data.iloc[k:i+1], cur_price, holdings)

            if  holding_future_contract_expired(holdings, data.iloc[i]):
                print("Future contract expired", holdings, data.iloc[i])
                k, holdings, total_realized_pnl, cash = future_contract_expired_close(holdings, cur_price, i, cash)
                trade_entry.update({
                        "Action": "Close_Future_Expired",
                        "Position Type": "None",
                        "Trade Price": cur_price,
                        "Total Money": cash,
                        "Total Point": total_realized_pnl
                })
                trade_log.append(trade_entry)
                continue
                            
            
            if close_action in [1, 2, 3]:
                pos_type = holdings[3]
                new_holdings, realized_pnl = close_positions(data.iloc[k:i+1], cur_price, holdings)
                value_in_cash = calculate_pnl_after_fee(realized_pnl)
                total_realized_pnl += value_in_cash
                cash += value_in_cash
                holdings = []
                log = {
                    "Date": data.iloc[i]["Date"], 
                    "Time": data.iloc[i]["Time"], 
                    "Price": cur_price,
                    "Action": "Close",
                    "Position Type": "None",
                    "Trade Price": cur_price,
                    "Total Money": cash,
                    "Total Point": total_realized_pnl
                }
                trade_log.append(log)
                # if close_action == 3:
                #     if pos_type == "LONG":
                #         is_reverse = 1
                #     else:
                #         is_reverse = 2
                # else: 
                #     is_reverse = 0
                                        
            else:
                continue
        
        if not holdings:
            open_action = open_position_type(data.iloc[k:i+1], cur_price)
            if (open_action in [1, 2]) and check_margin_ratio(cash, cur_price, MARGIN_REQUIREMENT,CONTRACT_SIZE,1):
     
                position_type = "LONG" if open_action == 1 else "SHORT"
                    
                holdings = open_position(position_type, cur_price, holdings,data.iloc[i], cash)
                trade_entry.update({
                    "Action": "Open", 
                    "Position Type": position_type,
                    "Trade Price": cur_price, 
                    "Total Money": cash,
                })
                trade_log.append(trade_entry)
                continue

        portfolio_value = cash
        portfolio_values.append({"Date": data.iloc[i]["Date"], "Portfolio Value": portfolio_value})

    trade_log_df = pd.DataFrame(trade_log)
    trade_log_df.to_csv("trade_log.csv", index=False)

    return portfolio_values

if __name__ == "__main__":
    data = pd.read_csv('dataByMinute.csv')
    portfolio_values=backtesting(data)
    # pprint.pprint(portfolio_values)
    plot_backtesting_results(portfolio_values)
