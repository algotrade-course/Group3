import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from utils import (
    close_positions, open_position, open_position_type, is_next_day, 
    close_position_type, holding_future_contract_expired, 
    calculate_pnl_after_fee, check_margin_ratio
)
from evaluation import maximumDrawdown,plot_all_portfolio_results,sharpe_ratio
import argparse
from tqdm import tqdm

load_dotenv()

INITIAL_CAPITAL = float(os.getenv("INITAL_CAPITAL"))
CONTRACT_SIZE = int(os.getenv("CONTRACT_SIZE"))
MARGIN_REQUIREMENT = float(os.getenv("MARGIN_REQUIREMENT"))
data_path_env= os.getenv('DATAPATH')
ema_fast=float(os.getenv('EMA_FAST'))
ema_slow=float(os.getenv('EMA_SLOW'))
atr_period=int(os.getenv('ATR_PERIOD'))
rsi_period=int(os.getenv('RSI_PERIOD'))
rsi_upper_threshold=float(os.getenv('RSI_UPPER'))
rsi_lower_threshold=float(os.getenv('RSI_LOWER'))
vol_window=int(os.getenv('VOL_WINDOW'))
vol_thres=float(os.getenv('VOL_THRES'))
max_loss=float(os.getenv('MAX_LOSS'))
min_profit=float(os.getenv('MIN_PROFIT'))
atr_multiplier=float(os.getenv('ATR_MULT'))
rsi_exit_threshold_range=float(os.getenv('RSI_EXIT'))


def future_contract_expired_close(holdings, cur_price, i, cash=INITIAL_CAPITAL):
    total_realized_pnl = 0.0
    _, entry_price, _, position_type, _, _ , ticketsymbol= holdings
    pnl = (cur_price - entry_price) if position_type == "LONG" else (entry_price - cur_price)
    value_in_cash = calculate_pnl_after_fee(pnl)
    total_realized_pnl += pnl * CONTRACT_SIZE * 1000
    cash += value_in_cash
    holdings = []
    return i + 1, [], total_realized_pnl, cash

def backtesting(data, ema_periods, rsi_period, atr_period, 
                vol_window, vol_thres, rsi_upper_threshold, 
                rsi_lower_threshold,max_loss, min_profit, 
                atr_multiplier, rsi_exit_threshold_range):
    
    cash = INITIAL_CAPITAL
    portfolio_values = []
    total_realized_pnl = 0.0
    holdings = []
    trade_log = []
    k=0

    for i in tqdm(range(len(data)), desc="Running backtest"):
        cur_price=data.iloc[i]['Close']
        trade_entry= {"Date": data.iloc[i]["Date"],
                      "Time": data.iloc[i]["Time"],
                      "Price": cur_price}

        if holdings:
            close_action = close_position_type(data.iloc[k:i+1], cur_price, holdings, ema_periods, 
                                               rsi_period, atr_period, max_loss, min_profit, 
                                               atr_multiplier, rsi_exit_threshold_range)

            if  holding_future_contract_expired(holdings, data.iloc[i]):
                # print("Future contract expired", holdings, data.iloc[i])
                k, holdings, total_realized_pnl, cash = future_contract_expired_close(holdings, cur_price, i, cash)
                trade_entry.update({
                        "Action": "Close_Future_Expired",
                        "Position Type": "None",
                        "Trade Price": cur_price,
                        "Total Money": cash,
                        "Total Point": total_realized_pnl
                })
                trade_log.append(trade_entry)
                portfolio_value = cash
                portfolio_values.append({"Date": data.iloc[i]["Date"], "Portfolio Value": portfolio_value})
                continue

            if close_action in [1, 2]:
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

            else:
                portfolio_value = cash
                portfolio_values.append({"Date": data.iloc[i]["Date"], "Portfolio Value": portfolio_value})
                continue

        if not holdings:
            open_action = open_position_type(data.iloc[k:i+1], cur_price, ema_periods, rsi_period,
                                             vol_window, vol_thres, rsi_upper_threshold, 
                                             rsi_lower_threshold)
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
                portfolio_value = cash
                portfolio_values.append({"Date": data.iloc[i]["Date"], "Portfolio Value": portfolio_value})
                continue

        portfolio_value = cash
        portfolio_values.append({"Date": data.iloc[i]["Date"], "Portfolio Value": portfolio_value})
    
    trade_log_df = pd.DataFrame(trade_log)
    portfolio_df = pd.DataFrame(portfolio_values)
    portfolio_series = portfolio_df["Portfolio Value"].values
    
    mdd, _ = maximumDrawdown(portfolio_series)
    sharpeRatio = sharpe_ratio(portfolio_series, rf_rate=0.03, periods_per_year=len(data))
    
    return portfolio_df, trade_log_df, sharpeRatio, mdd


def run_backtests(data_path: str,  result_dir: str, plot_path: str):
    data = pd.read_csv(data_path)
    # strategy_params = pd.read_csv(params_path)

    # for _, row in strategy_params.iterrows():
    ema_periods = (ema_fast, ema_slow) 
    print(vol_window)
    print(f"Running backtest for EMA {ema_periods}, RSI {rsi_period}")
    portfolio_df, trades_df, sharpe_ratio, mdd = backtesting(data=data, ema_periods=ema_periods, rsi_period=rsi_period, 
                                                             atr_period=atr_period, vol_window=vol_window, vol_thres=vol_thres, 
                                                             rsi_upper_threshold=rsi_upper_threshold, rsi_lower_threshold=rsi_lower_threshold,
                                                             max_loss=max_loss, min_profit=min_profit, atr_multiplier=atr_multiplier,
                                                             rsi_exit_threshold_range=rsi_exit_threshold_range)
    suffix = f"{ema_periods[0]}_{ema_periods[1]}_{rsi_period}"
    trades_df.to_csv(os.path.join(result_dir, f"trade_log_{suffix}.csv"), index=False)
    portfolio_df.to_csv(os.path.join(result_dir, f"portfolio_values_{suffix}.csv"), index=False)
    
    print(f'SHARPE RATIO: {sharpe_ratio:.4f}')
    print(f'MDD: {mdd:.2%}')

    plot_all_portfolio_results(result_dir=result_dir, output_file=plot_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run backtests")
    parser.add_argument("--dataset", type=str, required=True, help="Name of the dataset CSV file (in the 'data/' folder)")
    parser.add_argument("--result_dir", type=str, default="result", help="Directory to save result CSVs")
    args = parser.parse_args()
    if data_path_env is not None:
        data_path = os.path.join(data_path_env, args.dataset)
    else:
        data_path = os.path.join("src/data", args.dataset)
    
    plot_path = os.path.join(args.result_dir, "all_backtests.png")

    os.makedirs(args.result_dir, exist_ok=True)
    run_backtests(
        data_path=data_path,
        result_dir=args.result_dir,
        plot_path=plot_path
    )

    print(f"Backtesting completed. Results saved to {args.result_dir} and plot saved to {plot_path}.")

# python backtesting.py --dataset 2024-01-01_to_2025-01-01_by_5T.csv --result_dir result 