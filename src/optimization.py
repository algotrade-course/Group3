import os
import numpy as np
import pandas as pd
import json
import argparse

# from concurrent.futures import ProcessPoolExecutor, as_completed

import matplotlib.pyplot as plt

from itertools import product
from dotenv import load_dotenv
from utils import (
    close_positions, open_position, open_position_type, is_next_day, 
    close_position_type, holding_future_contract_expired, 
    calculate_pnl_after_fee, check_margin_ratio
)
from evaluation import plot_all_portfolio_results
from backtesting import future_contract_expired_close, backtesting
load_dotenv()

'''
    Usage:
    Calling optimization: python3 src/optimization.py -d src/data/2023-01-01_to_2023-12-31_by_5T.csv -p src/optimization.json -m 2500 -o src/OPTIMIZATION_RANDOM
    Calling visualization (after optimization sucessfully): python3 src/optimization.py -v <PATH_TO_SUMMARY_FILE (CREATED BY OPTIMIZATION PROCESS)>
'''

SEED = 42
rng = np.random.default_rng(SEED)

INITIAL_CAPITAL = float(os.getenv("INITAL_CAPITAL"))
CONTRACT_SIZE = int(os.getenv("CONTRACT_SIZE"))
MARGIN_REQUIREMENT = float(os.getenv("MARGIN_REQUIREMENT"))

def ensure_summary_file(path):
    header = [
        'SampleIndex','EMA_Short','EMA_Long','RSI_Period','RSI_lower','RSI_upper',
        'ATR_period','Max_Loss','Min_Profit','ATR_Mult','Volume_Threshold',
        'Volume_window','RSI_exit_threshold','Sharpe','mdd','net_profit'
    ]
    if not os.path.isfile(path):
        pd.DataFrame(columns=header).to_csv(path, index=False)
        
def visualization(data_path):
    df = pd.read_csv(data_path)

    plt.figure()
    plt.scatter(df['Sharpe'], df['mdd'], s=40, alpha=0.7)
    plt.xlabel('Sharpe Ratio')
    plt.ylabel('Max Drawdown')
    plt.title('Sharpe vs. Max Drawdown')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    plt.figure()
    plt.hist(df['net_profit'], bins=50)
    plt.xlabel('Net Profit')
    plt.ylabel('Frequency')
    plt.title('Distribution of Net Profit Across All Samples')
    plt.tight_layout()
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.show()

    plt.figure()
    plt.scatter(df['Sharpe'], df['net_profit'], s=40, alpha=0.7)
    plt.xlabel('Sharpe Ratio')
    plt.ylabel('Net Profit')
    plt.title('Sharpe vs. Net Profit')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def optimize_strategy(data, optimization_json="optimization.json", result_dir="OPTIMIZATION", max_models=2500):
    os.makedirs(result_dir, exist_ok=True)
    summary_path = os.path.join(result_dir, 'summary.csv')
    ensure_summary_file(summary_path)

    with open(optimization_json, "r") as f:
        pc = json.load(f)

    param_lists = [
        pc["ema_short_range"],
        pc["ema_long_range"],
        pc["rsi_range"],
        pc["rsi_lower_threshold_range"],
        pc["rsi_upper_threshold_range"],
        pc["atr_period_range"],
        pc["max_loss_range"],
        pc["min_profit_range"],
        pc["atr_mult_range"],
        pc["vol_thresh_range"],
        pc["volume_window_range"],
        pc["rsi_exit_threshold_range"]
    ]

    all_combos = list(product(*param_lists))
    perm = rng.permutation(len(all_combos))

    combos_to_run = [all_combos[i] for i in perm[:max_models]]

    results = []
    total = len(combos_to_run)
    for idx, combo in enumerate(combos_to_run, start=1):
        print(f"Sample {idx}/{total}")
        (
            ema_short, ema_long, rsi,
            rsi_lower, rsi_upper,
            atr_period, max_loss, min_profit,
            atr_mult, vol_thresh, vol_window,
            rsi_exit
        ) = combo

        portfolio_df, trades_df, sharpe, mdd = backtesting(
            data=data,
            ema_periods=(ema_short, ema_long),
            rsi_period=rsi,
            atr_period=atr_period,
            vol_window=vol_window,
            vol_thres=vol_thresh,
            rsi_upper_threshold=rsi_upper,
            rsi_lower_threshold=rsi_lower,
            max_loss=max_loss,
            min_profit=min_profit,
            atr_multiplier=atr_mult,
            rsi_exit_threshold_range=rsi_exit
        )

        initial = portfolio_df["Portfolio Value"].iloc[0]
        net_profit = portfolio_df["Portfolio Value"].iloc[-1] - initial

        row = {
            'SampleIndex': idx,
            'EMA_Short': ema_short,
            'EMA_Long': ema_long,
            'RSI_Period': rsi,
            'RSI_lower': rsi_lower,
            'RSI_upper': rsi_upper,
            'ATR_period': atr_period,
            'Max_Loss': max_loss,
            'Min_Profit': min_profit,
            'ATR_Mult': atr_mult,
            'Volume_Threshold': vol_thresh,
            'Volume_window': vol_window,
            'RSI_exit_threshold': rsi_exit,
            'Sharpe': sharpe,
            'mdd': mdd,
            'net_profit': net_profit
        }

        pd.DataFrame([row]).to_csv(summary_path, mode='a', header=False, index=False)
        results.append(row)

        trades_df.to_csv(os.path.join(result_dir, f"trade_log_{idx}.csv"), index=False)
        portfolio_df.to_csv(os.path.join(result_dir, f"portfolio_values_{idx}.csv"), index=False)

    return pd.DataFrame(results).sort_values('Sharpe', ascending=False)

def score_and_visualize(input_csv_path, output_csv_path="outscore.csv"):
    def calculate_score(sharpe, mdd, net_profit, alpha=0.5, beta=0.01):
        if np.isnan(sharpe) or np.isnan(mdd) or np.isnan(net_profit):
            return -np.inf
        score = sharpe - alpha * abs(mdd)
        return score

    df = pd.read_csv(input_csv_path)

    df["score"] = df.apply(lambda row: calculate_score(row["Sharpe"], row["mdd"], row["net_profit"]), axis=1)

    output_df = df.sort_values(by="score", ascending=False)

    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

    output_df.to_csv(output_csv_path, index=False)

    plt.figure(figsize=(10, 6))
    plt.hist(df["score"], bins=50, edgecolor='black')
    plt.title("Distribution of Optimization Scores")
    plt.xlabel("Score")
    plt.ylabel("Frequency")
    plt.grid(True)
    plt.tight_layout()

    fig_path = os.path.splitext(output_csv_path)[0] + "_score_distribution.png"
    plt.savefig(fig_path)
    plt.show()

    print(f"Output saved to: {output_csv_path}")
    print(f"Figure saved to: {fig_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Either run optimization or visualize existing results"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-d", "--data",
        help="Path to CSV file containing historical OHLCV data (will run optimization)"
    )
    group.add_argument(
        "-v", "--visualize",
        metavar="SUMMARY_CSV",
        help="Path to summary.csv to plot your results"
    )
    group.add_argument(
        "-s", "--score",
        metavar="SUMMARY_CSV",
        help="Path to summary.csv to calculate for models' score."
    )
    
    parser.add_argument(
        "-f", "--outscore",
        metavar="OUTPUT_SCORE_CSV",
        default="scored_models.csv",
        help="Path to save the scored output CSV file (used with --score). Default: scored_models.csv"
    )
    
    parser.add_argument(
        "-p", "--parameter",
        help="Path to the JSON file containing the possible values for each parameter to be optimized."
    )
    parser.add_argument(
        "-m", "--maximum-model",
        type=int,
        help="Number of models randomly selected from the list of parameter combinations."
    )
    parser.add_argument(
        "-o", "--result-dir",
        default="OPTIMIZATION",
        help="Directory to save optimization results"
    )

    args = parser.parse_args()

    if args.visualize:
        visualization(args.visualize)
    elif args.score:
        score_and_visualize(input_csv_path=args.score, output_csv_path=args.outscore)
    else:
        data = pd.read_csv(args.data, parse_dates=True)
        df = optimize_strategy(data, optimization_json=args.parameter, result_dir=args.result_dir, max_models=args.maximum_model)
        print(df.head())



if __name__ == '__main__':
    main()