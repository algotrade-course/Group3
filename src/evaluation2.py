import matplotlib.pyplot as plt
import numpy as np

def maximumDrawdown(portfolio_series):
    peak = np.maximum.accumulate(portfolio_series)  
    drawdown = (portfolio_series - peak) / peak  
    mdd = np.min(drawdown)  
    return (mdd, drawdown)
    
def sharpRatio(portfolio_series):
    returns = np.diff(portfolio_series) / portfolio_series[:-1]  # Compute returns
    sharpe_ratio = np.mean(returns) / np.std(returns)  
    return sharpe_ratio



def plot_backtesting_results(portfolio_values):
    dates = [x["Date"] for x in portfolio_values]  # Extract dates
    portfolio_series = np.array([x["Portfolio Value"] for x in portfolio_values])  # Portfolio values

    mdd, drawdown = maximumDrawdown(portfolio_series=portfolio_series) # MDD

    sharpe_ratio = sharpRatio(portfolio_series=portfolio_series) #Sharp ratio

    fig, ax1 = plt.subplots(figsize=(12, 6))

    ax1.plot(dates, portfolio_series, label="Portfolio Value", color="blue", linewidth=2)
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Portfolio Value", color="blue")
    ax1.tick_params(axis="y", labelcolor="blue")
    ax1.legend(loc="upper left")

    ax2 = ax1.twinx()
    ax2.fill_between(dates, drawdown, 0, color="red", alpha=0.3)
    ax2.set_ylabel("Drawdown", color="red")
    ax2.tick_params(axis="y", labelcolor="red")

    plt.title(f"Backtesting Results\nMDD: {mdd:.2%} | Sharpe Ratio: {sharpe_ratio:.4f}")

    plt.show()
