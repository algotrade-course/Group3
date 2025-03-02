import matplotlib.pyplot as plt
import numpy as np
import matplotlib.dates as mdates

def maximumDrawdown(portfolio_series):
    peak = np.maximum.accumulate(portfolio_series)  
    drawdown = (portfolio_series - peak) / peak  
    mdd = np.min(drawdown)  
    return (mdd, drawdown)

def sharpRatio(portfolio_series, periods_per_year=252):
    if len(portfolio_series) < 2:
        return 0  

    returns = np.diff(portfolio_series) / portfolio_series[:-1] 
    if np.std(returns) == 0:
        return 0 

    sharpe_ratio = np.mean(returns) / (np.std(returns) + 1e-8)  
    sharpe_ratio *= np.sqrt(periods_per_year) 
    return sharpe_ratio

def plot_backtesting_results(portfolio_values):
    if len(portfolio_values) < 2:
        print("Not enough data to plot backtesting results.")
        return

    dates = [x["Date"] for x in portfolio_values]
    portfolio_series = np.array([x["Portfolio Value"] for x in portfolio_values])

    mdd, drawdown = maximumDrawdown(portfolio_series)
    sharpe_ratio = sharpRatio(portfolio_series)

    fig, ax1 = plt.subplots(figsize=(12, 6))

    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    
    ax1.plot(dates, portfolio_series, label="Portfolio Value", color="blue", linewidth=2)
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Portfolio Value", color="blue")
    ax1.tick_params(axis="y", labelcolor="blue")
    ax1.grid(True, linestyle="--", alpha=0.5)

    ax2 = ax1.twinx()
    ax2.fill_between(dates, drawdown, 0, color="darkred", alpha=0.3)
    ax2.set_ylabel("Drawdown", color="red")
    ax2.tick_params(axis="y", labelcolor="red")

    plt.title(f"Backtesting Results\nMax Drawdown (MDD): {mdd:.2%} | Sharpe Ratio: {sharpe_ratio:.4f}")

    ax1.annotate(f"Max Drawdown: {mdd:.2%}", 
                 xy=(dates[-1], min(portfolio_series)), 
                 xytext=(-60, 20), 
                 textcoords="offset points", 
                 arrowprops=dict(arrowstyle="->", color="black"), 
                 fontsize=10, color="black")

    ax1.annotate(f"Sharpe Ratio: {sharpe_ratio:.4f}", 
                 xy=(dates[-1], max(portfolio_series)), 
                 xytext=(-60, -20), 
                 textcoords="offset points", 
                 arrowprops=dict(arrowstyle="->", color="black"), 
                 fontsize=10, color="black")

    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.show()
