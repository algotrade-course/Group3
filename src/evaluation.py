   

def asset_over_time(data, plt):
    data['Asset'].plot(kind='line', figsize=(8, 4), title='Asset Over Time')
    plt.gca().spines[['top', 'right']].set_visible(False)

def hold_period_return(data):
    cur_asset_value = data['Asset'].iloc[-1]
    init_asset_value = data['Asset'].iloc[0]
    accum_return_rate = (cur_asset_value / init_asset_value - 1) * 100
    return accum_return_rate


def calculate_max_drawdown(portfolio_values) -> float:
    cumulative_max = portfolio_values["Portfolio Value"].cummax()
    drawdown = (portfolio_values["Portfolio Value"] - cumulative_max) / cumulative_max
    return drawdown.min()



def calculate_sharpe_ratio(portfolio_values, risk_free_rate: float = 0.03) -> float:
 
    portfolio_values["Returns"] = portfolio_values["Portfolio Value"].pct_change().dropna()
    daily_risk_free = risk_free_rate / 252
    excess_returns = portfolio_values["Returns"] - daily_risk_free
    return excess_returns.mean() / excess_returns.std()

def plot_sharpe_ratio(portfolio_values):

    portfolio_values["Returns"] = portfolio_values["Portfolio Value"].pct_change().dropna()
    rolling_sharpe = portfolio_values["Returns"].rolling(window=30).mean() / portfolio_values["Returns"].rolling(window=30).std()

    plt.figure(figsize=(10, 5))
    plt.plot(portfolio_values["Date"], rolling_sharpe, label="Rolling Sharpe Ratio (30-day)", color='blue')
    plt.axhline(0, color='black', linestyle='--', linewidth=1)
    plt.xlabel("Date")
    plt.ylabel("Sharpe Ratio")
    plt.title("Rolling Sharpe Ratio Over Time")
    plt.legend()
    plt.show()
