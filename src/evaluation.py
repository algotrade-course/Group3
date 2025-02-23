def asset_over_time(data, plt):
    data['Asset'].plot(kind='line', figsize=(8, 4), title='Asset Over Time')
    plt.gca().spines[['top', 'right']].set_visible(False)

def hold_period_return(data):
    cur_asset_value = data['Asset'].iloc[-1]
    init_asset_value = data['Asset'].iloc[0]
    accum_return_rate = (cur_asset_value / init_asset_value - 1) * 100
    return accum_return_rate


def  maximum_drawdown(data):
    data['peak'] = data.apply(lambda row: data.loc[:row.name, 'Asset'].max(), axis=1)
    data['drawdown'] = data['Asset']/data['peak'] - 1
    mdd = data['drawdown'].min() * 100
    return mdd

def sharpe_ratio(data, plt, trading_day):
    daily_return = data['Asset'][1:].to_numpy() / data['Asset'][:-1].to_numpy() - 1
    x = list(range(len(daily_return)))
    plt.plot(x, daily_return)
    plt.title(label="Daily Return")
    plt.show()
    risk_free_rate = 0.03
    annual_std = np.sqrt(trading_day) * np.std(daily_return)
    annual_return = trading_day * np.mean(daily_return) - risk_free_rate
    sharpe = annual_return / annual_std
    return sharpe
