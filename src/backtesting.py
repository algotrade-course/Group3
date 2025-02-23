

holdings = []

def backtesting(data):
    prev_min = None
    trading_data = data.copy()

    for min in data.index:

        if prev_min is None:
            prev_min = min
            continue

        cur_price = data['Close'][min]
        
        # Determine if any positions need to be closed
        # If some cases in the trading algorithm, we need to make sure to close the positions first when we get a new price point
        # You need to specify the correct code to close the position here. One line is sufficient.
        # YOUR CODE HERE
        raise NotImplementedError()
        
        # Update asset value when position is realized
        asset_value = asset_value + total_realized_pnf

        # Update asset history in both cases
        if total_realized_pnf == 0:
            trading_data.loc[date, 'Asset'] = asset_value + total_unrealized_pnf
        else:
            trading_data.loc[date, 'Asset'] = asset_value

        # Make sure to open one contract only
        if holdings:
            continue

        # Calculating new signal
        prev_price = data['Close']['WTM'][prev_date]
        prev_sma = data[SMA_SYMBOL][prev_date]
        cur_sma = data[SMA_SYMBOL][date]
        
        # Open a LONG position when there are signals and add to the holdings
        # You need to specify the code to open a LONG position here. Refer to the SMA Algorithm in the lecture if needed.
        # YOUR CODE HERE
        raise NotImplementedError()

        # Open a SHORT position when there are signals and add to the holdings
        # You need to specify the code to open a SHORT position here. Refer to the SMA Algorithm in the lecture if needed.
        # YOUR CODE HERE
        raise NotImplementedError()
        
        # Prepare for the next iteration
        prev_date = date

    trading_data = trading_data.dropna()
    data = trading_data.copy()