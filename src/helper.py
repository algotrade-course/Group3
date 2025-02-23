def open_position(
    position_type: str,
    entry_point: float,
    holdings: List[[str, float]]
):
    """Opens position and add to the holding.

    Args:
        position_type (str): The position type. The algorithm has only two positions: `LONG` or `SHORT`.
        entry_point (float): The entry point of the position. The entry point is the price point at which the position is opened.
        holdings (List[[str, float]]): The holdings that contain the positions (represented by position type and entry point).

    Returns:
        The holdings contain positions of the algorithm.
    """
    # YOUR CODE HERE
    raise NotImplementedError()
    
    return holdings




# close position when there is trading signal. Remove the position from holdings.
def close_positions(
    cur_price: float,
    holdings: List[[str, float]]
):
    """Closes the position and remove from the holding. Also, performs some accounting tasks such as calculating realized and unrealized profit and loss (PnL).
    
    Args:
        cur_price (float): Current price point.
        holdings (List[[str, float]]): The holdings that contain the positions (represented by position type and entry point).

    Returns:
        The holdings contain the algorithm's positions and the total realized and unrealized profit and loss.
    """

    total_realized_pnl = 0
    total_unrealized_pnl = 0

    for position_type, entry_point in holdings[:]:
    # Loop through the opened position and check if the position has reached the TAKE_PROFIT_THRES or CUT_LOSS_THRES
    # then close (remove) the position from the holdings.
    # Remember to update the total_realized_pnl and total_unrealized_pnl accordingly
        # YOUR CODE HERE
        raise NotImplementedError()
    
    return holdings, total_realized_pnl, total_unrealized_pnl