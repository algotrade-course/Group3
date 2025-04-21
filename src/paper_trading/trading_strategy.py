import pandas as pd
import numpy as np

class TradingStrategy:
    def __init__(self, initial_cash=40000000, contract_size=100, margin_requirement=0.175):
        self.cash = initial_cash
        self.position = 0  # No position initially
        self.position_price = 0
        self.pnl = 0
        self.contract_size = contract_size
        self.margin_requirement = margin_requirement
        self.trade_log = []
        self.holdings = None  # Stores the current open position (None if no position)

    def calculate_ema(self, df, column, period):
        return df[column].ewm(span=period, adjust=False).mean()

    def calculate_rsi(self, df, column='Close', period=9):
        df = df.copy()
        df[column] = df[column].astype(float)
        delta = df[column].diff().to_numpy()
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        avg_gain = pd.Series(gain).ewm(span=period, adjust=False).mean()
        avg_loss = pd.Series(loss).ewm(span=period, adjust=False).mean()
        rs = np.where(avg_loss == 0, np.inf, avg_gain / avg_loss)
        rsi = 100 - (100 / (1 + rs))
        return pd.Series(rsi, index=df.index)

    def calculate_vwap(self, df):
        df = df.dropna().copy()
        df = df[df['Volume'] > 0].copy()
        typical_price = (df['High'] + df['Low'] + df['Close']) / 3
        vwap_series = (typical_price * df['Volume']).cumsum() / df['Volume'].cumsum()
        return vwap_series.iloc[-1]

    def calculate_atr(self, data, period=14):
        # Ensure all price columns are float
        data = data.copy()
        data["High"] = data["High"].astype(float)
        data["Low"] = data["Low"].astype(float)
        data["Close"] = data["Close"].astype(float)

        tr = pd.concat([
            data["High"] - data["Low"],
            (data["High"] - data["Close"].shift()).abs(),
            (data["Low"] - data["Close"].shift()).abs()
        ], axis=1).max(axis=1)
        
        return tr.rolling(window=period).mean()


    def check_volume_trend(self, data, direction="LONG"):
        avg_volume = data['Volume'].rolling(15).mean()
        if direction == "LONG":
            return data['Volume'].iloc[-1] > avg_volume.iloc[-1] * 1.05
        return data['Volume'].iloc[-1] < avg_volume.iloc[-1] * 0.95

    def detect_trend(self, data, short_period=10, long_period=50, column='Close'):
        ema_short = self.calculate_ema(data, column, short_period)
        ema_long = self.calculate_ema(data, column, long_period)
        atr = self.calculate_atr(data)
        sideway = (ema_short - ema_long).abs().rolling(window=10).mean() < atr * 0.2
        return [sideway.iloc[-1], self.check_volume_trend(data)]

    def calculate_adx(self, data, period=14):
        raw_plus_dm = data['High'].diff()
        raw_minus_dm = -data['Low'].diff()

        plus_dm = raw_plus_dm.copy()
        minus_dm = raw_minus_dm.copy()

        plus_condition = (raw_plus_dm > 0) & (raw_plus_dm > raw_minus_dm)
        minus_condition = (raw_minus_dm > 0) & (raw_minus_dm > raw_plus_dm)

        plus_dm[~plus_condition] = 0
        minus_dm[~minus_condition] = 0

        tr = pd.concat([data['High'] - data['Low'],
                        (data['High'] - data['Close'].shift()).abs(),
                        (data['Low'] - data['Close'].shift()).abs()], axis=1).max(axis=1)

        plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / tr.ewm(alpha=1/period, adjust=False).mean())
        minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / tr.ewm(alpha=1/period, adjust=False).mean())
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.ewm(alpha=1/period, adjust=False).mean()
        return adx

    def open_position(self, position_type, entry_point, data):
        if position_type not in {"LONG", "SHORT"}:
            raise ValueError("Invalid position type. Must be 'LONG' or 'SHORT'.")
        self.holdings = (data['Date'], entry_point, "OPEN", position_type, entry_point, self.cash, data['tickersymbol'])
        print(f"Opened position: {position_type} at {entry_point}, Cash: {self.cash}")
        self.trade_log.append((data['Date'], position_type, entry_point, entry_point, self.pnl))

    def close_position(self, data, cur_price):
        if not self.holdings:
            return

        entry_price = self.holdings[1]
        position_type = self.holdings[3]
        pnl = (cur_price - entry_price) if position_type == "LONG" else (entry_price - cur_price)

        atr = self.calculate_atr(data).iloc[-1]
        self.pnl += round(pnl, 3)

        self.holdings = None
        value_in_cash = self.calculate_pnl_after_fee(pnl) 
        self.cash += value_in_cash
        print(f"Closed position: {position_type} at {cur_price}, PnL: {self.pnl}")  
        self.trade_log.append((data['Date'], position_type, entry_price, cur_price, self.pnl))
  

    def process_tick(self, tick_data, data):
        cur_price = tick_data['Close']
        if self.holdings:
            close_type = self.close_position_type(data, cur_price, self.holdings, (10,30), 18, 21, 2.0, 0.5, 1.0, 55)
            if (close_type in [1,2]):
                self.close_position(data, cur_price)
            return 
        
        position_type = self.open_position_type(data, cur_price, (10,30), 18, 15, 0.8, 70, 25)
        print(f"Position Type: {position_type}")

        if position_type == 1 and self.holdings is None:
            self.open_position("LONG", cur_price, tick_data)
            return

        elif position_type == 2 and self.holdings is None:
            self.open_position("SHORT", cur_price, tick_data)
            return 

    def open_position_type(self, data, cur_price, ema_periods, rsi_period, vol_window, vol_thres, rsi_upper_threshold, rsi_lower_threshold):
        ema10 = self.calculate_ema(data, 'Close', ema_periods[0])
        ema30 = self.calculate_ema(data, 'Close', ema_periods[1])
        rsi = self.calculate_rsi(data, 'Close', rsi_period)

        if pd.isna(rsi.iloc[-1]) or pd.isna(ema10.iloc[-1]) or pd.isna(ema30.iloc[-1]):
            return 0  # not enough data

        current_vol = data['Volume'].iloc[-1]
        avg_vol = data['Volume'].rolling(window=vol_window).mean().iloc[-1]

        # Trend and momentum confirmation
        long_trend = ema10.iloc[-1] > ema30.iloc[-1] and cur_price > ema10.iloc[-1] and rsi.iloc[-1] > rsi_upper_threshold
        short_trend = ema10.iloc[-1] < ema30.iloc[-1] and cur_price < ema10.iloc[-1] and rsi.iloc[-1] < rsi_lower_threshold
        strong_volume = current_vol > vol_thres * avg_vol

        if long_trend and strong_volume:
            return 1  # Long
        elif short_trend and strong_volume:
            return 2  # Short
        else:
            return 0  # Do nthing
        
    def close_position_type(self, data, cur_price, holdings, ema_periods, rsi_period, atr_period, max_loss, min_profit, atr_multiplier, rsi_exit_threshold_range):
        ema10 = self.calculate_ema(data, 'Close', ema_periods[0])
        ema30 = self.calculate_ema(data, 'Close', ema_periods[1])
        rsi = self.calculate_rsi(data, 'Close', rsi_period)
        atr = self.calculate_atr(data,atr_period).iloc[-1]
        
        if pd.isna(rsi.iloc[-1]) or pd.isna(ema10.iloc[-1]) or pd.isna(ema30.iloc[-1]):
            return 0
        
        if not holdings or len(holdings) < 4:
            return 0
        
        entry_price = holdings[1]
        position_type = holdings[3].upper()
        
        # Configurable thresholds
        # max_loss = 3.0
        # min_profit = 0.5
        # atr_multiplier = 1.5

        if position_type == "LONG":
            pnl = cur_price - entry_price

            # CUt loss
            if pnl <= -max_loss:
                return 1
            
            if cur_price < ema30.iloc[-1] and rsi.iloc[-1] < rsi_exit_threshold_range:
                return 1

            # Trailing stop if profitable
            if pnl > min_profit:
                trailing_stop = max(entry_price + min_profit, data['Close'].iloc[-2] - atr * atr_multiplier)
                if cur_price < trailing_stop:
                    return 1

            return 0

        elif position_type == "SHORT":
            pnl = entry_price - cur_price

            if pnl <= -max_loss:
                return 2

            if cur_price > ema30.iloc[-1] and rsi.iloc[-1] > rsi_exit_threshold_range:
                return 2

            if pnl > min_profit:
                trailing_stop = min(entry_price - min_profit, data['Close'].iloc[-2] + atr * atr_multiplier)
                if cur_price > trailing_stop:
                    return 2

            return 0

        return 0
    
    def update_nav(self):
            # NAV = cash + sum of positions' values
            positions_value = sum(position['size'] for position in self.positions)
            current_nav = self.current_cash + positions_value
            self.nav_history.append(current_nav)

            # Update max NAV for MDD calculation
            if current_nav > self.max_nav:
                self.max_nav = current_nav

            # Calculate MDD
            drawdown = (self.max_nav - current_nav) / self.max_nav
            if drawdown > self.max_drawdown:
                self.max_drawdown = drawdown

    def calculate_pnl_after_fee(self, pnl: float):
        profit_after_fee = pnl - 0.47
        profit_in_cash=profit_after_fee*1000*1000
        return profit_in_cash

    def get_trade_log(self):
        return self.trade_log

    def get_nav_history(self):
        return self.nav_history

    def get_max_drawdown(self):
        return self.max_drawdown

    def get_current_nav(self):
        return self.nav_history[-1]
