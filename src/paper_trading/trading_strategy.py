import pandas as pd
import numpy as np

class TradingStrategy:
    def __init__(self, initial_cash=1_000_000, contract_size=100, margin_requirement=0.175):
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
        tr = pd.concat([data["High"] - data["Low"],
                        (data["High"] - data["Close"].shift()).abs(),
                        (data["Low"] - data["Close"].shift()).abs()], axis=1).max(axis=1)
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

    def close_position(self, data, cur_price):
        if not self.holdings:
            return

        entry_price = self.holdings[1]
        position_type = self.holdings[3]
        pnl = (cur_price - entry_price) if position_type == "LONG" else (entry_price - cur_price)

        atr = self.calculate_atr(data).iloc[-1]
        self.pnl += round(pnl, 3)

        self.holdings = None  # Position closed
        self.trade_log.append((data['Date'], position_type, entry_price, cur_price, self.pnl))

    def process_tick(self, tick_data, data):
        cur_price = tick_data['Close']

        position_type = self.open_position_type(data, cur_price)
        if position_type == 1:  # Long signal
            if self.holdings is None:
                self.open_position("LONG", cur_price, tick_data)
        elif position_type == 2:  # Short signal
            if self.holdings is None:
                self.open_position("SHORT", cur_price, tick_data)
        
        # Closing logic
        if self.holdings:
            self.close_position(data, cur_price)

    def open_position_type(self, data, cur_price, ema_periods=[10, 30], rsi_period=14):
        ema10 = self.calculate_ema(data, 'Close', ema_periods[0])
        ema30 = self.calculate_ema(data, 'Close', ema_periods[1])
        rsi = self.calculate_rsi(data, 'Close', rsi_period)

        if pd.isna(rsi.iloc[-1]) or pd.isna(ema10.iloc[-1]) or pd.isna(ema30.iloc[-1]):
            return 0  # not enough data

        long_trend = ema10.iloc[-1] > ema30.iloc[-1] and cur_price > ema10.iloc[-1] and rsi.iloc[-1] > 55
        short_trend = ema10.iloc[-1] < ema30.iloc[-1] and cur_price < ema10.iloc[-1] and rsi.iloc[-1] < 45

        if long_trend:
            return 1  # Long signal
        elif short_trend:
            return 2  # Short signal
        else:
            return 0  # No action
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

    def get_trade_log(self):
        return self.trade_log

    def get_nav_history(self):
        return self.nav_history

    def get_max_drawdown(self):
        return self.max_drawdown

    def get_current_nav(self):
        return self.nav_history[-1]
