import backtrader as bt
import pandas as pd
import numpy as np
import csv
from collections import deque
from tqdm import tqdm

class StatArb(bt.Strategy):
    def __init__(self, length, rolling_window=20, overbought_level=2.0, oversold_level=-2.0, bb_length=50, bb_multiplier=2.0):
        self.trade_log = []
        self.EURUSD = self.getdatabyname('EURUSD')
        self.EURGBP = self.getdatabyname('EURGBP')
        self.GBPUSD = self.getdatabyname('GBPUSD')
        self.rolling_window = rolling_window
        self.spread = deque(maxlen=rolling_window)
        self.oversold_level = oversold_level
        self.overbought_level = overbought_level
        self.bb_length = bb_length
        self.bb_multiplier = bb_multiplier
        self.length: int = length

    def next(self):
        if not all(d for d in [self.EURUSD, self.EURGBP, self.GBPUSD]):
            return

        spread_value = self.EURUSD.close[0] - self.GBPUSD.close[0]
        self.spread.append(spread_value)

        if len(self.spread) < self.rolling_window:
            return

        rolling_mean = np.mean(self.spread)
        rolling_std = np.std(self.spread)
        if rolling_std == 0:
            return

        z_score = (spread_value - rolling_mean) / rolling_std
        bb_values = self.EURGBP.close.get(size=self.bb_length)
        if len(bb_values) < self.bb_length:
            return

        bb_mean = np.mean(bb_values)
        bb_std = np.std(bb_values)
        bb_upper = bb_mean + (self.bb_multiplier * bb_std)
        bb_lower = bb_mean - (self.bb_multiplier * bb_std)

        if z_score < self.overbought_level and self.EURGBP.close[0] > bb_upper:
            if self.position:
                self.close(self.EURGBP)
            self.sell(self.EURGBP, size=100000)

        if z_score > self.oversold_level and self.EURGBP.close[0] < bb_lower:
            if self.position:
                self.close(self.EURGBP)
            self.buy(self.EURGBP, size=100000)

    def notify_trade(self, trade):
        if trade.justopened:
            self.trade_log.append({
                'tradeID': trade.ref,
                'open_datetime': self.data.datetime.datetime(0),
                'close_datetime': None,
                'size': trade.size,
                'commission': trade.commission,
                'entry_price': trade.price,
                'exit_price': None,
                'pnl': None,
                'pnl_after_commission': None
            })

        if trade.isclosed:
            for trd in self.trade_log:
                if trd['tradeID'] == trade.ref:
                    trd['close_datetime'] = self.data.datetime.datetime(0)
                    trd['exit_price'] = trade.price
                    trd['pnl'] = trade.pnl
                    trd['pnl_after_commission'] = trade.pnlcomm

    def stop(self):
        with open("./tests/latest/trades.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(['TradeID', 'Open Datetime', 'Close Datetime', 'Size', 'Commission', 'Entry Price', 'Exit Price', 'PnL', 'PnL After Commission'])
            for trade in self.trade_log:
                writer.writerow([
                    trade['tradeID'], trade['open_datetime'], trade['close_datetime'],
                    trade['size'], trade['commission'], trade['entry_price'],
                    trade['exit_price'], trade['pnl'], trade['pnl_after_commission']
                ])
