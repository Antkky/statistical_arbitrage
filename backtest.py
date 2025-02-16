import pandas as pd
import datetime as dt
import backtrader as bt
import backtrader.feeds as btfeeds
from strategy import StatArb

class HALLDATA(btfeeds.GenericCSVData):
    params = (
        ('nullvalue', 0.0),
        ('dtformat', ('%d.%m.%Y %H:%M:%S.%f')[:-3]),
        ('timeframe', bt.TimeFrame.Minutes),

        ('datetime', 0),
        ('high', 2),
        ('low', 3),
        ('open', 1),
        ('close', 4),
        ('volume', -1),
        ('openinterest', -1)
    )

def run_backtest(symbols: list):
    print("___________Backtester___________")
    # Backtester Config
    cerebro = bt.Cerebro()
    cerebro.broker.set_cash(100000)
    cerebro.broker.set_slippage_perc(0.005)
    fdate = dt.datetime(2019, 1, 2)
    tdate = dt.datetime(2025, 1, 1)
    length = 0

    print("Loading Processed Data...")

    datanames = []
    for symbol in symbols:
        datanames.append(f"{symbol}.csv")

    # Dynamic Data
    for data_file in datanames:
        data_path = f"./data/processed/{data_file}"
        data = HALLDATA(dataname=data_path, fromdate=fdate, todate=tdate)
        cerebro.adddata(data, name=data_file.split('.')[0])

    ratiodata = HALLDATA(dataname='./data/processed/ratio.csv', fromdate=fdate, todate=tdate)
    cerebro.adddata(ratiodata, name='Ratio')

    # get length
    tdata = pd.read_csv('./data/processed/ratio.csv', index_col=0)
    length = len(tdata)

    print("Data Loaded, Running Backtest...")

    # Run backtest
    cerebro.addstrategy(StatArb, length=length)
    cerebro.run()

    print("\nBacktest Completed...")
    cerebro.plot(style='line', stdstats=True)
