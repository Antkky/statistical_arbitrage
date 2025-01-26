import pandas as pd
import numpy as np
import random
import datetime as dt
import backtrader as bt
import backtrader.feeds as btfeeds

from strategy import StatArb
from Apexa.metrics import calculate_metrics
from Apexa.monte_carlo import run_monte_carlo

import json
import argparse

class HALLDATA(btfeeds.GenericCSVData):
    params = (
        ('nullvalue', 0.0),
        ('dtformat', ('%d.%m.%Y %H:%M:%S')),
        ('timeframe', bt.TimeFrame.Minutes),

        ('datetime', 0),
        ('high', 2),
        ('low', 3),
        ('open', 1),
        ('close', 4),
        ('volume', -1),
        ('openinterest', -1)
    )

def pre_process_data():
    print("Loading Raw Data...")
    # Read Data
    EURUSD = pd.read_csv("./data/EURUSD.csv", index_col=0, parse_dates=True)
    GBPUSD = pd.read_csv("./data/GBPUSD.csv", index_col=0, parse_dates=True)

    print("Data Loaded, Processing...")

    # Align the indexs
    common_timestamps = EURUSD.index.intersection(GBPUSD.index)
    EURUSD_aligned = EURUSD.loc[common_timestamps]
    GBPUSD_aligned = GBPUSD.loc[common_timestamps]

    # Merge datasets
    merged_df = EURUSD_aligned.merge(
        GBPUSD_aligned,
        left_index=True,
        right_index=True,
        suffixes=('_EURUSD', '_GBPUSD')
    )

    # Synthetically create EURGBP
    EURGBP = pd.DataFrame(index=merged_df.index)
    EURGBP["Open"] = merged_df["Open_EURUSD"] / merged_df["Open_GBPUSD"]
    EURGBP["High"] = merged_df["High_EURUSD"] / merged_df["High_GBPUSD"]
    EURGBP["Low"] = merged_df["Low_EURUSD"] / merged_df["Low_GBPUSD"]
    EURGBP["Close"] = merged_df["Close_EURUSD"] / merged_df["Close_GBPUSD"]

    # Declare new datasets
    nEURUSD = pd.DataFrame(index=merged_df.index)
    nGBPUSD = pd.DataFrame(index=merged_df.index)

    # Fill new datasets
    nEURUSD["Open"], nEURUSD["High"], nEURUSD["Low"], nEURUSD["Close"] = (
        merged_df["Open_EURUSD"], merged_df["High_EURUSD"], merged_df["Low_EURUSD"], merged_df["Close_EURUSD"]
    )
    nGBPUSD["Open"], nGBPUSD["High"], nGBPUSD["Low"], nGBPUSD["Close"] = (
        merged_df["Open_GBPUSD"], merged_df["High_GBPUSD"], merged_df["Low_GBPUSD"], merged_df["Close_GBPUSD"]
    )

    # Format seconds on 'Gmt time' column
    EURUSD_aligned.index = EURUSD_aligned.index.str.replace(r'\.000$', '', regex=True)
    GBPUSD_aligned.index = GBPUSD_aligned.index.str.replace(r'\.000$', '', regex=True)
    EURGBP.index = EURGBP.index.str.replace(r'\.000$', '', regex=True)

    # Drop Volume Volumn
    EURUSD_aligned.drop(["Volume"], axis=1, inplace=True)
    GBPUSD_aligned.drop(["Volume"], axis=1, inplace=True)

    print("Processing Complete, Saving Data...")

    # Save data to csv files
    EURGBP.to_csv("./data/processed/EURGBP.csv")
    EURUSD_aligned.to_csv("./data/processed/EURUSD.csv")
    GBPUSD_aligned.to_csv("./data/processed/GBPUSD.csv")

    print("Data Successfully Processed")

def run_backtest():
    print("___________Backtester___________")
    # Backtester Config
    cerebro = bt.Cerebro()
    cerebro.broker.set_cash(100000)
    cerebro.broker.set_slippage_perc(0.005)
    fdate = dt.datetime(2024, 11, 1)
    tdate = dt.datetime(2025, 1, 1)

    print("Loading Processed Data...")

    # Dynamic Data
    for data_file in ['EURGBP.csv', 'EURUSD.csv', 'GBPUSD.csv']:
        data_path = f'./data/processed/{data_file}'
        data = HALLDATA(dataname=data_path, fromdate=fdate, todate=tdate)
        cerebro.adddata(data, name=data_file.split('.')[0])
    tdata = pd.read_csv('./data/processed/EURGBP.csv', index_col='Gmt time')

    print("Data Loaded, Running Backtest...")

    # Run backtest
    cerebro.addstrategy(StatArb, length=len(tdata))
    cerebro.run()

    print("\nBacktest Completed...")

    # Parse Trades Data
    trades = pd.read_csv("./tests/latest/trades.csv", index_col="TradeID")
    pnls = trades["PnL After Commission"].astype(np.float32)

    print("Trade Data Loaded, Calculating Metrics...")

    # Calculate Metrics
    metrics = calculate_metrics(trades)
    metrics = {k: (int(v) if isinstance(v, np.int64) else v) for k, v in metrics.items()}
    with open('./tests/latest/metrics.json', 'w') as f:
        json.dump(metrics, f, indent=4)

    print("Metrics Saved, Running Monte Carlo...")

    # Run Monte Carlo
    run_monte_carlo(data=pnls, iterations=1000, seed=random.randint(0, 100))
    cerebro.plot(style='line')

def plot():
    trades = pd.read_csv("./tests/latest/trades.csv", index_col="TradeID")
    pnls = trades["PnL After Commission"].astype(np.float32)
    run_monte_carlo(pnls, iterations=1000, seed=random.randint(0, 100))

if __name__ == "__main__":
    pre_process_data()
    run_backtest()
    #plot()
