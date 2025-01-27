import json
import random
import argparse
import numpy as np
import pandas as pd
from preprocess import pre_process_data
from Apexa.metrics import calculate_metrics
from Apexa.monte_carlo import run_monte_carlo
from backtest import run_backtest

parser = argparse.ArgumentParser()

parser.add_argument('-a', '--assets', type=str, nargs=2, help='2 correlated assets, ex: EURUSD, GBPUSD')
parser.add_argument('-m', '--mode', type=str, nargs=1, help='The mode in which to run the backtester, ex: Full, Bare, MonteCarlo')

args = parser.parse_args()

raw_symbols = []

if len(args.assets) <= 0:
    raw_symbols = ['EURUSD', 'GBPUSD']
else:
    for n in args.assets:
        raw_symbols.append(n)

preprocess = False
montecarlo = False

if args.mode[0] == "full":
    preprocess = True
    montecarlo = True
elif args.mode[0] == "bare":
    preprocess = False
    montecarlo = False
elif args.mode[0] == "montecarlo":
    preprocess = False
    montecarlo = True

if __name__ == "__main__":
    if preprocess:
        pre_process_data(raw_symbols)

    raw_symbol_data = {}
    for symbol in raw_symbols:
        data = pd.read_csv(f"./data/processed/{symbol}.csv", index_col='Gmt time')
        raw_symbol_data[symbol] = data

    asset1 = raw_symbol_data[raw_symbols[0]]
    asset2 = raw_symbol_data[raw_symbols[1]]

    ratio = pd.DataFrame(index=asset1.index)
    ratio[["Open", "High", "Low", "Close"]] = asset1[["Open", "High", "Low", "Close"]] / asset2[["Open", "High", "Low", "Close"]]
    ratio.to_csv('./data/processed/ratio.csv')

    run_backtest(raw_symbols)
    trades = pd.read_csv("./tests/latest/trades.csv", index_col="TradeID")

    metrics = calculate_metrics(trades)
    metrics = {k: (int(v) if isinstance(v, np.int64) else v) for k, v in metrics.items()}
    with open('./tests/latest/metrics.json', 'w') as f:
        json.dump(metrics, f, indent=4)

    if montecarlo:
        pnls = trades["PnL After Commission"].astype(np.float32)
        run_monte_carlo(data=pnls, iterations=1000, seed=random.randint(0, 100))
