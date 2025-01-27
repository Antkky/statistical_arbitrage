import os
import pandas as pd

def pre_process_data(datanames: list, datapath: str = "./data/", output_path: str = "./data/processed/"):
    """
    Sync and process multiple dataframes, then save each synced dataset to separate CSV files.

    Args:
        datanames (list): List of dataset names (excluding '.csv').
        datapath (str): Path to the folder containing the CSV files.
        output_path (str): Path to the folder to save the processed datasets.

    Returns:
        dict: A dictionary where keys are dataset names and values are the processed DataFrames.
    """
    print("Loading Raw Data...")

    datas = {}

    # Load data
    for dataname in datanames:
        path = os.path.join(datapath, f"{dataname}.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")
        data = pd.read_csv(path, index_col=0, parse_dates=True)
        datas[dataname] = data

    print("Data Loaded, Processing...")

    # Find common timestamps
    common_timestamps = datas[datanames[0]].index
    for data in datas.values():
        common_timestamps = common_timestamps.intersection(data.index)

    # Align all datasets to common timestamps
    for key in datas:
        datas[key] = datas[key].loc[common_timestamps]

    # Ensure the output directory exists
    os.makedirs(output_path, exist_ok=True)

    for dataname, df in datas.items():
        df.index = df.index.str.replace(r'\.000$', '', regex=True)

    # Save each processed dataset to a separate CSV file
    for dataname, df in datas.items():
        output_file = os.path.join(output_path, f"{dataname}.csv")
        df.to_csv(output_file)
        print(f"Processed data for {dataname} saved to {output_file}.")

    return datas

if __name__ == "__main__":
    symbols = []
    pre_process_data(symbols)
