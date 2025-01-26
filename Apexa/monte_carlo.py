import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from joblib import Parallel, delayed

def montecarlo(data: pd.Series, iterations: int = 250, seed: int = None):
    """
    Generate Monte Carlo equity curves as a generator to reduce memory usage.

    Parameters:
        data (pd.Series): Input series of PnL values.
        iterations (int): Number of Monte Carlo iterations.
        seed (int, optional): Random seed for reproducibility.

    Yields:
        pd.Series: Single Monte Carlo equity curve.
    """
    if not isinstance(data, pd.Series):
        raise ValueError("Input data must be a pandas Series.")

    rng = np.random.default_rng(seed) if seed is not None else np.random.default_rng()

    def generate_equity_curve():
        smData = data.sample(n=len(data), replace=True, random_state=rng.integers(1e9)).reset_index(drop=True)
        return smData.cumsum()

    for i in range(iterations):
        yield generate_equity_curve()

def run_monte_carlo(data: pd.Series, iterations: int, output_file='./tests/latest/Montecarlo.csv', output_image='./tests/latest/Montecarlo.png', seed: int = None):
    curves_generator = montecarlo(data, iterations=iterations, seed=seed)
    final_equities = []  # List to store the final equities
    equity_curves = []  # List to store the equity curves for plotting

    with open(output_file, 'w') as f:
        first_curve = next(curves_generator)
        first_curve.to_frame().to_csv(f, header=True, index=True)
        final_equities.append(first_curve.iloc[-1])  # Append the last value
        equity_curves.append(first_curve)

        for i, curve in enumerate(curves_generator, start=1):
            curve.to_frame().to_csv(f, header=False, index=True)
            final_equities.append(curve.iloc[-1])  # Append the last value
            equity_curves.append(curve)

    mcData = pd.DataFrame(equity_curves).T

    curves = iterations * 0.1

    plotMC(mcData=mcData, final_equities=final_equities, num_curves=int(curves), output_file=output_image)


def plotMC(mcData: pd.DataFrame, final_equities: list, num_curves: int = 100,
                              output_file: str = 'monte_carlo_with_distribution.png'):
    # Use gridspec_kw to allocate more space to the equity curve plot (ax1)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 6), gridspec_kw={'width_ratios': [3, 1]})

    # Plot the Monte Carlo equity curves on the left subplot (ax1)
    sampled_columns = mcData.sample(n=num_curves, axis=1) if mcData.shape[1] > num_curves else mcData
    for column in sampled_columns.columns:
        ax1.plot(sampled_columns.index, sampled_columns[column], alpha=0.3, linewidth=0.8)

    # Add baseline at y=0 for equity curve chart
    ax1.axhline(0, color='red', linestyle='--', linewidth=1)

    ax1.set_xlabel('Time')
    ax1.set_ylabel('Equity Value')
    ax1.set_title('Monte Carlo Simulation: Equity Curves')
    ax1.grid(True, linestyle='--', alpha=0.7)

    # Plot the frequency distribution of final equities on the right subplot (ax2)
    ax2.hist(final_equities, bins=30, orientation='horizontal', color='skyblue', edgecolor='black')

    # Manually reverse the y-ticks to place the highest values at the top
    ax2.set_yticks(ax2.get_yticks()[::-1])

    # Add baseline at x=0 for frequency distribution chart
    ax2.axvline(0, color='red', linestyle='--', linewidth=1)

    ax2.set_xlabel('Frequency')
    ax2.set_ylabel('Final Equity Value')
    ax2.set_title('Frequency Distribution of Final Equities')
    ax2.grid(True, linestyle='--', alpha=0.7)

    # Adjust layout for better spacing
    plt.tight_layout()

    # Save the plot to a PNG file
    plt.savefig(output_file, dpi=300)
    plt.close()  # Close the plot to free memory
    print(f"Plot saved as {output_file}.")
