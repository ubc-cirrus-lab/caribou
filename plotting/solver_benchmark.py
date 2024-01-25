import matplotlib.pyplot as plt
import json
import sys
from matplotlib import cm



def plot_single(ax1, ax2, data, x_key, y_left_key, y_right_key):
    colormap = cm.get_cmap('viridis')
    num_solvers = len(data)
    width = 0.35
    handles, labels = [], []

    for i, (solver, solver_data) in enumerate(data.items()):
        color = colormap(i / num_solvers)
        x_values = [d[x_key] for d in solver_data]
        ax1.set_xlabel(x_key)
        ax1.set_ylabel(y_left_key, color=color)
        bars = ax1.bar([x - width/2 + i*width/num_solvers for x in x_values], [d[y_left_key] for d in solver_data], width/num_solvers, color=color, label=solver)
        ax1.tick_params(axis="y", labelcolor=color)
        handles.append(bars[0])
        labels.append(solver)

    ax1.set_xticks(x_values)
    fig = ax1.get_figure()
    fig.legend(handles, labels)

def plot_row(fig, axs, data, x_key, y_keys):
    for i, y_key in enumerate(y_keys):
        ax2 = axs[i].twinx()
        plot_single(axs[i], ax2, data, x_key, y_key, "runtime")


def plot_all(data):
    x_keys = ["number of regions", "number of instances", "number of sync nodes"]
    y_keys = ["best cost", "best runtime", "best carbon"]
    fig, axs = plt.subplots(3, 3, figsize=(15, 15))
    for i in range(3):
        plot_row(
            fig,
            axs[i],
            {solver: [d for d in solver_data if d[x_keys[i]]] for solver, solver_data in data.items()},
            x_keys[i],
            y_keys,
        )
    fig.tight_layout()
    plt.savefig("pots/solver_benchmark.png")


def read_data(path_to_data):
    results = {}
    with open(path_to_data, "r") as f:
        results = json.load(f)
    return results


if __name__ == "__main__":
    path_to_data = sys.argv[1]
    data = read_data(path_to_data)
    plot_all(data)
