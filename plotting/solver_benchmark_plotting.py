import matplotlib.pyplot as plt
from collections import defaultdict
import json
import sys
from matplotlib import colormaps
import os


def plot_single(ax1, ax2, data, x_key, y_left_key, y_right_key):
    colormap = colormaps["viridis"]
    num_solvers = len(data)
    width = 0.2
    handles, labels = [], []
    spacing = 0.05  # adjust this value to change the spacing

    for i, (solver, solver_data) in enumerate(data.items()):
        color = colormap(i / num_solvers)
        x_values = sorted([d[x_key] + (i + 1) * spacing for d in solver_data])
        ax1.set_ylabel(y_left_key)
        bars = ax1.bar(
            [
                x - width / 2 + (i * 2) * width / num_solvers - 0.01 for x in x_values
            ],
            [d[y_left_key] for d in solver_data],
            width / num_solvers,
            color=color,
            label=solver,
        )
        ax1.tick_params(axis="y")
        handles.append(bars[0])
        labels.append(solver)

        ax2.set_ylabel(y_right_key)
        ax2_boxplot_data = defaultdict(list)

        for d in solver_data:
            ax2_boxplot_data[d[x_key]].append(d[y_right_key])

        boxplot_data = list(ax2_boxplot_data.values())

        x_values = list(set(x_values))

        positions = sorted(
            list(set([x - width / 2 + (i * 2 + 1) * width / num_solvers + 0.01 for x in x_values]))
        )

        ax2.boxplot(
            boxplot_data,
            positions=positions,
            widths=width / num_solvers,
            patch_artist=True,
            boxprops=dict(facecolor=color, color=color),
            capprops=dict(color=color),
            whiskerprops=dict(color=color),
            flierprops=dict(color=color, markeredgecolor=color),
            medianprops=dict(linestyle="-", linewidth=0, color=color),
            showfliers=False,
            showmeans=True,
            meanline=False,
            meanprops=dict(marker="x", markerfacecolor=color, markersize=6),
        )

    positions = [((x - width / 2 + (i + 0.5) * width / num_solvers) - width / 2) for x in x_values]
    ax2.set_xticks(positions)
    x_tick_labels = [str(int(x - 0.1)) for x in x_values]
    ax2.set_xticklabels(x_tick_labels)
    ax2.set_yscale("log")
    fig = ax1.get_figure()
    fig.legend(handles, labels, loc="upper right")


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
    fig.tight_layout(rect=[0, 0, 1, 0.97])  # Adjust layout to make room for legend
    # get this file location
    current_path = os.path.dirname(os.path.realpath(__file__))
    plt.savefig(os.path.join(current_path, "plots", "solver_benchmark.pdf"))


def read_data(path_to_data):
    results = {}
    with open(path_to_data, "r") as f:
        results = json.load(f)
    return results


if __name__ == "__main__":
    path_to_data = sys.argv[1]
    data = read_data(path_to_data)
    plot_all(data)
