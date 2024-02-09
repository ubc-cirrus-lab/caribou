import matplotlib.pyplot as plt
from collections import defaultdict
import json
import sys
from matplotlib import colormaps
import os
import numpy as np
import matplotlib.colors as mcolors


def plot_single(ax1, ax2, data, x_key, y_left_key, y_right_key, only_fast=False):
    colormap = colormaps["tab10"]
    num_solvers = len(data)
    width = 0.4
    handles, labels = [], []

    for i, (solver, solver_data) in enumerate(data.items()):
        if solver == "BFSFineGrainedSolver" and only_fast:
            continue
        if not only_fast:
            solver_data = [d for d in solver_data if d["number of instances"] < 8 and d["number of regions"] < 8]
        ax1_data_dict = defaultdict(list)
        for d in solver_data:
            ax1_data_dict[d[x_key]].append(d[y_left_key])
        x_values = sorted(ax1_data_dict.keys())
        y_values = [ax1_data_dict[x] for x in x_values]
        color = colormap(i)

        parts = ax1.violinplot(
            y_values,
            positions=[x - width / 2 + i * width / num_solvers for x in x_values],
            widths=width,
            showmeans=True,
        )
        for pc in parts["bodies"]:
            pc.set_facecolor(color)

        darker_color = mcolors.to_rgb(color)
        darker_color = [max(0, c - 0.3) for c in darker_color]

        ax1.set_ylabel(y_left_key)
        labels.append(solver)
        handles.append(parts["bodies"][0])

        parts["cbars"].set_color(darker_color)
        parts["cmins"].set_color(darker_color)
        parts["cmaxes"].set_color(darker_color)
        parts["cmeans"].set_color(darker_color)

        y_ax2_data_dict = defaultdict(list)
        for d in solver_data:
            y_ax2_data_dict[d[x_key]].append(d[y_right_key])
        y_ax2_values = [np.percentile(y_ax2_data_dict[x], 95) for x in x_values]

        ax2.plot(x_values, y_ax2_values, color=color, linestyle="dashed", marker="x")

    ax1.set_xlabel(x_key)
    ax1.set_xticks(x_values)
    fig = ax1.get_figure()
    fig.legend(handles, labels, loc="upper right")


def plot_row(fig, axs, data, x_key, y_keys, only_fast=False):
    for i, y_key in enumerate(y_keys):
        ax2 = axs[i].twinx()
        plot_single(axs[i], ax2, data, x_key, y_key, "runtime", only_fast=only_fast)


def plot_all(data, only_fast=False):
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
            only_fast=only_fast,
        )
    fig.tight_layout(rect=[0, 0, 1, 0.97])  # Adjust layout to make room for legend
    # get this file location
    current_path = os.path.dirname(os.path.realpath(__file__))
    if only_fast:
        plt.savefig(os.path.join(current_path, "plots", "solver_benchmark_fast.pdf"))
    else:
        plt.savefig(os.path.join(current_path, "plots", "solver_benchmark.pdf"))


def read_data(path_to_data):
    results = {}
    with open(path_to_data, "r") as f:
        results = json.load(f)
    return results


if __name__ == "__main__":
    path_to_data = sys.argv[1]
    data = read_data(path_to_data)
    plot_all(data, only_fast=True)
    plot_all(data, only_fast=False)
