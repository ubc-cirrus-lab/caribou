import matplotlib.pyplot as plt
from collections import defaultdict
import json
import sys
import os
import numpy as np
import seaborn as sns


def plot_single_heatmap(ax, data, key, algorithm_1, algorithm_2):
    algorithm_1_data = data[algorithm_1]
    algorithm_2_data = data[algorithm_2]

    x_axis = "number of regions"
    y_axis = "number of instances"

    algorithm_1_extracted = extract_data(algorithm_1_data, key, x_axis, y_axis)
    algorithm_2_extracted = extract_data(algorithm_2_data, key, x_axis, y_axis)

    x = list(algorithm_1_extracted.keys())
    y = list(algorithm_1_extracted[x[0]].keys())

    X, Y = np.meshgrid(x, y)
    Z = np.zeros((len(y), len(x)))

    for i, x_val in enumerate(x):
        for j, y_val in enumerate(y):
            Z[j, i] = algorithm_1_extracted[x_val][y_val] - algorithm_2_extracted[x_val][y_val]

    # Use seaborn to create a heatmap
    sns.heatmap(Z, annot=True, fmt=".2f", cmap="YlGnBu", ax=ax, cbar=False)

    # We want to show all ticks...
    ax.set_xticks(np.arange(len(x)))
    ax.set_yticks(np.arange(len(y)))

    # ... and label them with the respective list entries
    ax.set_xticklabels(x)
    ax.set_yticklabels(y)

    # Rotate the tick labels and set their alignment.
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")

    ax.set_title(f"{key}")


def extract_data(data, key, x_axis, y_axis):
    result_data = defaultdict(dict)
    for data_item in data:
        result_data[data_item[x_axis]][data_item[y_axis]] = data_item[key]
    return result_data


def plot_all(data):
    algorithms = list(data.keys())
    keys = ["best cost", "best runtime", "best carbon"]
    fig, axs = plt.subplots(1, 3, figsize=(15, 5))  # Adjust the size of the figure
    for i, key in enumerate(keys):
        plot_single_heatmap(axs[i], data, key, algorithms[0], algorithms[1])

    #fig.tight_layout(rect=[0, 0, 1, 0.97])  # Adjust layout to make room for legend
    # get this file location
        
    fig.text(0.5, 0.04, 'Number of Regions', ha='center', va='center')
    fig.text(0.06, 0.5, 'Number of Instances', ha='center', va='center', rotation='vertical')

    fig.suptitle(f'Comparison of Algorithms {algorithms[0]} and {algorithms[1]}', fontsize=16)

    current_path = os.path.dirname(os.path.realpath(__file__))
    plt.savefig(os.path.join(current_path, "plots", "solver_benchmark_heatmap.pdf"))


def read_data(path_to_data) -> dict:
    results = {}
    with open(path_to_data, "r") as f:
        results = json.load(f)
    return results


if __name__ == "__main__":
    path_to_data = sys.argv[1]
    data = read_data(path_to_data)
    plot_all(data)
