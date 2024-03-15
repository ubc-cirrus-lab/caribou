#  Carbon Intensity under different configurations

## Plot outline

The plot generated from this experiment will be the following:

- x-axis: Benchmark and configuration
- y-axis: Relative carbon intensity

There will be a second plot with the same x-axis and the following y-axis:

- y-axis: Relative performance

The benchmarks we want to test are the following five applications:

- DNA Visualization
- Image Processing
- Text2Speech Censoring
- Video Analytics
- MapReduce

The configurations we want to test are the following:

- One region:
  - California
  - Alberta
  - Oregon
- Carbon & Performance aware (not more than 5% performance degradation) multi-region configurations:
  - California & Alberta
  - California & Oregon
  - Alberta & Oregon
  - California, Alberta & Oregon
- Carbon aware multi-region configurations:
  - California & Alberta
  - California & Oregon
  - Alberta & Oregon
  - California, Alberta & Oregon

## What data do we need?

We want to plot the data of the whole last year (2023).

We need the following data:

Given a day of the year and the data of the past week of that day, run the deployment algorithm to get the respective deployment configuration.
Run experiments for this deployment configuration to get the runtimes (overall and relative).
Using this and the data of the day, calculate the carbon intensity of that configuration.
This additionally collects the performance of the workflow.

## Experiment setup

For each benchmark, run the instance around 100 times to collect some initial data.

The data is automatically stored in the distributed relational database.

Once we have the initial data, we can start the different configurations and collect the data.

For each configuration, run the deployment algorithm to get the respective deployment configuration.

For each unique deployment configuration, run another 100 times to collect the data.

Based on this data, we can calculate the carbon intensity and the performance of the workflow.

## Initial test

For DNA Visualization, run the instance 100 times and collect the data.
