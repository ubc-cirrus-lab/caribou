#  Wrapper overhead of our project versus AWS Step Functions

## Plot outline

The plot generated from this experiment will be the following:

- x-axis: Benchmarks
- y-axis: Relative/Absolute End-To-End Latency

The benchmarks we want to test are the following applications:

- DNA Visualization
- Image Processing
- Text2Speech Censoring
- Video Analytics
- MapReduce

The configurations we want to test are the following:

- Direct calls (Boto3)
- Multi-X
- AWS Step Function

For each of the configurations, we will have the following cases
(this may instead be in different plots):

- All stages of workflows in one single AWS lambda function.
- All stages of workflow in seperate function which must invoke one another.

All configurations must run in a single region to ensure fairness, for simplicity we will use Alberta.

## What data do we need?

This experiment would not need any external data. The primary component of it is to try to ensure fair comparisons.

## Experiment setup

For each benchmark, we deploy each of the configuration and run 100 times to collect the data.

We then take the results of each of the configurations and compare the End-To-End Latency
between the first invocation to the last finished function of the workflow. 

## How to run experiments

Run all the code of the setup.ipynb jupyter notebook, this will setup
the experiments and deploy all necessarily code to AWS.

Then run the run.ipynb to perform experiments.

Finally use the plot.ipynb for generating plots of the experiments.