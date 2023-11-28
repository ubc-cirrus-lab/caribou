# Solver

## Description

Our solver is a mixed integer linear program (MILP) that takes as input the parameters described below and outputs a set of decisions that minimize the total cost of executing a workflow. A workflow is described as a directed acyclic graph (DAG) where each node represents a function and each edge represents a data dependency between functions.

The decision variables are binary variables that indicate whether a function is executed in a given region. The objective function is the total cost of executing the workflow.

There are no constraints on the resource requirements of the functions as all functions are deployed on a serverless platform providing theoretically infinite resources for a single client. There are also no constraints on the number of functions that can be executed in a given region. These constraints are not for the solver to decide, but rather for the serverless platform to enforce.

The user can define constraints on the maximum overall latency and the maximum overall data transfer cost between functions. Additionally, the user can define constraints on what regions are available for execution. Additionally, the user can define the maximum added cost of offloading a function to a different region.

The constraints affect three areas that the solver can optimize for:

- Minimize the total cost of executing the workflow
- Minimize the total execution time of the workflow
- Minimize the total carbon emissions of executing the workflow

## Inputs

- Function region to region execution time
- Function region to region data latency
- Grid CO2e emissions per region
- Region to region data transfer cost
- Region pricing
- Region to region round-trip time latency
- Resource requirements

## Assumptions

- The workflow is a DAG
- The workflow is known at the time of optimization
- The workflow is static

## Constraints

- Maximum overall latency
- Maximum overall data transfer cost
- Maximum added cost of offloading a function to a different region
- Regions available for execution

## Goals

The solver can optimize for three different goals:

- Minimize the total cost of executing the workflow
- Minimize the total execution time of the workflow
- Minimize the total carbon emissions of executing the workflow

Where each of the goals act as a filter on the set of possible solutions. For example, if the goal is to minimize the total cost of executing the workflow, then the solver will only return solutions that minimize the total cost of executing the workflow.

Filters can be combined. For example, if the goal is to minimize the total cost of executing the workflow and the maximum overall latency is 10 seconds, then the solver will only return solutions that minimize the total cost of executing the workflow and have a maximum overall latency of 10 seconds.

## Outputs

The solver outputs a set of decisions based on the user defined constraints and goals. The decisions are a set of binary variables that indicate whether a function can and should be executed in a given region.

Thus the solver returns a matrix of binary variables where each row represents a function and each column represents a region. The solver also returns the total estimated cost of executing the workflow, the total estimated execution time of the workflow and the total estimated carbon emissions of executing the workflow.
