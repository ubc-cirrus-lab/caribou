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

- Function region to region execution time (ms):
  - A list of execution times where each item represents a region
  - This is obtained by example runs and is not available at the initial deployment of the workflow (i.e. the data is collected over time)
- Function region to region data latency (ms)
  - A list of data transfer latencies where each item represents a region
  - This is obtained by example runs and is not available at the initial deployment of the workflow (i.e. the data is collected over time)
- Grid CO2e emissions per region (gCO2e/kWh)
  - A list of CO2e emissions where each item represents a region
  - This is obtained using the `grid_co2` package and is currently updated every 6 hours
- Region to region data transfer cost ($/GB)
- Region pricing ($/GB-s)
- Region to region round-trip time latency (ms)
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

## Formalisation

**Objective Function:**

Minimize: $$Z = \sum_{r \in R} \sum_{s \in R} \sum_{f \in F} (P_{\text{CO2}} \cdot \text{CO2}_{rf} + P_{\text{Latency}} \cdot L_{rs} + P_{\text{Cost}} \cdot C_{r}) \cdot Z_{rsf}$$
**Subject to the Constraints:**

1. Each function is deployed in exactly one region:
  $$\sum_{r \in R} X_{rf} = 1 \quad \forall f \in F$$

2. User-defined limits for latency, cost, and carbon emissions:
  $$L_{rs} \leq \text{UserLatencyLimit} \quad \forall r, s \in R$$
  $$C_{r} \leq \text{UserCostLimit} \quad \forall r \in R$$
  $$\text{CO2}_{rf} \leq \text{UserCarbonLimit} \quad \forall r \in R, f \in F$$

3. Additional constraints based on user priorities and acceptable boundaries:

- Explicit regions where the user does not want the function to run (for GDPR reasons):
   $$X_{rf} = 0 \quad \forall f \in F, r \in \text{ExplicitNonDeployRegions}$$

4. Constraints related to the relationship between regions for latency:
  $$L_{rs} \leq M \cdot (1 - Z_{rsf}) + M \cdot L_{\text{max}} \cdot Z_{rsf} \quad \forall r, s \in R, f \in F$$
  $$\sum_{r \in R} Z_{rsf} = 1 \quad \forall s \in R, f \in F$$
  $$\sum_{s \in R} Z_{rsf} = X_{rf} \quad \forall r \in R, f \in F$$
  $$Z_{rsf} \in \{0, 1\} \quad \forall r, s \in R, f \in F$$

**Where:**

- $R$ is the set of deployment regions.
- $F$ is the set of functions in the workflow.
- $C_{r}$ is the estimated monthly cost for region $r \in R$.
- $CO2_{rf}$ is the CO2 emission cost for function $f$ in region $r$.
- $L_{rs}$ is the latency between regions $r$ and $s$.
- $L_{\text{max}}$ is the maximum acceptable latency.
- $P$ is the set of user-defined priorities.
- $X_{rf}$ is a binary variable indicating whether function $f$ is deployed in region $r$.
- $Z_{rsf}$ is a binary variable indicating whether function $f$ is executed in region $r$ immediately after region $s$.
- $M$ is a sufficiently large constant.
- The user-defined limits are the maximum acceptable latency, cost, and carbon emissions. These can be absolute values or relative values (e.g. 10% increase in cost is acceptable compared to running the workflow in the home region).
