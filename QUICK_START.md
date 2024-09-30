#  Quickstart

**Please make sure you have the necessary dependencies according to how you intend to use Caribou installed. See the [Installation](INSTALL.md) guide for more information.**

The following guide will help you get started with the Caribou framework.

The Caribou framework consists of a client side CLI to develop and manage workflows as well as a server side orchestration framework to solve for optimal deployments and migrate the workflows to these deployments.

The server side orchestration framework consists of the deployment manager, deployment solver, deployment migrator, log syncer, and data collectors.

Each of these components can either be run locally for fast prototyping or deployed to AWS for a full-scale deployment.

For local framework component execution see the [Local Framework Component Execution](#local-framework-component-execution) section.
For deployment to AWS see the [Deployment to AWS](#deployment-to-aws) section.

Make sure you first have the necessary dependencies according to how you intend to use Caribou installed. See the [Installation](INSTALL.md) guide for more information.

## Client side CLI

### Setup a new workflow

To set up a new workflow, in your command line, navigate to the directory where you want to create the new workflow and run:

```bash
poetry run caribou new_workflow <workflow_name>
```

Where `<workflow_name>` is the name of the new workflow.

You can then use the Caribou Python API to define and develop the workflow.

### Workflow ID

When a new workflow is set up, it is initialized with the semantic version `0.0.1` as configured in the config and `app.py` workflow initialization.
This, together with the `<workflow_name>`, will constitute the workflow ID (`<workflow_name>-<version_number>`).
A workflow ID is unique in the Caribou middleware component, meaning if an ID is already deployed ([Deployment Utility](#deployment-utility)) it cannot be deployed again unless it is deleted ([Remove a workflow](#remove-a-workflow)).

### Example Workflow

In the `examples/small_sync_example` directory, you can find an example workflow that you can use to get started.

### Deployment Utility

The deployment utility can be found in `caribou/deployment/client` and can be run with:

```bash
poetry run caribou --workflow-dir <workflow_path> deploy
```

If this step fails with an error message indicating that the docker daemon is not running, you need to [start the docker daemon](https://docs.docker.com/engine/daemon/start/).

Alternatively, you can also navigate to the `<workflow_path>`; the same command will work without the `--workflow-dir` flag.

This will deploy the workflow to the defined home region as defined in the deployment manifest (`./.caribou/config.yml` in the workflow directory).
To change the home region, you need to adjust the manifest in the configuration file in `.caribou/config.yml` and set the `home_region` to the desired region.

This will also print the unique workflow ID generated from the workflow name and version.
The workflow ID is unique per deployed framework and is used to identify the workflow in the system.

If the deployment is successful, the workflow is now deployed to the cloud provider and can be run using the workflow ID.

If there are any issues with the deployment, the deployment utility will print the error message and exit.
In that case it might be necessary to delete the workflow and try to deploy it again.

Delete the workflow with:

```bash
poetry run caribou remove <workflow_id>
```

Where `<workflow_id>` is the id of the workflow you want to remove.

### Naming Restrictions

- `<workflow_name>`: Must be non-empty, up to 25 characters long, and can include only letters, numbers, or underscores.
  
- `<version_number>`: Must be non-empty, up to 10 characters long, and can consist only of numbers and dots.
  
- `<function_name>`: Must be non-empty, up to 15 characters long, and can include only letters, numbers, or underscores.

The resulting node name, following the [Node Naming Scheme](docs/node_naming_scheme.md), must be up to 64 characters long to comply with AWS Lambda Function naming restrictions.

### Run a workflow

To run a workflow, you can use the following command:

```bash
poetry run caribou run <workflow_id> -a '{"message": "Hello World!"}'
```

Where `<workflow_id>` is the id of the workflow you want to run.

The `-a` flag is used to pass arguments to the workflow. The arguments can be passed as any object but need to be handled by your client code at the workflow's entrypoint.

###  List all deployed workflows

To list all workflows, you can use the following command:

```bash
poetry run caribou list
```

We currently do not distinguish between different users and show all workflows that are currently managed by the framework.

###  Remove a workflow

To remove a workflow from the cloud provider, you can use the following command:

```bash
poetry run caribou remove <workflow_id>
```

Where `<workflow_id>` is the id of the workflow you want to remove.

## Local Framework Component Execution

The Caribou framework components can be run locally for fast prototyping and development.

### Synchronize Execution Logs

To sync the logs from all the workflows, you can use the following command:

```bash
poetry run caribou log_sync
```

This might take a while, depending on the number of workflows and the amount of logs that need to be synced.
Also, there is an inherent buffer of five minutes, meaning that logs are only synced if they are at least five minutes old.

### Data Collecting

Before we can generate a new deployment, we need to collect data from the providers.

1. Collect data from the carbon provider:

    ```bash
    poetry run caribou data_collect provider
    ```

2. Collect data from the other collectors:

    ```bash
    poetry run caribou data_collect all --workflow_id <workflow_id>
    ```

    Or collect data for a specific collector:

    ```bash
    poetry run caribou data_collect <collector>
    ```

    Where `<collector>` is the name of the collector you want to run. The available collectors are:

    - `carbon`
    - `provider`
    - `performance`
    - `workflow`
    - `all`

    The `all` and `workflow` collectors need a workflow id to be passed as an argument with `--workflow_id` or `-w`.

The workflow collector is invoked by the manager and collects data for the workflows that are currently being solved.

**Note:** For the data collectors to work locally, you must set some environment variables.

```bash
export ELECTRICITY_MAPS_AUTH_TOKEN=<your_token>
export GOOGLE_API_KEY=<your_key>
```

- **Note 1:** The needed Google API: Geocoding API, get your key from [here](https://developers.google.com/maps/documentation/geocoding/get-api-key).
- **Note 2:** Get your Electricity Maps token from [here](https://api-portal.electricitymaps.com).

### Find a new (optimal) Deployment

After syncing execution logs and collecting new data, you can use the manager to solve all workflows that have a check outstanding.
As documented in the paper, a check is outstanding if the workflow has had enough invocations to warrant a new deployment calculation or the estimated benefit of a new deployment is higher than the overhead of the system.

```bash
poetry run caribou manage_deployments
```

Refer to section 5.2 of the paper to learn about how we make this calculation.
If you execute this command and nothing happens it might be that the minimal threshold for the number of invocations has not been reached yet, which is set to 10 invocations.

### Run Deployment Migrator

Make sure that you have the crane dependency installed.
See the [Installation](INSTALL.md) guide for more information.

Once a new deployment has been found for a workflow, you can use the following command to deploy the new workflow:

```bash
poetry run caribou run_deployment_migrator
```

This will check if a new deployment is required for any workflow, and, if so, migrate the functions according to this new deployment.

## Deployment to AWS (AWS Remote CLI)
To deploy the framework to AWS after completing the local setup process, use the following command while inside the main `caribou` directory. 
Ensure that you can see both the `caribou` and `caribou-go` folders in this directory.


```bash
poetry run caribou deploy_remote_cli
```

You may also specify the `memory` (in MB), `timeout` (in seconds), and `ephemeral_storage` (in MB) using the following flags:
 - `memory`: Use `--memory` or `-m`. Default: 1,769 MB (1 Full vCPU)
 - `timeout`: Use `--timeout` or `-t`. Default: 900 seconds
 - `ephemeral_storage`: Use `--ephemeral_storage` or `-s`. Default: 5,120 MB

To remove the remote framework, use the following command:

```bash
poetry run caribou remove_remote_cli
```

**Note:** Caribou must be properly installed locally first (See the [Installation](INSTALL.md)). 
Additionally, the following environment variables must be set before remote deployment:

```bash
export ELECTRICITY_MAPS_AUTH_TOKEN=<your_token>
export GOOGLE_API_KEY=<your_key>
```

### How to Invoke the AWS Remote CLI
After deploying the `AWS Remote CLI`, you can run Caribou components by invoking the deployed lambda function using the returned Lambda ARN with the following event parameters.

- List workflows:
```json
{
  "action": "list"
}
```

- Invoke workflow:

Where `argument` is the payload of the application.

```json
{
  "action": "run",
  "workflow_id": "workflow_name-version_number",
  "argument": {}
}
```

- Remove Workflow:
```json
{
  "action": "remove",
  "workflow_id": "workflow_name-version_number"
}
```

- Perform Log Sync:
```json
{
  "action": "log_sync"
}
```

- Perform Data collect:

`collector` can be one of the following options: `provider`, `carbon`, `performance`, `workflow`, or `all`.

`workflow_id` is only required for the `workflow` or `all` collector options.

```json
{
    "action": "data_collect",
    "collector": "all",
    "workflow_id": "workflow_name-version_number"
}
```

- Manage Deployments:

`deployment_metrics_calculator_type` can be either `simple` (for the Python solver) or `go` (to use the Go solver) for deployment metrics determination.

```json
{
  "action": "manage_deployments",
  "deployment_metrics_calculator_type": "simple"
}
```

- Deployment Migration:
```json
{
  "action": "run_deployment_migrator"
}
```

- Inquire Caribou Version:
```json
{
  "action": "version"
}
```
