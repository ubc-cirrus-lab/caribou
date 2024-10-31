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

Some client-side commands can be executed in the `AWS Remote CLI`, provided it is deployed. 

**Note:** Remote CLI features, including all remote CLI commands and timer functionalities, are experimental. Please use them with caution and at your own risk.

### Setup a new workflow

To set up a new workflow, in your command line, navigate to the directory where you want to create the new workflow and run:

```bash
poetry run caribou new_workflow <workflow_name>
```

Where `<workflow_name>` is the name of the new workflow.

You can then use the Caribou Python API to define and develop the workflow.

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

**Note:** May be executed remotely with the `-r` or `--remote` flag to execute them remotely and asynchronously. 

## Local Framework Component Execution

The Caribou framework components can be run locally for fast prototyping and development.

### Synchronize Execution Logs

To sync the logs from all the workflows, you can use the following command:

```bash
poetry run caribou log_sync
```

This might take a while, depending on the number of workflows and the amount of logs that need to be synced.
Also, there is an inherent buffer of fifteen minutes, meaning that logs are only synced if they are at least fifteen minutes old.

**Note:** May be executed remotely with the `-r` or `--remote` flag to execute them remotely and asynchronously. 

### Data Collecting

Before we can generate a new deployment, we need to collect data from the providers.

1. Collect data from the carbon provider:

    ```bash
    poetry run caribou data_collect provider
    ```

2. Collect data from the other collectors:

    ```bash
    poetry run caribou data_collect all
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

    The `workflow` collectors need a workflow id to be passed as an argument with `--workflow_id` or `-w`.

The workflow collector is invoked by the manager and collects data for the workflows that are currently being solved.

**Note:** May be executed remotely with the `-r` or `--remote` flag to execute them remotely and asynchronously. 
**Note:** For the data collectors to work locally, you must set some environment variables.
**Note:** The `all` collector does not collect `workflow` information, as `manage_deployments` would perform this automatically, the `all` collector performs `provider`, `carbon`, and `performance` collector in that order. 

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

**Note:** May be executed remotely with the `-r` or `--remote` flag to execute them remotely and asynchronously. 

### Run Deployment Migrator

Make sure that you have the crane dependency installed.
See the [Installation](INSTALL.md) guide for more information.

Once a new deployment has been found for a workflow, you can use the following command to deploy the new workflow:

```bash
poetry run caribou run_deployment_migrator
```

This will check if a new deployment is required for any workflow, and, if so, migrate the functions according to this new deployment.

**Note:** May be executed remotely with the `-r` or `--remote` flag to execute them remotely and asynchronously. 

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

## Setup Automatic Components (For AWS Remote CLI)
After deploying the AWS remote CLI `deploy_remote_cli`, the user can set up automatic timers for all relevant Caribou components.
This includes automating data collection (provider, performance, carbon, etc.), log synchronization, deployment management (solving for new deployments when needed), and deployment migration.
This is implemented through the use of `EventBridge` which execute the Caribou remote framework Lambda function with customized JSON inputs.

The user may simply set up all the component timers automatically through the following command:


```bash
poetry run caribou setup_all_timers
```
You may also specify the time configurations of any of the following components:
 - `provider_collector`: Use `--provider_collector` or `-prc`. By default, invokes the Lambda function at 12:05 AM on the first day of the month. Schedule expression: 'cron(5 0 1 * ? *)'
 - `carbon_collector`: Use `--carbon_collector` or `-cac`. By default, invokes the Lambda function daily at 12:30 AM. Schedule expression: 'cron(30 0 * * ? *)'
 - `performance_collector`: Use `--performance_collector` or `-pec`. By default, invokes the Lambda function daily at 12:30 AM. Schedule expression: 'cron(30 0 * * ? *)'
 - `log_syncer`: Use `--log_syncer` or `-los`. By default, invokes the Lambda function daily at 12:05 AM. Schedule expression: 'cron(5 0 * * ? *)'
 - `deployment_manager`: Use `--deployment_manager` or `-dma`. By default, invokes the Lambda function daily at 01:00 AM. Schedule expression: 'cron(0 1 * * ? *)'
 - `deployment_migrator`: Use `--deployment_migrator` or `-dmi`. By default, invokes the Lambda function daily at 02:00 AM. Schedule expression: 'cron(0 2 * * ? *)'

**Note:** Running this command will reset all previously customized time configurations.

At any time, the user can see all available timers and their configurations (and if setup) by running the following command:
```bash
poetry run caribou list_timers
```

Optionally, if the user wishes to have only some components run automatically or to modify the timer of any specific components, they can use the following command:
```bash
poetry run caribou setup_timer <timer>
```
Where `<timer>` is the name of the timer you want to configure or modify. It can be one of the following options:
  - `provider_collector`
  - `carbon_collector`
  - `performance_collector`
  - `log_syncer`
  - `deployment_manager`
  - `deployment_migrator`

Optionally, you may also specify the time configurations using the following parameter:
 - `schedule_expression`: Use `--schedule_expression` or `-se`. Default: The same default times set for `setup_all_timers` for each individual timer.

To remove a specific timer, use the following command:
```bash
poetry run caribou remove_timer <timer>
```
Where `<timer>` is the name of the timer you want to remove, using the same names as in `setup_timer`.


To remove all the configured timers, use the following command:

```bash
poetry run caribou remove_all_timers
```

## Teardown Framework
Teardown of the Caribou framework is a very simple and automated process. All workflows, system components, and necessary tables for Caribou can be removed simply with the following command:

```bash
poetry run caribou teardown_framework
```

**Note:** This command cannot be undone, and all data will be removed (with the exception of CloudWatch data). The user will have to perform the `Setup AWS Environment` process from [Installation](INSTALL.md) again to use the capabilities of Caribou.

**Note:** This does not delete any custom data buckets that the user manually created for benchmarking or other purposes; this only concerns components and data that are automatically created by Caribou.