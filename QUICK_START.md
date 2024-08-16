#  Quickstart

The following guide will help you get started with the Caribou framework.

##  Permissions

To use the Caribou framework, you need to have the necessary permissions to deploy and run workflows. Additionally, the workflows themselves need permissions to interact with the AWS services.

More information on the required permissions can be found in the [IAM Policies](docs/iam_policies.md) document.

## Setup a new workflow

To set up a new workflow, in your command line, navigate to the directory where you want to create the new workflow and run:

```bash
caribou new_workflow <workflow_name>
```

Where `<workflow_name>` is the name of the new workflow.

## Deployment Utility

The Deployment Utility has an additional dependency on `docker`.
To install it, follow the instructions on the [docker website](https://docs.docker.com/engine/install/).
Ensure you have the docker daemon running before running the deployment utility.

The deployment utility can be found in `caribou/deployment/client` and can be run with:

```bash
caribou --project-dir <project_dir> deploy
```

Alternatively, you can also navigate to the `<project_dir>`; the same command will work without the `--project-dir` flag.

This will deploy the workflow to the defined home region as defined in the deployment manifest.
To change the home region, you need to adjust the manifest in the configuration file in `.caribou/config.yml` and set the `home_region` to the desired region.

This will also print the unique workflow ID generated from the workflow name and version.
The workflow ID is unique per deployed framework and is used to identify the workflow in the system.

## Run a workflow

To run a workflow, you can use the following command:

```bash
caribou run <workflow_id> -a '{"message": "Hello World!"}'
```

Where `<workflow_id>` is the id of the workflow you want to run.

The `-a` flag is used to pass arguments to the workflow. The arguments can be passed as any object but need to be handled by your client code at the workflow's entrypoint.

##  List all workflows

To list all workflows, you can use the following command:

```bash
caribou list
```

We currently do not distinguish between different users and show all workflows that are currently managed by the framework.

##  Remove a workflow

To remove a workflow, you can use the following command:

```bash
poetry run caribou remove <workflow_id>
```

Where `<workflow_id>` is the id of the workflow you want to remove.

## Synchronize Execution Logs

To sync the logs from all the workflows to the global table, you can use the following command:

```bash
poetry run caribou log_sync
```

## Data Collecting
First 
```bash
poetry run caribou data_collect provider
```

The data collecting can be found in `caribou/data_collector` and can be run individually with:

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

This manual data collecting is only necessary if you want to collect data for a specific workflow for testing purposes. The first three collectors are automatically run periodically.

The workflow collector is invoked by the manager and collects data for the workflows that are currently being solved.

**Note:** For the data collectors to work locally, you must set some environment variables.

```bash
export ELECTRICITY_MAPS_AUTH_TOKEN=<your_token>
export GOOGLE_API_KEY=<your_key>
```
**Note:** The needed API: Geocoding API

## Find a new (optimal) Deployment

Use the manager to solve for all workflows that have a check outstanding:

```bash
poetry run caribou manage_deployments
```

## Run Deployment Migrator

Make sure that you have the crane dependency installed.
See the [Installation](INSTALL.md) guide for more information.

Once a new deployment has been found for a workflow, you can use the following command to deploy the new workflow:

```bash
poetry run caribou run_deployment_migrator
```

This will check if a new deployment is required for any workflow, and, if so, migrate the functions according to this new deployment.

##  Deploying the framework components to AWS

**TODO:** We currently have the scripts for packaging the framework components into a container in `scripts/deploy_to_aws.py`.
This script works for simple deployments, but for example the crane dependency is not taken care of.
We need to extend the script to deploy all the data collectors, log syncer, deployment manager, and deployment migrator to AWS.
Potentially we also want to decouple the deployment manager and deployment solver in a future version.
