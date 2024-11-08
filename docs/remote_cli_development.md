# Remote Framework CLI Development Documentation
For most users, this may not be necessary, as they can invoke remote CLI components by simply using the `-r` or `--remote` flag with applicable commands. 
This information is primarily intended for internal documentation useful to Caribou developers. 

**Note:** Remote CLI features, including all remote CLI commands and timer functionalities, are experimental. Please use them with caution and at your own risk.

**Note:** Exercise caution when developing or modifying source code related to such events to prevent the risk of infinite or recurring loops!

**Note:** Many commands in the Remote CLI are designed to separate and concurrently execute different workflows on distinct AWS Lambda function instances of the Caribou Remote CLI to address the 15-minute Lambda time limit. However, this approach has limitations: complex workflows with numerous regions or frequent invocations may encounter issues with log syncing that exceeds the time limit or with the deployment migrator taking too long to complete a full migration. This is a current limitation of the Remote CLI.

## How to Invoke the AWS Remote CLI Through events
After deploying the `AWS Remote CLI`, you can run Caribou components by invoking the deployed lambda function using the returned Lambda ARN with the following event parameters.

### Public/External Commands
The following commands are associated with the local Caribou CLI.

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

`workflow_id` is only required for the `workflow` collector option.

```json
{
    "action": "data_collect",
    "collector": "all"
}
```

```json
{
    "action": "data_collect",
    "collector": "workflow",
    "workflow_id": "workflow_name-version_number"
}
```

**Note:** The `all` collector does not collect `workflow` information, as `manage_deployments` would perform this automatically, the `all` collector performs `provider`, `carbon`, and `performance` collector in that order. 

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

### Internal Commands
The following commands are internal events triggered by either external or other internal commands.

All internal commands share the `action` of internal_action, with `type` indicating distinct events. They may also include an `event` with type-specific parameters.
```json
{
  "action": "internal_action",
  "type": "Type of internal event",
  "event": { ... }
}
```

- Perform Log Synchronization Event - sync_workflow:

Triggered by the "log_sync" `action`.

```json
{
  "action": "internal_action",
  "type": "sync_workflow",
  "event": { 
    "workflow_id": "workflow_name-version_number"
  }
}
```

- Manage Deployments - check_workflow:

Triggered by the "manage_deployments" `action`.

`deployment_metrics_calculator_type` can be either `simple` (for the Python solver) or `go` (to use the Go solver) for deployment metrics determination.

```json
{
  "action": "internal_action",
  "type": "check_workflow",
  "event": { 
    "workflow_id": "workflow_name-version_number",
    "deployment_metrics_calculator_type": "simple"
  }
}
```

- Manage Deployments - run_deployment_algorithm:

Triggered by the "internal_action" `action`, "run_deployment_algorithm" `type`.

`deployment_metrics_calculator_type` can be either `simple` (for the Python solver) or `go` (to use the Go solver) for deployment metrics determination.

```json
{
  "action": "internal_action",
  "type": "run_deployment_algorithm",
  "event": { 
    "workflow_id": "workflow_name-version_number",
    "deployment_metrics_calculator_type": "simple",
    "solve_hours": [1, 2, ...],
    "leftover_tokens": 15
  }
}
```

- Deployment Migrator - re_deploy_workflow:

Triggered by the "run_deployment_migrator" `action`.

```json
{
  "action": "internal_action",
  "type": "re_deploy_workflow",
  "event": { 
    "workflow_id": "workflow_name-version_number",
  }
}
```
