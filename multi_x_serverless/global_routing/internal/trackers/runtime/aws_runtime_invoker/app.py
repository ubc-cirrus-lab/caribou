import datetime
import logging
import socket
import json
import time
import numpy as np
from timeit import default_timer as timer
from typing import Any

import boto3
from chalice import Chalice

RUNTIME_RESULTS_TABLE_NAME = "multi-x-serverless-runtime-tracker"

DEFAULT_REGION = "us-west-2"

app = Chalice(app_name='runtime_invoker')

@app.schedule("rate(1 hour)")
def invoke_lambda_functions(event: Any) -> None:  # pylint: disable=unused-argument
    # Get the current location of the invoker and the starting time
    current_region = get_curr_aws_region()
    current_time = get_current_time(False)
    current_time_abr = get_current_time(True)
    provider = 'aws'
    iterations = 10

    print("Running fpo tests for region:", current_region)
    print("Started at:", str(current_time))

    # Get all the lambda functions in the current region
    functions = get_lambda_functions_with_name('fpo-io-prod-lambda_handler', current_region)
    
    experiment_and_payload = [
        ('Pure CPU', {"n": 30,"c": 4000,"i": 0,"s": 0}, []),
        ('Pure IO', {"n": 1,"c": 0,"i": 1500,"s": 0}, []),
    ]
    for function_name, FunctionArn in functions:
        for i in range(iterations):
            for experiment_name, payload, execution_times in experiment_and_payload:
                execution_only_duration = invoke_lambda_function(FunctionArn, current_region, payload)
                if (execution_only_duration != 0): # Suceed if it is not 0
                    execution_times.append(execution_only_duration)

        for experiment_name, payload, execution_times in experiment_and_payload:
            payload = str(payload)
            successfull_invocations = len(execution_times)
            timing_result = calculate_stats(execution_times)

            results = (function_name, provider, current_time, current_time_abr, current_region, experiment_name, payload, successfull_invocations, timing_result['mean'], timing_result['std_dev'], timing_result['min'], timing_result['max'], timing_result['5th_percentile'], timing_result['50th_percentile'], timing_result['90th_percentile'], timing_result['95th_percentile'], timing_result['99th_percentile'])

            write_results(results)

# Function to calculate statistics
def calculate_stats(data):
    data = np.array(data)
    stats = {
        'mean': np.mean(data),
        'std_dev': np.std(data),
        'min': np.min(data),
        'max': np.max(data)
    }
    
    percentiles = np.percentile(data, [5, 50, 90, 95, 99])
    stats.update({
        f'{p}th_percentile': val for p, val in zip([5, 50, 90, 95, 99], percentiles)
    })
    
    return stats

def invoke_lambda_function(FunctionArn, function_region, payload = None):
    lambda_client = boto3.client('lambda',region_name=function_region)

    response = lambda_client.invoke(
        FunctionName=FunctionArn,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )

    response_payload = response['Payload'].read().decode('utf-8')

    try:
        data = json.loads(response_payload)
        execution_only_duration = int(data['duration'])
    except (KeyError, TypeError, ValueError) as e:
        execution_only_duration = 0

    return execution_only_duration

def get_current_time(abr=True) -> str:
    if abr:
        time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H UCT")
    else:
        time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return time

def write_results(result) -> None:
    dynamodb = boto3.resource("dynamodb", region_name=DEFAULT_REGION)
    target_table = dynamodb.Table(RUNTIME_RESULTS_TABLE_NAME)

    # Unpack result input
    function_name, provider, current_time, current_time_abr, current_region, experiment_name, payload, successful_invocations, mean, std_dev, min, max, p5, p50, p90, p95, p99 = result
    # Create an Item to be stored in DynamoDB
    item = {
        "timestamp": current_time,
        "compound": f"{function_name}_{provider}_{current_region}_{experiment_name}",
        "abbreviated_time": current_time_abr,
        "function_name": function_name,
        "provider": provider,
        'region': current_region,
        'experiment_name': experiment_name,
        'payload': payload,
        'successful_invocations': str(successful_invocations),
        'mean': str(mean),
        'std_dev': str(std_dev),
        'min': str(min),
        'max': str(max),
        'p5': str(p5),
        'p50': str(p50),
        'p90': str(p90),
        'p95': str(p95),
        'p99': str(p99)
    }

    # Write the Item to DynamoDB
    target_table.put_item(Item=item)

def get_lambda_functions_with_name(name, region):
    lambda_client = boto3.client('lambda',region_name=region)
    functions = lambda_client.list_functions()
    filtered_functions = [(func['FunctionName'], func['FunctionArn']) for func in functions['Functions'] if not name or name in func['FunctionName']]

    return filtered_functions


def get_curr_aws_region() -> str:
    my_session = boto3.session.Session()
    my_region = my_session.region_name
    if my_region:
        return my_region
    return DEFAULT_REGION