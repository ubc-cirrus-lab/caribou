from typing import Any

import numpy as np
import scipy.sparse as sp
import scipy.sparse.linalg as spla
import json
from caribou.deployment.client import CaribouWorkflow
import boto3
from datetime import datetime

workflow = CaribouWorkflow(name="FEM_simulation", version="0.0.1")

s3_bucket_name = "caribou-FEM-simulation"
s3_bucket_region_name = "us-east-1"

@workflow.serverless_function(
    name="generate_stiffness",
    entry_point=True,
)
def generate_stiffness(event: dict[str, Any]) -> dict[str, Any]:
    
    if isinstance(event, str):
        event = json.loads(event)
    
    if "mesh_size" in event and "num_parallel_tasks" in event:
        mesh_size = event["mesh_size"]
        num_parallel_tasks = event["num_parallel_tasks"]

    elements_per_task = mesh_size // num_parallel_tasks

    global_force_vector = np.random.rand(mesh_size)

    stiffness_matrix = sp.random(mesh_size, mesh_size, density=0.01, format='csr')
    
    stiffness_matrix = stiffness_matrix + stiffness_matrix.T

    payload_1 = {
        "stiffness_matrix": stiffness_matrix[elements_per_task * 0:elements_per_task * 1, elements_per_task * 0:elements_per_task * 1],
        "force_vector": global_force_vector[elements_per_task * 0:elements_per_task * 1],
    }

    payload_2 = {
        "stiffness_matrix": stiffness_matrix[elements_per_task * 1:elements_per_task * 2, elements_per_task * 1:elements_per_task * 2],
        "force_vector": global_force_vector[elements_per_task * 1:elements_per_task * 2],
    }

    payload_3 = {
        "stiffness_matrix": stiffness_matrix[elements_per_task * 2:elements_per_task * 3, elements_per_task * 2:elements_per_task * 3],
        "force_vector": global_force_vector[elements_per_task * 2:elements_per_task * 3],
    }

    payload_4 = {
        "stiffness_matrix": stiffness_matrix[elements_per_task * 3:elements_per_task * 4, elements_per_task * 3:elements_per_task * 4],
        "force_vector": global_force_vector[elements_per_task * 3:elements_per_task * 4],
    }

    workflow.invoke_serverless_function(solve_fem, payload_1)
    workflow.invoke_serverless_function(solve_fem, payload_2)
    workflow.invoke_serverless_function(solve_fem, payload_3)
    workflow.invoke_serverless_function(solve_fem, payload_4)

    return {"status": 200}


@workflow.serverless_function(name="solve_fem")
def solve_fem(event: dict[str, Any]) -> dict[str, Any]:

    stiffness_matrix = event["stiffness_matrix"]

    force_vector = event["force_vector"]

    displacement = spla.spsolve(stiffness_matrix, force_vector)

    payload = {
        "displacement": displacement,
    }

    workflow.invoke_serverless_function(calc_displacement, payload)

    return {"status": 200}


@workflow.serverless_function(name="calc_displacement")
def calc_displacement(event: dict[str, Any]) -> dict[str, Any]:

    events = workflow.get_predecessor_data()
    
    total_displacement = np.sum([np.sum(event["displacement"]) for event in events])

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)

    result_data = {
        "total_displacement": total_displacement
    }

    result_json = json.dump(result_data)

    file_name = datetime.now().strftime("%Y%m%d-%H%M%S") + "output.txt"

    s3.put_object(
        Bucket=s3_bucket_name,
        Key=f"output/{file_name}",
        Body=result_json,
        ContentType="application/json"
    )

    return {"status": 200}
