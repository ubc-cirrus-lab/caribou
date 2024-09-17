from typing import Any

import numpy as np
import scipy.sparse as sp
import scipy.sparse.linalg as spla
import json
from caribou.deployment.client import CaribouWorkflow
import boto3
from datetime import datetime
import logging
from tempfile import TemporaryDirectory

workflow = CaribouWorkflow(name="FEM_simulation", version="0.0.2")

s3_bucket_name = "caribou-fem-simulation"
s3_bucket_region_name = "us-east-1"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def csr_matrix_to_dict(matrix):
    return {
        "data": matrix.data.tolist(),
        "indices": matrix.indices.tolist(),
        "indptr": matrix.indptr.tolist(),
        "shape": matrix.shape
    }

def dict_to_csr_matrix(matrix_dict):
    return sp.csr_matrix(
        (matrix_dict["data"], matrix_dict["indices"], matrix_dict["indptr"]),
        shape=matrix_dict["shape"]
    )

@workflow.serverless_function(
    name="generate_stiffness",
    entry_point=True,
)
def generate_stiffness(event: dict[str, Any]) -> dict[str, Any]:
    
    if isinstance(event, str):
        event = json.loads(event)
        
    if "mesh_size" in event and "num_parallel_tasks" in event:
        mesh_size = event["mesh_size"]
        num_parallel_tasks = min(event["num_parallel_tasks"], 4)
    else:
        raise ValueError("Invalid input")

    elements_per_task = mesh_size // num_parallel_tasks

    global_force_vector = np.random.rand(mesh_size)

    stiffness_matrix = sp.random(mesh_size, mesh_size, density=0.01, format='csr')
    
    stiffness_matrix = stiffness_matrix + stiffness_matrix.T

    payloads=[]

    for task_index in range(num_parallel_tasks):
        start_idx = elements_per_task * task_index
        end_idx = elements_per_task * (task_index + 1)

        payload = {
            "stiffness_matrix": csr_matrix_to_dict(stiffness_matrix[start_idx:end_idx, start_idx:end_idx]),
            "force_vector": global_force_vector[start_idx:end_idx].tolist(),
        }

        payloads.append(payload)

    workflow.invoke_serverless_function(solve_fem, payloads[0])
    workflow.invoke_serverless_function(solve_fem, payloads[1], 1 < num_parallel_tasks)
    workflow.invoke_serverless_function(solve_fem, payloads[2], 2 < num_parallel_tasks)
    workflow.invoke_serverless_function(solve_fem, payloads[3], 3 < num_parallel_tasks)

    return {"status": 200}


@workflow.serverless_function(name="solve_fem")
def solve_fem(event: dict[str, Any]) -> dict[str, Any]:

    stiffness_matrix = dict_to_csr_matrix(event["stiffness_matrix"])

    force_vector = np.array(event["force_vector"])

    displacement = spla.lsqr(stiffness_matrix, force_vector)[0]

    payload = {
        "displacement": displacement.tolist(),
    }

    workflow.invoke_serverless_function(calc_displacement, payload)

    return {"status": 200}


@workflow.serverless_function(name="calc_displacement")
def calc_displacement(event: dict[str, Any]) -> dict[str, Any]:

    events = workflow.get_predecessor_data()
    
    total_displacement = np.sum([np.sum(np.array(event["displacement"])) for event in events])

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)

    result_data = {
        "total_displacement": total_displacement
    }

    with TemporaryDirectory() as tmp_dir:
        with open(f"{tmp_dir}/output.txt", "w") as f:
            f.write(json.dumps(result_data))

        s3.upload_file(f"{tmp_dir}/output.txt", s3_bucket_name, f"output/{datetime.now().strftime('%Y%m%d-%H%M%S')}-output.txt")

    return {"status": 200}
