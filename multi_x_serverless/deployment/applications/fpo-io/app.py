import json

# Function specific informaitons
import math
import os
import time
from typing import Any, Optional

import boto3
from chalice import Chalice

app = Chalice(app_name="fpo-io")


def handle_request(args: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    start_time = time.perf_counter()  # START TIMER

    # Default values
    default_values = {"n": 20, "c": 5000, "i": 2, "s": 0}

    # Update with provided arguments
    if args:
        default_values.update({k: int(args.get(k, str(v))) for k, v in default_values.items()})

    for _ in range(default_values["n"]):
        # Simulating CPU Burst
        for u in range(default_values["c"]):
            sin_u = math.sin(u)
            cos_u = math.cos(u)
            sqrt_u = math.sqrt(u)

        # Simulating IO Burst
        for _ in range(default_values["i"]):
            with open("/tmp/test.data", "wb") as f:
                f.write(os.urandom(4096))
            with open("/tmp/test.data", "rb") as f:
                data = f.read(4096)

        # Simulating Sleep
        time.sleep(default_values["s"] / 1000)

    end_time = time.perf_counter()  # END TIMER
    execution_time = (end_time - start_time) * 1000  # Convert to milliseconds

    return {"message": "OK", "duration": execution_time, "inputs": default_values}


@app.lambda_function()
def lambda_handler(event: dict[str, Any]) -> dict[str, Any]:
    return handle_request(event)
