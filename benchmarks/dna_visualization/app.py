from typing import Any

from caribou.deployment.client import CaribouWorkflow
import json
from dna_features_viewer import BiopythonTranslator
import matplotlib.pyplot as plt
import uuid
import boto3
import os

# Change the following bucket name and region to match your setup
s3_bucket_name = "caribou-dna-visualization"
s3_bucket_region_name = "us-east-1"

workflow = CaribouWorkflow(name="dna_visualization", version="0.0.1")


@workflow.serverless_function(
    name="visualize", 
    entry_point=True,
)
def visualize(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "gen_file_name" in event:
        gen_file_name = event["gen_file_name"]
    else:
        raise ValueError("No gen_file_name provided")

    req_id = uuid.uuid4()

    local_gen_filename = f"/tmp/genbank-{req_id}.gb"
    local_result_filename = f"/tmp/result-{req_id}.png"

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)
    s3.download_file(
        s3_bucket_name,
        f"genbank/{gen_file_name}",
        local_gen_filename,
    )

    graphic_record = BiopythonTranslator().translate_record(local_gen_filename)
    ax, _ = graphic_record.plot(figure_width=10, strand_in_label_threshold=7)
    ax.figure.tight_layout()
    ax.figure.savefig(local_result_filename)

    # Close the figure to free up memory
    plt.close(ax.figure)

    s3.upload_file(
        local_result_filename,
        s3_bucket_name,
        f"result/{gen_file_name}.png",
    )

    os.remove(local_gen_filename)
    os.remove(local_result_filename)

    return {"status": 200}