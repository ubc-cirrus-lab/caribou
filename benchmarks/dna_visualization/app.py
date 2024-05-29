from typing import Any

from caribou.deployment.client import CaribouWorkflow
import json
from dna_features_viewer import BiopythonTranslator
import uuid
import boto3
import os


workflow = CaribouWorkflow(name="dna_visualization", version="0.0.1")


@workflow.serverless_function(name="Visualize", entry_point=True)
def visualize(event: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "gen_file_name" in event:
        gen_file_name = event["gen_file_name"]
    else:
        raise ValueError("No gen_file_name provided")

    req_id = uuid.uuid4()

    local_gen_filename = f"/tmp/genbank-{req_id}.gb"
    local_result_filename = f"/tmp/result-{req_id}.png"

    s3 = boto3.client("s3", region_name='us-west-2')
    s3.download_file(
        "caribou-dna-visualization",
        f"genbank/{gen_file_name}",
        local_gen_filename,
    )

    graphic_record = BiopythonTranslator().translate_record(local_gen_filename)
    ax, _ = graphic_record.plot(figure_width=10, strand_in_label_threshold=7)
    ax.figure.tight_layout()
    ax.figure.savefig(local_result_filename)

    s3.upload_file(
        local_result_filename,
        "caribou-dna-visualization",
        f"result/{gen_file_name}.png",
    )

    os.remove(local_gen_filename)
    os.remove(local_result_filename)

    return {"status": 200}
