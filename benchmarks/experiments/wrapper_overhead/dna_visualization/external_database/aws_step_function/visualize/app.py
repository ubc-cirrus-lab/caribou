from dna_features_viewer import BiopythonTranslator
import logging 
import datetime
import boto3
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

def visualize(event, context):
    if "gen_file_name" not in event:
        raise ValueError("No gen_file_name provided")
    if "metadata" not in event:
        raise ValueError("No metadata provided")
    
    gen_file_name = event["gen_file_name"]
    req_id = event["metadata"]["request_id"]

    local_gen_filename = f"/tmp/genbank-{req_id}.gb"
    local_result_filename = f"/tmp/result-{req_id}.png"

    s3 = boto3.client("s3")
    s3.download_file(
        "multi-x-serverless-dna-visualization",
        f"genbank/{gen_file_name}",
        local_gen_filename,
    )

    graphic_record = BiopythonTranslator().translate_record(local_gen_filename)
    ax, _ = graphic_record.plot(figure_width=10, strand_in_label_threshold=7)
    ax.figure.tight_layout()
    ax.figure.savefig(local_result_filename)

    s3.upload_file(
        local_result_filename,
        "multi-x-serverless-dna-visualization",
        f"result/{gen_file_name}.png",
    )

    os.remove(local_gen_filename)
    os.remove(local_result_filename)

    # Log the end time of the function
    ## Get the current time
    current_time = datetime.datetime.now()

    ## Get the start time from the metadata
    start_time = event["metadata"]["start_time"]
    start_time = datetime.datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S.%f')

    ## Calculate the time delta in microseconds
    microseconds_from_start = (current_time - start_time).microseconds

    ## Get the workload name from the metadata
    workload_name = event["metadata"]["workload_name"]

    ## Log the time taken along with the request ID and workload name
    time_info_log = f"Workload Name: {workload_name}, Request ID: {req_id}, Time Taken from workload start: {microseconds_from_start} microseconds"
    logger.info(time_info_log)

    # Return a success response
    return {"status": "Visualization completed", "file": f"{gen_file_name}.png", time_info_log: time_info_log}