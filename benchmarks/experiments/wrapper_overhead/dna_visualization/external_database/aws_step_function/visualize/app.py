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

    # Log finished time
    log_finish(event)

    # Log additional information
    log_additional_info(event)

    return {"status": 200}

def log_finish(event):
    # Log the end time of the function
    ## Get the current time
    current_time = datetime.datetime.now(datetime.timezone.utc)
    final_function_end_time = current_time.strftime("%Y-%m-%d %H:%M:%S,%f%z")

    ## Get the start time from the metadata
    start_time_str = event["metadata"]["start_time"]
    start_time = datetime.datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S,%f%z")

    first_function_start_time_str = event["metadata"]["first_function_start_time"]
    first_function_start_time = datetime.datetime.strptime(first_function_start_time_str, "%Y-%m-%d %H:%M:%S,%f%z")
    
    ## Calculate the time delta in miliseconds
    ### For the time taken from the perspective of customer
    time_difference = current_time - start_time
    ms_from_start = (time_difference.days * 24 * 3600 * 1000) + (time_difference.seconds * 1000) + (time_difference.microseconds / 1000)

    ### For the time taken from the first function running
    time_difference = current_time - first_function_start_time
    ms_from_first_function = (time_difference.days * 24 * 3600 * 1000) + (time_difference.seconds * 1000) + (time_difference.microseconds / 1000)

    ## Get the workload name from the metadata
    workload_name = event["metadata"]["workload_name"]
    request_id = event["metadata"]["request_id"]

    ## Log the time taken along with the request ID and workload name
    logger.info(f"Workload Name: {workload_name}, "
                f"Request ID: {request_id}, "
                f"Client Start Time: {start_time_str}, "
                f"First Function Start Time: {first_function_start_time_str}, "
                f"Time Taken from workload invocation from client: {ms_from_start} ms, "
                f"Time Taken from first function: {ms_from_first_function} ms, "
                f"Function End Time: {final_function_end_time}")

def log_additional_info(event):
    # Log the CPU model, workflow name, and the request ID
    cpu_model = ""
    with open('/proc/cpuinfo') as f:
        for line in f:
            if "model name" in line:
                cpu_model = line.split(":")[1].strip()  # Extracting and cleaning the model name
                break  # No need to continue the loop once the model name is found

    workload_name = event["metadata"]["workload_name"]
    request_id = event["metadata"]["request_id"]

    logger.info(f"Workload Name: {workload_name}, Request ID: {request_id}, CPU Model: {cpu_model}")