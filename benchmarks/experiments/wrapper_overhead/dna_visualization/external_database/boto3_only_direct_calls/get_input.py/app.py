import boto3

def get_input(event, context):
    if "gen_file_name" in event:
        gen_file_name = event["gen_file_name"]
    else:
        raise ValueError("No gen_file_name provided")

    payload = {
        "gen_file_name": gen_file_name,
    }

    # Invoke the next visualize function
    # With boto3 only, the function is invoked directly
    lambda_client = boto3.client("lambda")
    lambda_client.invoke(
        FunctionName="wo-dna_vis-ed-direct-visualize",
        InvocationType="Event",
        Payload=payload,
    )

    return {"status": 200}