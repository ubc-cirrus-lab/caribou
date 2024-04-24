from caribou.common.models.remote_client.integration_test_remote_client import IntegrationTestRemoteClient
import shutil
import os

def test_run_function(workdir: str):
    # This test runs the functions in the integration test mock provider (an sqlite database)
    print("Test running the functions.")

    remote_client = IntegrationTestRemoteClient()
    added_resources = remote_client.select_all_from_table("resources")

    assert len(added_resources) == 1

    # Move the zip file to the working directory
    os.mkdir(os.path.join(workdir, "function"))

    function_blob = added_resources[0][1]

    with open(os.path.join(workdir, "function", "function.zip"), "wb") as f:
        f.write(function_blob)

    # Unzip the file
    shutil.unpack_archive(os.path.join(workdir, "function", "function.zip"), os.path.join(workdir, "function"))

    # Run the function
    result = os.system("poetry run python " + os.path.join(workdir, "function", "app.py"))

    assert result == 0

    print("Test running the functions passed.")
