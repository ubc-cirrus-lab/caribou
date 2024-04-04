from typing import Any
import json

import string
import random
import pyaes

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow

workflow = MultiXServerlessWorkflow(name="py_aes", version="0.0.1")


@workflow.serverless_function(
    name="Run-AES-Encryption",
    entry_point=True,
)
def first_function(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "length_of_message" in event and "num_of_iterations" in event:
        length_of_message = event["length_of_message"]
        num_of_iterations = event["num_of_iterations"]
    else:
        raise ValueError("No message provided")

    message = generate(length_of_message)

    # 128-bit key (16 bytes)
    KEY = b"\xa1\xf6%\x8c\x87}_\xcd\x89dHE8\xbf\xc9,"

    for _ in range(num_of_iterations):
        aes = pyaes.AESModeOfOperationCTR(KEY)
        ciphertext = aes.encrypt(message)

        aes = pyaes.AESModeOfOperationCTR(KEY)
        plaintext = aes.decrypt(ciphertext)

        if plaintext != message:
            return {"status": 500}
        aes = None

    return {"status": 200}

def generate(length):
    letters = string.ascii_lowercase + string.digits
    return "".join(random.choice(letters) for i in range(length))

