import ctypes
import json
from pathlib import Path

"""
This file is intended for testing/debugging of caribou-go code.
"""

CaribouGo = ctypes.CDLL("caribougo.so")
IM_DATAFILE = "go_data.json"
COMMAND_PIPE = "command_pipe"
SEND_GO = "data_py_go"
REC_GO = "data_go_py"


def send_to_go(channel_path, command, data):
    with open(channel_path, "w") as ch:
        pkt = json.dumps({
            "command": command,
            "data": data
        })
        ch.write(pkt + '\n')
        ch.flush()


def receive_from_go(channel_path):
    with open(channel_path, "r") as ch:
        data = json.load(ch)
        return data


# Step 1: Read data from the json file
im_data = json.dumps(json.load(open(IM_DATAFILE, 'r')))

# Step 2: Prompt cli for command and data
while True:
    command = input("command: ")
    data = input("data: ")
    if command == "setup":
        send_to_go(SEND_GO, "Setup", im_data)
    elif command == "quit":
        break
    else:
        # data = [int(x) for x in data[1:-1].split(', ')]
        send_to_go(SEND_GO, "CalculateDeploymentMetrics", data)

    result = receive_from_go(REC_GO)
    print(result)

im_data = json.dumps(json.load(open(IM_DATAFILE, 'r')))
print("Python: Starting up Go")
CaribouGo.start(str(Path(__file__).parent.resolve()).encode('utf-8'))

print("Python: Setting up InputManager")
CaribouGo.goRead()
send_to_go(SEND_GO, "Setup", im_data)
ret_data = receive_from_go(REC_GO)
print("PYREC ", ret_data)

print("Python: Check InputManager")
CaribouGo.goRead()
send_to_go(SEND_GO, "Check", "")
ret_data = receive_from_go(REC_GO)
print("PYREC ", ret_data)

print("Python: TestFunc1 InputManager")
CaribouGo.goRead()
send_to_go(SEND_GO, "TestFunc1", "")
ret_data = receive_from_go(REC_GO)
print("PYREC ", ret_data)

print("Python: TestFunc2 InputManager")
CaribouGo.goRead()
send_to_go(SEND_GO, "TestFunc2", "")
ret_data = receive_from_go(REC_GO)
print("PYREC ", ret_data)
