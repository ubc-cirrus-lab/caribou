#!/usr/bin/env python3

import socket
import json

HOST = '172.31.34.109'  # The server's hostname or IP address
PORT = 9000        # The port used by the server


def lambda_handler(event, context):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(b'Hello, world')
        data = s.recv(1024)

    print('Received', repr(data))

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
