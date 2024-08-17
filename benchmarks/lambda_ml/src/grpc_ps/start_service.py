import argparse

from concurrent import futures
from threading import Thread

import sys
sys.path.append("../")

import grpc
from grpc_ps import ps_service_pb2_grpc
from grpc_ps.server.ps_server import PSHandler

# python start_service.py --host 172.31.39.144 --port 27000


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default="127.0.0.1")
    parser.add_argument('--port', type=int, default=27000)
    args = parser.parse_args()
    print(args)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=[
        ('grpc.max_send_message_length', 128 * 1024 * 1024),
        ('grpc.max_receive_message_length', 128 * 1024 * 1024)])
    ps = PSHandler()

    ps_service_pb2_grpc.add_ParameterServerServicer_to_server(ps, server)

    server.add_insecure_port("{}:{}".format(args.host, args.port))
    print("------------------start Python GRPC server")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
