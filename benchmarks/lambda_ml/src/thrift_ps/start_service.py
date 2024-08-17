import sys

import argparse

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TJSONProtocol
from thrift.server import TServer

sys.path.append("../")

from thrift_ps.ps_service import ttypes
from thrift_ps.ps_service import ParameterServer

from thrift_ps.server.ps_server import PSHandler, PSHandler2
from thrift_ps.model.model_moniter import ModelMonitor
from thrift_ps import constants


# python start_service.py --host 172.31.21.243 --port 27000 --interval 60 --expired 6000 --dir ~/tmp/lambda


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default=constants.HOST)
    parser.add_argument('--port', type=int, default=constants.PORT)
    parser.add_argument('--interval', type=int, default=constants.INTERVAL)
    parser.add_argument('--expired', type=int, default=constants.EXPIRED_TIME)
    parser.add_argument('--dir', type=str, default="/tmp")
    args = parser.parse_args()
    print(args)

    # PS handler
    handler = PSHandler()
    #handler = PSHandler2(args.dir)
    # Monitor thread of PS handler
    mm = ModelMonitor(handler, args.interval, args.expired)
    mm.start()
    # Start PS process
    processor = ParameterServer.Processor(handler)
    transport = TSocket.TServerSocket(host=args.host, port=args.port)
    # Buffering is critical. Raw sockets are very slow
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    # server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    # You could do one of these for a multithreaded server
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    print('Starting the server >>> IP = {}, PORT = {}'.format(args.host, args.port))
    server.serve()
    print('done.')
