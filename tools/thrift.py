from thrift_bindings.thrift010 import Cassandra
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport


def get_thrift_client(host='127.0.0.1', port=9160):
    socket = TSocket.TSocket(host, port)
    transport = TTransport.TFramedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Cassandra.Client(protocol)
    client.transport = transport
    return client
