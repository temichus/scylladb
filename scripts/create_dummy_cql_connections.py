from cassandra.cluster import Cluster

import argparse

parser = argparse.ArgumentParser(description='Creates multiple dummy connections to Scylla.')
parser.add_argument('address', type=str,
                    help='scylla ip address')
parser.add_argument('connections', type=int,
                    help='numbers of connections to create')

if __name__ == "__main__":

    args = parser.parse_args()
    address, connections = (args.address, args.connections)
    cluster = Cluster([address])
    sessions = []
    for _ in range(connections):
        sessions.append(cluster.connect())

    input(f"{connections} cql connections created. Press any key to close them and quit...\n")
    [session.shutdown() for session in sessions]
