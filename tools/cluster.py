from ccmlib.cluster import Cluster
from ccmlib.dse_cluster import DseCluster


def new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None, ipformat=None, use_single_interface=False):
    i = len(cluster.nodes) + 1

    ipprefix = cluster.ipprefix or ''

    if not ipformat:
        ipformat = ipprefix + "%d"

    binary = None
    if cluster.cassandra_version() >= '1.2':
        if use_single_interface:
            # Always leave 9042 and 9043 clear, in case someone defaults to adding
            # a node with those ports
            binary = (ipformat % 1, 9042 + 2 + (i * 2))
        else:
            binary = (ipformat % i, 9042)

    thrift = None
    if cluster.__class__ in (Cluster, DseCluster):
        if cluster.cassandra_version() < '4':
            thrift = (ipformat % i, 9160)
    else:
        thrift = (ipformat % i, 9160)

    storage_interface = ((ipformat % i), 7000)

    node = cluster.create_node(name='node%s' % i,
                               auto_bootstrap=bootstrap,
                               thrift_interface=thrift,
                               storage_interface=storage_interface,
                               jmx_port=str(7000 + i * 100 + cluster.id),
                               remote_debug_port=remote_debug_port,
                               initial_token=token,
                               binary_interface=binary,
                               )
    cluster.add(node, not bootstrap, data_center=data_center)
    return node
