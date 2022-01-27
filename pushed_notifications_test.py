import time
import pytest
import logging

from filelock import FileLock
from cassandra import ReadTimeout, ReadFailure
from cassandra import ConsistencyLevel as CL
from cassandra.query import SimpleStatement
from dtest_class import Tester, create_ks, get_ip_from_node, create_cf
from tools.data import insert_c1c2
from threading import Event
from tools.assertions import assert_invalid
from pkg_resources import parse_version
from tools.funcutils import assertDictContainsSubset


logger = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def using_localhost(fixture_dtest_setup):
    """
    make sure tests using localhost are not running at the same time is pytest-xdist is used
    """
    logging.getLogger("filelock").setLevel(logging.INFO)
    with FileLock('/tmp/localhost'):
        yield


class NotificationWaiter(object):
    """
    A helper class for waiting for pushed notifications from
    Cassandra over the native protocol.
    """

    def __init__(self, tester, node, notification_types, keyspace=None):
        """
        `address` should be a ccmlib.node.Node instance
        `notification_types` should be a list of
        "TOPOLOGY_CHANGE", "STATUS_CHANGE", and "SCHEMA_CHANGE".
        """
        self.node = node
        self.address = node.network_interfaces['binary'][0]
        self.notification_types = notification_types
        self.keyspace = keyspace

        # get a single, new connection
        session = tester.patient_cql_connection(node)
        self.connection = session.cluster.connection_factory(self.address, is_control_connection=True)

        # coordinate with an Event
        self.event = Event()

        # the pushed notification
        self.notifications = []

        # register a callback for the notification type
        for notification_type in notification_types:
            self.connection.register_watcher(notification_type, self.handle_notification, register_timeout=5.0)

    def handle_notification(self, notification):
        """
        Called when a notification is pushed from Cassandra.
        """
        logger.debug("Source {} sent {}".format(self.address, notification))

        if self.keyspace and notification['keyspace'] and self.keyspace != notification['keyspace']:
            return  # we are not interested in this schema change

        self.notifications.append(notification)
        self.event.set()

    def wait_for_notifications(self, timeout, num_notifications=1):
        """
        Waits up to `timeout` seconds for notifications from Cassandra. If
        passed `num_notifications`, stop waiting when that many notifications
        are observed.
        """

        deadline = time.time() + timeout
        while time.time() < deadline:
            self.event.wait(deadline - time.time())
            self.event.clear()
            if len(self.notifications) >= num_notifications:
                break

        return self.notifications

    def clear_notifications(self):
        self.notifications = []
        self.event.clear()

    def close(self):
        self.connection.close()


@pytest.mark.dtest_full
class TestPushedNotifications(Tester):
    """
    Tests for pushed native protocol notification from Cassandra.
    """

    @pytest.mark.no_vnodes
    def test_move_single_node(self, request: pytest.FixtureRequest):
        """
        @jira_ticket CASSANDRA-8516
        Moving a token should result in NODE_MOVED notifications.
        """
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)

        # Despite waiting for each node to see the other nodes as UP, there is apparently
        # still a race condition that can result in NEW_NODE events being sent.  We don't
        # want to accidentally collect those, so for now we will just sleep a few seconds.
        time.sleep(3)

        waiters = [NotificationWaiter(self, node, ["TOPOLOGY_CHANGE"])
                   for node in self.cluster.nodes.values()]
        request.addfinalizer(lambda: [_waiter.close() for _waiter in waiters])
        node1 = self.cluster.nodes.values()[0]
        node1.move("123")

        for waiter in waiters:
            logger.debug("Waiting for notification from {}".format(waiter.address,))
            notifications = waiter.wait_for_notifications(60.0)
            assert 1 == len(notifications)
            notification = notifications[0]
            change_type = notification["change_type"]
            address, port = notification["address"]
            assert "MOVED_NODE" == change_type
            assert get_ip_from_node(node1) == address

    @pytest.mark.no_vnodes
    @pytest.mark.usefixtures('using_localhost')
    def test_move_single_node_localhost(self, request: pytest.FixtureRequest):
        """
        @jira_ticket  CASSANDRA-10052
        Test that we don't get NODE_MOVED notifications from nodes other than the local one,
        when rpc_address is set to localhost.

        To set-up this test we override the rpc_address to "localhost" for all nodes, and
        therefore we must change the rpc port or else processes won't start.
        """
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()

        # change node3 'rpc_address' from '127.0.0.x' to 'localhost', increase port numbers
        i = 0
        for node in cluster.nodelist():
            node.network_interfaces['thrift'] = ('localhost', node.network_interfaces['thrift'][1] + i)
            node.network_interfaces['binary'] = ('localhost', node.network_interfaces['thrift'][1] + 1)
            node.import_config_files()  # this regenerates the yaml file and sets 'rpc_address' to the 'thrift' address
            logger.debug(node.show())
            i = i + 2

        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        # Despite waiting for each node to see the other nodes as UP, there is apparently
        # still a race condition that can result in NEW_NODE events being sent.  We don't
        # want to accidentally collect those, so for now we will just sleep a few seconds.
        time.sleep(3)

        waiters = [NotificationWaiter(self, node, ["TOPOLOGY_CHANGE"])
                   for node in self.cluster.nodes.values()]
        request.addfinalizer(lambda: [_waiter.close() for _waiter in waiters])
        node1 = self.cluster.nodes.values()[0]
        node1.move("123")

        for waiter in waiters:
            logger.debug("Waiting for notification from {}".format(waiter.address,))
            notifications = waiter.wait_for_notifications(30.0)
            assert (1 if waiter.node is node1 else 0) == len(notifications)

    def test_restart_node(self, request: pytest.FixtureRequest):
        """
        @jira_ticket CASSANDRA-7816
        Restarting a node should generate exactly one DOWN and one UP notification
        """
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()

        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])
        request.addfinalizer(lambda: waiter.close())
        # need to block for up to 2 notifications (NEW_NODE and UP) so that these notifications
        # don't confuse the state below.
        logger.debug("Waiting for unwanted notifications...")
        waiter.wait_for_notifications(timeout=30, num_notifications=2)
        waiter.clear_notifications()

        # On versions prior to 2.2, an additional NEW_NODE notification is sent when a node
        # is restarted. This bug was fixed in CASSANDRA-11038 (see also CASSANDRA-11360)
        version = self.cluster.cassandra_version()
        logger.debug("Version={}".format(version))
        expected_notifications = 2 if version >= '2.2' else 3
        for i in range(5):
            logger.debug("Restarting second node...")
            node2.stop(wait_other_notice=True)
            node2.start(wait_other_notice=True)
            logger.debug("Waiting for notifications from {}".format(waiter.address))
            notifications = waiter.wait_for_notifications(timeout=60.0, num_notifications=expected_notifications)
            assert expected_notifications, len(notifications) == notifications
            for notification in notifications:
                assert get_ip_from_node(node2) == notification["address"][0]
            assert "DOWN" == notifications[0]["change_type"]
            if version >= '2.2':
                assert "UP" == notifications[1]["change_type"]
            else:
                # pre 2.2, we'll receive both a NEW_NODE and an UP notification,
                # but the order is not guaranteed
                assert {"NEW_NODE", "UP"} == set([n["change_type"] for n in notifications[1:]])

            waiter.clear_notifications()

    def test_sleep_and_restart_node(self, request: pytest.FixtureRequest):
        """
        Sleep 120 seconds after cluster is ready, then restart the second node,
        check we get correct client notifications during restart
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        cluster.start(wait_for_binary_proto=True)
        # Sleep 120 to wait the pending joined notification to be sent
        time.sleep(120)

        # register for notification with node1
        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])
        request.addfinalizer(lambda: waiter.close())
        # restart node 2
        logger.debug("Restarting second node...")
        node2.stop(wait_other_notice=True)
        node2.start(wait_other_notice=True)

        # check that node1 did not send UP or DOWN notification for node2
        logger.debug("Waiting for notifications from {}".format(waiter.address,))
        notifications = waiter.wait_for_notifications(timeout=30.0, num_notifications=2)
        assert 2 == len(notifications)
        for notification in notifications:
            assert node2.address() == notification["address"][0]
        assert "DOWN" == notifications[0]["change_type"]
        assert "UP" == notifications[1]["change_type"]

    @pytest.mark.usefixtures('using_localhost')
    @pytest.mark.parametrize("wait_and_restart", [True, False], ids=["wait_and_restart=True", "wait_and_restart=False"])
    def test_restart_node_localhost(self, wait_and_restart, request: pytest.FixtureRequest):
        """
        Test that we don't get client notifications when rpc_address is set to localhost Pre 4.0.
        Test that we get correct client notifications when rpc_address is set to localhost Post 4.0.
        @jira_ticket  CASSANDRA-10052
        @jira_ticket  CASSANDRA-15677

        Scylla doesn't support nodes with same IP, it's worth to test it.

        To set-up this test we override the rpc_address to "localhost" for all nodes, and
        therefore we must change the rpc port or else processes won't start.

        when wait_and_restart == True
        Wait a while to ensure the join event of node2 is sent by node1,
        then restart node2
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        i = 0  # change 'rpc_address' from '127.0.0.x' to 'localhost' and diversify port numbers
        for node in cluster.nodelist():
            node.network_interfaces['thrift'] = ('localhost', node.network_interfaces['thrift'][1] + i)
            node.network_interfaces['binary'] = ('localhost', node.network_interfaces['thrift'][1] + 1)
            node.import_config_files()  # this regenerates the yaml file and sets 'rpc_address' to the 'thrift' address
            # `native_shard_aware_transport_port' has default value (19042) in scylla.yaml,
            # so it's need to be unique in this test.
            node.set_configuration_options(
                values={'native_shard_aware_transport_port': node.network_interfaces['thrift'][1] + 10000})
            logger.debug(node.show())
            i = i + 2

        cluster.start(wait_for_binary_proto=True)
        # Wait a while to ensure the join event of node2 is sent by node1
        if wait_and_restart:
            time.sleep(120)

        # register for notification with node1
        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])
        request.addfinalizer(lambda: waiter.close())
        # restart node 2
        logger.debug("Restarting second node...")
        node2.stop(wait_other_notice=True)
        node2.start(wait_other_notice=True)

        # check that node1 did not send UP or DOWN notification for node2
        logger.debug("Waiting for notifications from {}".format(waiter.address,))
        expected_notifications = 2 if wait_and_restart else 3
        notifications = waiter.wait_for_notifications(timeout=30.0, num_notifications=expected_notifications)
        logger.debug("Received {} notifications: {}".format(len(notifications), notifications))
        if wait_and_restart:
            assert len(notifications) == expected_notifications
        else:
            assert len(notifications) >= expected_notifications
        for notification in notifications:
            assert node2.address() == notification["address"][0]
        assert "DOWN" == notifications[0]["change_type"]
        if len(notifications) == 3:
            assert "NEW_NODE" == notifications[1]["change_type"]
        assert "UP" == notifications[-1]["change_type"]

    def test_schema_changes(self, request: pytest.FixtureRequest):
        """
        @jira_ticket CASSANDRA-10328
        Creating, updating and dropping a keyspace, a table and a materialized view
        will generate the correct schema change notifications.
        """

        self.cluster.populate(2).start(wait_for_binary_proto=True)
        node1, node2 = self.cluster.nodelist()

        session = self.patient_cql_connection(node1)
        waiter = NotificationWaiter(self, node2, ["SCHEMA_CHANGE"], keyspace='ks')
        request.addfinalizer(lambda: waiter.close())

        create_ks(session, 'ks', 3)
        session.execute("create TABLE t (k int PRIMARY KEY , v int)")
        session.execute("alter TABLE t add v1 int;")

        session.execute(
            "create MATERIALIZED VIEW mv as select * from t WHERE v IS NOT NULL AND k IS NOT NULL PRIMARY KEY (v, k)")
        session.execute(" alter materialized view mv with min_index_interval = 100")

        session.execute("drop MATERIALIZED VIEW mv")
        session.execute("drop TABLE t")
        session.execute("drop KEYSPACE ks")

        logger.debug("Waiting for notifications from {}".format(waiter.address,))
        notifications = waiter.wait_for_notifications(timeout=60.0, num_notifications=14)
        assert 10 == len(notifications)

        assertDictContainsSubset({'change_type': u'CREATED', 'target_type': u'KEYSPACE'}, notifications[0])
        assertDictContainsSubset(
            {'change_type': u'CREATED', 'target_type': u'TABLE', u'table': u't'}, notifications[1])
        assertDictContainsSubset(
            {'change_type': u'UPDATED', 'target_type': u'TABLE', u'table': u't'}, notifications[2])
        assertDictContainsSubset({'change_type': u'CREATED', 'target_type': u'TABLE',
                                  u'table': u'mv'}, notifications[3])
        assertDictContainsSubset({'change_type': u'UPDATED', 'target_type': u'TABLE',
                                  u'table': u't'}, notifications[4])
        assertDictContainsSubset({'change_type': u'UPDATED', 'target_type': u'TABLE',
                                  u'table': u't'}, notifications[5])
        assertDictContainsSubset({'change_type': u'UPDATED', 'target_type': u'TABLE',
                                  u'table': u'mv'}, notifications[6])
        assertDictContainsSubset({'change_type': u'DROPPED', 'target_type': u'TABLE',
                                  u'table': u'mv'}, notifications[7])
        assertDictContainsSubset({'change_type': u'DROPPED', 'target_type': u'TABLE',
                                  u'table': u't'}, notifications[8])
        assertDictContainsSubset({'change_type': u'DROPPED', 'target_type': u'KEYSPACE'}, notifications[9])

    def test_new_node_event_delay(self, request: pytest.FixtureRequest):
        """
        NEW_NODE event is delayed, otherwise cql client will connect Scylla server
        even the new node isn't ready.
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node1.start(wait_for_binary_proto=True)

        # Register for notifications with node1
        waiter = NotificationWaiter(self, node1, ["STATUS_CHANGE", "TOPOLOGY_CHANGE"])
        request.addfinalizer(lambda: waiter.close())

        logger.debug("Start the second node, expect the NEW_NODE event is delayed until the cql server is ready")
        node2.start()

        logger.debug("Waiting for notifications from {}".format(waiter.address,))
        notifications = waiter.wait_for_notifications(timeout=30.0, num_notifications=1)

        # Try to connect the server when any notification is received
        session = self.cql_connection(node2)
        create_ks(session, 'ks', 2)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(100))

        received_new_node_event = False
        for notification in notifications:
            assert node2.address() == notification["address"][0]
            if "NEW_NODE" == notification["change_type"]:
                received_new_node_event = True
        assert received_new_node_event, "NEW_NODE event isn't received"


@pytest.mark.dtest_full
class TestVariousNotifications(Tester):
    """
    Tests for various notifications/messages from Cassandra.
    """

    @pytest.mark.skip("Scylla doesn't support `tombstone_failure_threshold', read railure won't be triggered")
    def test_tombstone_failure_threshold_message(self):
        """
        Ensure nodes return an error message in case of TombstoneOverwhelmingExceptions rather
        than dropping the request. A drop makes the coordinator waits for the specified
        read_request_timeout_in_ms.
        @jira_ticket CASSANDRA-7886
        """

        self.cluster.set_configuration_options(
            values={
                'tombstone_failure_threshold': 500,
                'read_request_timeout_in_ms': 30000,  # 30 seconds
                'range_request_timeout_in_ms': 40000
            }
        )
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        create_ks(session, 'test', 3)
        session.execute(
            "CREATE TABLE test ( "
            "id int, mytext text, col1 int, col2 int, col3 int, "
            "PRIMARY KEY (id, mytext) )"
        )

        # Add data with tombstones
        values = map(lambda i: str(i), range(1000))
        for value in values:
            session.execute(SimpleStatement(
                "insert into test (id, mytext, col1) values (1, '{}', null) ".format(
                    value
                ),
                consistency_level=CL.ALL
            ))

        failure_msg = ("Scanned over.* tombstones.* query aborted")
        self.ignore_log_patterns += [failure_msg]

        @pytest.mark.timeout(25)
        def read_failure_query():
            assert_invalid(
                session, SimpleStatement("select * from test where id in (1,2,3,4,5)", consistency_level=CL.ALL),
                expected=ReadTimeout if parse_version(self.cluster.version()) < parse_version('3.0') else ReadFailure,
            )

        read_failure_query()

        failure = (node1.grep_log(failure_msg) or
                   node2.grep_log(failure_msg) or
                   node3.grep_log(failure_msg))

        assert failure, ("Cannot find tombstone failure threshold error in log "
                         "after failed query")
        mark1 = node1.mark_log()
        mark2 = node2.mark_log()
        mark3 = node3.mark_log()

        @pytest.mark.timeout(35)
        def range_request_failure_query():
            assert_invalid(
                session, SimpleStatement("select * from test", consistency_level=CL.ALL),
                expected=ReadTimeout if parse_version(self.cluster.version()) < parse_version('3.0') else ReadFailure,
            )

        range_request_failure_query()

        failure = (node1.watch_log_for(failure_msg, from_mark=mark1, timeout=5) or
                   node2.watch_log_for(failure_msg, from_mark=mark2, timeout=5) or
                   node3.watch_log_for(failure_msg, from_mark=mark3, timeout=5))

        assert failure, ("Cannot find tombstone failure threshold error in log "
                         "after range_request_timeout_query")
