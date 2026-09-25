#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.rest_client import HTTPError, read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.mark.asyncio
@pytest.mark.xfail(strict=True, raises=HTTPError, reason=(
    "POST /raft/read_barrier without `timeout` on a strongly consistent tablet group hits on_internal_error "
    "'raft operation [read_barrier], timeout requested ... but no value for it has been defined' "
    "(service/raft/raft_group_registry.cc:488): get_request_timeout() in api/raft.cc turns a missing parameter "
    "into a timeout without a value, and SC groups, unlike group0, have no default_op_timeout_in_ms to fill it in. "
    "api-doc/raft.json documents a 60s default. With abort-on-internal-error, the default in tests, the node aborts."))
async def test_read_barrier_without_timeout_on_sc_group(manager: ScyllaClusterManager):
    """Verify that a read barrier on a strongly consistent tablet group, requested through
    the REST API without the optional `timeout` parameter, succeeds using the documented
    60s default instead of failing an internal invariant.

    Found while writing the SC failure tests (SCYLLADB-4514), which call this endpoint to
    wait for a restarted replica to catch up and had to pass `timeout` explicitly. The node
    runs with abort-on-internal-error off, so that the bug shows up as a failed request
    rather than a crashed node that would take the rest of the test down with it.
    """
    server = await manager.server_add(config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE + ['--abort-on-internal-error', '0'])
    await manager.get_ready_cql([server])

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'"
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
            await read_barrier(manager.api, server.ip_addr, group_id, timeout=60)  # the explicit form works
            await read_barrier(manager.api, server.ip_addr, group_id)
