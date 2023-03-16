import cassandra


def change_schema_safely(session, nodes, query, timeout=30):
    """
    Run a schema-altering query (e.g. ALTER KEYSPACE)
    and watch all given nodes' log for a "Schema version changed" message
    (naively) indicating that the schema was modified on all nodes.
    This prevents races with following queries that depend on the schema change.
    Note: this function is suitable if no other schema-modifications are
    expected to happen in parallel.
    """
    marks = [(node, node.mark_log()) for node in nodes]
    session.execute(query)
    for node, mark in marks:
        node.watch_log_for("Schema version changed", timeout=timeout)
