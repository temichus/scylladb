import logging

logger = logging.getLogger(__name__)


def create_stress_compatible_table(self, node, rf=1, dclocal_read_repair_chance=0.1, gc_grace_seconds=864000,
                                   read_repair_chance=0.0, default_time_to_live=0, speculative_retry="'99.0PERCENTILE'",
                                   compaction="'class': 'SizeTieredCompactionStrategy', 'sstable_size_in_mb': '100'"):
    session = self.patient_cql_connection(node)
    session.execute(f"""CREATE KEYSPACE keyspace1 WITH replication = {{
    'class': 'SimpleStrategy',
    'replication_factor': {rf} }};""")

    session.execute(f"""CREATE TABLE keyspace1.standard1(
    key blob PRIMARY KEY,
    "C0" blob,
    "C1" blob,
    "C2" blob,
    "C3" blob,
    "C4" blob,
    ) WITH bloom_filter_fp_chance = 0.01
    AND caching = {{'keys': 'ALL', 'rows_per_partition': 'ALL'}}
    AND comment = ''
    AND compaction = {{{compaction}}}
    AND compression = {{}}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = {dclocal_read_repair_chance}
    AND default_time_to_live = {default_time_to_live}
    AND gc_grace_seconds = {gc_grace_seconds}
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = {read_repair_chance}
    AND speculative_retry = {speculative_retry};""")


def fill_data_by_cs(node, n_range=[500, 550, 600, 650], start=0, duration_range=[],
                    other_opt=['-rate', 'threads=10', '-col', 'size=FIXED(1024)'],
                    overlap_rate=0, flush=True):
    """
    fill data by multiple cassandra-stress workloads
    """
    opts = []
    for num in n_range:
        opts.append([f'n={num}', '-pop', f'seq={start}..{start + num}'])
        start += int(num * (1 - overlap_rate))
    for t in duration_range:
        opts.append([f'duration={t}s'])
    for opt in opts:
        cs_cmdline = ['write', 'no-warmup'] + opt + other_opt
        node.stress(cs_cmdline)
        if flush:
            logger.debug("Flush after writing data .....")
            node.flush()


def format_cs_output(output: tuple):
    if output.__class__.__name__ == 'Subprocess_Return':
        return f'stderr:\n{output.stderr}\n\nstdout:\n{output.stdout}'
    elif isinstance(output, tuple):
        return "\n".join(output)
    else:
        return NotImplementedError()


def assert_cs_success(output: tuple):
    stdout = output.stdout if output.__class__.__name__ == 'Subprocess_Return' else output[0]
    assert stdout.strip().endswith(("END", "DONE")), f"Run c-s failed: {format_cs_output(output)}"
