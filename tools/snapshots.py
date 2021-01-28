import logging
import glob
import os
import shutil
import subprocess

from distutils import dir_util

from ccmlib.scylla_node import ScyllaNode

from .misc import safe_mkdtemp

logger = logging.getLogger(__name__)


def make_snapshot(node: ScyllaNode, ks: str = None, cf: str = None, cf_param_name: str = '-cf', name: str = None) -> str:
    """Create snapshot for all keyspaces or for specified ks, ks.cf, with name

    Create snapshot for:
    - if ks is none, for all keyspaces
    - if ks is provided, create snapshot for all tables in keyspace
    - if ks and cf provided, create snapshot for ks.cf table only
    - if name is set, create snapshot with tag name, datetime otherwise

    and then copy created snapshots to temp directory

    :param node: Scylla Node instance where create snapshot
    :type node: ScyllaNode
    :param ks: keyspace name, defaults to None
    :type ks: str, optional
    :param cf: column factory name, defaults to None
    :type cf: str, optional
    :param name: tag name of snapshot, defaults to None
    :type name: str, optional
    :returns: path where all snapshots stored, temp directory
    :rtype: {str}
    """
    logger.debug("Making snapshot....")
    node.flush()
    snapshot_cmd = 'snapshot '
    if ks:
        snapshot_cmd += f"{ks} "
        if cf:
            snapshot_cmd += f"{cf_param_name} {cf} "
        if name:
            snapshot_cmd += f"-t {name}"

    logger.debug("Running snapshot cmd: {snapshot_cmd}".format(snapshot_cmd=snapshot_cmd))
    node.nodetool(snapshot_cmd)
    tmpdir = safe_mkdtemp()
    node_dir = node.get_path()

    # # Find the snapshot dir, it's different in various C* versions:
    snapshot_dirs = []
    tables = [f"{t}-*/" for t in cf.split(',')] if cf else ['*/']
    for table in tables:
        snapshot_dir_pattern = f"{node_dir}/data/"
        if ks:
            snapshot_dir_pattern += f"{ks}/"
            snapshot_dir_pattern += f"{table}"
            if name:
                snapshot_dir_pattern += f"snapshots/{name}"
            else:
                snapshot_dir_pattern += f"snapshots/*"
        else:
            snapshot_dir_pattern += f"/*/*/snapshots/*"
        snapshot_dir = glob.glob(snapshot_dir_pattern)
        if snapshot_dir:
            snapshot_dirs.extend(snapshot_dir)
        else:
            snapshot_dirs.append('')

    logger.debug(f"snapshot_dir is : {snapshot_dirs}")
    logger.debug(f"snapshot copy is : {tmpdir}")

    # # Copy files from the snapshot dir to existing temp dir
    for snapshot_dir in snapshot_dirs:
        save_dir = snapshot_dir.replace('/snapshots', '').replace(os.path.join(node_dir, "data/"), '')
        os.makedirs(os.path.join(tmpdir, save_dir), exist_ok=False)
        dir_util.copy_tree(str(snapshot_dir), os.path.join(tmpdir, save_dir))

    return tmpdir


def get_cf_snapshot_saved_dir(base_snapshot_dir: str, keyspace: str, table: str, name: str = None) -> str:
    """Get path to specified snapshot of ks.cf by name or first one

    return path to directory with sstables from snapshot store in
    base_snapshot_dir. base_snapshot_dir is a path to temp folder returned by
    make_snapshot method or any folder where all snapshots located
        - <base_snapshot_dir>/ks/cf-*/[name|any]/

    :param base_snapshot_dir: path to folder with snapshots
    :type base_snapshot_dir: str
    :param keyspace: keyspace name
    :type keyspace: str
    :param table: column family name
    :type table: str
    :param name: name of snapshot, defaults to None
    :type name: str, optional
    :returns: path to first matched snapshot dir for ks.cf by [name| of first one]
    :rtype: {str}
    """
    path_pattern = f"{base_snapshot_dir}/{keyspace}/{table}-*"
    if name:
        path_pattern += f"/{name}"
    else:
        path_pattern += f"/*/"
    return glob.glob(path_pattern)[0]


def restore_snapshot_with_refresh(snapshot_dir, node, keyspace, table, name=None):
    logger.debug("Restoring snapshot....")
    node_dir = node.get_path()
    restore_dir = glob.glob("{node_dir}/data/{keyspace}/{table}-*/upload/".format(**locals()))[0]
    snapshot_dir = get_cf_snapshot_saved_dir(base_snapshot_dir=snapshot_dir, keyspace=keyspace, table=table, name=name)
    logger.debug("Copying from %s to %s" % (str(snapshot_dir), str(restore_dir)))
    dir_util.copy_tree(snapshot_dir, restore_dir)
    node.nodetool("refresh %s %s" % (keyspace, table))


def restore_snapshot_with_sstableloader(snapshot_dir, node, keyspace, table, name=None):
    logger.debug("Restoring snapshot....")
    snapshot_dir = get_cf_snapshot_saved_dir(snapshot_dir, keyspace, table, name)
    ip = node.address()
    # copy sstables to ks.cf folder to properly load with sstableloader
    tmpdir = safe_mkdtemp()
    os.makedirs(os.path.join(tmpdir, keyspace, table), exist_ok=True)
    dir_util.copy_tree(snapshot_dir, os.path.join(tmpdir, keyspace, table))

    args = [node.get_tool('sstableloader'), '-d', ip, os.path.join(tmpdir, keyspace, table)]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    exit_status = p.wait()

    if exit_status != 0 or 'exception' in str(stderr):
        raise Exception("sstableloader command '%s' failed; exit status: %d'; stdout: %s; stderr: %s" %
                        (" ".join(args), exit_status, stdout, stderr))
    shutil.rmtree(tmpdir, ignore_errors=True)
