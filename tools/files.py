import fileinput
import os
import re
import shutil
import subprocess
import sys
import tempfile
import logging
import glob
from pathlib import Path

from typing import Optional, List

logger = logging.getLogger(__name__)

DEFAULT_DIR = './'


def replace_in_file(filepath, search_replacements):
    """
    In-place file search and replace.

    filepath - The path of the file to edit
    search_replacements - a list of tuples (regex, replacement) that
    represent however many search and replace operations you wish to
    perform.

    Note: This does not work with multi-line regexes.
    """
    for line in fileinput.input(filepath, inplace=True):
        for regex, replacement in search_replacements:
            line = re.sub(regex, replacement, line)
        sys.stdout.write(line)


def safe_mkdtemp():
    tmpdir = tempfile.mkdtemp()
    # \ on Windows is interpreted as an escape character and doesn't do anyone any favors
    return tmpdir.replace('\\', '/')


def size_of_files_in_dir(dir_name, verbose=True):
    """
    Return the size of all files found in a non-recursive ls of the argument.
    Based on http://stackoverflow.com/a/1392549
    """
    files = [os.path.join(dir_name, f) for f in os.listdir(dir_name)]
    if verbose:
        logger.debug('getting sizes of these files: {}'.format(files))
    return sum(os.path.getsize(f) for f in files)


def copy_files_to(from_dir, to_dir, files_only=False, create_to_dir=False):
    """
    Copy files from `from_dir` to `to_dir`, optionally create `to_dir`

    :param files_only: if true, only copy files and ignore sub directories
    :param create_to_dir: if true, create `to_dir` if it doesn't exist
    """
    if create_to_dir and not os.path.exists(to_dir):
        os.makedirs(to_dir)
    for f in os.listdir(from_dir):
        if os.path.isfile(os.path.join(from_dir, f)):
            shutil.copy2(os.path.join(from_dir, f), os.path.join(to_dir, f))
        elif not files_only:
            shutil.copytree(os.path.join(from_dir, f), os.path.join(to_dir, f))


def get_sstables_files(cf_dir, f_type=''):
    """
    Returns a set of sstable(s) files for a given KS and CF
    """
    tocs = glob.glob(os.path.join(cf_dir, '*-TOC.txt'))
    files = []
    if not f_type:
        for t in tocs:
            files += glob.glob(t[:-7] + '*')
    elif f_type == 'TOC':
        files = tocs
    else:
        for t in tocs:
            files += glob.glob(t[:-7] + f"*{f_type}*")
    return set([os.path.basename(fname) for fname in files])


def get_node_cf_dir(node, ks_name='ks', cf_name='cf', latest=False):
    """
    Return the first CF directory for a CF with a given name
    in the given keyspace and node
    """
    return get_cf_dir(os.path.join(node.get_path(), 'data', ks_name), cf_name, latest)


def get_cf_dir(ks_dir, cf_name, latest=False):
    """
    Return the first CF directory for a CF with a given name
    """
    if latest:
        return get_latest_dir(ks_dir, cf_name + '-')

    cf_pattern = re.compile("{}-".format(cf_name))
    for root, dirs, files in os.walk(ks_dir):
        for d in dirs:
            if cf_pattern.match(d):
                return os.path.join(root, d)


def get_latest_dir(srcdir: str, pattern: Optional[str] = '') -> Optional[str]:
    """
    Get latest created directory path, which matches with the pattern
    """
    sorted_list = sorted(os.listdir(srcdir), reverse=True,
                         key=lambda x: os.path.getctime(os.path.join(srcdir, x)))
    for item in sorted_list:
        item_path = os.path.join(srcdir, item)
        if os.path.isdir(item_path) and re.compile(pattern).match(item):
            return item_path


def copy_directory(srcdir, destdir, ignore_subdir=True):
    """
    Copy file from srcdir to destdir, it supports to optionally ignore sub directories.
    """
    if not ignore_subdir:
        shutil.copytree(srcdir, destdir)
    for item in os.listdir(srcdir):
        srcfile = os.path.join(srcdir, item)
        if not os.path.exists(destdir):
            os.mkdir(destdir)
        if os.path.isfile(srcfile):
            shutil.copy2(srcfile, destdir)


def get_list_of_sstables(node, keyspace_name, table_name, suffix='-Statistics.db'):
    ks_path = os.path.join(node.get_path(), 'data', keyspace_name)
    statistics_files = []
    for dirpath, dirnames, filenames in os.walk(ks_path):
        elems = os.path.split(dirpath)
        # We are in the table's dir
        if elems[-1].startswith(table_name):
            statistics_files += [os.path.join(dirpath, f) for f in filenames if f.endswith(suffix)]

        # prune all dirs that are not a table dir
        for dirname in dirnames:
            if not dirname.startswith(table_name):
                dirnames.remove(dirname)

    return statistics_files


def check_file_lists_are_equal(file_list_a: List[Path], file_list_b: List[Path]) -> bool:
    """
    Checks for equality for filenames in 2 lists of files (e.g. from a glob of a directory).
    """
    files_a = sorted([item.name for item in file_list_a])
    files_b = sorted([item.name for item in file_list_b])

    return files_a == files_b


def load_files_with_sstableloader(files_dir, node, keyspace, table, extra_args=None):
    logger.info("Loading files with sstableloader from dir: %s", files_dir)
    ip = node.address()
    # copy sstables to ks.cf folder to properly load with sstableloader
    tmpdir = safe_mkdtemp()
    try:
        copy_directory(srcdir=files_dir, destdir=os.path.join(tmpdir, keyspace, table), ignore_subdir=False)

        args = [node.get_tool('sstableloader'), '-d', ip, os.path.join(tmpdir, keyspace, table)]
        if extra_args:
            args += extra_args
        run_cmd = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = run_cmd.communicate()
        exit_status = run_cmd.wait()

        if exit_status != 0 or 'exception' in str(stderr):
            raise Exception("sstableloader command '%s' failed; exit status: %d'; stdout: %s; stderr: %s" %
                            (" ".join(args), exit_status, stdout, stderr))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
