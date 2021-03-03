import fileinput
import os
import re
import shutil
import sys
import tempfile
import logging

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
