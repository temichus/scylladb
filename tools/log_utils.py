import os
import logging
import time
from collections.abc import MutableMapping


from ccmlib.node import TimeoutError as CCMLibTimeoutError
from tools.misc import get_current_test_name


class ProcessLocal(MutableMapping):
    """
    Provides a basic per-process mapping container that wipes itself if the current PID changed since the last get/set.
    Aka `threading.local()`, but for processes instead of threads.

    This is used cache global per process things like log handlers and test information
    CATION: this data won't be shared by other processes, for example when pytest-xdist is being used
    """

    __pid__ = -1

    def __init__(self, mapping_factory=dict):
        self.__mapping_factory = mapping_factory

    def __handle_pid(self):
        new_pid = os.getpid()
        if self.__pid__ != new_pid:
            self.__pid__, self.__store = new_pid, self.__mapping_factory()

    def __delitem__(self, key):
        self.__handle_pid()
        return self.__store.__delitem__(key)

    def __getitem__(self, key):
        self.__handle_pid()
        return self.__store.__getitem__(key)

    def __setitem__(self, key, val):
        self.__handle_pid()
        return self.__store.__setitem__(key, val)

    def __len__(self) -> int:
        self.__handle_pid()
        return len(self.__store)

    def __iter__(self):
        self.__handle_pid()
        return iter(self.__store)


log_per_process_data = ProcessLocal()


class TestNameFilter(logging.Filter):
    """
    Filter to add the test name information, so it would be available for dtest.log
    """

    def filter(self, record):
        record.test_name = get_current_test_name()
        return True


def wait_for_any_log(nodes, patterns, timeout, dispersed=False):
    """
    Look for a pattern in the system.log of any in a given list
    of nodes.
    :param nodes: The list of nodes whose logs to scan
    :param patterns: The target pattern (a string, or a list of strings)
    :param timeout: How long to wait for the pattern. Note that
                    strictly speaking, timeout is not really a timeout,
                    but a maximum number of attempts. This implies that
                    the all the grepping takes no time at all, so it is
                    somewhat inaccurate, but probably close enough.
    :return: The first node in whose log the pattern was found, if not dispersed.
             Otherwise, if dispersed=True, return a list of all nodes with the any of the patterns.
    """

    if dispersed:
        remaining = patterns
        ret = []
        for _ in range(timeout):
            for node in nodes:
                for p in remaining:
                    try:
                        if node.watch_log_for(p, timeout=0):
                            remaining.remove(p)
                            if node not in ret:
                                ret.append(node)
                    except (CCMLibTimeoutError, TimeoutError):
                        pass
            if not remaining:
                return ret
            time.sleep(1)
    else:
        for _ in range(timeout):
            for node in nodes:
                try:
                    found = node.watch_log_for(patterns, timeout=0)
                    if found:
                        return node
                except (CCMLibTimeoutError, TimeoutError):
                    pass
            time.sleep(1)

    raise TimeoutError(time.strftime("%d %b %Y %H:%M:%S", time.gmtime()) +
                       (" Unable to find :%s in any node log within " % patterns) + str(timeout) + "s")
