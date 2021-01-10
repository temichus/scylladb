import os
import logging
from collections.abc import MutableMapping

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
