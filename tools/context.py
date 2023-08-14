"""
Home for functionality that provides context managers, and anything related to
making those context managers function.
"""
import logging
from contextlib import contextmanager

from tools.env import ALLOW_NOISY_LOGGING


@contextmanager
def log_filter(log_id, expected_strings=None):
    """
    Context manager which allows silencing logs until exit.
    Log records matching expected_strings will be filtered out of logging.
    If expected_strings is not provided, everything is filtered for that log.
    """
    logger = logging.getLogger(log_id)
    log_filter = _make_filter_class(expected_strings)
    logger.addFilter(log_filter)
    yield
    if log_filter.records_silenced > 0:
        print("Logs were filtered to remove messages deemed unimportant, total count: %d" % log_filter.records_silenced)
    logger.removeFilter(log_filter)


def _make_filter_class(expected_strings):
    """
    Builds an anon-ish filtering class and returns it.

    Returns a logfilter if filtering should take place, otherwise a nooplogfilter.

    We're just using a class here as a one-off object with a filter method, for
    use as a filter object on the desired log.
    """
    class nooplogfilter(object):
        records_silenced = 0

        @classmethod
        def filter(cls, record):
            return True

    class logfilter(object):
        records_silenced = 0

        @classmethod
        def filter(cls, record):
            if expected_strings is None:
                cls.records_silenced += 1
                return False

            for s in expected_strings:
                if s in record.msg or s in record.name:
                    cls.records_silenced += 1
                    return False

            return True

    if ALLOW_NOISY_LOGGING:
        return nooplogfilter
    else:
        return logfilter


@contextmanager
def nodetool_context(node, start_command, end_command):
    """
    To be used when a nodetool command can affect the state of the node,
    like disablebinary/disablegossip, and it is needed to keep the node in said
    state temporarily.

    :param node: the db node where the nodetool command are to be executed on
    :param start_command: the command to execute before yielding the context
    :param end_command: the command to execute as the closing step
    :return:
    """
    try:
        result = node.nodetool(start_command)
        yield result
    finally:
        node.nodetool(end_command)
