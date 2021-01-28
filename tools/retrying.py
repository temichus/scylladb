import inspect
import logging
import time

logger = logging.getLogger(__name__)


class retrying(object):
    """
        Used as a decorator to retry function run that can possibly fail with allowed exceptions list
    """

    def __init__(self, num_attempts=3, sleep_time=1, allowed_exceptions=(Exception,), message=""):
        self.num_attempts = num_attempts  # number of times to retry
        self.sleep_time = sleep_time  # number seconds to sleep between retries
        self.allowed_exceptions = allowed_exceptions  # if Exception is not allowed will raise
        self.message = message  # string that will be printed between retries

    def __call__(self, func):
        def inner(*args, **kwargs):
            func_args = inspect.getfullargspec(func)
            num_attempts = self.num_attempts
            if 'num_attempts' in func_args.args:
                num_attempts = kwargs.get('num_attempts')
                if not num_attempts:
                    default_args = func_args.args[-len(func_args.defaults):]
                    num_attempts_position = default_args.index('num_attempts')
                    num_attempts = func_args.defaults[num_attempts_position]

            for i in range(num_attempts - 1):
                try:
                    if self.message:
                        logger.debug("trying {} [{}/{}] ({})".format(func.__name__, i + 1, num_attempts, self.message))
                    return func(*args, **kwargs)
                except self.allowed_exceptions as e:
                    logger.debug("{} [{}/{}]: {}: will retry in {} second(s)".format(func.__name__,
                                                                                     i + 1, num_attempts, e,
                                                                                     self.sleep_time))
                    time.sleep(self.sleep_time)
            if self.message:
                logger.debug(f"trying {func.__name__} [{'last try' if num_attempts > 1 else 'single try'}] "
                             f"({self.message})")
            return func(*args, **kwargs)

        return inner
