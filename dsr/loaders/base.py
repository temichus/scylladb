from threading import Thread, Event
from dsr.base.d_entity import DEntity
from ccmlib.node import Node
from dtest_class import Tester
from cassandra import ConsistencyLevel


class LoaderBase(DEntity, Thread):
    create_session_retry = 10
    _tester: Tester = None
    _target_node: Node = None
    _session = None
    consistency_level = ConsistencyLevel.QUORUM
    serial_consistency_level = None

    def __init__(self, **kwargs):
        Thread.__init__(self)
        DEntity.__init__(self, **kwargs)
        self._to_stop = Event()
        self._to_stop.clear()

    def factual_serial_consistency(self):
        raise NotImplementedError(f'Should be overridden for class {self.__class__.__name__}')

    def check_if_can_operate(self):
        raise NotImplementedError(f'Should be overridden for class {self.__class__.__name__}')

    def merge_result(self, output):
        raise NotImplementedError(f'Should be overridden for class {self.__class__.__name__}')

    def _create_session(self):
        raise NotImplementedError(f'Should be overridden for class {self.__class__.__name__}')

    def _run_workload(self):
        raise NotImplementedError(f'Should be overridden for class {self.__class__.__name__}')

    def run(self):
        if not self.check_if_can_operate():
            return
        self._session = None
        for _ in range(self.create_session_retry):
            self._session = self._create_session()
        if not self._session:
            return
        while not self._to_stop.is_set():
            self._run_workload()

    def stop(self, timeout=None):
        self._to_stop.set()
        try:
            self.join(timeout)
        except:  # pylint: disable=bare-except
            pass

    def bind(self, tester: Tester, node: Node):
        self._tester = tester
        self._target_node = node


class NoopLoader(LoaderBase):
    def start(self):
        pass

    def stop(self, timeout=None):
        pass

    def merge_result(self, output):
        pass

    def check_if_can_operate(self):
        return True
