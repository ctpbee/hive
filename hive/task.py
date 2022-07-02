from types import FunctionType
from .log import logger
from datetime import datetime
from multiprocessing import Process
from typing import Text


class Task:
    """ 
    基础的任务实现  
    """

    def __init__(
        self, name: Text
    ):
        self.name = name
        self.create_time = datetime.now()
        self._pro: None or Process = None

    def alive(self) -> bool:
        if self._pro is None:
            return False
        else:
            if self._pro.is_alive():
                return True
            else:
                self._pro = None
                return False

    def should_run(self, c_time: datetime) -> bool:
        """ 用户应该重写运行逻辑 """
        raise NotImplemented

    def run(self) -> FunctionType:
        raise NotImplemented

    def _run(self):
        logger.info(f"task: {self.name} started")
        self._pro = Process(target=self.run(),daemon=True)
        self._pro.start()

    def kill(self):
        logger.info(f"task: {self.name} killed")
        return self._kill(self._pro)

    def flush(self, c_time: datetime) -> int:
        """ 此方法应该被task的提供者进行重写 """
        raise NotImplemented

    def _kill(self, process: Process):
        return process.kill()

    def __repr__(self) -> str:
        return f"{self.name} with {self.create_time}"


class LoopTask(Task):

    def flush(self, c_time: datetime) -> int:
        if not self.alive() and self.should_run(c_time=c_time):
            self._run()
            return 1
        elif self.alive() and not self.should_run(c_time=c_time):
            self.kill()
            return -1
        return 0


class OnceTask(Task):
    def flush(self, c_time: datetime) -> int:
        if not self.alive() and self.should_run(c_time=c_time):
            self._run()
            return -1
        return 0
