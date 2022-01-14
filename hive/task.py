from datetime import datetime
from enum import Enum
from multiprocessing import Process
from typing import Text


class TaskType(Enum):
    LOOP = 1
    ONCE = 2


class TaskStatus(Enum):
    READY = 0
    PENDING = 1
    FINISHED = 2
    EXIT = 3


class Owner(Enum):
    MAIN = "Main System"


class Task(object):
    """
    任务执行
    """

    def __init__(self, name: Text, owner: Owner, type_: TaskType):
        self.name = name
        self.create_time = datetime.now()
        self._owner = owner
        self._type = type_
        self.result = 0

        self._pro: None or Process = None

    def __execute__(self):
        """ 任务执行主体函数 由execute函数主动执行调用 并进行结果清晰"""
        raise NotImplemented

    def auth_time(self, time: datetime) -> bool:
        """ 实现此类以实现任务正确时间开启与关闭 """
        raise NotImplemented

    def __run__(self):
        self._pro = Process(target=self.__execute__())
        self._pro.start()


    def pid(self) -> int:
        """ 返回子进程的ID """
        return self._pro.pid

    def alive(self) -> bool:
        """ 判断子进程是否还活着 """
        if self._pro is None:
            return False
        else:
            return self._pro.is_alive()

    def kill(self):
        """ 关闭该子进程 """
        self._pro.kill()

    def __repr__(self):
        return f"Task: {self.name} At {self.create_time}"
