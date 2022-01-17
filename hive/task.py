from datetime import datetime
from enum import Enum
from multiprocessing import Process
from typing import Text


class TaskType(Enum):
    """
    ONCE 符合要求执行一次 适合每天执行一次的任务
    LOOP 指定时间段内符合一次 适合长期任务 需要关闭的那种
    """

    LOOP = 1
    ONCE_AGAIN = 2


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

    def __init__(
        self, name: Text, owner: Owner = Owner.MAIN, type_: TaskType = TaskType.LOOP
    ):
        self.name = name
        self.create_time = datetime.now()
        self._owner = owner
        self._type = type_
        self._task_run = False
        self._pro: None or Process = None

    def __execute__(self):
        """任务执行主体函数 由execute函数主动执行调用 并进行结果清晰"""
        raise NotImplemented

    def auth_time(self, time: datetime) -> bool:
        """实现此类以实现任务正确时间开启与关闭"""
        raise NotImplemented

    def run(self):
        self._pro = Process(target=self.__execute__)
        self._pro.start()
        self._task_run = True

    def reset_task(self):
        self._task_run = False

    def pid(self) -> int or None:
        """返回子进程的ID"""
        if self._pro is None:
            return None
        else:
            return self._pro.pid

    def task_type(self) -> TaskType:
        return self._type

    def task_run_status(self):
        return self._task_run

    def alive(self) -> bool:
        """判断子进程是否还活着"""
        if self._pro is None:
            return False
        else:
            return self._pro.is_alive() and self._task_run

    def kill(self):
        """关闭该子进程"""
        self._pro.kill()
        self._task_run = False

    def __repr__(self):
        return f"Task: {self.name} At {self.create_time}"
