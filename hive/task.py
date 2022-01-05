from datetime import datetime
from enum import Enum
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


class Task:
    """
    任务执行
    """

    def __init__(self, name: Text, owner: Owner, type_: TaskType):
        """
        :param name: 任务名称
        :param owner:任务拥有者
        :param type_:
        """
        self.name = name
        self.create_time = datetime.now()
        self._owner = owner
        self._type = type_
        self.result = 0

    def __execute__(self):
        """ 任务执行主体函数 由execute函数主动执行调用 并进行结果清晰"""
        raise NotImplemented

    def __repr__(self):
        return f"Task: {self.name} At {self.create_time}"
