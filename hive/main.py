import os
from datetime import datetime
from json import load
from time import time
from typing import Text, Dict, List

from ctpbee.date import trade_dates

from hive.log import logger
from hive.task import Task


class Hive(object):
    def __init__(self):
        self.task_set = {}
        self.__wait_task_queue = []
        self._day = []

    def init_from_config(self):
        """从配置中读取Task任务"""

    @staticmethod
    def root_path() -> Text:
        """获取当前的根目录"""
        return os.environ["ROOT_PATH"]

    def insert(self, *task):
        for ta in task:
            self.task_set[ta.name] = ta

    def read_config_from_json(self, json_path: Text):
        """
        从json文件中获取配置信息
        """
        with open(json_path, "r") as fp:
            data = load(fp=fp)
        self.read_from_mapping(data)

    def read_from_mapping(self, data: Dict):
        """从字典中获取配置信息"""
        for i, v in data.items():
            setattr(self, i, v)

    def run(self):
        """主体运行函数 执行Task"""
        logger.info(f"Hive Started")
        self.init_from_config()
        while True:
            current = datetime.now()
            for task_name in list(self.task_set.keys()):
                task: Task = self.task_set[task_name]
                result = task.flush(c_time=current)
                if result == -1:
                    """ 任务结束运行 """
                    self.task_set.pop(task.name)
                    self.__wait_task_queue.append(task)
                    logger.info(
                        f"task: {task_name} ends and move to next_date")
            if current.hour == 2 and current.minute == 31:
                """ 凌晨2点31 将任务恢复回来 """
                for task in self.__wait_task_queue:
                    self.task_set[task.name] = task
                self.__wait_task_queue.clear()


if __name__ == "__main__":
    hive = Hive()
    hive.run()
