import os
from datetime import datetime
from json import load
from time import sleep, time
from typing import Text, Dict

from hive.log import logger
from hive.task import Task


class Hive(object):
    def __init__(self):
        self.task_set = {}
        self.__wait_task_queue = []
        self._day = []

    @staticmethod
    def root_path() -> Text:
        """获取当前的根目录"""
        return os.environ["ROOT_PATH"]

    def insert(self, *tasks):
        for task in tasks:
            self.task_set[task.name] = task

    def run(self):
        """主体运行函数 执行Task"""
        logger.info(f"Hive Started")
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

            if current.hour == 2 and current.minute == 40:
                """ 凌晨2点40 将任务恢复回来 """
                if len(self.__wait_task_queue) > 0:
                    for task in self.__wait_task_queue:
                        self.task_set[task.name] = task
                    self.__wait_task_queue.clear()
                    logger.info(f"更新任务队列")
            # 休息 1s 降低cpu使用
            sleep(1)


if __name__ == "__main__":
    hive = Hive()
    hive.run()
