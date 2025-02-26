from datetime import datetime
from time import sleep

from hived.config import Config
from hived.log import logger
from hived.task import Task


class Hive(object):
    def __init__(self):
        self.task_set: {str: Task} = {}
        self.__wait_task_queue = []
        self._day = []
        self._config = Config()

    @property
    def config(self):
        return self._config

    def insert(self, *tasks):
        for task in tasks:
            self.task_set[task.name] = task

    def terminal(self):
        for i in self.task_set.values():
            if i is not None:
                i.terminal()

    def kill(self):
        for i in self.task_set.values():
            if i is not None:
                i.kill()

    def kill_task(self, name):
        self.task_set[name].kill()

    def terminal_task(self, name):
        self.task_set[name].terminal()

    def remove(self, *name):
        for i in name:
            if self.task_set[i].is_alive():
                self.task_set[i].terminal()
            self.task_set.pop(i)

    def run(self):
        logger.info(f"Hive Started")
        while True:
            current = datetime.now()
            for task_name in self.task_set.keys():
                task: Task = self.task_set[task_name]
                task.flush(c_time=current, config=self.config)
            sleep(self.config.get("interval", 1))
