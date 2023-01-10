from datetime import datetime
from time import sleep

from hive.config import Config
from hive.log import logger
from hive.task import Task


class Hive(object):
    def __init__(self):
        self.task_set = {}
        self.__wait_task_queue = []
        self._day = []
        self._config = Config()

    @property
    def config(self):
        return self._config

    def insert(self, *tasks):
        for task in tasks:
            self.task_set[task.name] = task

    def remove(self, *name):
        for i in name:
            self.task_set.pop(i)

    def run(self):
        logger.info(f"Hive Started")
        while True:
            current = datetime.now()
            for task_name in list(self.task_set.keys()):
                task: Task = self.task_set[task_name]
                result = task.flush(c_time=current, config=self.config)
                if result == -1:
                    """ delete task from queue """
                    self.task_set.pop(task.name)
                    self.__wait_task_queue.append(task)
                    logger.info(
                        f"task: {task_name} ends and move to next_date")

            if current.hour == 2 and current.minute == 40:
                """recover task at 2:40 am """
                if len(self.__wait_task_queue) > 0:
                    for task in self.__wait_task_queue:
                        self.task_set[task.name] = task
                    self.__wait_task_queue.clear()
                    logger.info(f"更新任务队列")

            sleep(1)


if __name__ == "__main__":
    hive = Hive()
    hive.config.from_mapping({
        "MAIL": {
            "USER_EMAIL": "somewheve@gmail.com",
            "TO": ["somewheve@gmail.com"],
            "SERVER_URI": "smtp.exmail.qq.com",
            "PASSWD": "passwd",
            "PORT": 465
        },
        "MAIL_TITLE": "qmt日内邮件提醒"
    })
    hive.run()
