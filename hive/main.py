from datetime import datetime
from json import load
from typing import Text, Dict, List

from hive.log import logger
from hive.task import Task, TaskStatus


def run(task: Task):
    task.__run__()


class Hive(object):
    RD_URI = "tcp://127.0.0.1:6379"
    CLICKHOUSE_URI = "http://127.0.0.1:8123"
    ROOT_PATH = ""

    def __init__(self):
        self.task_set: {Text: Task} = {}
        self.wait_task_queue = []

    def init_from_config(self):
        """ 从配置中读取Task任务 """

    def root_path(self) -> Text:
        """ 获取当前的根目录 """
        return self.ROOT_PATH

    def insert(self, task: Task):
        self.task_set[task.name] = task

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

    def detect_task(self) -> List[Task]:
        """
        探测root_path的task本地文件内容有无发生改变

        """
        tasks = []
        import os
        # todo: 探测数据
        for file in os.listdir(self.root_path()):
            pass
        return tasks

    def hot_load_task(self, task: Task):
        """ 热更新当前任务 """
        if task.name in self.task_set:
            if self.task_set[task.name].status == TaskStatus.PENDING:
                self.wait_task_queue.append(task)
            else:
                self.task_set[task.name] = task

    def run(self):
        """主体运行函数 执行Task"""
        logger.info(f"Hive started")

        self.init_from_config()

        while True:
            # 探测是否更新task任务载入

            current = datetime.now()
            for task in self.task_set.values():
                if task.auth_time(time=current) and not task.alive():
                    """ 当前时间正确且当前进程不活跃 """
                    logger.info(f"start task: {task.name}")
                    run(task)

                if not task.auth_time(time=current) and task.alive():
                    logger.info(f"stop task: {task.name}")
                    task.kill()


if __name__ == '__main__':
    hive = Hive()
    hive.run()
