
from src.task import DataInsertTask, CleanDataTask
from hive import Hive


class Terminal:

    def __init__(self) -> None:
        self.hive = Hive()

    def _build_task(self):
        tasklist = [DataInsertTask(),  CleanDataTask()]
        self.hive.insert(*tasklist)

    def start(self):
        self.hive.run()
