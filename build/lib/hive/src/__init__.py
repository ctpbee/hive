from hive.src.task import DataUpdateTask, CleanDataTask
from hive import Hive


class Terminal:

    def __init__(self) -> None:
        self.hive = Hive()

    def _build_task(self):
        tasklist = [DataUpdateTask(), CleanDataTask()]
        self.hive.insert(*tasklist)

    def start(self):
        self.hive.run()
