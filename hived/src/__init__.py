from hived.src.task import DataUpdateTask, CleanDataTask
from hived import Hive


class Terminal:

    def __init__(self) -> None:
        self.hive = Hive()

    def _build_task(self):
        tasklist = [DataUpdateTask(), CleanDataTask()]
        self.hive.insert(*tasklist)

    def start(self):
        self.hive.run()
