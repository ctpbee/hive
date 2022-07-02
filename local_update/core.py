from task import DataInsertTask, CleanDataTask
from hive import Hive

if __name__ == "__main__":
    hive = Hive()
    data_task_1 = DataInsertTask()
    data_task_2 = CleanDataTask()
    hive.insert(data_task_1, data_task_2,)
    hive.run()
