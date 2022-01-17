from data_update.real import DataInsertTask, CleanDataTask
from hive import Hive
from hive.task import TaskType

if __name__ == "__main__":
    hive = Hive()
    data_task_1 = DataInsertTask(c_type=TaskType.LOOP)
    data_task_2 = CleanDataTask(c_type=TaskType.ONCE_AGAIN)
    hive.insert(data_task_1, data_task_2)
    hive.run()
