from data_update.real import DataInsertTask
from hive import Hive
from hive.task import Owner, TaskType

if __name__ == '__main__':
    hive = Hive()
    data_task = DataInsertTask(name="数据录制", owner=Owner.MAIN, type_=TaskType.LOOP)
    hive.insert(data_task)
    hive.run()
