import sys
import os
from threading import Thread
from datetime import datetime
from time import sleep
from types import FunctionType
from datetime import time

from hive import Hive
from hive.task import LoopTask, OnceTask

import atexit


def func():
    while True:
        print(1)
        sleep(1)


def run(config):
    a = Thread(target=func)
    a.start()
    while True:
        sleep(1)
        print("running")


class K(LoopTask):

    def run(self) -> FunctionType:
        return run

    def should_run(self, c_time: datetime) -> bool:
        a = time(12, 15) < c_time.time() < time(12, 20)
        b = time(12, 25) < c_time.time() < time(12, 30)
        return a or b


@atexit.register
def back():
    print("stop me")
    global hive
    hive.terminal()
    sys.exit(0)


if __name__ == "__main__":
    k = K(name="ccc")
    hive = Hive()
    hive.insert(k)
    hive.run()
