from datetime import datetime

from ctpbee import hickey
from ctpbee.date import trade_dates

from hived import LoopTask, OnceTask
from hived.src.env import FILE_CLEAN_TIME
from hived.src.func import record_data, clean_data_from_redis


class DataUpdateTask(LoopTask):
    def __init__(self, **kwargs):
        super().__init__("数据录制")
        interface = kwargs.get("interface", "ctp")
        front = kwargs.get("front", 600)
        for i, v in hickey.open_trading[interface].items():
            setattr(hickey, i, hickey.add_seconds(getattr(hickey, i), front))

    def run(self):
        # 此处实现数据每日自动插入redis
        return record_data

    def should_run(self, c_time: datetime) -> bool:
        return hickey.auth_time(c_time)


class CleanDataTask(OnceTask):
    def __init__(self, **kwargs):
        super().__init__("数据清洗")

    def run(self):
        """清洗数据"""
        return clean_data_from_redis

    def should_run(self, c_time: datetime) -> bool:
        if (
                str(c_time.date()) in trade_dates
                and c_time.hour == FILE_CLEAN_TIME.hour
                and c_time.minute == FILE_CLEAN_TIME.minute
                and c_time.second == FILE_CLEAN_TIME.second
        ):
            return True
        return False
