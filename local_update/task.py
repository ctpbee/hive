from calendar import c
import os
from datetime import datetime
from time import sleep

import pandas as pd
from ctpbee import CtpbeeApi, CtpBee, auth_time
from ctpbee import hickey
from ctpbee import loads
from ctpbee.constant import TickData, ContractData
from ctpbee.date import trade_dates
from redis import Redis
from ctpbee.jsond import dumps

from hive import OnceTask, LoopTask, logger

RD_CONTRACT_NAME = "contract"


class RecordApi(CtpbeeApi):
    def __init__(self, rd: Redis):
        super().__init__("data_update")
        self.rd = rd
        self.initd = {}

    def on_tick(self, tick: TickData) -> None:
        """当天的第一个TICK 是不需要的 因为在开盘前启动 会推送一个脏TICK"""
        if not self.initd.get(tick.local_symbol, False):
            self.initd[tick.local_symbol] = True
            return None
        else:
            self.rd.rpush(tick.local_symbol, dumps(tick))

    def on_contract(self, contract: ContractData) -> None:
        """订阅行情代码 且更新redis中的数据"""
        codes = [
            str(x, encoding="utf8") for x in self.rd.lrange(RD_CONTRACT_NAME, 0, -1)
        ]
        if len(contract.symbol) <= 6:
            self.action.subscribe(contract.local_symbol)
            if contract.local_symbol not in codes:
                self.rd.rpush(RD_CONTRACT_NAME, contract.local_symbol)


def record_data():
    front = 300
    for i, v in hickey.open_trading["ctp"].items():
        setattr(hickey, i, hickey.add_seconds(getattr(hickey, i), front))
    uri = os.environ.get("REDIS_URI") or "127.0.0.1:6379"
    host, port = uri.split(":")
    redis = Redis(host=host, port=port)

    app = CtpBee("task", __name__, refresh=True)
    app.config.from_json("login.json")
    api = RecordApi(rd=redis)
    app.add_extension(api)
    app.start()


class DataInsertTask(LoopTask):
    def __init__(self):
        super().__init__("数据录制")

    def run(self):
        # 此处实现数据每日自动插入redis
        return record_data

    def should_run(self, c_time: datetime) -> bool:
        return hickey.auth_time(c_time)


def delete_data():

    uri = os.environ.get("REDIS_URI") or "127.0.0.1:6379"
    host, port = uri.split(":")
    redis = Redis(host=host, port=port)
    contracts = set(
        [str(x, encoding="utf8")
            for x in redis.lrange(RD_CONTRACT_NAME, 0, -1)]
    )
    logger.info("Get Contracts: Len:{}".format(len(contracts)))
    index = 0
    for contract in contracts:
        index += 1
        tick_array = redis.lrange(contract, 0, -1)

        def _to_dict(d) -> dict:
            return loads(d)
        data = list(map(_to_dict, tick_array))
        print(data)
        tick = pd.DataFrame(data)
        # fixme: 这里的地址需要进行修改 
        tick.to_csv(f"D:\\Test\\{contract}.csv")
        del tick
        del data

    redis.flushall()
    logger.info("当天数据清理完毕")


class CleanDataTask(OnceTask):
    def __init__(self):
        super().__init__("数据清洗",)

    def run(self):
        """清洗数据"""
        return delete_data

    def should_run(self, c_time: datetime) -> bool:
        # if (
        #         str(c_time.date()) in trade_dates
        #         and c_time.hour == 20
        #         and c_time.minute == 0
        #         and c_time.second == 0
        # ):
        #     return True
        # return False
        if c_time.hour == 0 and c_time.minute == 51:
            return True
        return False
