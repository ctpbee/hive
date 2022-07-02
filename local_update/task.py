import os
from datetime import datetime
from time import sleep

from ctpbee import CtpbeeApi, CtpBee, auth_time
from ctpbee import hickey
from ctpbee import loads
from ctpbee.constant import TickData, ContractData
from ctpbee.date import trade_dates
from redis import Redis
import pandas as pd

from hive import OnceTask, LoopTask, logger

RD_CONTRACT_NAME = "contract"


class Data(CtpbeeApi):
    def __init__(self, rd: Redis):
        super().__init__("data_update")
        self.rd = rd

    def on_tick(self, tick: TickData) -> None:
        from ctpbee.jsond import dumps

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


class DataInsertTask(LoopTask):
    def __init__(self, front=300):
        super().__init__("数据录制")
        for i, v in hickey.open_trading["ctp"].items():
            setattr(hickey, i, hickey.add_seconds(getattr(hickey, i), front))
        uri = os.environ.get("REDIS_URI") or "127.0.0.1:6379"
        host, port = uri.split(":")
        self.redis = Redis(host=host, port=port)
        self.hickey = hickey

    def run(self):
        # 此处实现数据每日自动插入redis
        app = CtpBee("task", __name__, refresh=True)
        app.config.from_json("data_update/login.json")
        api = Data(rd=self.redis)
        app.add_extension(api)
        app.start()

    def should_run(self, c_time: datetime) -> bool:
        return self.hickey.auth_time(c_time)


class CleanDataTask(OnceTask):
    def __init__(self):
        super().__init__("数据清洗",)

    def run(self):
        """清洗数据"""

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
            logger.info("parse {}'s tick".format(contract))
            tick_array = list(redis.lrange(contract, 0, -1))

            def _to_dict(d) -> dict:
                return loads(d)
            data = list(map(_to_dict, tick_array))
            tick = pd.DataFrame(data)
            tick.to_csv(f"{contract}.csv")
            del tick
            del data

        logger.info("Delete redis contract data!")

    def should_run(self, c_time: datetime) -> bool:
        if (
                str(c_time.date()) in trade_dates
                and c_time.hour == 20
                and c_time.minute == 0
                and c_time.second == 0
        ):
            return True
        return False
