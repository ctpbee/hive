from http.client import IM_USED
import os
from datetime import datetime

import csv
from ctpbee import CtpbeeApi, CtpBee
from ctpbee import hickey
from ctpbee.constant import TickData, ContractData
from ctpbee.date import trade_dates
from redis import Redis

from hive import OnceTask, LoopTask, logger

from env import RD_CONTRACT_NAME, FILE_CLEAN_TIME, FILE_SAVE_PATH


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
            takeit = [tick.local_symbol, str(tick.datetime), tick.last_price, tick.volume, tick.turnover, tick.open_interest,
                      tick.ask_price_5, tick.ask_volume_5,
                      tick.ask_price_4, tick.ask_volume_4,
                      tick.ask_price_3, tick.ask_volume_3,
                      tick.ask_price_2, tick.ask_volume_2,
                      tick.ask_price_1, tick.ask_volume_1,
                      tick.bid_price_1, tick.bid_volume_1,
                      tick.bid_price_2, tick.bid_volume_2,
                      tick.bid_price_3, tick.bid_volume_3,
                      tick.bid_price_4, tick.bid_volume_4,
                      tick.bid_price_5, tick.bid_price_5
                      ]
            self.rd.rpush(tick.local_symbol, str(takeit))

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
    # 创建当天的文件夹 
    date = str(datetime.now().date())
    dir_path = os.path.join(FILE_SAVE_PATH, date)
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
    # 逐个合约写入csv
    for contract in contracts:
        index += 1
        tick_array = redis.lrange(contract, 0, -1)
        filepath = os.path.join(dir_path, f"{contract}.csv")
        with open(filepath, "w") as csvfile:
            writer = csv.writer(csvfile)
            # 先写入columns_name
            writer.writerow(["local_symbol", "datetime", "last_price", "volume", "turnover", "open_interest",
                             "ask_price_5", "ask_volume_5",
                             "ask_price_4", "ask_volume_4",
                             "ask_price_3", "ask_volume_3",
                             "ask_price_2", "ask_volume_2",
                             "ask_price_1", "ask_volume_1",
                             "bid_price_1", "bid_volume_1",
                             "bid_price_2", "bid_volume_2",
                             "bid_price_3", "bid_volume_3",
                             "bid_price_4", "bid_volume_4",
                             "bid_price_5", "bid_volume_5",
                             ])
            array = [eval(str(x, encoding="utf8")) for x in tick_array]
            writer.writerows(array)
        del tick_array

    redis.flushall()
    logger.info("当天数据清理完毕")


class CleanDataTask(OnceTask):
    def __init__(self):
        super().__init__("数据清洗",)

    def run(self):
        """清洗数据"""
        return delete_data

    def should_run(self, c_time: datetime) -> bool:
        if (
                str(c_time.date()) in trade_dates
                and c_time.hour == FILE_CLEAN_TIME.hour
                and c_time.minute == FILE_CLEAN_TIME.minute
                and c_time.second == FILE_CLEAN_TIME.second
        ):
            return True
        return False
