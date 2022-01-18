import os
from datetime import datetime

from ctpbee import CtpbeeApi, CtpBee, auth_time
from ctpbee import hickey
from ctpbee import loads
from ctpbee.constant import TickData, ContractData
from ctpbee.date import trade_dates
from data_api import Tick, DataApi
from redis import Redis

from hive import Task, logger
from hive.task import TaskType

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

    def on_init(self, init: bool) -> None:
        print(self.usage)


def get_redis_connection() -> Redis:
    uri = os.environ.get("REDIS_URI") or "127.0.0.1:6379"
    host, port = uri.split(":")
    redis = Redis(host=host, port=port)
    return redis


class DataInsertTask(Task):
    def __init__(self, c_type=TaskType.LOOP, front=300):
        super().__init__("数据录制", type_=c_type)
        for i, v in hickey.open_trading["ctp"].items():
            setattr(hickey, i, hickey.add_seconds(getattr(hickey, i), front))

        self.hickey = hickey

    def __execute__(self):
        # 此处实现数据每日自动插入redis
        app = CtpBee("task", __name__, refresh=True)
        app.config.from_json("data_update/login.json")

        redis = get_redis_connection()
        api = Data(rd=redis)

        app.add_extension(api)
        app.start()

    def auth_time(self, time: datetime) -> bool:
        return self.hickey.auth_time(time)


class CleanDataTask(Task):
    def __init__(self, c_type=TaskType.LOOP):
        super().__init__("数据清洗", type_=c_type)

    def __execute__(self):
        """清洗数据"""
        redis = get_redis_connection()
        contracts = set(
            [str(x, encoding="utf8") for x in redis.lrange(RD_CONTRACT_NAME, 0, -1)]
        )
        logger.info("Get Contracts: Len:{}".format(len(contracts)))
        ck_uri = os.environ.get("CLICKHOUSE_URI", "http://192.168.1.239:8124/")
        data_api = DataApi(uri=ck_uri)
        index = 0
        for contract in contracts:
            index += 1
            logger.info("parse {}'s tick".format(contract))
            try:
                ticks = []
                amount = 0
                volume = 0
                for x in redis.lrange(contract, 0, -1):
                    tick = loads(str(x, encoding="utf8"))
                    if auth_time(tick.datetime.time()):
                        vol = tick.volume - volume
                        amount = tick.turnover - amount
                        ck_tick = Tick(
                            open_interest=int(tick.open_interest),
                            local_symbol=tick.local_symbol,
                            last_price=tick.last_price,
                            high_price=tick.high_price,
                            datetime=tick.datetime,
                            low_price=tick.low_price,
                            volume=int(vol),
                            amount=amount,
                            ask_price_1=tick.ask_price_1,
                            ask_price_2=tick.ask_price_2,
                            ask_price_3=tick.ask_price_3,
                            ask_price_4=tick.ask_price_4,
                            ask_price_5=tick.ask_price_5,
                            bid_price_1=tick.bid_price_1,
                            bid_price_2=tick.bid_price_2,
                            bid_price_3=tick.bid_price_3,
                            bid_price_4=tick.bid_price_4,
                            bid_price_5=tick.bid_price_5,
                            ask_volume_1=tick.ask_volume_1,
                            ask_volume_2=tick.ask_volume_2,
                            ask_volume_3=tick.ask_volume_3,
                            ask_volume_4=tick.ask_volume_4,
                            ask_volume_5=tick.ask_volume_5,
                            bid_volume_1=tick.bid_volume_1,
                            bid_volume_2=tick.bid_volume_2,
                            bid_volume_3=tick.bid_volume_3,
                            bid_volume_4=tick.bid_volume_4,
                            bid_volume_5=tick.bid_volume_5,
                        )
                        amount = tick.turnover
                        volume = tick.volume
                        ticks.append(ck_tick)
                logger.info(
                    "{}: insert ticks len:{} to {}".format(index, len(ticks), contract)
                )
                data_api.insert_ticks(ticks)
                redis.delete(contract)
            except Exception as e:
                logger.error("find {} when update {}".format(e, contract))
        # 清除合约数据
        redis.delete(RD_CONTRACT_NAME)

    def auth_time(self, time: datetime) -> bool:
        if (
                str(time.date()) in trade_dates
                and time.hour == 14
                and time.minute == 56
                and time.second == 0
        ):
            return True
        return False
