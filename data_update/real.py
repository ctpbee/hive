import os
from datetime import datetime

from ctpbee import CtpbeeApi, CtpBee, auth_time
from ctpbee import loads
from ctpbee.constant import TickData, ContractData
from ctpbee.date import trade_dates
from redis import Redis

from hive import Task, logger

RD_CONTRACT_NAME = "contract"


class Data(CtpbeeApi):
    def __init__(self, rd: Redis):
        super().__init__("data_update")
        self.rd = rd

    def on_tick(self, tick: TickData) -> None:
        from ctpbee.jsond import dumps
        self.rd.rpush(tick.local_symbol, dumps(tick))

    def on_contract(self, contract: ContractData) -> None:
        """ 订阅行情代码 且更新redis中的数据 """
        self.action.subscribe(contract.local_symbol)
        self.rd.rpush(RD_CONTRACT_NAME, contract.local_symbol)


def get_redis_connection() -> Redis:
    uri = os.environ.get("REDIS_URI") or "127.0.0.1:6379"
    host, port = uri.split(":")
    redis = Redis(host=host, port=port)
    return redis


class DataInsertTask(Task):
    def __execute__(self):
        # 此处实现数据每日自动插入redis
        app = CtpBee("task", __name__, refresh=True)
        app.config.from_json("data_update/login.json")

        redis = get_redis_connection()
        api = Data(rd=redis)

        app.add_extension(api)
        app.start()

    def auth_time(self, time: datetime) -> bool:
        return auth_time(time.time())
        # return True


class CleanDataTask(Task):
    def __execute__(self):
        """ 清洗数据 """
        # todo: 清洗数据
        redis = get_redis_connection()
        contracts = set([str(x, encoding="utf8") for x in redis.lrange(RD_CONTRACT_NAME, 0, -1)])
        logger.info("Get Contracts: len:{}".format(len(contracts)))
        for contract in contracts:
            ticks = []
            for x in redis.lrange(contract, 0, -1):
                logger.info("parse {}'s day_tick".format(contract))
                try:
                    tick = loads(str(x, encoding="utf8"))
                    if auth_time(tick.datetime.time()):
                        ticks.append(tick)
                    # todo: Insert Into clickhouse data
                    logger.info("update {} successful".format(contract))
                except Exception as e:
                    logger.error("find {} when update {}".format(e, contract))

    def auth_time(self, time: datetime) -> bool:
        if str(time.date()) in trade_dates and \
                time.hour == 15 and \
                time.minute == 30 and time.second == 0:
            return True
        return False


if __name__ == '__main__':
    dd = CleanDataTask(0, 1)
    dd.__execute__()
