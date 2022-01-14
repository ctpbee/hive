from datetime import datetime

from ctpbee import CtpbeeApi, CtpBee
from ctpbee.constant import TickData, ContractData
from redis import Redis

from hive import Task


class Data(CtpbeeApi):
    def __init__(self, rd: Redis):
        super().__init__("app")
        self.rd = rd

    def on_tick(self, tick: TickData) -> None:
        from ctpbee.jsond import dumps
        self.rd.rpush(tick.local_symbol, dumps(tick))

    def on_contract(self, contract: ContractData) -> None:
        self.action.subscribe(contract.local_symbol)


class DataInsertTask(Task):
    def __execute__(self):
        # 此处实现数据每日自动插入redis
        app = CtpBee("ok", __name__, refresh=True)
        app.config.from_json("data_update/login.json")
        # app.config.from_json("login.json")

        redis = Redis()
        api = Data(rd=redis)

        app.add_extension(api)
        app.start()

    def auth_time(self, time: datetime) -> bool:
        # return auth_time(time.time())
        return True


if __name__ == '__main__':
    task = DataInsertTask(1, 1, 1)
    task.__run__()
