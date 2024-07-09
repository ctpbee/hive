import pandas as pd
import redis
from ctpbee import CtpbeeApi, CtpBee, auth_time
from ctpbee import hickey
from ctpbee import Mode
from ctpbee.constant import *

from redis import Redis
from hive.src.obj import Message
from hive import logger
from hive.src.env import RD_CONTRACT_NAME


class WorkBench(CtpbeeApi):
    def __init__(self, name: str, rd: Redis, config, subscribe_contract="*"):
        super().__init__("data_update")
        self.name = name
        self.rd = rd
        self.config = config
        self.tick_save = config.get("tick_save", False)
        self.tick_dispatch = config.get("tick_dispatch", False)
        self.trade_dispatch = config.get("trade", False)
        self.subscribe_contract = subscribe_contract
        self.order_mapping = {}

    def on_tick(self, tick: TickData) -> None:
        if not auth_time(tick.datetime):
            return None
        tick_value = [tick.local_symbol, str(tick.datetime), tick.last_price, tick.volume, tick.turnover,
                      tick.open_interest,
                      tick.ask_price_5, tick.ask_volume_5,
                      tick.ask_price_4, tick.ask_volume_4,
                      tick.ask_price_3, tick.ask_volume_3,
                      tick.ask_price_2, tick.ask_volume_2,
                      tick.ask_price_1, tick.ask_volume_1,
                      tick.bid_price_1, tick.bid_volume_1,
                      tick.bid_price_2, tick.bid_volume_2,
                      tick.bid_price_3, tick.bid_volume_3,
                      tick.bid_price_4, tick.bid_volume_4,
                      tick.bid_price_5, tick.bid_volume_5
                      ]
        if self.tick_save:
            self.rd.rpush(tick.local_symbol, str(tick_value))
        if self.tick_dispatch:
            self.rd.publish(tick.local_symbol, str(tick_value))

    @property
    def name_info(self):
        return f"channel_{self.name}"

    def on_order(self, order: OrderData) -> None:
        if self.trade_dispatch:
            entity = Message(order)
            self.rd.publish(self.name_info, entity.encode())

    def on_trade(self, trade: TradeData) -> None:
        pass

    def on_contract(self, contract: ContractData) -> None:
        codes = [
            str(x, encoding="utf8") for x in self.rd.lrange(RD_CONTRACT_NAME, 0, -1)
        ]
        self.action.subscribe(contract.local_symbol)
        if self.subscribe_contract == "*":
            if contract.local_symbol not in codes:
                self.rd.rpush(RD_CONTRACT_NAME, contract.local_symbol)
            self.rd.publish(self.name_info, Message(contract).encode())
            self.info(f"subscribe: {contract.local_symbol}")
        elif contract.local_symbol in self.subscribe_contract:
            if contract.local_symbol not in codes:
                self.rd.rpush(RD_CONTRACT_NAME, contract.local_symbol)
            self.rd.publish(self.name_info, Message(contract).encode())
            self.info(f"subscribe: {contract.local_symbol}")


def listen_order(app: CtpBee, client: redis.Redis):
    sub = client.pubsub()
    sub.subscribe(f"ctpbee_{app.name}")
    for signal in sub.listen():
        recv_data = signal["data"]
        if recv_data == 1:
            continue
        recv_data = Message(recv_data)
        order_request = recv_data.decode()
        if type(order_request) == OrderRequest:
            app.action.send_order(order_request)
        elif type(order_request) == CancelRequest:
            app.action.cancel_order(order_request)
        else:
            pass


def record_data(config):
    front = 300
    for i, v in hickey.open_trading["ctp"].items():
        setattr(hickey, i, hickey.add_seconds(getattr(hickey, i), front))
    uri = config["redis"]
    host, port = uri.split(":")
    redis_client = Redis(host=host, port=port)
    if config.get("dispatch", False):
        app = CtpBee("task", __name__, refresh=True, work_mode=Mode.DISPATCHER)
    else:
        app = CtpBee("task", __name__, refresh=True)

    config_path = os.path.abspath(config["cf"])
    app.config.from_json(config_path)
    subscribe_contract = app.config.get("SUBSCRIBE_CONTRACT", "*")
    bench = WorkBench(rd=redis_client, name=config["name"], subscribe_contract=subscribe_contract, config=config)
    app.add_extension(bench)
    app.start()
    if config.get("trade_dispatch", False):
        listen_order(app, redis_client)


def clean_data_from_redis(config):
    uri = config.get("redis", "127.0.0.1:6379")
    host, port = uri.split(":")
    redis_client = Redis(host=host, port=port)
    contracts = set(
        [str(x, encoding="utf8")
         for x in redis_client.lrange(RD_CONTRACT_NAME, 0, -1)]
    )
    logger.info("获取当前:{}个合约数据".format(len(contracts)))
    index = 0
    dated = str(datetime.now().date())
    dir_path = os.path.join(os.path.abspath(config["path"]), dated)
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
    for contract in contracts:
        index += 1
        if config["tick_save"]:
            tick_array = redis_client.lrange(contract, 0, -1)
            filepath = os.path.join(dir_path, f"{contract}.{config['ff']}")
            columns = ["local_symbol", "datetime", "last_price", "volume", "turnover", "open_interest",
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
                       ]
            array = [eval(str(x, encoding="utf8")) for x in tick_array]
            tick_data = pd.DataFrame(array, columns=columns)
            if config["ff"] == "csv":
                tick_data.to_csv(filepath, index=False, encoding="utf8")
            elif config["ff"] == "parquet":
                tick_data.to_parquet(filepath, index=False)
            elif config["ff"] == "h5":
                tick_data.to_hdf(filepath, key="df", index=False)
            else:
                logger.info("error file format of data")
            del tick_array
            del tick_data
        redis_client.delete(contract)
    redis_client.delete(RD_CONTRACT_NAME)
    logger.info("当天数据清理完毕")

from datetime import datetime, time
from ctpbee.date import trade_dates
def auth_time(self, current: datetime):
    DAY_START = time(8, 55)  # 日盘启动和停止时间
    DAY_END = time(15, 5)
    NIGHT_START = time(20, 55)  # 夜盘启动和停止时间
    NIGHT_END = time(2, 35)
    current_string = str(current.date())
    last_day = str((current + timedelta(days=-1)).date())
    """
    如果前一天是交易日, 今天不是 那么交易到今晚晚上2点：30
    
    如果前一天不是交易日,今天是  那么早盘前 不启动 
    
    如果前一天不是交易日, 今天也不是交易日 那么不启动 
    """
    if (last_day in trade_dates and current_string not in trade_dates and current.time() > NIGHT_END) or \
            (last_day not in trade_dates and current_string in trade_dates and current.time() < DAY_START) or \
            (last_day not in trade_dates and current_string not in trade_dates):
        return False

    if DAY_END >= current.time() >= DAY_START:
        return True
    if current.time() >= NIGHT_START:
        return True
    if current.time() <= NIGHT_END:
        return True
    return False
