"""
订单回报与 实现回报 
"""
import json

from ctpbee.constant import OrderData, Entity, ContractData, TradeData, CancelRequest, OrderRequest, TickData

from hive.src.env import DATA_TYPE_KEY, DATA_FIELD

MAPPING = {
    "contract": ContractData,
    "order_data": OrderData,
    "trade_data": TradeData,
    "order_request": OrderRequest,
    "cancel_request": CancelRequest,
    "tick_data": TickData,
}

REV_MAPPING = {v: i for i, v in MAPPING.items()}


class Message(object):

    def __init__(self, body):
        self._data: Entity or str = body

    @property
    def data(self):
        return self._data

    def decode(self):
        if type(self._data) == str:
            data: dict = json.loads(self.data)
        elif type(self._data) == dict:
            data: dict = self.data
        else:
            raise ValueError(f"error type {type(self._data)}")
        c = data[DATA_TYPE_KEY]
        return MAPPING[c](**data[DATA_FIELD])

    def encode(self):
        return json.dumps({DATA_TYPE_KEY: REV_MAPPING[type(self.data)], DATA_FIELD: self.data._to_dict()})
