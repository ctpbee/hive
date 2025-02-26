"""
订单回报与 实现回报 
"""
import json

from ctpbee.constant import OrderData, Entity, ContractData, TradeData, CancelRequest, OrderRequest, TickData

from hived.src.env import DATA_TYPE_KEY, DATA_FIELD
from ctpbee import dumps, loads


class Message(object):

    def __init__(self, body):
        self._data: Entity or str = body

    @property
    def data(self):
        return self._data

    def decode(self):
        if type(self._data) == str:
            data: dict = loads(self.data)
        elif type(self._data) == dict:
            data: dict = self.data
        else:
            raise ValueError(f"error type {type(self._data)}")
        return data[DATA_FIELD]

    def encode(self):
        return dumps({DATA_FIELD: self.data._to_dict()})
