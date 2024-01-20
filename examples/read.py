import os
import pandas as pd
import redis

data_path = "/data/data_csv"
dir_list = sorted(os.listdir(data_path))
redis_client = redis.Redis(decode_responses=True)


def read_from_redis(code):
    return pd.DataFrame([eval(x) for x in redis_client.lrange(f"{code}", 0, -1)],
                        columns=["local_symbol", "datetime", "last_price", "volume", "turnover", "open_interest",
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


def process_frame(frame):
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    frame["date"] = frame["datetime"].dt.date.astype(str)
    frame["time"] = frame["datetime"].dt.time.astype(str)
    frame.drop(frame[(frame.time > "08:00:00") & (frame.time < "08:59:59")].index, inplace=True)
    frame.drop(frame[(frame.time > "20:00:00") & (frame.time < "20:59:59")].index, inplace=True)
    frame.drop_duplicates(subset="datetime", inplace=True)
    return frame


def generate_files_path(local_symbol: str, exchange: str, start: str, end: str, slice: int):
    """
    生成数据文件路径
    local_symbol:指定代码名草
    exchange: 交易所代码
    start: 起始日期
    end: 结束日期
    slice: 切片ID
    """
    if "." not in local_symbol:
        local_symbol = f"{local_symbol}.{exchange}"
    if slice is None:
        list_files = [x for x in dir_list if start <= x <= end]
    else:
        list_files = dir_list[slice:]
    return [os.path.join(data_path, x, f"{local_symbol}.csv") for x in list_files]


# In[18]:


def read(local_symbol: str, exchange="SHFE", start: str = "", end: str = "", slice=None, if_current: bool = True):
    """
    读取单个指定合约的数据
    """
    files = generate_files_path(local_symbol, exchange, start, end, slice)
    frame = [process_frame(pd.read_csv(x)) for x in files]
    if if_current:
        frame.append(process_frame(read_from_redis(local_symbol)))
    return pd.concat(frame, ignore_index=True)
