import os
import threading

import pandas as pd
from data_api import DataApi, Tick

root_dir = r"\\192.168.2.173\\Data Futures\\Tick FutData/"

code_map = {
    "SC": {"code": "sc", "exchange": "INE"},
    "BU": {"code": "bu", "exchange": "SHFE"},
    "AL": {"code": "al", "exchange": "SHFE"},
    "AU": {"code": "au", "exchange": "SHFE"},
    "CU": {"code": "cu", "exchange": "SHFE"},
    "HC": {"code": "hc", "exchange": "SHFE"},
    "NI": {"code": "ni", "exchange": "SHFE"},
    "EB": {"code": "eb", "exchange": "DCE"},
    "EG": {"code": "eg", "exchange": "DCE"},
    "JD": {"code": "jd", "exchange": "DCE"},
    "LH": {"code": "lh", "exchange": "DCE"},
    "PG": {"code": "pg", "exchange": "DCE"},
    "ZN": {"code": "zn", "exchange": "SHFE"},
    "LU": {"code": "lu", "exchange": "INE"},
    "FU": {"code": "fu", "exchange": "SHFE"},
    "SS": {"code": "ss", "exchange": "SHFE"},
    "RB": {"code": "rb", "exchange": "SHFE"},
    "SN": {"code": "sn", "exchange": "SHFE"},
    "RU": {"code": "ru", "exchange": "SHFE"},
    "WR": {"code": "wr", "exchange": "SHFE"},
    "AG": {"code": "ag", "exchange": "SHFE"},
    "PB": {"code": "pb", "exchange": "SHFE"},
    "IH": {"code": "IH", "exchange": "CFFEX"},
    "IF": {"code": "IF", "exchange": "CFFEX"},
    "A": {"code": "a", "exchange": "DCE"},
    "B": {"code": "b", "exchange": "DCE"},
    "BB": {"code": "bb", "exchange": "DCE"},
    "C": {"code": "c", "exchange": "DCE"},
    "CS": {"code": "cs", "exchange": "DCE"},
    "FB": {"code": "fb", "exchange": "DCE"},
    "I": {"code": "i", "exchange": "DCE"},
    "J": {"code": "j", "exchange": "DCE"},
    "JM": {"code": "jm", "exchange": "DCE"},
    "L": {"code": "l", "exchange": "DCE"},
    "M": {"code": "m", "exchange": "DCE"},
    "P": {"code": "p", "exchange": "DCE"},
    "PP": {"code": "pp", "exchange": "DCE"},
    "RR": {"code": "rr", "exchange": "DCE"},
    "V": {"code": "v", "exchange": "DCE"},
    "Y": {"code": "y", "exchange": "DCE"},
    "CF": {"code": "CF", "exchange": "CZCE"},
    "CJ": {"code": "CJ", "exchange": "CZCE"},
    "CY": {"code": "CY", "exchange": "CZCE"},
    "FG": {"code": "FG", "exchange": "CZCE"},
    "JR": {"code": "JR", "exchange": "CZCE"},
    "LR": {"code": "LR", "exchange": "CZCE"},
    "MA": {"code": "MA", "exchange": "CZCE"},
    "OI": {"code": "OI", "exchange": "CZCE"},
    "PF": {"code": "PF", "exchange": "CZCE"},
    "PM": {"code": "PM", "exchange": "CZCE"},
    "RI": {"code": "RI", "exchange": "CZCE"},
    "RM": {"code": "RM", "exchange": "CZCE"},
    "RS": {"code": "RS", "exchange": "CZCE"},
    "SA": {"code": "SA", "exchange": "CZCE"},
    "SF": {"code": "SF", "exchange": "CZCE"},
    "SM": {"code": "SM", "exchange": "CZCE"},
    "SR": {"code": "SR", "exchange": "CZCE"},
    "TA": {"code": "TA", "exchange": "CZCE"},
    "UR": {"code": "UR", "exchange": "CZCE"},
    "WH": {"code": "WH", "exchange": "CZCE"},
    "NR": {"code": "nr", "exchange": "INE"},
    "BC": {"code": "bc", "exchange": "INE"},
    "SP": {"code": "sp", "exchange": "SHFE"},
    "AP": {"code": "AP", "exchange": "CZCE"},
    "ZC": {"code": "ZC", "exchange": "CZCE"},
    "PK": {"code": "PK", "exchange": "CZCE"},
    "T": {"code": "T", "exchange": "CFFEX"},
    "TF": {"code": "TF", "exchange": "CFFEX"},
    "TS": {"code": "TS", "exchange": "CFFEX"},
    "IC": {"code": "IC", "exchange": "CFFEX"},
}

data_api = DataApi(uri="http://192.168.1.239:8124/")
result = []

lock = threading.Lock()
last_date = 0


def insert_tick(params):
    global result
    global last_date

    path, dt_local_symbol, f_date = params
    file_data: pd.DataFrame = pd.read_csv(path).fillna(0)
    file_data.rename(columns={"turnover": "amount"}, inplace=True)
    vol = file_data.volume[0]
    file_data["volume"] = file_data["volume"] - file_data["volume"].shift(1)
    file_data.loc[0, ["volume"]] = [vol]
    file_data["local_symbol"] = dt_local_symbol
    file_data.drop(
        [
            "millisecond",
            "open_price",
            "settlement_price",
            "pre_close",
            "pre_open_interest",
            "pre_settlement_price",
        ],
        axis=1,
        inplace=True,
    )
    file_data["datetime"] = pd.to_datetime(
        file_data["datetime"].astype("str"), format="%Y%m%d%H%M%S"
    ).apply(lambda x: int(x.timestamp()))
    data = file_data.to_dict(orient="index").values()

    # 上面使用多线程保证io快速读取处理
    # 下面准备写入全局变量或者写入数据库时候加锁保证唯一性
    if lock.acquire():
        try:
            for i in data:
                # print(i["datetime"].to_pydatetime(), type(i["datetime"]))
                result.append(Tick(**i))

            print(f_date, dt_local_symbol, "--->", len(result))
            if f_date != last_date:
                print("插入TICK 更新result")
                data_api.insert_ticks(result)
                result = []
                last_date = max(f_date, last_date)
        finally:
            lock.release()
    del file_data


file_path = os.listdir(root_dir)
if __name__ == "__main__":
    from concurrent import futures

    pool = futures.ThreadPoolExecutor(max_workers=10)
    args = []
    for date in file_path:
        level2_path = os.path.join(root_dir, date)
        if not date.startswith("2") or len(date) > 8 or date.startswith("2020"):
            continue

        for code_csv in os.listdir(level2_path):
            csv_path = os.path.join(level2_path, code_csv)
            code = code_csv.split(".")[0]
            symbol = "".join(filter(str.isalpha, code))
            code_date = "".join(filter(str.isdigit, code))
            local_symbol = (
                f"{code_map[symbol]['code']}{code_date}.{code_map[symbol]['exchange']}"
            )
            args.append((csv_path, local_symbol, int(date)))

    tasks = pool.map(insert_tick, args)
    futures.wait(tasks)
    pool.shutdown()
