"""
Provide easy and fast boot command
"""
import json
import os.path

import click

from hive.log import logger
from hive.main import Hive
from hive.src.task import DataUpdateTask, CleanDataTask

bool_map = {"false": False, "true": True}


def fix_bool(name):
    if name not in ["false", "true"]:
        raise TypeError(f"error type of {name.__name__}")
    else:
        return bool_map[name]


@click.command()
@click.option("--name", default="hive", prompt="hive")
@click.option("--path", help="the dirname of file_save_path", default="./")
@click.option("--ff", help="format of file", default="csv")
@click.option("--cf", help="config of file", default="config.json")
@click.option("--rd", help="uri of redis", default="127.0.0.1:6379")
@click.option("--dispatch", default="false", help="tick/order dispatch")
@click.option("--tick_save", default="true", help="save tick data or not")
@click.option("--config_path", default="", help="config file path ")
def run_command(name, path, ff, cf, rd, dispatch, tick_save, config_path):
    if os.path.exists(config_path):
        if not config_path.endswith(".json"):
            raise FileNotFoundError("当前配置文件不是json格式")
        with open(config_path, "r") as f:
            config = json.load(f)
    else:
        if ff not in ["csv", "parquet", "h5"]:
            raise TypeError("错误的文件导出格式 请检查你的ff参数")
        dispatch = fix_bool(dispatch)
        tick_save = fix_bool(tick_save)
        config = {"name": name,
                  "path": path,
                  "ff": ff,
                  "cf": cf,
                  "redis": rd,
                  "dispatch": dispatch,
                  "tick_save": tick_save,
                  }
    logger.info(f"read config: {config}")
    insert = DataUpdateTask(config)
    clean = CleanDataTask(config)
    hive = Hive()
    hive.config.from_mapping(config)
    hive.insert(insert, clean)
    hive.run()
