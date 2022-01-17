"""
Provide easy and fast boot command
"""
import argparse

from hive.main import Hive

parser = argparse.ArgumentParser(description="hive command~")
"""
-f 指定参数文件 
-l 设定logger级别 
-c 生成默认配置文件到指定位置
"""
parser.add_argument("-f", "-l", "-c", help="for linux usage")


def execute():
    args = parser.parse_args()

    hive = Hive()
    for arg in args:
        if arg == "f":
            """读取文件配置信息"""
            hive.read_config_from_json(json_path=arg)
        if arg == "l":
            """设置Logger等级"""
            # todo
    hive.init_from_config()
    hive.run()
