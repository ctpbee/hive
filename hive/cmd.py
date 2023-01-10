"""
Provide easy and fast boot command
"""
from hive.main import Hive
import click
from hive.src.task import DataUpdateTask, CleanDataTask


@click.command()
@click.option("--name", default="hive", prompt="hive")
@click.option("--path", help="the dirname of file_save_path", default="./")
@click.option("--ff", help="format of file", default="csv")
@click.option("--cf", help="config of file", default="config.json")
@click.option("--mode", default="market",
              help="work mode, like market(only market support), trade(only trading support), rr(for both of them)",
              )
def run_command(name, path, ff, cf, mode):
    config_file = {"name": name, "output_path": path, "ff": ff, "cf": cf, "mode": mode}

    insert = DataUpdateTask()
    clean = CleanDataTask()

    hive = Hive()
    hive.config.from_mapping(config_file)
    hive.insert(clean)
    hive.run()
