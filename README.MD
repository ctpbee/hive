# Hive for [ctpbee](https://github.com/ctpbee/ctpbee)

此项目提供一个简单有效的的期货交易数据分发的应用, 同时提供了行情数据自动落地文件功能.

# 安装

```bash
pip install hived-c
# or 
pip install git+https://github.com/ctpbee/hive
```

# 环境要求

- `redis数据库`
- `python3.6+`
- `ctpbee`

# App

- `ctpbee`交易终端 `多进程模型`
    - [x] 支持分发多策略交易终端
    - [x] 支持行情自动保存
    - [x] 支持行情自动分发
    - [ ] 交易数据每日自动保存
    - [ ] 轻量化图表分析

# 如何快速执行行情落地+交易分发

1. 在任意目录下面创建一个`data`文件夹,并创建`config.json`
2. `cd`到`data`的同级目录
3. 执行下面的命令(可以根据参数解析自己做更改)

```bash
hived --name hived --path ./data --ff csv --cf config.json --rd 127.0.0.1:6379 --tick_save true --dispatch true
```

### 如果自己定义订阅指定行情

`examples`目录下提供了2个`config`文件, `config.json`订阅了全部, `config_1.json`则是订阅了"rb2405.SHFE", 请参阅配置文件以自定义

### 参数解析

- `name`: 名字
- `path`: `tick`数据文件保存目录
- `ff`: 文件格式 支持 `csv/parquet/h5`等文件格式
- `cf`: 行情交易配置文件 `->json`格式
- `rd`: `redis`链接地址
- `tick_save`: 是否开启行情落地功能
- `trade_dispatch`: 是否开启行情/交易分发功能

### 行情读取

`examples`下面提供了一个`read.py`请参阅此API 自己读取历史数据即可

# License

`Apache2`