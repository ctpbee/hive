# Hive for [ctpbee](https://github.com/ctpbee/ctpbee)

此项目是为了`ctpbee`个人用户提供快速提供简单的任务调用, 同时编写了一个ctpbee的多策略交易终端, 可以在ctpbee[1.6+]
修改mode直接连接进行交易。

# 安装

```bash
pip install git+https://github.com/ctpbee/hive
```

# 环境要求

- `redis数据库`
- `python3.6+`
- `ctpbee`

# App

- `ctpbee`交易终端 `多进程模型`
    - [ ] 支持分发多策略交易终端
    - [x] 支持行情自动保存
    - [x] 支持行情自动分发
    - [ ] 交易数据每日自动保存
    - [ ] 轻量化图表分析

# 如何快速执行行情落地+交易分发

1. 在任意目录下面创建一个`data`文件夹,并创建`config.json`
2. `cd`到`data`的同级目录
3. 执行下面的命令(可以根据参数解析自己做更改)

```bash
hive --name hive --path ./data --ff csv --cf config.json --rd 127.0.0.1:6379 --tick_save true --tick_dispatch true --trade_dispatch true
```

### 参数解析

- `name`: 名字
- `path`: `tick`数据文件保存目录
- `ff`: 文件格式 支持 `csv/parquet/h5`等文件格式
- `cf`: 行情交易配置文件 `->json`格式
- `rd`: `redis`链接地址
- `tick_save`: 是否开启行情落地功能
- `trade_dispatch`: 是否开启交易分发功能
- `tick_dispatch`: 是否开启行情`tick`分发功能

> 注意此项目暂时还未完成, 会配合`ctpbee1.6`进行发布.

# License

`Apache2`