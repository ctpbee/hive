# Hive for [ctpbee](https://github.com/ctpbee/ctpbee)

此项目是为了`ctpbee`个人用户提供快速提供简单的任务调用, 同时编写了一个ctpbee的多策略交易终端, 可以在ctpbee[1.6+]修改mode直接连接进行交易。

# 安装

```bash
pip install https://github.com/ctpbee/hive
```

# 环境要求

- `redis数据库`
- `python3.6+`
- `ctpbee`
- `sqlite3`

# App

- ctpbee交易终端 `多线程模型`
    - [ ] 支持分发多策略交易终端
    - [x] 支持行情自动保存
    - [ ] 交易数据每日自动保存 
    - [ ] 轻量化图表分析 

```python3
# cd terminal
# python3 run.py
```

> 注意此项目暂时还未完成, 会配合`ctpbee1.6`进行发布. 


# Lincese
`Mit`

