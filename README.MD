# Hive for [ctpbee](https://github.com/ctpbee/ctpbee)

此项目是为了`ctpbee`个人用户提供快速提供标准数据API支持, 提供每日数据自动维护,清洗,邮件告警等服务。

# 安装

```bash
pip install https://github.com/ctpbee/hive
```

# 环境要求
- `redis数据库`
- `python3.6+`
- `ctpbee`

# App

- 期货数据每日自动修改csv下载
    - 需要redis数据库 注意你需要修改ENV下面的文件保存地址 
    - 如果需要稳定的数据每日下载 推荐替换login.json下面交易信息为实盘账号以及地址 
    - 如果想编写自己的任务,请详细参阅源码。 


```python3
# cd csv_update_examples
# python3 core.py
```

# 功能支持
- [x] 每日自动调度任务
- [ ] 数据sdk支持
- [ ] 邮件提醒

# Todo
替换底层为asyncio, 用以支持阻塞性io. 


