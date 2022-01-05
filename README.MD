# Hive for [ctpbee](https://github.com/ctpbee/ctpbee)

> 基于ctpbee提供自动维护, 数据清洗功能 也可以当作策略进程管控器使用

# 安装

```bash
pip3 install hive
```

# 环境要求

- `clickhouse数据库`
- `redis数据库`
- `python3.6+`
- `ctpbee`

# 快速启动

```
hive -f config.json -l info
```

# 功能支持

-[ ] 支持热度更新任务
-[ ] 每日自动调度任务
-[ ] 每日邮件执行任务通知