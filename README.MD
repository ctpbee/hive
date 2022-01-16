# Hive for [ctpbee](https://github.com/ctpbee/ctpbee)

此项目是为了`ctpbee`个人用户提供快速提供标准数据API支持, 提供每日自动维护,清洗,策略运维,邮件告警等服务。 同时结合`数据访问API`和`ctpbee`回测功能(速度慢, 矩阵回测请自行探索), 提供快速的策略回测功能

`data_update` 提供了基础的数据运维功能
`data_import` 提供了各家的数据导入功能

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
hive -f config.json
```

# 功能支持

- [x] 每日自动调度任务
- [ ] 支持热度更新任务
- [ ] 每日邮件执行任务通知
