# Hive for [ctpbee](https://github.com/ctpbee/ctpbee)

此项目是为了`ctpbee`个人用户提供快速提供标准数据API支持, 提供每日自动维护,清洗,策略运维,邮件告警等服务。 同时结合`数据访问API`和`ctpbee`回测功能(速度慢, 矩阵回测请自行探索), 提供快速的策略回测功能

`local_update` 提供了基础的数据运维功能, 包含简单的每日自动清洗以及录制数据 

`data_import` 暂不支持 

# 安装

```bash
pip install https://github.com/ctpbee/hive
```

# 环境要求
- `redis数据库`
- `python3.6+`
- `ctpbee`

# 快速启动 
> 需要redis数据库
```
# cd local_import
# python3 core.py
```

# 功能支持

- [x] 每日自动调度任务
- [ ] 支持热度更新任务
- [ ] 每日邮件执行任务通知
