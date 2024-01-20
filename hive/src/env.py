# 合约写入名称
from datetime import time

# redis中保存的合约名称的key
RD_CONTRACT_NAME = "contract"

# 期货数据csv下载路径
FILE_SAVE_PATH = "/"

# 数据清洗时间 注意应该在15:05:00 之后 20:30:00之前最好
FILE_CLEAN_TIME = time(hour=15, minute=30, second=0)

DATA_TYPE_KEY = "data_type"
DATA_FIELD = "data"
