import pandas as pd
from src.process.earning import EarningETL
from src.process.statistic import StatisticModelETL
from src.process.index import IndexETL
from src.process.selection import IndexSelectionETL

PERIOD = 7
# TW000T0101Y3: 兆豐寶鑽貨幣市場證券投資信託
# TW000T2507Y9: 永豐趨勢平衡基金 -> 0
# TW000T0502Y2: 元大多福基金
table = pd.read_hdf('data/nav/TW000T0502Y2.h5', 'raw', auto_close=True)
earning_table = EarningETL.run(table, period=PERIOD)
model_result_table = StatisticModelETL.run(earning_table, period=PERIOD)
index_table = IndexETL.run(model_result_table)
# BackTesting:
index_table = IndexSelectionETL.run(index_table, earning_table, period=PERIOD)
print(index_table)
