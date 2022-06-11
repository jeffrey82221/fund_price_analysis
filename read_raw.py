import pandas as pd
import time
from datetime import timedelta
import gc
def calculate_earning(nav_list):
    return (nav_list[-1] - nav_list[0]) / nav_list[0]

def get_earning_rate(table, period=7):
    _table = table.copy()
    earning_rate = _table.nav.rolling(period).apply(calculate_earning, raw=True, engine='cython')
    table['earning_rate'] = earning_rate
    return table[['earning_rate']].dropna()

def apply_simple_model(table, period=7):
    _table = table.copy()
    earning_rate_var = _table.earning_rate.rolling(7).var()
    earning_rate_mean = _table.earning_rate.rolling(7).mean()
    _table['earning_rate_var'] = earning_rate_var
    _table['earning_rate_mean'] = earning_rate_mean
    model_result_table = _table[['earning_rate_var', 'earning_rate_mean']].dropna()
    del _table
    gc.collect()
    return model_result_table


_count = 0
_sum = 0.
_square_sum = 0.
def agg_var(x):
    global _sum
    global _count
    global _square_sum
    _count += 1
    _sum += x
    mean = _sum / _count 
    _square_sum += (x ** 2)
    var = (_square_sum - 2*_sum*mean + _count * (mean)**2)/_count
    return var

def agg_mean(x):
    global _sum
    global _count 
    _count += 1
    _sum += x
    mean = _sum / _count 
    return mean

def reset_global():
    global _count
    global _sum 
    global _square_sum
    _count = 0
    _sum = 0.
    _square_sum = 0.

def apply_index_calculation(model_result_table):
    _table = model_result_table.copy()
    reset_global()
    _table['mean_of_var'] = _table.earning_rate_var.rolling(1).apply(
        agg_mean, raw=True, engine='cython')
    reset_global()
    _table['var_of_mean'] = _table.earning_rate_mean.rolling(1).apply(
        agg_var, raw=True, engine='cython')

    _table['earning_rate_std'] = (_table['var_of_mean'] + _table['mean_of_var']) ** 0.5
    _table['sharp_ratio'] = _table['earning_rate_mean'] / _table['earning_rate_std']

    index_table = _table[['earning_rate_mean', 'earning_rate_std', 'sharp_ratio']]
    return index_table


def find_earliest_plausible_index_date(index_table, threshold = 0.95, hit_rate_window=100):
    _index_table = index_table.copy()
    _index_table['earning_rate_upper'] = _index_table['earning_rate_mean'] + _index_table['earning_rate_std']
    _index_table['earning_rate_lower'] = _index_table['earning_rate_mean'] - _index_table['earning_rate_std']
    # Index 是去看七天以後的狀況用的，因此將時間往後七天
    _index_table.index = _index_table.index.map(lambda x: x + timedelta(days=PERIOD))
    _index_table.reset_index(inplace=True)
    analysis_table = _index_table.merge(earning_table.reset_index(), how='inner', on='date').set_index('date')
    analysis_table['hit'] = (analysis_table['earning_rate'] <= analysis_table['earning_rate_upper']) & (analysis_table['earning_rate'] >= analysis_table['earning_rate_lower'])
    analysis_table['hit'] = analysis_table['hit'].map(int)
    hit_rate_table = analysis_table.rolling(hit_rate_window).mean().dropna()
    hit_rate_table['hit_rate'] = hit_rate_table['hit']
    hit_rate_table = hit_rate_table[['hit_rate']]
    if sum(hit_rate_table.hit_rate>=threshold) > 0:
        earliest_plausible_date = hit_rate_table[hit_rate_table.hit_rate>=threshold].index[0] + timedelta(days=PERIOD)
        print('Earliest Plausible Index Date:', earliest_plausible_date)
        return earliest_plausible_date
    else:
        return None

PERIOD = 7
# TW000T0101Y3: 兆豐寶鑽貨幣市場證券投資信託
# TW000T2507Y9: 永豐趨勢平衡基金 -> 0
# TW000T0502Y2: 元大多福基金
table = pd.read_hdf('data/nav/TW000T2507Y9.h5', 'raw', auto_close=True)
earning_table = get_earning_rate(table, period=PERIOD)
model_result_table = apply_simple_model(earning_table, period=PERIOD)
index_table = apply_index_calculation(model_result_table)
# BackTesting:
earliest_plausible_date = find_earliest_plausible_index_date(index_table)
if earliest_plausible_date:
    index_table = index_table[index_table.index >= earliest_plausible_date]
    print(index_table)
else:
    print('No plausible Sharp / Std Index')