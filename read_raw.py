import pandas as pd
import time
import gc
def calculate_earning(nav_list):
    return (nav_list[-1] - nav_list[0]) / nav_list[0]

def apply_simple_model(table, period=7):
    period = 7
    earning_rate = table.nav.rolling(period).apply(calculate_earning, raw=True, engine='cython')
    table['earning_rate'] = earning_rate
    earning_rate_var = table.earning_rate.rolling(7).var()
    earning_rate_mean = table.earning_rate.rolling(7).mean()
    table['earning_rate_var'] = earning_rate_var
    table['earning_rate_mean'] = earning_rate_mean
    model_result_table = table[['earning_rate_var', 'earning_rate_mean']].dropna()
    del table
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
    reset_global()
    model_result_table['mean_of_var'] = model_result_table.earning_rate_var.rolling(1).apply(
        agg_mean, raw=True, engine='cython')
    reset_global()
    model_result_table['var_of_mean'] = model_result_table.earning_rate_mean.rolling(1).apply(
        agg_var, raw=True, engine='cython')

    model_result_table['earning_rate_std'] = (model_result_table['var_of_mean'] + model_result_table['mean_of_var']) ** 0.5
    model_result_table['sharp_ratio'] = model_result_table['earning_rate_mean'] / model_result_table['earning_rate_std']

    index_table = model_result_table[['earning_rate_mean', 'earning_rate_std', 'sharp_ratio']]
    return index_table

table = pd.read_hdf('data/nav/TW000T0101Y3.h5', 'raw', auto_close=True)
model_result_table = apply_simple_model(table)
print(model_result_table)
index_table = apply_index_calculation(model_result_table)
print(index_table)