
class _MeanAggregator:
    def __init__(self):
        self._count = 0
        self._sum = 0.
    
    def run(self, x):
        self._count += 1
        self._sum += x
        mean = self._sum / self._count 
        return mean

class _VarianceAggregator:
    def __init__(self):
        self._count = 0
        self._sum = 0.
        self._square_sum = 0.
    def run(self, x):
        self._count += 1
        self._sum += x
        mean = self._sum / self._count 
        self._square_sum += (x ** 2)
        var = (self._square_sum - 2*self._sum*mean + self._count * (mean)**2)/self._count
        return var

class IndexETL:
    @staticmethod
    def run(table):
        _table = table.copy()
        mean_agg = _MeanAggregator()
        _table['mean_of_var'] = _table.earning_rate_var.rolling(1).apply(
            mean_agg.run, raw=True, engine='cython')
        var_agg = _VarianceAggregator()
        _table['var_of_mean'] = _table.earning_rate_mean.rolling(1).apply(
            var_agg.run, raw=True, engine='cython')
        _table['earning_rate_std'] = (_table['var_of_mean'] + _table['mean_of_var']) ** 0.5
        _table['sharp_ratio'] = _table['earning_rate_mean'] / _table['earning_rate_std']
        index_table = _table[['earning_rate_mean', 'earning_rate_std', 'sharp_ratio']]
        return index_table