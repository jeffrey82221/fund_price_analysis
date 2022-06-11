
class StatisticModelETL:
    @staticmethod
    def run(table, period=7):
        _table = table.copy()
        earning_rate_var = _table.earning_rate.rolling(7).var()
        earning_rate_mean = _table.earning_rate.rolling(7).mean()
        _table['earning_rate_var'] = earning_rate_var
        _table['earning_rate_mean'] = earning_rate_mean
        return _table[['earning_rate_var', 'earning_rate_mean']].dropna()