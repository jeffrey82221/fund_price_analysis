
class StatisticModelETL:
    @staticmethod
    def run(table, period=7):
        _table = table.copy()
        earning_rate_var = _table.earning_rate.rolling(period).var()
        earning_rate_mean = _table.earning_rate.rolling(period).mean()
        earning_rate_neg_var = table.earning_rate.rolling(period).apply(
            StatisticModelETL._calculate_neg_var,
            raw=True, engine='cython')
        _table['earning_rate_var'] = earning_rate_var
        _table['earning_rate_mean'] = earning_rate_mean
        _table['earning_rate_neg_var'] = earning_rate_neg_var
        return _table[['earning_rate_var', 'earning_rate_mean', 'earning_rate_neg_var']].dropna()

    def _calculate_neg_var(earning_rate_list):
        neg_sum = 0.
        neg_count = 0
        indices = []
        for i, earning_rate in enumerate(earning_rate_list):
            if earning_rate < 0:
                neg_count += 1
                neg_sum += earning_rate
                indices.append(i)
        if neg_count == 0:
            return 0.
        else:
            neg_mean = neg_sum / neg_count
            sqr_sum = 0.
            for i in indices:
                sqr_sum += (earning_rate_list[i] - neg_mean) ** 2
            neg_var = sqr_sum / neg_count
            return neg_var