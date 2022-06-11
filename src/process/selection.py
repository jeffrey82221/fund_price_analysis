from datetime import timedelta


class _StartDateETL:
    """
    Find earliest plausible index date
    """
    @staticmethod
    def run(index_table, earning_table, period=7, threshold = 0.95, hit_rate_window=100):
        _index_table = index_table.copy()
        # 計算上下界
        _index_table['earning_rate_upper'] = _index_table['earning_rate_mean'] + _index_table['earning_rate_std']
        _index_table['earning_rate_lower'] = _index_table['earning_rate_mean'] - _index_table['earning_rate_std']
        # Index 是去看七天以後的狀況用的，因此將時間往後七天
        _index_table.index = _index_table.index.map(lambda x: x + timedelta(days=period))
        _index_table.reset_index(inplace=True)
        # 合併起來看hit與否
        analysis_table = _index_table.merge(earning_table.reset_index(), how='inner', on='date').set_index('date')
        analysis_table['hit'] = (analysis_table['earning_rate'] <= analysis_table['earning_rate_upper']) & (analysis_table['earning_rate'] >= analysis_table['earning_rate_lower'])
        analysis_table['hit'] = analysis_table['hit'].map(int)
        # hit rate由間隔時間來計算
        hit_rate_table = analysis_table.rolling(hit_rate_window).mean().dropna()
        hit_rate_table['hit_rate'] = hit_rate_table['hit']
        hit_rate_table = hit_rate_table[['hit_rate']]
        # 拉取hit rate > threshold的最早時間
        if sum(hit_rate_table.hit_rate >= threshold) > 0:
            earliest_plausible_date = hit_rate_table[hit_rate_table.hit_rate>=threshold].index[0] + timedelta(days=period)
            return earliest_plausible_date
        else:
            return None

class IndexSelectionETL:
    def run(index_table, earning_table, period=7):
        earliest_plausible_date = _StartDateETL.run(index_table, earning_table, period=period)
        if earliest_plausible_date:
            index_table = index_table[index_table.index >= earliest_plausible_date]
            return index_table
        else:
            return None