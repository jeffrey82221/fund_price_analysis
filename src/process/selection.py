from datetime import timedelta
import abc

class _StartDateBase:
    """
    Find earliest plausible index date based on certain boundary & Hit rate condition
    """
    def __init__(self, period=7, threshold=0.95, std_count=2, hit_rate_window=100):
        self._period = period
        self._threshold = threshold
        self._hit_rate_window = hit_rate_window
        self.std_cnt = std_count

    @abc.abstractmethod
    def add_boundary(self, _index_table):
        """
        Add Boundary Columns to Index Table
        Example: 
        _index_table['earning_rate_upper'] = _index_table['earning_rate_mean'] + _index_table['earning_rate_std']
        _index_table['earning_rate_lower'] = _index_table['earning_rate_mean'] - _index_table['earning_rate_std']
        """
        pass

    @abc.abstractmethod
    def hit(self, analysis_table):
        """
        Generate Hit Column to analysis_table based on boundary condition.
        Example:
            hit_col = (analysis_table['earning_rate'] <= analysis_table['earning_rate_upper']) & (analysis_table['earning_rate'] >= analysis_table['earning_rate_lower'])
            return hit_col
        """
        pass
    
    def get(self, index_table, earning_table):
        _index_table = index_table.copy()
        # 計算boundary
        self.add_boundary(_index_table)
        hit_rate_table = self._calculate_hit_rate(_index_table, earning_table)
        # 拉取hit rate > threshold的最早時間
        if sum(hit_rate_table.hit_rate >= self._threshold) > 0:
            earliest_plausible_date = hit_rate_table[hit_rate_table.hit_rate>=self._threshold].index[0] + timedelta(days=self._period)
            return earliest_plausible_date
        else:
            return None

    def _calculate_hit_rate(self, _index_table, earning_table):
        # Index 是去看七天以後的狀況用的，因此將時間往後七天
        _index_table.index = _index_table.index.map(lambda x: x + timedelta(days=self._period))
        _index_table.reset_index(inplace=True)
        # 合併起來看hit與否
        analysis_table = _index_table.merge(earning_table.reset_index(), how='inner', on='date').set_index('date')
        analysis_table['hit'] = self.hit(analysis_table)
        analysis_table['hit'] = analysis_table['hit'].map(int)
        # hit rate由間隔時間來計算
        hit_rate_table = analysis_table
        hit_rate_table['hit_rate'] = analysis_table.hit.rolling(self._hit_rate_window).mean().dropna()
        return hit_rate_table

class StdStartDate(_StartDateBase):
    def add_boundary(self, _index_table):
        """
        Add Boundary Columns to Index Table
        """
        _index_table['earning_rate_upper'] = _index_table['earning_rate_mean'] + (self.std_cnt * _index_table['earning_rate_std'])
        _index_table['earning_rate_lower'] = _index_table['earning_rate_mean'] - (self.std_cnt * _index_table['earning_rate_std'])
        
    def hit(self, analysis_table):
        """
        Generate Hit Column to analysis_table based on boundary condition.
        """
        hit_col = (
            analysis_table['earning_rate'] <= analysis_table['earning_rate_upper']
        ) & (
            analysis_table['earning_rate'] >= analysis_table['earning_rate_lower']
        )
        return hit_col

class NegStdStartDate(_StartDateBase):
    def add_boundary(self, _index_table):
        """
        Add Boundary Columns to Index Table
        """
        _index_table['earning_rate_lower'] = _index_table['earning_rate_mean'] - (self.std_cnt * _index_table['earning_rate_neg_std'])
        
    def hit(self, analysis_table):
        """
        Generate Hit Column to analysis_table based on boundary condition.
        """
        hit_col = (
            analysis_table['earning_rate'] >= analysis_table['earning_rate_lower']
        )
        return hit_col

class IndexSelectionETL:
    def run(index_table, earning_table, period=7):
        std_start_date = StdStartDate(period=period, threshold=0.95, std_count=2, hit_rate_window=100)
        neg_std_start_date = NegStdStartDate(period=period, threshold=0.95, std_count=2, hit_rate_window=100)
        boundary_dates = [std_start_date.get(index_table, earning_table), neg_std_start_date.get(index_table, earning_table)]
        if any(map(lambda x: x is None, boundary_dates)):
            return None
        else:
            index_table = index_table[index_table.index >= max(boundary_dates)]
            return index_table