class EarningETL:
    @staticmethod
    def run(table, period=7):
        _table = table.copy()
        earning_rate = _table.nav.rolling(period).apply(EarningETL._calculate_earning, raw=True, engine='cython')
        table['earning_rate'] = earning_rate
        return table[['earning_rate']].dropna()
    
    @staticmethod
    def _calculate_earning(nav_list):
        return (nav_list[-1] - nav_list[0]) / nav_list[0]