"""
TODO:
- [ ] Do not update every date for ETL
    - [ ] EarningETL.run
    - [ ] StatisticModelETL.run
    - [ ] IndexETL.run
"""
import pandas as pd
from src.process.earning import EarningETL
from src.process.statistic import StatisticModelETL
from src.process.index import IndexETL
from src.process.selection import IndexSelectionETL
from src.path import INDEX_PATH
import os
class ProcessNavETL:
    def __init__(self, period=7):
        self.__period = period
        self.__period_path = f'{INDEX_PATH}/{self.__period}days'
        if not os.path.exists(self.__period_path):
            os.makedirs(self.__period_path)

    def run(self, nav_path):
        file_name = nav_path.split('/')[-1].split('.')[0]
        table = pd.read_hdf(nav_path, 'raw', auto_close=True)
        earning_table = EarningETL.run(table, period=self.__period)
        model_result_table = StatisticModelETL.run(earning_table, period=self.__period)
        index_table = IndexETL.run(model_result_table)
        index_table = IndexSelectionETL.run(index_table, earning_table, period=self.__period)
        if (index_table is not None) and isinstance(index_table, pd.DataFrame) and len(index_table) > 0:
            index_table.to_hdf(f'{self.__period_path}/{file_name}.h5',
                         'index', append=False, format='table',
                         data_columns=index_table.columns)
            return self._assert_result(file_name)
        else:
            return False

    def _assert_result(self, file_name):
        try:
            pd.read_hdf(f'{self.__period_path}/{file_name}.h5', 'index', auto_close=True)
            return True
        except:
            if os.path.exists(f'{self.__period_path}/{file_name}.h5'):
                os.remove(f'{self.__period_path}/{file_name}.h5')
            return False