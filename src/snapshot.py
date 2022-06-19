"""
SnapshotETL

Collect index columns of the same date for every funds and 
save them into a h5 table at data/snapshot
"""
from src.path import index_path_generator
import pandas as pd
import concurrent.futures
from src.utils import batch_generator
from src.path import SNAPSHOT_PATH
import tqdm
import os
import traceback
from src.process.index import IndexETL

VERBOSE = True

class SnapshotETL:
    def __init__(self, date, 
        period=7, batch_size=100, id=0):
        self._success_read_count = 0
        self._success_save_count = 0
        self._date = date
        self._cols = IndexETL.result_columns
        self._period = period # match to the {_period}days folder
        self._batch_size = batch_size
        self._id = id

    def run(self):
        paths = list(index_path_generator(period=self._period))
        path_batch_gen = batch_generator(paths, batch_size=self._batch_size)
        self._drop_h5()
        for batch in tqdm.tqdm(
                path_batch_gen, 
                desc=str(self._date), 
                position=self._id,
                total=int(len(paths)/self._batch_size)):
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                table = pd.concat(
                    list(filter(lambda x: x is not None, executor.map(
                        self._get_row, batch))), 
                    axis=0
                )
            self._save_h5(table)
        if VERBOSE:
            print('read count:', self._success_read_count, 
                'save count:', self._success_save_count)

    def _get_row(self, path):
        try:
            table = pd.read_hdf(path, where=f'index="{self._date}"', columns=self._cols)
            if len(table.columns) == len(self._cols):
                cols = table.columns
                table['ISIN'] = path.split('/')[-1].split('.h5')[0]
                table = table.reset_index(drop=True).set_index('ISIN')
                self._success_read_count += 1
                return table
            else:
                # Not such date in the index table of the fund
                return None
        except BaseException:
            # Error happended during accessing the index h5 of the fund
            print(f'Error happend on {path}')
            print(traceback.format_exc())
            return None

    def _drop_h5(self):
        period_path = os.path.join(SNAPSHOT_PATH, f'{self._period}days')
        if not os.path.exists(period_path):
            os.remove(period_path)

    def _save_h5(self, table):
        period_path = os.path.join(SNAPSHOT_PATH, f'{self._period}days')
        if not os.path.exists(period_path):
            os.makedirs(period_path)
        table.to_hdf(os.path.join(period_path, self._date+'.h5'),
                    'index', append=True, format='table',
                    data_columns=table.columns)
        self._success_save_count += 1
