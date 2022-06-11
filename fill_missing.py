import pandas as pd
from datetime import datetime
import os
import tqdm
import traceback
import ray
from ray.util import ActorPool
import concurrent.futures

class CleanNavETL:
    @staticmethod
    def run(path):
        try:
            file_name = path.split('/')[-1].split('.')[0]
            table = pd.read_hdf(path, 'nav', auto_close=True)
            table = CleanNavETL._fill_nav(table)
            table.to_hdf(f'data/nav/{file_name}.h5',
                         'raw', append=False, format='table',
                         data_columns=table.columns)
        except:
            print(f"unable to handle: {path}")
            print(traceback.format_exc())

    @staticmethod
    def _fill_nav(table):
        table['date'] = table.date.map(lambda x: datetime.strptime(x, '%Y/%m/%d'))
        idx = pd.date_range(
                start=table.date.min(),
                end=table.date.max(),
                freq="D")
        everyday_table = pd.DataFrame(idx, columns=['date'])
        table = everyday_table.merge(table, how='left', on='date')
        table.set_index('date', inplace=True)
        table.interpolate(inplace=True)
        table = table.reset_index().drop_duplicates(
                subset='date').set_index('date')
        return table

def path_generator(nav_dir):
    for company_path in os.listdir(nav_dir):
        if company_path != 'README.md':
            for table_path in os.listdir(os.path.join(nav_dir, company_path)):
                if '.h5' in table_path:
                    yield os.path.join(nav_dir, company_path, table_path)

paths = list(path_generator('../efficient_selenium/data/nav'))
with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    _ = list(tqdm.tqdm(executor.map(CleanNavETL.run, paths), total=len(paths)))
