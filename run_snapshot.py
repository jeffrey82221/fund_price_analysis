"""
TODO:
Refactor: 
- [ ] Build a ETL class for snapshot / fill_missing / index_calculation 
    - [X] SnapshotETL
    - [ ] FillMissingETL
    - [ ] IndexCalculationETL
- [ ] Move snapshot / fill_missing / index_calculation to an etl/ folder
- [ ] Use a run outside of src
- [ ] Build dynamic visualization using snapshot & ray.remote

Feature:
- [ ] Make sure don't save duplicate rows (e.g., same ISIN) 
    - [ ] check if the date exist on the result h5 & ignore the ISIN
"""
import concurrent.futures
from src.snapshot import SnapshotETL
import time
from datetime import datetime, timedelta
from src.path import nav_path_gen
import pandas as pd
import tqdm
import pickle
from multiprocessing import freeze_support, Manager
from src.path import OLDEST_DATE_PATH, index_path_generator
import os
import traceback
from functools import partial
from src.config import PARALLEL_CNT, SHOT_PERIOD, PERIOD
from src.utils import get_path_locks


def get_earliest_date_by_fund(path):
    store = pd.HDFStore(
        path, 'r')
    result = store['raw'].iloc[:1].index[0]
    store.close()
    return result

def get_earliest_date():
    if os.path.exists(OLDEST_DATE_PATH):
        return pickle.load(open(OLDEST_DATE_PATH, 'rb'))
    else:    
        nav_paths = list(nav_path_gen)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            result = min(tqdm.tqdm(executor.map(get_earliest_date_by_fund, nav_paths), 
                total=len(list(nav_paths)))).date()
        pickle.dump(result, open(OLDEST_DATE_PATH, 'wb'))
        return result

earliest_fund_date = get_earliest_date()

def date_generator(shot_period):
    if shot_period == 'month':
        date = datetime.now().date().replace(day=1)
    elif shot_period == 'year':
        date = datetime.now().date().replace(day=1).replace(month=1)
    elif shot_period == 'day':
        date = datetime.now().date()
    while True:
        if date < earliest_fund_date:
            break
        yield str(date)
        if shot_period == 'month':
            date = (date - timedelta(days=1)).replace(day=1)
        elif shot_period == 'year':
            date = (date - timedelta(days=1)).replace(day=1).replace(month=1)
        elif shot_period == 'day':
            date = date - timedelta(days=1)





def run(data, lockpool=None):
    try:
        i, date = data
        time.sleep((i%PARALLEL_CNT) * 10)
        etl = SnapshotETL(date, period=PERIOD, id=i, lockpool=lockpool)
        etl.run()
    except BaseException as e:
        print(traceback.format_exc())
        raise e

if __name__ == '__main__':
    freeze_support()
    paths = list(index_path_generator(period=PERIOD))
    lockpool = get_path_locks(paths)
    print('lock pool created')
    dates = list(date_generator(SHOT_PERIOD))
    with concurrent.futures.ProcessPoolExecutor(max_workers=PARALLEL_CNT) as executor:
        _ = list(tqdm.tqdm(executor.map(partial(run, lockpool=lockpool), enumerate(dates)), 
            desc='process snapshot', 
            total=len(dates)
        ))