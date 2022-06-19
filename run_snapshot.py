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
import jum

def get_earliest_date_by_fund(path):
    store = pd.HDFStore(
        path, 'r')
    result = store['raw'].iloc[:1].index[0]
    store.close()
    return result

@jum.cache(cache_dir='.jum')
def get_earliest_date():
    nav_paths = list(nav_path_gen)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        result = min(tqdm.tqdm(executor.map(get_earliest_date_by_fund, nav_paths), 
        total=len(list(nav_paths))))
    return result.date()

earliest_fund_date = get_earliest_date()

def date_generator(period='month'):
    if period == 'month':
        date = datetime.now().date().replace(day=1)
    elif period == 'year':
        date = datetime.now().date().replace(day=1).replace(month=1)
    elif period == 'day':
        date = datetime.now().date()
    while True:
        if date < earliest_fund_date:
            break
        yield str(date)
        if period == 'month':
            date = (date - timedelta(days=1)).replace(day=1)
        elif period == 'year':
            date = (date - timedelta(days=1)).replace(day=1).replace(month=1)
        elif period == 'day':
            date = date - timedelta(days=1)

PARALLEL_CNT = 5
def run(data):
    i, date = data
    if i <=PARALLEL_CNT:
        time.sleep((i%PARALLEL_CNT) * 5)
    etl = SnapshotETL(date, period=7, id=i)
    etl.run()

dates = list(date_generator())
with concurrent.futures.ProcessPoolExecutor(max_workers=PARALLEL_CNT) as executor:
    _ = list(tqdm.tqdm(executor.map(run, enumerate(dates)), 
        desc='process snapshot', 
        total=len(dates)
    ))