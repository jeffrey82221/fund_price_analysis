"""
TODO:
- [ ] Allow update only the most recent nav
- [ ] Allow Re-calculation of failed cases
"""
import concurrent.futures
import tqdm
from src.process.process import ProcessNavETL
from src.process.index import IndexETL
from src.path import nav_path_gen
from multiprocessing import freeze_support
from src.config import PERIOD, PARALLEL_CNT

if __name__ == '__main__':
    freeze_support()
    print('Index Columns:', IndexETL.result_columns)
    nav_paths = list(nav_path_gen)
    # lockpool = get_path_locks(nav_paths)
    func = ProcessNavETL(period=PERIOD).run
    with concurrent.futures.ProcessPoolExecutor(max_workers=PARALLEL_CNT) as executor:
        records = list(tqdm.tqdm(executor.map(func, nav_paths), desc='generate index 1st retry', total=len(nav_paths)))
        failed_paths = list(map(lambda x: x[1], filter(lambda x: not x[0], records)))
    while len(failed_paths) > 0:
        records = list(tqdm.tqdm(map(func, failed_paths), desc='generate index another retry', total=len(failed_paths)))
        failed_paths = list(map(lambda x: x[1], filter(lambda x: not x[0], records)))



