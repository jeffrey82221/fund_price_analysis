"""
TODO:
- [ ] Allow update only the most recent nav
"""
import concurrent.futures
import tqdm
from src.process.process import ProcessNavETL
from src.process.index import IndexETL
from src.path import nav_path_gen

print('Index Columns:', IndexETL.result_columns)
nav_paths = list(nav_path_gen)
func = ProcessNavETL(period=364).run
with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
    success_list = list(tqdm.tqdm(executor.map(func, nav_paths), total=len(nav_paths)))
print('Number of plausible index table:', sum(success_list))

