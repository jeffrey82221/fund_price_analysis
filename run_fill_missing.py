"""
TODO:
- [ ] Allow update only the most recent nav
"""
import concurrent.futures
import tqdm
from src.process.raw import CleanNavETL
from src.path import source_fund_path_gen
paths = list(source_fund_path_gen)
with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    _ = list(tqdm.tqdm(executor.map(CleanNavETL.run, paths), total=len(paths)))
