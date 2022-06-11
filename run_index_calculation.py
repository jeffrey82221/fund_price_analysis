import concurrent.futures
import tqdm
from src.process.process import ProcessNavETL
from src.path import nav_path_gen

nav_paths = list(nav_path_gen)
func = ProcessNavETL(period=7).run
with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
    _ = list(tqdm.tqdm(executor.map(func, nav_paths), total=len(nav_paths)))

