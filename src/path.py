import os
from pathlib import Path

FUND_INFO_PATH = os.path.join(Path(__file__).parent.parent.parent, 'efficient_selenium', 'data', 'fund_info.h5')
SRC_PATH = os.path.join(Path(__file__).parent.parent.parent, 'efficient_selenium', 'data', 'nav')
NAV_PATH = os.path.join(Path(__file__).parent.parent, 'data', 'nav')
INDEX_PATH = os.path.join(Path(__file__).parent.parent, 'data', 'index')
SNAPSHOT_PATH = os.path.join(Path(__file__).parent.parent, 'data', 'snapshot')

print('src path:', SRC_PATH)
print('nav path:', NAV_PATH)
def source_path_generator(nav_dir):
    for company in os.listdir(nav_dir):
        full_company_path = os.path.join(nav_dir, company)
        if os.path.isdir(full_company_path):
            for table_name in os.listdir(full_company_path):
                if '.h5' in table_name and len(table_name.split('.h5')[0]) >= 12:
                    yield os.path.join(nav_dir, company, table_name)

source_fund_path_gen = source_path_generator(SRC_PATH)

def clean_nav_path_generator(nav_path):
    for table_name in os.listdir(nav_path):
        if '.h5' in table_name:
            yield os.path.join(nav_path, table_name)

nav_path_gen = clean_nav_path_generator(NAV_PATH)

def index_path_generator(period=7):
    dir_path = os.path.join(INDEX_PATH, f'{period}days')
    for file_name in os.listdir(dir_path):
        yield os.path.join(dir_path, file_name)