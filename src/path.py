import os
from pathlib import Path

SRC_PATH = os.path.join(Path(__file__).parent.parent.parent, 'efficient_selenium', 'data', 'nav')
NAV_PATH = os.path.join(Path(__file__).parent.parent, 'data', 'nav')
INDEX_PATH = os.path.join(Path(__file__).parent.parent, 'data', 'index')
print('src path:', SRC_PATH)
print('nav path:', NAV_PATH)
def path_generator(nav_dir):
    for company in os.listdir(nav_dir):
        full_company_path = os.path.join(nav_dir, company)
        if os.path.isdir(full_company_path):
            for table_name in os.listdir(full_company_path):
                if '.h5' in table_name:
                    yield os.path.join(nav_dir, company, table_name)

source_fund_path_gen = path_generator(SRC_PATH)

def clean_nav_path_generator(nav_path):
    for table_name in os.listdir(nav_path):
        if '.h5' in table_name:
            yield os.path.join(nav_path, table_name)

nav_path_gen = clean_nav_path_generator(NAV_PATH)