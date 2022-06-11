import os
from pathlib import Path

SRC_PATH = os.path.join(Path(__file__).parent.parent.parent, 'efficient_selenium', 'data', 'nav')
print('src path:', SRC_PATH)
def path_generator(nav_dir):
    for company_path in os.listdir(nav_dir):
        full_company_path = os.path.join(nav_dir, company_path)
        if os.path.isdir(full_company_path):
            for table_path in os.listdir(full_company_path):
                if '.h5' in table_path:
                    yield os.path.join(nav_dir, company_path, table_path)

source_fund_path_gen = path_generator(SRC_PATH)
