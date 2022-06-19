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
from src.snapshot import SnapshotETL

etl = SnapshotETL('2022-06-01', period=7)
etl.run()