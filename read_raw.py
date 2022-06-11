import pandas as pd


table = pd.read_hdf('data/nav/TW000T0101Y3.h5', 'raw', auto_close=True)
print(table)
table2 = pd.read_hdf('data/nav/TW000T0101Y3.h5', 'x', auto_close=True)
print(table2)