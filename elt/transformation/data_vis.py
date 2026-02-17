# Data visualizations for each CAMELSH historical table.

import pandas as pd
from pathlib import Path

folder_path = Path("elt/transformation/seeds")

for csv_file in folder_path.glob("*.csv"):
    print(f"\n{'='*50}")
    print(f"File: {csv_file.name}")
    print('='*50)
    
    df = pd.read_csv(csv_file)
    
    print("Rows: ", df.shape[0], " Columns: ", df.shape[1])
    print("Column Names:\n", df.columns.to_list(), '\n')
    # print(df.info())
    # # Count unique values for each column
    # print("\nUnique values per column:")
    # print(df.nunique())

