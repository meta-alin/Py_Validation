import pandas as pd
import glob

# Listing all the file paths to read
file_paths = glob.glob('./WILDCARD_FILES/CRD*.csv')

df = pd.concat(map(pd.read_csv, file_paths))
print(len(df))
print(df)
