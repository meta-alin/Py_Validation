import pandas as pd
import numpy as np
import datetime as dt
import multiprocessing as mp

if __name__ == '__main__':

    initial_start_time = dt.datetime.now()
    print(f"Execution begins to generate bulk data at: {dt.datetime.now().time()}")

    # Read data 
    crd = pd.read_excel("SampleData_Security_CRD.xlsx")
    
    start_time = dt.datetime.now()
    multiplied_fact = pd.concat([crd] * 10000, ignore_index=True)
    print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    print("Multiplication done")
    start_time = dt.datetime.now()
    multiplied_fact['EXT_SEC_ID'] = multiplied_fact['EXT_SEC_ID'] + ((multiplied_fact.index // 26))
    multiplied_fact.to_csv('SampleData_Security_CRD_1.3Mtest.csv', index=False)
    print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    multiplied_fact.drop(index=multiplied_fact.index, inplace=True)
    print("Fact Done")