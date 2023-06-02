# No. of Records multiplying in a csv file 

import pandas as pd
import datetime as dt


if __name__ == '__main__':

    initial_start_time = dt.datetime.now()
    print(f"Execution begins to generate bulk data at: {dt.datetime.now().time()}")

    # Read data
    # fact = pd.read_csv("FactSet_Position_202212124_130K.csv") 
    crd = pd.read_csv("erroneous_CRD_Position_202212124.csv")
    # gwp = pd.read_csv("GWP_Position_202212124_130K.csv")


    # Read data
    # start_time = dt.datetime.now()
    # multiplied_fact = pd.concat([fact] * 3, ignore_index=True)
    # print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    # print("Multiplication done")
    # start_time = dt.datetime.now()
    # multiplied_fact['Portfolio Table'] = multiplied_fact['Portfolio Table'] + ((multiplied_fact.index // 13))
    # multiplied_fact.to_csv('FactSet_Position_202212124_390K.csv', index=False)
    # print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    # multiplied_fact.drop(index=multiplied_fact.index, inplace=True)
    # print("Fact Done")

    start_time = dt.datetime.now()
    multiplied_crd = pd.concat([crd] * 15, ignore_index=True)
    print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    print("Multiplication done")
    start_time = dt.datetime.now()
    multiplied_crd['ACCT_CD'] = multiplied_crd['ACCT_CD'] + ((multiplied_crd.index // 10))
    multiplied_crd.to_csv('erroneous_CRD_Position_202212124_150.csv', index=False)
    print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    multiplied_crd.drop(index=multiplied_crd.index, inplace=True)
    print("crd Done")

    # start_time = dt.datetime.now()
    # multiplied_gwp = pd.concat([gwp] * 3, ignore_index=True)
    # print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    # print("Multiplication done")
    # start_time = dt.datetime.now()
    # multiplied_gwp['Portfolio Code'] = multiplied_gwp['Portfolio Code'] + ((multiplied_gwp.index // 13))
    # multiplied_gwp.to_csv('GWP_Position_202212124_390K.csv', index=False)
    # print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    # multiplied_gwp.drop(index=multiplied_gwp.index, inplace=True)
    # print("gwp Done")