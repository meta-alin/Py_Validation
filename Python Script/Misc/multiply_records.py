import pandas as pd
import numpy as np
import datetime as dt
import multiprocessing as mp


# Function to generate bulk records
# def multiply_data(args):
#     data, label = args

#     start_time = dt.datetime.now()
    
#     multiplied_data = pd.concat([data] * 100000, ignore_index=True)
#     multiplied_data['label'] = label

#     print(f"Execution time for generating bulk {label}: {(dt.datetime.now() - start_time).total_seconds()} seconds")
#     return multiplied_data

if __name__ == '__main__':

    initial_start_time = dt.datetime.now()
    print(f"Execution begins to generate bulk data at: {dt.datetime.now().time()}")

    # Read data
    fact = pd.read_excel("FactSet_Position_202212124.xlsx") 
    crd = pd.read_excel("CRD_Position_202212124.xlsx")
    gwp = pd.read_excel("GWP_Position_202212124.xlsx")

    # # Normalizing date values
    # crd['ASOF_DATE'] = pd.to_datetime(crd['ASOF_DATE']).dt.date
    # gwp['Position Effective Date'] = gwp['Position Effective Date'].dt.date

    # # Prepare args to process parallelly
    # labels = ['gwp']
    # datasets = [gwp]
    # args = list(zip(datasets, labels))

    # multiplied_data = pd.DataFrame()

    # # Create a pool 
    # pool = mp.Pool(mp.cpu_count())

    # # Process all three data sets
    # multiplied_data = pool.map(multiply_data, args)

    # # Close the worker pool
    # pool.close()

    # # Extract each bulk dataset
    # multiplied_fact = pd.concat([df for df in multiplied_data if df['label'].iloc[0] == 'fact'])
    # multiplied_crd = pd.concat([df for df in multiplied_data if df['label'].iloc[0] == 'crd'])
    # multiplied_gwp = pd.concat([df for df in multiplied_data if df['label'].iloc[0] == 'gwp'])

 
    # print("generating fact data")
    # added_value = 0
    # processed_fact = pd.DataFrame()
    # for chunk in np.array_split(fact, 1000000):
    #     # print(added_value)
    #     chunk.loc[:, 'Portfolio Table'] += added_value
    #     added_value += 1000
    #     processed_fact = pd.concat([processed_fact, chunk], ignore_index=True)
    #     # print(processed_fact)

    # processed_fact.to_csv("processed_fact.csv")
    # print("finished generating fact data")

    # print("generating crd data")
    # added_value = 0
    # processed_fact = pd.DataFrame()
    # for chunk in np.array_split(multiplied_crd, 100000):
    #     # print(added_value)
    #     chunk.loc[:, 'ACCT_CD'] += added_value
    #     added_value += 1000
    #     processed_fact = pd.concat([processed_fact, chunk], ignore_index=True)

    # processed_fact.to_csv("processed_crd.csv")
    # print("finished generating crd data")

    # print("generating gwp data")
    # added_value = 0
    # processed_fact = pd.DataFrame()
    # for chunk in np.array_split(multiplied_gwp, 100000):
    #     # print(added_value)
    #     chunk.loc[:, 'Portfolio Code'] += added_value
    #     added_value += 1000
    #     processed_fact = pd.concat([processed_fact, chunk], ignore_index=True)

    # processed_fact.to_csv("processed_gwp.csv")
    # print("finished generating gwp data")

    # Read data
    start_time = dt.datetime.now()
    fact = pd.read_excel("FactSet_Position_202212124.xlsx")
    multiplied_fact = pd.concat([fact] * 100000, ignore_index=True)
    print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    print("Multiplication done")
    start_time = dt.datetime.now()
    multiplied_fact['Portfolio Table'] = multiplied_fact['Portfolio Table'] + ((multiplied_fact.index // 13))
    multiplied_fact.to_csv('FactSet_Position_202212124_1.3M.csv', index=False)
    print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    multiplied_fact.drop(index=multiplied_fact.index, inplace=True)
    print("Fact Done")

    start_time = dt.datetime.now()
    multiplied_crd = pd.concat([crd] * 100000, ignore_index=True)
    print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    print("Multiplication done")
    start_time = dt.datetime.now()
    multiplied_crd['ACCT_CD'] = multiplied_crd['ACCT_CD'] + ((multiplied_crd.index // 13))
    multiplied_crd.to_csv('CRD_Position_202212124_1.3M.csv', index=False)
    print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    multiplied_crd.drop(index=multiplied_crd.index, inplace=True)
    print("crd Done")

    start_time = dt.datetime.now()
    multiplied_gwp = pd.concat([gwp] * 100000, ignore_index=True)
    print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    print("Multiplication done")
    start_time = dt.datetime.now()
    multiplied_gwp['Portfolio Code'] = multiplied_gwp['Portfolio Code'] + ((multiplied_gwp.index // 13))
    multiplied_gwp.to_csv('GWP_Position_202212124_1.3M.csv', index=False)
    print(f"Execution time for the multiplication process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    multiplied_gwp.drop(index=multiplied_gwp.index, inplace=True)
    print("gwp Done")




# import pandas as pd
# import numpy as np

# df = pd.DataFrame({'ID':[1,1,2,2,2,3,3,3,3], 'X':1})

# batch_size = 3
# batches = np.ceil(df.shape[0]/batch_size)
# df.index = pd.cut(df.index,batches,labels=range(batches))

# ###########

# def myFunc(batch_data :pd.DataFrame):
#     #print(batch_data.unique(),'\n')
#     return batch_data.nunique()

# output1 = df.groupby(df.index).aggregate({'ID':myFunc})
# output2 = df.groupby(df.index).aggregate(myFunc)
# output3 = df.groupby(df.index).aggregate({'ID':myFunc,'X':'std'})

# import pandas as pd
# import numpy as np

# # create a sample DataFrame
# df = pd.DataFrame(np.random.randint(0, 10, size=(5, 26)), columns=[f"col{i}" for i in range(26)])

# # print the original DataFrame
# print("Original DataFrame:\n", df)

# # define the new column values
# new_values = ["new_value_1", "new_value_2", "new_value_3"]

# # loop through every 13th column and change the values
# for i in range(0, df.shape[1], 13):
#     df.iloc[:, i] = new_values

# # print the updated DataFrame
# print("Updated DataFrame:\n", df)

# import pandas as pd

# # create a sample dataframe
# df = pd.DataFrame({'A': [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20], 'B': ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t']})

# # loop through the dataframe
# for i, row in df.iterrows():
#     # check if the current row number is a multiple of 13
#     if i % 13 == 0:
#         # assign the new value to column 'B'
#         df.loc[i, 'B'] = 'new_value'

# # print the updated dataframe
# print(df)

# import numpy as np
# import pandas as pd

# data = pd.DataFrame(np.random.rand(10, 3))
# for chunk in np.array_split(data, 2):
#   print(chunk["0"] + 1)

# print(data)