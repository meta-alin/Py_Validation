# # import modin.pandas as pd
# import dask.dataframe as pd
# import pandera as pa
# import datetime as dt

# if __name__ == '__main__':
        
#     start_time = dt.datetime.now()
#     fact = pd.read_csv("FactSet_Position_202212124_13M.csv") 
#     crd = pd.read_csv("CRD_Position_202212124_13M.csv")
#     gwp = pd.read_csv("GWP_Position_202212124_13M.csv")

#     print(f"Total time for reading data: {(dt.datetime.now()-start_time).total_seconds()} seconds")

#     # Merge CRD and GWP data with FactSet
#     start_time = dt.datetime.now()
#     fact_crd_data = pd.merge(fact, crd, how='outer', left_on=['Portfolio Table','Fisher Sec ID'], right_on=['ACCT_CD','EXT_SEC_ID'])
#     fact_crd_gwp_data = pd.merge(fact_crd_data, gwp, how='outer', left_on=['Portfolio Table', 'Fisher Sec ID'], right_on=['Portfolio Code','Security Code'])
#     print(f"Execution time for the merge process: {(dt.datetime.now() - start_time).total_seconds()} seconds")

#     start_time = dt.datetime.now()
#     checks = [
#         pa.Check(lambda df: df['L/S'] == df['LONG_SHORT_IND']),
#         pa.Check(lambda df: df['Quantity_x'] == df['QTY_SOD']),
#         pa.Check(lambda df: df['Quantity_x'] == df['Quantity_y']),
#         pa.Check(lambda df: df['MKT_VAL_SOD_x'] == df['MKT_VAL_SOD_y']),
#         pa.Check(lambda df: df['MKT_VAL_SOD_x'] == df['Market Value Base'])
#     ]

#     schema = pa.DataFrameSchema(columns=None, checks=checks)
    
#     try:
#         schema(fact_crd_gwp_data, lazy=True).compute()
#         print("Successfully Validated")
#         print(f"Execution time for the Comparison process: {(dt.datetime.now() - start_time).total_seconds()} seconds")

#     except pa.errors.SchemaErrors as errors:
#         print(f"Total mismatched records are : {errors.failure_cases['index'].nunique()}")
#         print(errors.failure_cases['index'].unique())
#         print(f"Execution time for the Comparison process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
#     except Exception as err:
#         print(err.args)
#         print(f"Execution time for the Comparison process: {(dt.datetime.now() - start_time).total_seconds()} seconds")


# import modin.pandas as pd
import pandas as pd
import pandera as pa
import datetime as dt
import duckdb

if __name__ == '__main__':
        
    start_time = dt.datetime.now()
    fact = pd.read_csv("FactSet_Position_202212124_1.3M.csv") 
    crd = pd.read_csv("CRD_Position_202212124_1.3M.csv")
    gwp = pd.read_csv("GWP_Position_202212124_1.3M.csv")

    print(f"Total time for reading data: {(dt.datetime.now()-start_time).total_seconds()} seconds")

    # Merge CRD and GWP data with FactSet
    start_time = dt.datetime.now()
    fact_crd_data = pd.merge(fact, crd, how='outer', left_on=['Portfolio Table','Fisher Sec ID'], right_on=['ACCT_CD','EXT_SEC_ID'])
    fact_crd_gwp_data = pd.merge(fact_crd_data, gwp, how='outer', left_on=['Portfolio Table', 'Fisher Sec ID'], right_on=['Portfolio Code','Security Code'])
    print(f"Execution time for the merge process: {(dt.datetime.now() - start_time).total_seconds()} seconds")

    start_time = dt.datetime.now()

    mismatch_quantity_records = fact_crd_gwp_data.query("Quantity_x != QTY_SOD | Quantity_x != Quantity_y")
    mismatch_mkv_records = fact_crd_gwp_data.query("MKT_VAL_SOD_x != MKT_VAL_SOD_y")

    # mismatch_quantity_records = duckdb.query("SELECT * FROM fact_crd_gwp_data WHERE Quantity_x != QTY_SOD OR Quantity_x != Quantity_y").to_df()
    # mismatch_mkv_records = duckdb.query("SELECT * FROM fact_crd_gwp_data WHERE MKT_VAL_SOD_x != MKT_VAL_SOD_y").to_df()
    
    print(len(mismatch_quantity_records))
    print(len(mismatch_mkv_records))
    
    print(f"Execution time for the Comparison process: {(dt.datetime.now() - start_time).total_seconds()} seconds")

    # checks = [
    #     pa.Check(lambda df: df['L/S'] == df['LONG_SHORT_IND']),
    #     pa.Check(lambda df: df['Quantity_x'] == df['QTY_SOD']),
    #     pa.Check(lambda df: df['Quantity_x'] == df['Quantity_y']),
    #     pa.Check(lambda df: df['MKT_VAL_SOD_x'] == df['MKT_VAL_SOD_y']),
    #     pa.Check(lambda df: df['MKT_VAL_SOD_x'] == df['Market Value Base'])
    # ]

    # schema = pa.DataFrameSchema(columns=None, checks=checks)
    
    # try:
    #     schema.validate(fact_crd_gwp_data, lazy=True)
    #     print("Successfully Validated")
    #     print(f"Execution time for the Comparison process: {(dt.datetime.now() - start_time).total_seconds()} seconds")

    # except pa.errors.SchemaErrors as errors:
    #     print(f"Total mismatched records are : {errors.failure_cases['index'].nunique()}")
    #     print(errors.failure_cases['index'].unique())
    #     print(f"Execution time for the Comparison process: {(dt.datetime.now() - start_time).total_seconds()} seconds")
    # except Exception as err:
    #     print(err.args)
    #     print(err)
    #     print(f"Execution time for the Comparison process: {(dt.datetime.now() - start_time).total_seconds()} seconds")

