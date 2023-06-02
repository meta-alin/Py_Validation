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
# import duckdb

if __name__ == '__main__':
        
    start_time = dt.datetime.now()
    fact = pd.read_csv("FactSet_Position_202212124.csv") 
    crd = pd.read_csv("CRD_Position_202212124.csv")
    gwp = pd.read_csv("GWP_Position_202212124.csv")

    print(f"Total time for reading data: {(dt.datetime.now()-start_time).total_seconds()} seconds")

    # Merge CRD and GWP data with FactSet
    start_time = dt.datetime.now()
    fact_crd_data = pd.merge(fact, crd, how='outer', left_on=['Portfolio Table','Fisher Sec ID'], right_on=['ACCT_CD','EXT_SEC_ID'])
    fact_crd_gwp_data = pd.merge(fact_crd_data, gwp, how='outer', left_on=['Portfolio Table', 'Fisher Sec ID'], right_on=['Portfolio Code','Security Code'])
    print(f"Execution time for the merge process: {(dt.datetime.now() - start_time).total_seconds()} seconds")

    # print(fact_crd_gwp_data)
    fact_crd_gwp_data.to_csv('demotest.csv')
    
    start_time = dt.datetime.now()
    fact_crd_gwp_data['UNREALIZED_GAIN_LOSS_x'] = fact_crd_gwp_data['UNREALIZED_GAIN_LOSS_x'].astype(object, errors='raise')
    
    print('UNREALIZED_GAIN_LOSS_x: ',fact_crd_gwp_data['UNREALIZED_GAIN_LOSS_x'])
    print('UNREALIZED_GAIN_LOSS_y: ',fact_crd_gwp_data['UNREALIZED_GAIN_LOSS_y'])
    
    if(str(fact_crd_gwp_data['UNREALIZED_GAIN_LOSS_x'][0]) == str(fact_crd_gwp_data['UNREALIZED_GAIN_LOSS_y'][0])):
        print("True")
    elif(fact_crd_gwp_data['UNREALIZED_GAIN_LOSS_x'][0] != fact_crd_gwp_data['UNREALIZED_GAIN_LOSS_y'][0]):
        print("trfalse")
    else:
        print("False")
    
    mismatch_quantity_records = fact_crd_gwp_data.query("Quantity_x != QTY_SOD | Quantity_x != Quantity_y")
    mismatch_mkv_records = fact_crd_gwp_data.query("MKT_VAL_SOD_x != MKT_VAL_SOD_y | MKT_VAL_SOD_x != `Market Value Base`")
    mismatch_long_short = fact_crd_gwp_data.query("LONG_SHORT_IND != `L/S`")
    mismatch_cost = fact_crd_gwp_data.query("COST != Cost | Cost != `Total Cost Base`")
    mismtach_unrealized_gain_loss = fact_crd_gwp_data.query("UNREALIZED_GAIN_LOSS_x == UNREALIZED_GAIN_LOSS_y | UNREALIZED_GAIN_LOSS_y == `Unrealized GainLoss`")
    mismatch_unit_cost = fact_crd_gwp_data.query("UNIT_COST != `Unit Cost Base`")
    mismatch_date = fact_crd_gwp_data.query("ASOF_DATE != `Position Effective Date`")
    
    print(fact_crd_gwp_data.dtypes)

    # mismatch_quantity_records = duckdb.query("SELECT * FROM fact_crd_gwp_data WHERE Quantity_x != QTY_SOD OR Quantity_x != Quantity_y").to_df()
    # mismatch_mkv_records = duckdb.query("SELECT * FROM fact_crd_gwp_data WHERE MKT_VAL_SOD_x != MKT_VAL_SOD_y").to_df()
    
    print('mismatch_quantity_records',len(mismatch_quantity_records))
    print('mismatch_mkv_records',len(mismatch_mkv_records))
    print('mismatch_long_short',len(mismatch_long_short))
    print('mismatch_cost',len(mismatch_cost))
    print('mismtach_unrealized_gain_loss',len(mismtach_unrealized_gain_loss))
    print('mismatch_unit_cost',len(mismatch_unit_cost))
    print('mismatch_date',len(mismatch_date))
    
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

