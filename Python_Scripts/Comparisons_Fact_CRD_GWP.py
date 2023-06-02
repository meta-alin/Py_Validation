# All comparisons using pyspark.pandas

import pyspark.pandas as ps
import datetime as dt


if __name__ == '__main__':
        
        
    # Read files 
    start_time = dt.datetime.now()
    fact13 = ps.read_csv("FactSet_Position_202212124_390K.csv") 
    crd13 = ps.read_csv("CRD_Position_202212124_390K.csv")
    gwp13 = ps.read_csv("GWP_Position_202212124_390K.csv")
    
    print(f"Total time for reading data: {(dt.datetime.now()-start_time).total_seconds()} seconds")


    # Merge CRD and GWP data with FactSet
    start_time = dt.datetime.now()
    
    # Conversion done to avoid pyspark datetime64 type error
    crd13['ASOF_DATE'] = crd13['ASOF_DATE'].astype("str")
    gwp13['Position Effective Date'] = gwp13['Position Effective Date'].astype("str")
    
    # Conversion of dataframe done before merging to overcome same column name ambiguity while merging in pyspark
    fact13P = fact13.to_pandas()
    crd13P = crd13.to_pandas()
    gwp13P = gwp13.to_pandas()
    
    suffixes = ("_w", "_x")
    suffixes2 = ("_y", "_z")  
    
    fact_crd_data = fact13P.merge(crd13P, how='outer', left_on=['Portfolio Table','Fisher Sec ID'], right_on=['ACCT_CD','EXT_SEC_ID'], suffixes=suffixes)
    fact_crd_gwp_data_pandas = fact_crd_data.merge(gwp13P, how='outer', left_on=['Portfolio Table', 'Fisher Sec ID'], right_on=['Portfolio Code','Security Code'], suffixes=suffixes2)
    
    fact_crd_gwp_data = ps.DataFrame(fact_crd_gwp_data_pandas)
    
    print(f"Execution time for the merge process: {(dt.datetime.now() - start_time).total_seconds()} seconds")


    # Comparison Rules
    start_time = dt.datetime.now()
    
    mismatch_quantity_records = fact_crd_gwp_data.query("Quantity_y != QTY_SOD OR Quantity_y != Quantity_z")
    mismatch_mkv_records = fact_crd_gwp_data.query("MKT_VAL_SOD_w != MKT_VAL_SOD_x OR MKT_VAL_SOD_w != `Market Value Base`")
    mismatch_long_short = fact_crd_gwp_data.query("LONG_SHORT_IND != `L/S`")
    mismatch_cost = fact_crd_gwp_data.query("Cost_w != Cost_x OR Cost_w != `Total Cost Base`")
    mismtach_unrealized_gain_loss = fact_crd_gwp_data.query("UNREALIZED_GAIN_LOSS_w != UNREALIZED_GAIN_LOSS_x OR UNREALIZED_GAIN_LOSS_w != `Unrealized GainLoss`")
    mismatch_unit_cost = fact_crd_gwp_data.query("UNIT_COST != `Unit Cost Base`")
    mismatch_date = fact_crd_gwp_data.query("ASOF_DATE != `Position Effective Date`")
    
    print('mismatch_quantity_records',len(mismatch_quantity_records))
    print('mismatch_mkv_records',len(mismatch_mkv_records))
    print('mismatch_long_short',len(mismatch_long_short))
    print('mismatch_cost',len(mismatch_cost))
    print('mismtach_unrealized_gain_loss',len(mismtach_unrealized_gain_loss))
    print('mismatch_unit_cost',len(mismatch_unit_cost))
    print('mismatch_date',len(mismatch_date))
    
    print(f"Execution time for the Comparison process: {(dt.datetime.now() - start_time).total_seconds()} seconds")

