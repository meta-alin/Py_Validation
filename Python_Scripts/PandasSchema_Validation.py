import pandas as pd
import datetime as dt
from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation, DateFormatValidation, InListValidation, CustomElementValidation

start_time = dt.datetime.now()

df = pd.read_csv("erroneous_CRD_Position_202212124_200.csv")

def validate_not_empty(value):
    if isinstance(value, str):
        return value.strip() != '' 
    else:
        return pd.notnull(value)

validation_functions = CustomElementValidation(validate_not_empty, 'Mandatory col should not be empty')

expected_schema = Schema([
    Column("ACCT_CD", [CanConvertValidation(int), validation_functions]),
    Column("EXT_SEC_ID", [CanConvertValidation(int)]),
    Column("ASOF_DATE", [DateFormatValidation(date_format='%Y-%m-%d %H:%M:%S.%f')]),
    Column("LONG_SHORT_IND", [InListValidation(['L','S']), validation_functions]),
    Column("QTY_SOD", [CanConvertValidation(int)]),
    Column("COST", [CanConvertValidation(float)]),
    Column("UNIT_COST", [CanConvertValidation(float)]),
    Column("MKT_VAL_SOD", [CanConvertValidation(float)])
])

# Validate DataFrame against the expected schema
errors = expected_schema.validate(df, columns=['ACCT_CD', 'EXT_SEC_ID', 'ASOF_DATE', 'LONG_SHORT_IND', 'QTY_SOD', 'COST', 'UNIT_COST', 'MKT_VAL_SOD'])

if errors:
    
    errorsDf = pd.DataFrame(errors) 
    strErrors = errorsDf.to_string()
    
    print("Schema validation failed.")
    print(errorsDf)
    
    # Create a file for writing errors into
    error_file = open("error_messagesNew.txt", "w")
    error_file.write(strErrors)
    error_file.close()
    
    # Create an empty DataFrame to store erroneous rows
    erroneous_rows = pd.DataFrame(columns=df.columns)
    
    # Create a set to store the indices of erroneous rows
    erroneous_indices = set(map(lambda error: error.row, errors))
    
    erroneous = list(map(lambda errorIndex: erroneous_rows._append(df.iloc[errorIndex]), erroneous_indices))
    erroneous_rows = pd.concat(erroneous, ignore_index=True)
    
    erroneous_rows.to_csv("erroneous_rowsNew.csv", index=False)
    df = df.drop(list(erroneous_indices))
    df = df.reset_index(drop=True) 

else:
    print("Schema validation successful")
    
print("Dataframe after dropping erroneous rows: ")
print(df)  
    
print(f"Validation Time using pandas: {(dt.datetime.now() - start_time).total_seconds()} seconds")



   