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
    
    # Create a file for writing errors into
    error_file = open("error_messages.txt", "w")
    
    # Create an empty DataFrame to store erroneous rows
    erroneous_rows = pd.DataFrame(columns=df.columns)
    
    # Create a set to store the indices of erroneous rows
    erroneous_indices = set()
    
    print("Schema validation failed. Mismatched rows:")
    
    for error in errors:
        error_message = f"Row: {error.row}, Column: {error.column} - Error: {error.message}"
        print(error_message)
        
        error_file.write(error_message + "\n")
        
        if error.row not in erroneous_indices:
            # Collect the erroneous row into the DataFrame
            erroneous_rows = erroneous_rows._append(df.iloc[error.row])
            erroneous_indices.add(error.row)
            
    erroneous_rows.to_csv("erroneous_rows.csv", index=False)
    error_file.close()

else:
    print("Schema validation successful")
    
    
print(f"Validation Time using pandas: {(dt.datetime.now() - start_time).total_seconds()} seconds")

# pandas_schema doc, performance issue
# timing stats for pandas and pyspark