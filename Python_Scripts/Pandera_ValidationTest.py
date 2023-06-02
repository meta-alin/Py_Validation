# Pandera validation and equal columns check 


import pyspark.pandas as ps
import pandera as pa

# .............................................................

# dfA1 = ps.read_csv('StudentData1.csv')
# dfB1 = ps.read_csv('StudentData2.csv')

# print(dfA1)

# merged_dff = ps.merge(dfA1, dfB1, left_on='ID', right_on='Student_ID', how='inner')
# merged_dff = merged_dff.drop("Student_ID", axis=1)

# merged_dff.to_csv('finalList.csv',num_files=4)

# testSchema = pa.DataFrameSchema({
#     'ID': pa.Column(pa.String, nullable=False),
#     'Name': pa.Column(pa.String, nullable=False),
#     'Class': pa.Column(pa.Int32, pa.Check(lambda s: s > 0), nullable=False),
#     'Subject': pa.Column(pa.String, nullable=False),
#     'Marks': pa.Column(pa.Int32, pa.Check(lambda s: s >= 0), nullable=False),
# })

# try:
#     testSchema.validate(merged_dff)
#     print("Validation successful. No errors.")
# except pa.errors.SchemaErrors as err:
#     print("Validation errors:")
    
# ................................................................

# dfA2 = ps.read_csv('Test1.csv')
# dfB2 = ps.read_csv('Test2.csv')

# print(dfB2)
# print(dfA2)

# dfA2 = dfA2.drop("COST",axis=1)

# merged_New = ps.merge(dfA2, dfB2, left_on='ACCT_CD', right_on='Portfolio Table', how='inner')
# merged_New = merged_New.drop("Portfolio Table", axis=1)

# merged_New.to_csv('mergedTest.csv',num_files=1) 

# ...............................................................

dummyDf = ps.read_csv('checkEqualCols.csv')
dfResult = dummyDf.query("dummyDf['Marks1'] == dummyDf['Marks2']")

print(dfResult)

d1 = dummyDf['Marks1'].count()
d2 = dfResult['Marks1'].count()

print(d1)
print(d2)
if d1 == d2:
    print(True)
else:
    print(False)

