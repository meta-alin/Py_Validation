import pandas as pd
df = pd.DataFrame({"name":["rashi","kush","abhi","juli", "rashi"], "class":[12,10,11,9,8], "marks":[99, 97, 94, 92, 99]})
print(df)
df.to_csv('result.csv', index=False)



# Get Number of Rows in DataFrame
# Use len(df.index):- df.index returns RangeIndex(start=0, stop=last_index+1, step=1) and then use it on len() to get the count
# Also, can use len(df.axes[0]) to get he no of rows (axes[0] represents rows)
print("\n")
print("No of rows in dataframe: ")
print(len(df.index))
print(len(df.axes[0]))



# count of columns in each resource
print("\n")
print("No of cols in dataframe: ")
print(len(df.columns))
print(len(df.axes[1]))



# min/max/average values for various numeric columns
# describe() function gives several calculated values like mean, max, min etc for all numerical columns
print("\n")
print("The min, max, avg, etc. values for all numeric columns are: \n")
print(df.describe())

print("Avg of Marks")
print(df.describe())

print("\n")
for col in df:
    if(df[col].dtype==int or df[col].dtype==float):
        print("Min values in the column: ")
        print(df[col].name)
        print(df[col].min())
        print("\n")

for col in df:
    if(df[col].dtype==int or df[col].dtype==float):
        print("Max values in the column: ")
        print(df[col].name)
        print(df[col].max())
        print("\n")

for col in df:
    if(df[col].dtype==int or df[col].dtype==float):
        print("Mean or avg values in the column: ")
        print(df[col].name)
        print(df[col].mean())
        print("\n")



# distinct values per column
# To get unique values of any column we can use the unique function

# To get unique value of any particular column
print("\nunique name values are: ")
print(df["name"].unique())
print("\n")

# To get unique values of all columns in dataframe
for col in df:
    print("Unique values in the column: ")
    print(df[col].name)
    print(df[col].unique())
    print("\n")
    
    print("No. of unique values in the column: ")
    print(df[col].name)
    print(df[col].nunique())
    print("\n")

