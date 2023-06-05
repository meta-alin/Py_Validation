import duckdb 

# Making database connection instance
conn = duckdb.connect()

# Reading csv files
df1 = conn.execute("select * FROM read_csv_auto('CRD_Position_202212124.csv')").df()
print(df1)  

# Writing to csv files 
conn.execute("COPY df1 TO 'output.csv' (HEADER)")


## Performing sql queries on the data 

# Reading data from a csv file into a table
conn.execute("CREATE TABLE crdTable AS SELECT * FROM 'CRD_Position_202212124.csv'")
List1 = conn.execute("Select * from crdTable").fetchall()
# print(List1)

# Creating view (virtual table) from table 
conn.execute("CREATE VIEW crd_view as SELECT * FROM crdTable")
list2 = conn.execute("SELECT * FROM crd_view").fetchall()
# print(list2)

# Querying with where clause
print("Where query:-")
print(conn.execute("SELECT * from crdTable where ACCT_CD = 1465 and EXT_SEC_ID = 54655").fetchall())

# Querying with order by clause
print("Order by query:-")
print(conn.execute("SELECT * from crdTable ORDER BY EXT_SEC_ID desc limit 2").fetchall())


## Performing FTS (Full Text Search)

# -- create a table and fill it with text data
conn.execute("CREATE TABLE documents(docId INTEGER, text_content VARCHAR, author VARCHAR, doc_version INTEGER)")
conn.execute("""INSERT INTO documents
             VALUES (1, 'This is document first','Rashi Kinra', 2),
             (2, 'This is document second', 'Alin Paul', 4)""")

# -- build the index to make both the 'text_content' and 'author' columns searchable
conn.execute("PRAGMA create_fts_index('documents', 'docId', 'text_content', 'author')")

# -- search the 'author' field index for documents with given name
print(conn.execute("""SELECT text_content, score
                   FROM (SELECT *, fts_main_documents.match_bm25(docId, 'Kinra', fields := 'author') 
                         AS score
                         FROM documents) 
                   WHERE score IS NOT NULL
                   AND doc_version > 1
                   ORDER BY score DESC """).fetchall())


# Also, client APIs for Python, Java etc languagesa are provided, so we can perform queries using duckdb in various languages.
# Also, various extensions are provided like, HTTPfs which enable us to query files directly over HTTP, various other extensions like this are also provided.