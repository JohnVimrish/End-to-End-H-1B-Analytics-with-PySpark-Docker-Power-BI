# from pyspark.sql import SparkSession

# # Step 2: Initialize SparkSession with Configurations
# spark = SparkSession.builder.config(conf=conf).getOrCreate()

# # Step 3: Load Some Sample Data
# data = [
#     ("John", 28, "California"),
#     ("Doe", 22, "New York"),
#     ("Alice", 30, "Texas"),
#     ("Bob", 35, "Florida")
# ]
# columns = ["Name", "Age", "State"]
# df = spark.createDataFrame(data, columns)

# # Step 4: Perform Some Transformations and Actions
# print("Original DataFrame:")
# df.show()

# # Filter rows where Age > 25
# df_filtered = df.filter(df.Age > 25)
# print("Filtered DataFrame (Age > 25):")
# df_filtered.show()

# # Group by State and calculate the average age
# df_grouped = df.groupBy("State").avg("Age")
# print("Grouped DataFrame with Average Age by State:")
# df_grouped.show()

# # Step 5: Stop the Spark Session
# spark.stop()



from utility.utils import Common_Utils_Operations  as rfl 
from itertools import groupby

input_json_file_path = "/root/docker_dataset/h1b_data/"  
file_pattern = r'.*\.(csv|xlsx)$'
input_file_list  = rfl.return_files_list(input_json_file_path,file_pattern)
input_file_list.sort()

# Group by the first three characters
grouped = {k: list(v) for k, v in groupby(input_file_list, key=lambda x: x[:3])}

print(grouped)



import pandas as pd 

filePath = '/root/docker_dataset/h1b_data/TRK_13139_I129_H1B_Registrations_FY21_FY24_FOIA_FIN.xlsx'
pandasDF = pd.read_excel(io = filePath, engine='openpyxl', sheet_name = 'Data Dictionary',header=None) 
table_name = ['Lookup Table for TRK_Metadata' ,'Lookup Table for Class_Preference' ,'Lookup Table for Basis for Classification' ,'Lookup Table for Requested Action' ,'Lookup Table for Ben Education Code']
groups = pandasDF[0].isin(table_name).cumsum() 
tables = {g.iloc[0,0]: g.iloc[1:].dropna(how='all') for k,g in pandasDF.groupby(groups)}



filePath = '/root/docker_dataset/h1b_data/TRK_13139_I129_H1B_Registrations_FY21_FY24_FOIA_FIN.xlsx'
pandasDFF = pd.read_excel(io = filePath, engine='openpyxl', sheet_name = 'I-129 H1B Job Codes',header=0,usecols="A:C") 
# print (pandasDFF)



import pandas as pd
from sqlalchemy import create_engine, Table, MetaData

# Read the CSV file
filePath = '/root/docker_dataset/h1b_data/TRK_13139_I129_H1B_Registrations_FY21_FY24_FOIA_FIN.xlsx'
pandasDF = pd.read_excel(io = filePath, engine='openpyxl', sheet_name = 'Data Dictionary',header=None) 
table_name = ['Lookup Table for TRK_Metadata' ,'Lookup Table for Class_Preference' ,'Lookup Table for Basis for Classification' ,'Lookup Table for Requested Action' ,'Lookup Table for Ben Education Code']
groups = pandasDF[0].isin(table_name).cumsum() 
tables = {g.iloc[0,0]: g.iloc[1:].dropna(how='all') for k,g in pandasDF.groupby(groups)}
try  :
    engine = create_engine('postgresql+psycopg://john_user:abc@12345@host.docker.internal:5432/crime_data_la', client_encoding="utf8")
except  Exception as e  :
    raise e 
for table_name, table_dft  in tables.items():
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine, autoload_replace=True)

    # Reflect the DataFrame schema to the table
    table_dft.head(0).to_sql(table_name, engine, if_exists='replace', index=False)

    # Generate the DDL script
    ddl_script = str(table.compile(dialect=engine.dialect))
    print(ddl_script)