
import pandas as pd 
from io import StringIO
import psycopg as p

input_file_path = "/root/docker_dataset/updated_job_categories_2.csv"  
pandasDF = pd.read_csv(input_file_path,header=0) 
csv_data = StringIO()
pandasDF.to_csv(csv_data, index=False, header=True)


conn = p.connect(
    dbname='H1b_Analysis',
    user='john_user',
    password='abc@12345',
    host='host.docker.internal',
    port=5432
)

cur = conn.cursor()
table_name  =  "h1b_analysis_stg.job_catergory"
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    {", ".join([f"{col} TEXT" for col in pandasDF.columns])}
)
"""
cur.execute(create_table_query)
conn.commit()
copy_qyery  =  f"""
        COPY {table_name} (
{", ".join([col for col in pandasDF.columns])}
        ) FROM STDIN WITH (        
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    QUOTE '"',
    ESCAPE '"',
    ENCODING 'UTF8'
);
"""
csv_data.seek(0)
with cur.copy(statement =copy_qyery) as copy:
     copy.write(csv_data.getvalue())
conn.commit()
cur.close()
# print(create_table_query)
# print(copy_qyery)
# print(csv_data.getvalue())