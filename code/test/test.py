
class CPV:
        pass

#  print all the variables insidea object with its value
for key, value in CPV.__dict__.items():
    if not key.startswith('__'):
        print(f"{key}: {value}")



# import pandas as pd
# from io import StringIO
# import psycopg as p
# # Create a sample DataFrame
# data = {
#     'Name': ['Alice', 'Bob', 'Charlie'],
#     'Age': [25, 30, 35],
#     'City': ['New York', 'Los Angeles', 'Chicago']
# }
# df = pd.DataFrame(data)
# csv_data = StringIO()
# df.to_csv(csv_data, index=False, header=True)


# conn = p.connect(
#     dbname='H1b_Analysis',
#     user='john_user',
#     password='abc@12345',
#     host='host.docker.internal',
#     port=5432
# )

# cur = conn.cursor()
# columns = ", ".join(df.columns)
# table_name  =  "h1b_analysis_stg.test_df"
# create_table_query = f"""
# CREATE TABLE IF NOT EXISTS {table_name} (
#     {", ".join([f"{col} TEXT" for col in df.columns])}
# )
# """
# cur.execute(create_table_query)
# conn.commit()
# copy_qyery  =  f"""
#         COPY {table_name} (
# {", ".join([col for col in df.columns])}
#         ) FROM STDIN WITH CSV
# """
# csv_data.seek(0)
# with cur.copy(statement =copy_qyery) as copy:
#      copy.write(csv_data.getvalue())
# conn.commit()
# cur.close()



import os

def print_directory_structure(root_dir, indent=""):
    for item in sorted(os.listdir(root_dir)):
        path = os.path.join(root_dir, item)
        if os.path.isdir(path):
            print(f"{indent}📂 {item}")
            print_directory_structure(path, indent + "  ")
        else:
            print(f"{indent}📄 {item}")

# Change 'your_project_directory' to the path of your project
project_dir = "/home/john_user/pyspark_learning"
print_directory_structure(project_dir)
