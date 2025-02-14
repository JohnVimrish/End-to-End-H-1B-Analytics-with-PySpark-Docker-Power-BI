import pandas as pd
from io import StringIO
class Pandas_Utilities:
    def __init__(self):
        self.dataframe = None
        self.multiple_tables = None

    def read_csv(self, file_path, **kwargs):
        dataframe = pd.read_csv(file_path, **kwargs)
        return dataframe

    def read_excel(self, file_path, sheet_name=None, usecols=None, **kwargs):
        dataframe = pd.read_excel(file_path, sheet_name=sheet_name, usecols=usecols, **kwargs)
        return dataframe

    def rename_columns(self, column_mapping, dataframe):
            dataframe.rename(columns=column_mapping, inplace=True)
            return  dataframe

    def extract_multiple_tables(self, file_path, sheet_name, table_names, usecols=None):
        pandasDF = pd.read_excel(file_path, sheet_name=sheet_name, header=None, usecols=usecols)
        groups = pandasDF[0].isin(table_names).cumsum()
        self.multiple_tables = {g.iloc[0, 0]: g.iloc[1:].dropna(how='all').rename(columns=g.iloc[1]) for k, g in pandasDF.groupby(groups)}
        return self.multiple_tables
    
    def convert_to_inmemory_csv_obj(self,dataframe) :
           csv_data = StringIO()
           dataframe.to_csv(csv_data, index=False, header=True)
           csv_data.seek(0)
           return  csv_data
   
    def handle_pandas_exception(self, func_name, exception):
        error_message = f"Error in function {func_name.__name__}: {str(exception)}"
        raise RuntimeError(error_message) from exception
    






