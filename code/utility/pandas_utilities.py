import pandas as pd

class Pandas_Utilities:
    def __init__(self):
        self.dataframe = None
        self.multiple_tables = None

    def read_csv(self, file_path, **kwargs):
        self.dataframe = pd.read_csv(file_path, **kwargs)
        return self.dataframe

    def read_excel(self, file_path, sheet_name=None, usecols=None, **kwargs):
        self.dataframe = pd.read_excel(file_path, sheet_name=sheet_name, usecols=usecols, **kwargs)
        return self.dataframe

    def rename_columns(self, column_mapping):
        if self.dataframe is not None:
            self.dataframe.rename(columns=column_mapping, inplace=True)

    def extract_multiple_tables(self, file_path, sheet_name, table_names, usecols=None):
        pandasDF = pd.read_excel(file_path, sheet_name=sheet_name, header=None, usecols=usecols)
        groups = pandasDF[0].isin(table_names).cumsum()
        self.multiple_tables = {g.iloc[0, 0]: g.iloc[1:].dropna(how='all') for k, g in pandasDF.groupby(groups)}
        return self.multiple_tables
   
    def handle_pandas_exception(self, func_name, exception):
        error_message = f"Error in function {func_name.__name__}: {str(exception)}"
        raise RuntimeError(error_message) from exception