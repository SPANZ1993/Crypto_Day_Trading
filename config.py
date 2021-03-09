import os

database_folder_path = 'C:\\Users\\riggi\\Data\\Crypto_Day_Trading'
database_table_columns_path = 'C:\\Users\\riggi\\Data\\Crypto_Day_Trading\\Database_Table_Columns'

if not os.path.exists(database_folder_path):
    os.makedirs(database_folder_path)
if not os.path.exists(database_table_columns_path):
    os.makedirs(database_table_columns_path)