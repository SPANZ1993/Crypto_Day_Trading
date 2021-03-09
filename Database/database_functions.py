import os
import sys
import copy
import requests
import json
import traceback
import datetime
import numpy as np
import pandas as pd
import tsfresh as ts
import time
import matplotlib.pyplot as plt
import sqlite3
from sqlite3 import Error
from tqdm import tqdm
import re


import config as c




# BASE DATABASE FUNCTIONS
# THESE CAN BE USED TO DO BASIC DATABASE OPERATIONS

# def _query(con, sql_command):
#     try:
#         return con.cursor().execute(sql_command).fetchall()
#     except Exception as e:
#         traceback.print_exc()
#         print(e)



def _query(con, sql_command, lock_timeout=None):
    try:
        # print(sql_command)
        if lock_timeout is None:
            while True:
                try:
                    print("TRYING TO QUERY: ", sql_command)
                    return con.cursor().execute(sql_command).fetchall()
                except Exception as e:
                    print(e)
                    if isinstance(e, sqlite3.OperationalError) and 'database is locked' in str(e):
                        print("DB Locked, waiting to execute: ", sql_command)
                        time.sleep(1)
        else:
            n_retries = 0
            while n_retries < n_retries:
                try:
                    return con.cursor().execute(sql_command).fetchall()
                except Exception as e:
                    if isinstance(e, sqlite3.OperationalError) and 'database is locked' in str(e):
                        print("DB Locked, waiting to execute: ", sql_command)
                        time.sleep(1)
                n_retries += 1
        raise Exception('Tried to Connect Too Many Times')
    except Exception as e:
        traceback.print_exc()
        print(e)










# def _execute(con, sql_command, lock_timeout=None):
#     try:
#         print(sql_command)
#         con.cursor().execute(sql_command)
#         return True
#     except Exception as e:
#         traceback.print_exc()
#         print(e)





def _execute(con, sql_command, lock_timeout=None):
    try:
        # print(sql_command)
        if lock_timeout is None:
            while True:
                try:
                    print("TRYING TO EXECUTE: ", sql_command)
                    con.cursor().execute(sql_command)
                    return True
                except Exception as e:
                    if isinstance(e, sqlite3.OperationalError) and 'database is locked' in str(e):
                        print("DB Locked, waiting to execute: ", sql_command)
                        time.sleep(1)
        else:
            n_retries = 0
            while n_retries < n_retries:
                try:
                    con.cursor().execute(sql_command)
                    return True
                except Exception as e:
                    if isinstance(e, sqlite3.OperationalError) and 'database is locked' in str(e):
                        print("DB Locked, waiting to execute: ", sql_command)
                        time.sleep(1)
                n_retries += 1
        raise Exception('Tried to Connect Too Many Times')
    except Exception as e:
        traceback.print_exc()
        print(e)








# def _execute_script(con, sql_command):
#     try:
#         # print(sql_command)
#         con.cursor().executescript(sql_command)
#         return True
#     except Exception as e:
#         traceback.print_exc()
#         print(e)

def _execute_script(con, sql_command, lock_timeout=None):
    try:
        # print(sql_command)
        if lock_timeout is None:
            while True:
                try:
                    con.cursor().executescript(sql_command)
                    return True
                except Exception as e:
                    if isinstance(e, sqlite3.OperationalError) and 'database is locked' in str(e):
                        print("DB Locked, waiting to execute: ", sql_command)
                        time.sleep(1)
        else:
            n_retries = 0
            while n_retries < lock_timeout:
                try:
                    con.cursor().executescript(sql_command)
                    return True
                except Exception as e:
                    if isinstance(e, sqlite3.OperationalError) and 'database is locked' in str(e):
                        print("DB Locked, waiting to execute: ", sql_command)
                        time.sleep(1)
                n_retries += 1
        raise Exception('Tried to Connect Too Many Times')
    except Exception as e:
        traceback.print_exc()
        print(e)


def _get_db_name(con):
    return os.path.split(con.cursor().execute("PRAGMA database_list").fetchall()[0][2])[1]



def create_connection(db_path):
    # Create a database connection to a SQLite database
    con = None
    try:
        con = sqlite3.connect(db_path)
        print('Connected at: ' + str(db_path))
        return con
    except Exception as e:
        traceback.print_exc()
        print("DB CONNECTION ERROR")
        print(e)



def test_db_exists(con, table_name):
    # Check if the table already exists in the database
    sql_command = """SELECT name FROM sqlite_master WHERE type='table' AND name='""" + str(table_name) + """';"""
    r = _query(con, sql_command)
    return r != [] and r[0][0] == table_name




def create_table(con, table_name, column_dict):
    # Create a database table at the given connection if None exists
    # Data dict contains column names and datatypes

    cnames = []
    db_col_name_folder = os.path.join(c.database_table_columns_path, _get_db_name(con))
    if not os.path.exists(db_col_name_folder):
        if not os.path.exists(os.path.split(db_col_name_folder)[0]):
            os.makedirs(os.path.split(db_col_name_folder)[0])
        os.makedirs(db_col_name_folder)

    col_sql = ''
    for i, cname in enumerate(column_dict.keys()):
        cnames.append(cname)
        col_sql += "'" + cname + "' " + column_dict[cname]
        if i != len(column_dict.keys()) - 1:
            col_sql += ', '
    np.save(os.path.join(db_col_name_folder, str(table_name) + '.npy'), np.array(cnames))
    sql_command = """CREATE TABLE """ + str(table_name) + """ (""" + col_sql + """)"""
    _execute(con, sql_command)




def query_entire_table(con, table_name):
    # Just pull the whole table
    db_col_name_folder = os.path.join(c.database_table_columns_path, _get_db_name(con))
    columns = np.load(os.path.join(db_col_name_folder, str(table_name) + '.npy'))

    sql_command = """SELECT * FROM """ + table_name
    if columns is None:
        return pd.DataFrame(_query(con, sql_command))
    else:
        return pd.DataFrame(_query(con, sql_command), columns=columns)



def query_entire_columns(con, table_name, columns):
    # Query the entire database column

    sql_command = """SELECT """
    for i, col in enumerate(columns):
        sql_command += col
        if i != len(columns)-1:
            sql_command += ", "
        else:
            sql_command += " "
    sql_command += """FROM """ + table_name
    return pd.DataFrame(_query(con, sql_command, lock_timeout=None), columns=columns)




def insert_rows(con, table_name, data):
    # Insert Some Rows In The Table
    # Data is a Pandas Dataframe with Matchin Column Names to the DB

    sql_command = """BEGIN TRANSACTION; """
    col_data = "'" + "', '".join(tuple([str(x) for x in data.columns])) + "'"
    for i, row in enumerate(data.values):
        row_data = "'" + "', '".join(tuple([str(x) for x in row])) + "'"
        sql_command += """INSERT INTO '""" + str(
            table_name) + """' (""" + col_data + """) VALUES (""" + row_data + """); """
    sql_command += """COMMIT;"""
    return _execute_script(con, sql_command)


# def delete_duplicate_rows(con, table_name):
#   # Delete any duplicate rows in the database

#   get_cols_command = """SELECT c.name FROM pragma_table_info('""" + str(table_name) + """') c;"""
#   cols = [x[0] for x in _query(con, get_cols_command)]
#   cols_sql = ""
#   for i, col in enumerate(cols):
#     cols_sql += "'" + col + "'"
#     #cols_sql += '"' + col + '"'
#     if i != len(cols)-1:
#       cols_sql += ', '
#   sql_command = """DELETE FROM '""" + str(table_name) + """' WHERE rowid NOT IN (SELECT MIN(rowid) FROM '""" + str(table_name) + """' GROUP BY """ + cols_sql + """)"""
#   print(sql_command[-100:])
#   return _execute(con, sql_command)


def delete_duplicate_rows(con, table_name, key_col='open_time'):  # FROM OTHER FILE
    # Delete any duplicate rows in the database
    print("TRYING TO REMOVE DUPS IN ", table_name)

    perform_delete = True  # SHOULD WE ACTUALLY DO THE DELETE

    if key_col is None:
        get_cols_command = """SELECT c.name FROM pragma_table_info('""" + str(table_name) + """') c;"""
        cols = [x[0] for x in _query(con, get_cols_command)]
    else:
        cols = [key_col]
        print("CHECKING WITH QUERY")
        key_col_df = query_entire_columns(con, table_name, cols)
        #print(key_col_df.head())
        if len(key_col_df[key_col].values) == len(set(key_col_df[key_col].values)):
            perform_delete = False

    if perform_delete:
        cols_sql = ""
        for i, col in enumerate(cols):
            cols_sql += '"' + col + '"'
            if i != len(cols) - 1:
                cols_sql += ', '
        sql_command = """DELETE FROM '""" + str(table_name) + """' WHERE rowid NOT IN (SELECT MIN(rowid) FROM """ + str(
            table_name) + """ GROUP BY """ + cols_sql + """)"""
        return _execute(con, sql_command)
    else:
        print("SKIPPING DUP DELETE FOR ", table_name)
        return True



def query_min_value_in_col(con, table_name, col_name):
    # Pulls the minimum value out of a numeric column in your table

    sql_command = """SELECT MIN(""" + str(col_name) + """) FROM """ + table_name
    return pd.DataFrame(_query(con, sql_command)).values[0][0]




def query_max_value_in_col(con, table_name, col_name):
    # Pulls the minimum value out of a numeric column in your table

    sql_command = """SELECT MAX(""" + str(col_name) + """) FROM """ + table_name
    return pd.DataFrame(_query(con, sql_command)).values[0][0]




def drop_table(con, table_name):
    # Pulls the minimum value out of a numeric column in your table

    sql_command = """DROP TABLE """ + table_name
    return _execute(con, sql_command)



if __name__ == '__main__':
    print("HELLO WORLD")
    db_path = os.path.join(c.database_folder_path, 'BTCUSD_1m.db')
    data_table_name = 'BTCUSD_1m'


    all_features = ['open',
                      'high',
                      'low',
                      'close',
                      'volume',
                      'quote_asset_volume',
                      'number_of_trades',
                      'taker_buy_base_asset_volume',
                      'taker_buy_quote_asset_volume']

    tsfresh_data_table_name_base = 'BTCUSD_1m_tsfresh_features_z_normed_endpoint_minmax_scaled_X_60m_window_30m_forward_'

    table_names = []
    for feature in all_features:
        table_names.append(tsfresh_data_table_name_base + feature)


    for table_name in table_names:
        with create_connection(db_path) as con:
            delete_duplicate_rows(con, table_name)