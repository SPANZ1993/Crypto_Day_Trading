import time
import os


from Database import create_connection, query_entire_table, test_db_exists, delete_duplicate_rows, create_table, insert_rows, query_entire_columns
import config as c

class Historical_TSFresh_Data_Extraction_Manager():
    '''
    Class that takes in a path to a database containing raw API data
    and one or more TSFresh_Data_Extractors (each with a new db table name),
    which will calculate TSFresh features datasets from the API data and
    save them into the new database
    '''

    def __init__(self,
                 db_path,
                 data_table_name,
                 tsfresh_data_table_names,
                 tsfresh_data_extractors,
                 time_col='open_time',
                 tsfresh_df_primary_keys=None,
                 tsfresh_df_foreign_key_dicts=None,
                 n_samples_per_iteration=1000):
        # db_path: The path to the database we are using
        # data_table_name: The path to the original database table from kline_loader
        # tsfresh_data_table_name: The name of the new table we will be saving our data into
        # time_col: The column in the data table to use as the column name (will save this column in the new db as well so TSFresh features are related to a timestamp)
        # n_samples_per_iteration: The max number of timesteps we will calculate TSFresh features for per loop iteration (Probably shouldn't go much over 1000 or it freezes up)
        self.db_path = db_path
        self.data_table_name = data_table_name
        if hasattr(tsfresh_data_table_names, '__iter__') and isinstance(tsfresh_data_table_names[0], str) and not len(
                tsfresh_data_table_names) == 0:
            self.tsfresh_data_table_names = tsfresh_data_table_names
        else:
            self.tsfresh_data_table_names = [tsfresh_data_table_names]
        if not hasattr(tsfresh_data_extractors, '__iter__'):
            self.tsfresh_data_extractors = [tsfresh_data_extractors]
        else:
            self.tsfresh_data_extractors = tsfresh_data_extractors
        self.time_col = time_col
        self.n_samples_per_iteration = n_samples_per_iteration
        if tsfresh_df_primary_keys is None:
            self.tsfresh_df_primary_keys = [None] * len(self.tsfresh_data_table_names)
        if tsfresh_df_foreign_key_dicts is None:
            self.tsfresh_df_foreign_key_dicts = [None] * len(self.tsfresh_data_table_names)
    # def run(self):

    #   features = [[]*len(self.tsfresh_data_extractors)]
    #   with create_connection(self.db_path) as con:
    #     with create_connection(self.db_path) as con:
    #       all_data = query_entire_table(con, self.data_table_name)
    #     # THESE VALUES BELOW ARE LITERALLY JUST TO DETERMINE IF MORE DATA
    #     # IS CURRENTLY BEING ADDED TO THE DB
    #     min_time_in_db = None
    #     max_time_in_db = None
    #     prev_min_time_in_db = -1
    #     prev_max_time_in_db = -1
    #     while min_time_in_db != prev_min_time_in_db or max_time_in_db != prev_max_time_in_db:
    #       if min_time_in_db is not None and max_time_in_db is not None:
    #         prev_min_time_in_db = min_time_in_db
    #         prev_max_time_in_db = max_time_in_db
    #       min_time_in_db = query_min_value_in_col(con, self.data_table_name, self.time_col)
    #       max_time_in_db = query_max_value_in_col(con, self.data_table_name, self.time_col)
    #       print("MIN TIME: ", min_time_in_db)
    #       print("MAX TIME: ", max_time_in_db)
    #       print("PREV MIN TIME:", prev_min_time_in_db)
    #       print("PREV MAX TIME: ", prev_max_time_in_db)

    #       ################################################
    #       for i in range(0)
    #         for e in self.tsfresh_data_extractors:
    #           cur_features = e.extract_data(all_data, 1000, 2000)
    #       ################################################
    #       # FIGURE OUT WHICH 1000 Timestamps To Operate On
    #       time.sleep(30)

    def strip_chars_from_df_cols(self, df, chars=['"']):
        # We need to drop quotation marks from features dataframes
        # It screws up SQLite DB
        cols = list(df.columns)
        new_cols = []
        for i in range(len(cols)):
            cur_col = cols[i]
            for c in chars:
                cur_col = cur_col.replace(c, '')
            new_cols.append(cur_col)
        assert (len(new_cols) == len(set(new_cols)))
        df.columns = new_cols
        return df

    def run(self):
        features = []
        for i in range(len(self.tsfresh_data_extractors)):
            features.append([])
        with create_connection(self.db_path) as con:
            all_data = query_entire_table(con, self.data_table_name)
            print("ALL DATA: ", len(all_data))
        min_time_in_db = None
        max_time_in_db = None
        prev_min_time_in_db = -1
        prev_max_time_in_db = -1

        ################################################

        with create_connection(self.db_path) as con:
            # drop_table(con, cur_table_name) #SHOULD WE DO THIS? I DON'T THINK SO.
            min_len_saved_features = 0
            all_len_saved_features = []
            for cur_table_name in self.tsfresh_data_table_names:

                if not test_db_exists(con, cur_table_name):
                    pass
                else:
                    # REMOVE DUPS RIGHT AT THE BEGINNING
                    delete_duplicate_rows(con, cur_table_name)
                    cur_ts_data = query_entire_columns(con, cur_table_name, ['open_time'])
                    all_len_saved_features.append(len(cur_ts_data))
                    print("THIS DB IS LEN: ", len(cur_ts_data))
                    # if min_len_saved_features > len(cur_ts_data):
                    #     min_len_saved_features = len(cur_ts_data)
        # max_len_saved_features = 150000 # DEBUG
        if len(all_len_saved_features) != 0:
            min_len_saved_features = min(all_len_saved_features)
        else:
            min_len_saved_features = -1
        print("MLSF: ", min_len_saved_features)
        time.sleep(10)
        start_i = max(0, min_len_saved_features)
        end_i = start_i + self.n_samples_per_iteration
        print("START I: ", start_i)
        print("END I: ", end_i)
        while end_i <= len(all_data):
            s = time.time()
            end_i = start_i + self.n_samples_per_iteration
            if end_i <= len(all_data):
                for e_i, e in enumerate(self.tsfresh_data_extractors):
                    print(start_i, end_i)
                    cur_features = e.extract_data(all_data, start_i, end_i)
                    cur_features = self.strip_chars_from_df_cols(cur_features, chars=['"'])
                    assert (all(['"' not in c for c in cur_features]))
                    print(cur_features.values.shape)
                    features[e_i].append(cur_features)
                    # ---------------------------------------------------------------------------
                    assert (len(self.tsfresh_data_extractors) == len(self.tsfresh_data_table_names))
                    cur_df = features[e_i][len(features[e_i]) - 1]
                    print("_____________________________________________________________")
                    print(cur_features)
                    print("_____________________________________________________________")
                    cur_table_name = self.tsfresh_data_table_names[e_i]
                    cur_primary_key = self.tsfresh_df_primary_keys[e_i]
                    cur_foreign_key_dict = self.tsfresh_df_foreign_key_dicts[e_i]
                    print("COLS: ", cur_df.columns)
                    with create_connection(self.db_path) as con:
                        # drop_table(con, cur_table_name) #DEBUG?
                        print("TSF TN: ", self.tsfresh_data_table_names)
                        print("TSF TN[i]: ", self.tsfresh_data_table_names[e_i])
                        print("CTN: ", cur_table_name)
                        if not test_db_exists(con, cur_table_name):
                            col_dtype_d = {}
                            for c in cur_df.columns:
                                col_dtype_d[c] = 'REAL'
                            create_table(con, cur_table_name, col_dtype_d, primary_key=cur_primary_key, foreign_key_dict=cur_foreign_key_dict)
                        insert_rows(con, cur_table_name, cur_df)
                        print("########################################")
                        # print("LEN TABLE BEFORE: ", len(query_entire_table(con, cur_table_name)))
                        delete_duplicate_rows(con, cur_table_name)
                        # print("LEN TABLE AFTER: ", len(query_entire_table(con, cur_table_name)))
                        print("########################################")
            print("ITERATION TOOK: ",  time.time()-s)
            start_i = end_i
        with create_connection(self.db_path):
            #REMOVE DUPS AT THE END TOO
            for cur_table_name in self.tsfresh_data_table_names:
                delete_duplicate_rows(con, cur_table_name)


if __name__ == '__main__':
    pass
