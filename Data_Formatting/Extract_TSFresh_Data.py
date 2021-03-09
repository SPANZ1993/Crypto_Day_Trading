
import os
import copy
import numpy as np
import pandas as pd
import tsfresh as ts
from tqdm import tqdm
from sklearn.preprocessing import MinMaxScaler

import config as c
from Database import create_connection, drop_table, query_entire_table, test_db_exists
from Data_Formatting import Historical_TSFresh_Data_Extraction_Manager, TSFresh_Data_Extractor



def convert_to_dataset(data, start_i, end_i, time_col='open_time', cols=None, window_size=60, timesteps_ahead=30, scale_data=True):
  # Convert a dataframe into a dataset suitable for ML
  # start_i: The first index to start at (Assume is at least equal to window_size)
  # time_col: The time column in the data (Makes sure we don't have any dups in this column and data is ordered by this):
  # cols: The columns being added to the dataset
  # window_size: The number of "moments" per datapoint in the dataset
  # timesteps_ahead: How far into the future to consider for predicting
  # pred_col: The column we are considering for prediction
  # pred_func: The function to pull out the actual prediction value (Ex: We may want to predict the max() 'high' in the next 30 timesteps)
  # scale_data: Do we want to MinMax Scale On a Per window basis? (You Probably Do)
  assert(start_i >= window_size)
  assert(end_i <= len(data)-timesteps_ahead-1)

  data = copy.deepcopy(data)
  if data.iloc[len(data)-1][time_col] ==  data.iloc[len(data)-2][time_col]:
    data.drop([len(data)-2], inplace=True)
    assert(not data.iloc[len(data)-1][time_col] ==  data.iloc[len(data)-2][time_col])
  if not list(data[time_col].values) == sorted(list(data[time_col].values)):
    data.sort_values(by=[time_col])
    assert(list(data[time_col].values) == sorted(list(data[time_col].values)))
  if cols is None:
    cols = [str(c) for c in data.columns]

  data = data[cols].values


  X = []
  print("Converting")
  for i in tqdm(range(start_i, end_i+1)):
    cur_data = data[i-window_size:i,:]
    if scale_data:
      scaler = MinMaxScaler()
      cur_data = scaler.fit_transform(cur_data)
    X.append(cur_data)

  return np.array(X)






def convert_dataset_to_tsfresh_dataset(X, col_names=None):
    # X is a 3d numpy array of "windowed data"
    # col_names... if provided... is the column name for each col vector in X (like a dataframe column)

    # Converts X into a dataframe of TSFresh Features
    n_samples = X.shape[0]
    n_timesteps = X.shape[1]
    n_signals = X.shape[2]
    if col_names is None:
        col_names = [chr(65 + i) for i in range(n_signals)]
    assert (len(col_names) == n_signals)
    assert ('time' not in col_names)
    assert ('id' not in col_names)
    d = {}
    for c in col_names:
        d[c] = []
    d['time'] = []
    d['id'] = []

    timestep_list = np.array(list(range(n_timesteps)))
    for sample_i in tqdm(range(n_samples)):
        d['time'].append(timestep_list)
        cur_id = np.array([sample_i] * n_timesteps)
        d['id'].append(cur_id)
        for signal_i in range(n_signals):
            cur_col_name = col_names[signal_i]
            d[cur_col_name].append(X[sample_i, :, signal_i])

    del X

    data = {}
    for k in d.keys():
        data[k] = np.concatenate(tuple(d[k]), axis=0)
        print('k:', len(data[k]))
    del d

    assert (len(set([len(data[k]) for k in data.keys()])) == 1)

    data = pd.DataFrame.from_dict(data)

    data = ts.extract_features(data, column_id="id", column_sort="time")

    return data



def extract_data_z_norm_data_vs_end_of_window(df, start_i, end_i, prediction_cols, feature_cols, time_col,
                                              window_size, timesteps_ahead, scale_data=True):
    # Pull Out The Feature Column Only, and Create TSFresh Features
    # for i in range(len())
    X = convert_to_dataset(df, start_i, end_i, time_col=time_col, cols=feature_cols, window_size=window_size,
                           timesteps_ahead=timesteps_ahead, scale_data=True)
    features = convert_dataset_to_tsfresh_dataset(X, col_names=feature_cols)

    # TODO: ALRIGHT BUDDY SO WE'VE GOT THAT CONVERT TO DATASET FUNCTION STARTED ABOVE
    # WE NEED TO FIGURE OUT A GOOD WAY TO RELIABLY TAKE A DATAFRAME
    # PULL OUT A CHUNK OF IT WITH THE START AND END I BUT ALSO PULL OUT THE
    # TARGET COLUMNS AND OTHER RELEVANT COLUMNS, ADDING THEM TO OUR DATAFRAME

    # .......HOPEFULLY THAT STUFF ABOVE WORKS....

    # TRY SPINNING UP A DATA EXTRACTION MANAGER AND PASSING IT
    # AN EXTRACTOR THAT USES THIS FUNCTION

    # PULL OUT OTHER STUFF WE WANT
    z_normed_scaled_vs_mean_arrs = []
    true_value_arrs = []
    window_mean_arrs = []
    window_std_arrs = []
    true_window_end_arrs = []
    time_arr = []

    for pi, prediction_col in enumerate(prediction_cols):
        cur_z_normed_scaled_vs_mean = []
        cur_true_value = []
        cur_window_mean = []
        cur_window_std = []
        cur_true_window_end = []


        for i in range(start_i, end_i + 1):
            if pi == 0:
                time_arr.append(df[time_col][i])
            cur_window = df[prediction_col].values[i - window_size:i]
            cur_prediction_window = df[prediction_col].values[i:i + timesteps_ahead]
            m = np.mean(cur_window)
            std = np.std(cur_window)
            w_end = cur_window[len(cur_window) - 1]
            true_val = cur_prediction_window[len(cur_prediction_window) - 1]
            scaled_val = np.divide(np.subtract(true_val, m), std)

            cur_z_normed_scaled_vs_mean.append(scaled_val)
            cur_true_value.append(true_val)
            cur_window_mean.append(m)
            cur_window_std.append(std)
            cur_true_window_end.append(w_end)
        for l in [cur_z_normed_scaled_vs_mean, cur_true_value, cur_window_mean, cur_window_std, cur_true_window_end]:
            assert (len(l) == len(features))

        z_normed_scaled_vs_mean_arrs.append(cur_z_normed_scaled_vs_mean)
        true_value_arrs.append(cur_true_value)
        window_mean_arrs.append(cur_window_mean)
        window_std_arrs.append(cur_window_std)
        true_window_end_arrs.append(cur_true_window_end)


    assert(len(set([len(x) for x in [z_normed_scaled_vs_mean_arrs, true_value_arrs, window_mean_arrs, window_std_arrs, true_window_end_arrs]]))==1)
    assert (len(set([len(x[0]) for x in [z_normed_scaled_vs_mean_arrs, true_value_arrs, window_mean_arrs, window_std_arrs, true_window_end_arrs]])) == 1)
    assert(len(time_arr) == len(features))

    # ADD THAT NEWLY PULLED OUT STUFF TO OUR FEATURES

    for i, prediction_col in enumerate(prediction_cols):
        z_normed_scaled_vs_mean_col_name = '_' + str(prediction_col) + '_normed_scaled_vs_mean'
        true_value_col_name = '_' + str(prediction_col) + '_true_value'
        window_mean_col_name = '_' + str(prediction_col) + '_window_mean'
        window_std_col_name = '_' + str(prediction_col) + '_window_std'
        true_window_end_col_name = '_' + str(prediction_col) + '_true_window_end'

        features[z_normed_scaled_vs_mean_col_name] = z_normed_scaled_vs_mean_arrs[i]
        features[true_value_col_name] = true_value_arrs[i]
        features[window_mean_col_name] = window_mean_arrs[i]
        features[window_std_col_name] = window_std_arrs[i]
        features[true_window_end_col_name] = true_window_end_arrs[i]
    features[time_col] = time_arr

    # features['_high_z_normed_scaled_vs_mean'] = y_z_norm_scaled_vs_mean
    # features['_true_high'] = true_y
    # features['_high_window_mean'] = X_window_mean
    # features['_high_window_std'] = X_window_std
    # features['_high_window_end'] = X_true_window_end

   #features = features.dropna(axis=1)

    # print(features.head())
    # print(len(features.columns))

    return features


if __name__ == '__main__':

    db_path = os.path.join(c.database_folder_path, 'BTCUSD_1m.db')
    data_table_name = 'BTCUSD_1m'







    # tsfresh_data_table_name = 'BTCUSD_1m_tsfresh_features_z_normed_endpoint_minmax_scaled_X_30m_forward'
    #
    # _1m_60m_30m_data_extractor = TSFresh_Data_Extractor(
    #     extract_data_func=extract_data_z_norm_data_vs_end_of_window,
    #     feature_cols=['open',
    #                   'high',
    #                   'low',
    #                   'close',
    #                   'volume',
    #                   'quote_asset_volume',
    #                   'number_of_trades',
    #                   'taker_buy_base_asset_volume',
    #                   'taker_buy_quote_asset_volume'],
    #     prediction_cols=['open', 'close', 'high', 'low'],
    #     time_col='open_time',
    #     window_size=60,
    #     timesteps_ahead=30)
    #
    # extraction_manager = Historical_TSFresh_Data_Extraction_Manager(
    #     db_path=db_path,
    #     data_table_name=data_table_name,
    #     tsfresh_data_table_names=[tsfresh_data_table_name],
    #     tsfresh_data_extractors=[_1m_60m_30m_data_extractor],
    #     time_col='open_time',
    #     n_samples_per_iteration=1000)
    #
    # extraction_manager.run()

    ####################################################################################################################

    #
    # #IF SHIT GETS FUCKY AND YOU WANNA DROP THE TABLE
    # with create_connection(db_path) as con:
    #     drop_table(con, tsfresh_data_table_name)


    ####################################################################################################################
    # DO IT FOR EVERY FEATURE

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

    extractors = []
    table_names = []

    for feature in all_features:
        cur_table_name = tsfresh_data_table_name_base + feature

        cur_extractor = TSFresh_Data_Extractor(
            extract_data_func=extract_data_z_norm_data_vs_end_of_window,
            feature_cols=[feature],
            prediction_cols=['open', 'close', 'high', 'low'],
            time_col='open_time',
            window_size=60,
            timesteps_ahead=30)
        extractors.append(cur_extractor)
        table_names.append(cur_table_name)

        # with create_connection(db_path) as con:
        #     if test_db_exists(con, cur_table_name):
        #         drop_table(con, cur_table_name)


    extraction_manager = Historical_TSFresh_Data_Extraction_Manager(
        db_path=db_path,
        data_table_name=data_table_name,
        tsfresh_data_table_names=table_names,
        tsfresh_data_extractors=extractors,
        time_col='open_time',
        n_samples_per_iteration=1000)

    extraction_manager.run()