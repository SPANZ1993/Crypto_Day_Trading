import numpy as np
import pandas as pd


def train_test_split_by_start_end(args_list, train_split = 0.75):
  # DO A TRAIN TEST SPLIT BUT DON'T SHUFFLE AT ALL (TEST SET IS END OF DF)

  assert(len(set([len(a) for a in args_list]))==1)
  l = len(args_list[0])
  tsl = int(float(l)*train_split)

  mask = [False] * l
  train_mask = [mask[i] if i > tsl else True for i in range(len(mask))]
  test_mask = np.logical_not(train_mask)

  args_list_train = [a[train_mask] for a in args_list]
  args_list_test = [a[test_mask] for a in args_list]
  return args_list_train, args_list_test




def pull_out_nan_y_col_vals(data, y_col):
  # DROP ANY ROWS FROM THE DATAFRAME FOR WHICH THE PREDICTION COLUMN IS str(NaN)

  good_mask = [str(x).replace('.', '').replace('-', '').isnumeric() and not np.isnan(x) and not np.isinf(x) for x in data[y_col].values]
  return data[good_mask]




def drop_nan_cols(X):
  # DROP COLUMNS FROM A DATAFRAME THAT CONTAIN NANS

  df = pd.DataFrame(X)
  df = df.replace([np.inf, -np.inf], np.nan)
  df = df.dropna(axis='columns')
  X = df.values
  return X

def replace_nan_str_with_nan(data):
  # CHANGE THE STRING REP OF NAN AND INF TO THE NUMERICAL REP
  # I THINK THIS WORKS WITH DFS AND NP ARRAYS

  data = data.replace('nan', np.nan)
  data = data.replace('-nan', -np.nan)
  data = data.replace('inf', np.inf)
  data = data.replace('-inf', -np.inf)
  return data


def percent_change_vals_below_above_threshold(preds, true_change, threshold):
  # Given Some Predictions and a threshold, Return the true changes for the predictions above and below your threshold

  below_threshold_mask = preds <= threshold
  above_threshold_mask = preds > threshold

  true_changes_below_threshold = true_change[below_threshold_mask]
  true_changes_above_threshold = true_change[above_threshold_mask]
  return true_changes_below_threshold, true_changes_above_threshold



def drop_samples_outside_y_percentile_range(arg_list, y, min_y_percentile=1, max_y_percentile=99):
  # TAKE A SUBSET OF Y AND OTHER DATASET ELEMENTS FOR WHICH Y VALUES ARE WITHIN A PERCENTILE RANGE WITHIN Y

  try:
    assert(min_y_percentile < max_y_percentile)
  except:
    raise Exception('Max Y Percentile Must Be Greater Than Min Y Percentile')


  arg_list_has_multiple_elements = False
  if isinstance(arg_list, list) and len(arg_list) != len(y):
    arg_list_has_multiple_elements = True

  good_y_percentile_mask = np.logical_and(y < np.percentile(y, max_y_percentile), y > np.percentile(y, min_y_percentile))

  arg_list_masked = None
  if arg_list_has_multiple_elements:
    arg_list_masked = [a[good_y_percentile_mask] for a in arg_list]
  else:
    arg_list_masked = arg_list[good_y_percentile_mask]
  y_masked = y[good_y_percentile_mask]
  return arg_list_masked, y_masked



if __name__ == '__main__':
    pass