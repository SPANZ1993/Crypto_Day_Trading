class TSFresh_Data_Extractor():

    def __init__(self, extract_data_func, feature_cols, prediction_cols, time_col='open_time', window_size=60,
                 timesteps_ahead=30):
        # extract_data_func: function that actually makes the TSfresh features
        # time_col: the column that we are using as our time column (database key)
        # window_size: The size of the window we want to examine # NOTE: The last index in the window is the current i
        # timesteps_ahead: The number of timesteps out we are trying to predict from the end of the window # NOTE: The number of timesteps in the future from the current i
        self.extract_data_func = extract_data_func
        self.feature_cols = feature_cols
        self.prediction_cols = prediction_cols
        self.time_col = time_col
        self.window_size = window_size
        self.timesteps_ahead = timesteps_ahead

    def _is_usable_extract_data(self, df, start_i, end_i, prediction_cols, feature_cols, time_col, window_size,
                                timesteps_ahead):
        # THIS IS A TEST TO MAKE SURE THE INPUTS WE ARE GIVING CAN BE PROCESSED BY AN EXTRACT
        # DATA FUNCTION
        print("START i:", start_i)
        print("END i:", end_i)
        if not all([c in df.columns for c in feature_cols]):
            raise Exception("Missing Feature Cols")
        if not time_col in df.columns:
            raise Exception("Missing Time Col")
        if not (start_i >= 0 and start_i < len(df)):
            raise Exception("Start index Out of Range")
        if not (end_i >= 0 and end_i < len(df)):
            raise Exception("End Index Out of Range")

    def _trim_is(self, df, start_i, end_i, window_size, timesteps_ahead):
        # IF WE ARE GOING TO ASK FOR DATA THAT IS "OFF THE EDGE" OF THE DATA
        # THAT WE HAVE, THEN CHANGE THE INDICES
        new_start_i = start_i
        new_end_i = end_i
        if start_i - window_size < 0:
            new_start_i = window_size
        if end_i + timesteps_ahead >= len(df):
            new_end_i = len(df) - timesteps_ahead - 1
        if new_start_i != start_i:
            print("CHANGED START i FROM", start_i, "TO", new_start_i)
        if new_end_i != end_i:
            print("CHANGED END i FROM ", end_i, "TO", new_end_i)
        return new_start_i, new_end_i

    def extract_data(self, df, start_i, end_i):
        start_i, end_i = self._trim_is(df=df, start_i=start_i, end_i=end_i, window_size=self.window_size,
                                       timesteps_ahead=self.timesteps_ahead)
        self._is_usable_extract_data(df=df, start_i=start_i, end_i=end_i, prediction_cols=self.prediction_cols,
                                     feature_cols=self.feature_cols, time_col=self.time_col,
                                     window_size=self.window_size, timesteps_ahead=self.timesteps_ahead)
        return self.extract_data_func(df=df, start_i=start_i, end_i=end_i, prediction_cols=self.prediction_cols,
                                      feature_cols=self.feature_cols, time_col=self.time_col,
                                      window_size=self.window_size, timesteps_ahead=self.timesteps_ahead)

if __name__ == '__main__':
    pass