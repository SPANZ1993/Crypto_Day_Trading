from Database.database_functions import *
from Binance_API.advanced_calls import get_klines, find_oldest_kline_open_time_available



class HistoricalKlineDataLoader():

    def __init__(self, symbol, interval, db_path, table_name):
        self.symbol = symbol
        self.interval = interval
        self.db_path = db_path
        self.table_name = table_name
        self.db_con = None # PLACEHOLDER FOR NOW
        self.column_dict = {
            'open_time': 'INTEGER',
            'open': 'REAL',
            'high': 'REAL',
            'low': 'REAL',
            'close': 'REAL',
            'volume': 'REAL',
            'close_time': 'INTEGER',
            'quote_asset_volume': 'REAL',
            'number_of_trades': 'INT',
            'taker_buy_base_asset_volume': 'REAL',
            'taker_buy_quote_asset_volume': 'REAL'
        }
        self.primary_key = 'open_time'
        self.limit = 1000
        self.interval_to_time_offset_dict = {  # Number of milliseconds per range
            '1m': 60000,
            '3m': 180000,
            '5m': 300000,
            '15m': 900000,
            '30m': 1800000,
            '1h': 3600000,
            '2h': 7200000,
            '4h': 14400000,
            '6h': 21600000,
            '8h': 28800000,
            '12h': 43200000,
            '1d': 86400000,
            '3d': 259200000,
            '1w': 604800000,
            '1M': 2628000000
        }

    def __exit__(self):
        if self.db_con is not None:
            try:
                self.db_con.close()
            except:
                pass
        # If we happened to pull any duplicate columns then clean those up
        with create_connection(self.db_path) as con:
            delete_duplicate_rows(con, self.table_name)

    def run(self):
        try:
            with create_connection(self.db_path) as con:

                if not test_db_exists(con, self.table_name):
                    # Make the table if it doesn't exist
                    create_table(con, self.table_name, self.column_dict, primary_key=self.primary_key)
                    # Find the earliest possible time available for the symbol and interval we are interested in
                    time.sleep(10)
                    latest_time = find_oldest_kline_open_time_available(symbol=self.symbol, interval=self.interval) - 1
                else:
                    # Pull the latest time for which we have data in our database table
                    latest_time = query_max_value_in_col(con, self.table_name, 'open_time') - 1

                print("THE LATEST TIME WAS: ", str(datetime.datetime.fromtimestamp(latest_time / 1000)))
                time.sleep(5)

                prev_latest_time = -1
                while True:
                    prev_latest_time = latest_time
                    print("PULLING FROM: ", str(datetime.datetime.fromtimestamp(latest_time / 1000)), "TO", str(
                        datetime.datetime.fromtimestamp(
                            (latest_time + (self.interval_to_time_offset_dict[self.interval] * 1000)) / 1000)))
                    time.sleep(0.1)
                    cur_klines = get_klines(symbol=self.symbol, interval=self.interval, start_time=latest_time,
                                            end_time=latest_time + (
                                                        self.interval_to_time_offset_dict[self.interval] * self.limit),
                                            limit=self.limit)
                    insert_rows(con, self.table_name, pd.DataFrame(cur_klines))
                    latest_time = query_max_value_in_col(con, self.table_name, 'open_time') - 1
                    if latest_time == prev_latest_time:  # Keep Going Until We Aren't Pulling New Data Anymore
                        break

                # If we happened to pull any duplicate columns then clean those up
                delete_duplicate_rows(con, self.table_name)
        except Exception as e:
            traceback.print_exc()
            print(e)
            try:
                with create_connection(self.db_path) as con:
                    delete_duplicate_rows(con, self.table_name)
            except:
                pass
        except KeyboardInterrupt:
            traceback.print_exc()
            try:
                with create_connection(self.db_path) as con:
                    delete_duplicate_rows(con, self.table_name)
            except:
                pass

if __name__ == '__main__':
    pass