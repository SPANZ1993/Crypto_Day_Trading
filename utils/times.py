

import time
from datetime import datetime

def cur_time():
  # Return the current Unix timestamp in milliseconds
  return round(time.time() * 1000)

def unix_to_datetime(t):
  return datetime.fromtimestamp(float(t)/1000.0)


if __name__ == '__main__':
  import datetime
  print("SHOULD BE NOW: ", datetime.datetime.fromtimestamp(int(cur_time()/1000)))

