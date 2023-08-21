

import time
import datetime

from enum import Enum

class TimeScale(Enum):
  SECONDS = 1
  MILLISECONDS = 1000
  MICROSECONDS = 1000000
  NANOSECONDS = 1000000000

def cur_time(timescale=TimeScale.MILLISECONDS):
  # Return the current Unix timestamp in milliseconds (UTC)
  return datetime_to_unix(datetime.datetime.now(datetime.timezone.utc), timescale=timescale)

def unix_to_datetime(t, timescale=TimeScale.MILLISECONDS):
  return datetime.datetime.fromtimestamp(int(t/timescale.value))


def datetime_to_unix(dt, timescale=TimeScale.MILLISECONDS):
  utc_time = dt.replace(tzinfo=datetime.timezone.utc)
  return round(utc_time.timestamp() * timescale.value)


if __name__ == '__main__':
  t = cur_time()
  print("TIMESTAMP: ", t)
  print("DATETIME: ", unix_to_datetime(t))

