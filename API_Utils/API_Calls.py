# BASE LEVEL API CALLS
# THESE JUST CALL THE API

import os
import sys
import requests
import json
import traceback
import datetime
import numpy as np
import pandas as pd
import time
import matplotlib.pyplot as plt
import sqlite3
from sqlite3 import Error

from API_Utils.Response_Exceptions import BaseResponseException, TooManyRequestsException



def _rest_api_call(url):
    # Make the call with the given API
    try:
        rurl = r'{}'.format(url)
        r = requests.get(rurl)
        print("WOO HERES THE RESPONSE!!!:", r.status_code)

        if int(r.status_code) == 200:
            return json.loads(r.text)
        else:
            print("Failed API Call on URL: ", url)
            print("Bad Status Code: " + str(r.status_code))
            if int(r.status_code) == 429:
                raise(TooManyRequestsException(url=url, response_code=r.status_code))
            else:
                raise (BaseResponseException(url=url, response_code=r.status_code))
    except BaseResponseException as e:
        traceback.print_exc()
        raise (e)
    except:
        print("ERROR IN CALL: ", url)
        traceback.print_exc()
        return None
