import numpy as np
import pandas as pd
from datetime import datetime, timedelta,time,date
import os

currentDate = date.today()
currentDate = currentDate.strftime("%Y-%m-%dT%H:%M:%S")
currentDate = currentDate.split('T')[0]

filename = os.path.basename(__file__)
path = os.path.abspath(filename)
directory = os.path.dirname(path)
saveLocationLogs = directory + "\\Logs\\"

def drop_consecutive_duplicates(a):
    ar = a.values
    return ar[np.concatenate(([True],ar[:-1]!= ar[1:]))]

def logPostProcess():
    logText = []
    file = saveLocationLogs + f'{currentDate}' + '.log'

    with open(file) as f:
        lines = f.readlines()

    for line in lines:
        logText.append(line.split(' - ')[1])

    df = pd.DataFrame()
    df['logtext'] = logText
    a = df.logtext

    dfNew = pd.DataFrame()
    dfNew['finaltext'] = drop_consecutive_duplicates(a)
    dfNew.to_csv(file, index=False, header=False)
