import numpy as np
import pandas as pd
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import datetime, timedelta,time,date
import glob
import time
import sys
import os
from SearchAPI import *
from CONFIG import configFile
from GenerateDataset import *
from Logger import *
#import seaborn as sns
#import matplotlib.pyplot as plt

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# Load the config file - pass ElasticSearch IP
CONFIG = configFile()
es_client = Elasticsearch([CONFIG['elasticsearchIP']],http_compress=True)

# time constants
dt = datetime.now()
localTime = dt.date()
currentDate = date.today()


def volumeIndicatorTimeSeriesAll(index, lookbackPeriod, timeframe, customDate=None, customInterval=None):
    """
        Method used to generate zipped list of timestamp,ticker,news count for all tickers as Time Series.

        index: index to query on (news or tweets) - Mandatory
        lookbackPeriod: periods to lookback - Mandatory
        timeframe: Preset timeframes or set to custom for custom date - Mandatory
        customDate: Optional - Use only if timeframe is set to custom
        customInterval: factor * hours, its factor to use to multiply by hours

        Returns a zipped list which can be used to generate dataframe that contains entire set of data
        USE ONLY if you want to collect ALL tickers for a given period - use function above for subset of tickers.

        """
    filename = os.path.basename(__file__)
    log = datalogger()
    log.info('Filename: ' + f'{filename}')
    log.info('Index: ' + f'{index}')
    log.info('Date: ' + f'{currentDate}')
    log.info('ElasticSearch Volume Timeseries Data Acquisition: ')

    startEndDates = {
        'startDate': [],
        'endDate': []
    }

    tickerList = []
    countList = []
    timestamp = []

    # logic for starting date for timeseries
    if customDate != None:
        date = pd.to_datetime(customDate)

    else:
        date = datetime.utcnow()

    for i in [i for i in [i for i in range(lookbackPeriod + 1)][::-1]][:-1]:

        formattedDateNowStart = date.strftime('%Y-%m-%dT%H:%M:%S')

        if timeframe == '1 minute':

            dayBeforeStart = pd.to_datetime(formattedDateNowStart) - timedelta(minutes=i)

            startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
            startEndDates['startDate'].append(startDate)

            formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
            dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - timedelta(minutes=i - 1)
            endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
            startEndDates['endDate'].append(endDate)

        elif timeframe == '1 hour':

            dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (timedelta(hours=i))

            startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
            startEndDates['startDate'].append(startDate)

            formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
            dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (timedelta(hours=i - 1))
            endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
            startEndDates['endDate'].append(endDate)

        elif timeframe == '1 day':

            dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (timedelta(days=i))

            startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
            startEndDates['startDate'].append(startDate)

            formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
            dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (timedelta(days=i - 1))
            endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
            startEndDates['endDate'].append(endDate)

        elif timeframe == '1 week':

            dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (7 * timedelta(days=i))

            startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
            startEndDates['startDate'].append(startDate)

            formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
            dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (7 * timedelta(days=i - 1))
            endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
            startEndDates['endDate'].append(endDate)

        elif timeframe == 'custom':

            dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (customInterval * timedelta(days=i))

            startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
            startEndDates['startDate'].append(startDate)

            formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
            dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (customInterval * timedelta(days=i - 1))
            endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
            startEndDates['endDate'].append(endDate)

        aggregation = NewsVolumeQuery()
        query = aggregation.allTickersQuery(index, startDate, endDate)

        try:
            res = es_client.search(index=index, body=query, size=1)

            for j in range(len(res['aggregations']['ticker_counts']['buckets'])):
                timestamp.append(endDate)
                ticker = res['aggregations']['ticker_counts']['buckets'][j]['key']
                tickerList.append(ticker)

                countList.append(res['aggregations']['ticker_counts']['buckets'][j]['doc_count'])

        except Exception as e:
            countList.append(None)

    log.info('Length of Timestamp List: ' + str(len(timestamp)))
    log.info('Length of Ticker List: ' + str(len(tickerList)))
    log.info('Length of Count List: ' + str(len(countList)))

    return list(zip(timestamp, tickerList, countList)), log

def newsVolumeDailyTS(index,lookback,customtime):
    global log
    list_of_tuples = (volumeIndicatorTimeSeriesAll(index, lookback, 'custom', customDate=customtime, customInterval=1))[0]
    log = (volumeIndicatorTimeSeriesAll(index, lookback, 'custom', customDate=customtime, customInterval=1))[1]
    df = pd.DataFrame(list_of_tuples, columns=['date', 'ticker','count'])
    df = df[df['count']>1]
    sorted_df = (df.sort_values(by='date',ascending=False))
    sorted_df['date'] = sorted_df['date'].apply(lambda x: x.split('T')[0])
    sorted_df['key'] = sorted_df['date']+sorted_df['ticker']

    log.info('Length of dataframe after Count filter: ' + str(len(df)))
    log.info('Length of dataframe after sorted and new column created: ' + str(len(sorted_df)))

    return sorted_df

def saveData(index,dataframe):
    filename = 'NewsTweetsVolume.py'
    path = os.path.abspath(filename)
    directory = os.path.dirname(path)
    saveLocation = directory + "\\Data\\"

    try:
        path = saveLocation + f'{currentDate}' +f'{index}.csv'
        dataframe.to_csv(path,index=False)
        log.info('Successfully sent: ' + path)

    except Exception as e:
        log.debug('ERROR SENDING FILE: ' + path)
