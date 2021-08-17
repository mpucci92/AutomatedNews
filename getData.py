from elasticsearch import Elasticsearch
from SearchAPI import *
from GenerateDataset import *
from Logger import *
from CONFIG import configFile
import numpy as np
import pandas as pd
import itertools
from datetime import datetime, timedelta,time,date
import json
import glob
import time
import sys
import os

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# Time Constants
currentDate = date.today()
currentDate = currentDate.strftime("%Y-%m-%dT%H:%M:%S")
currentDate = currentDate.split('T')[0]

yesterdayDate = (datetime.now() - timedelta(days=1))
yesterdayDate = yesterdayDate.strftime("%Y-%m-%dT%H:%M:%S")
yesterdayDate = yesterdayDate.split('T')[0] + 'T00:00:00'

# ElasticSearch Configuration
CONFIG = configFile()
es_client = Elasticsearch([CONFIG['elasticsearchIP']],http_compress=True)

# Saving Data Path Location
filename = 'getData.py'
path = os.path.abspath(filename)
directory = os.path.dirname(path)
saveLocation = directory + "\\Data\\"

saveLocationLogs = directory + "\\Logs\\"
finalSaveLocationNews =  directory + "\\Final Data\\News\\"
finalSaveLocationTweets = directory + "\\Final Data\\Tweets\\"

def themedf(index,keyword,start_time,end_time):
    """
    index: Index to query on the ElasticSearch Cluster
    keyword: "" to get general query or specify 'keyword' for specific keyword search
    start_time: Start period of time %Y-%m-%d HH:MM:SS
    end_time: end period of time %Y-%m-%d HH:MM:SS

    return: Returns either tweet or news article dataframe with timestamp, title/tweet, tickers/cashtag,
            sentiment score, length (how many tags per title/tweet)   Length of tags 1 to 4(inclusive)
    """

    apisearch = APISearch()
    items=[]

    if index == 'news':

        log = datalogger(index, 'getData.py')
        log.info('Filename: getData.py')
        log.info('Index: ' + f'{index}')
        log.info('Date: ' + f'{currentDate}')
        log.info('ElasticSearch Data Acquisition: ')

        query = (APISearch.search_news(apisearch, search_string=keyword, timestamp_from=start_time, timestamp_to=end_time))
        res = es_client.search(index=index, body=query, size=10000,scroll='2m')
        scroll_id = res['_scroll_id']
        while True:

            if len(res['hits']['hits']) > 0:
                items.extend(res['hits']['hits'])
            else:
                break

            res = es_client.scroll(scroll_id=scroll_id, scroll='2m')
            log.info("Number of items: " + str(len(items)))

        df = GenerateDataset(index)


        dfTicker = GenerateDataset.createDataStructure(df, items)
        dfTicker.drop_duplicates(subset=['title'],inplace=True,ignore_index=True)
        dfTicker['length'] = dfTicker['tickers'].str.len()
        dfTicker = dfTicker.loc[:, ['published_datetime', 'title','sentiment_score','tickers','length']]
        dfTicker['published_datetime'] = dfTicker['published_datetime'].apply(lambda x: x.split('T')[0])
        dfTicker = dfTicker[(dfTicker['length']>=1) & dfTicker['length']<=4]

        log.info('Completion of raw ElasticSearch Dataframe and document taggging')
        log.info('Length of Dataframe, dfTicker: ' + str(len(dfTicker)))

    elif index == 'tweets':

        log = datalogger(index, 'getData.py')
        log.info('Filename: getData.py')
        log.info('Index: ' + f'{index}')
        log.info('Date: ' + f'{currentDate}')
        log.info('ElasticSearch Data Acquisition: ')

        query = (APISearch.search_tweets(apisearch, search_string=keyword, timestamp_from=start_time, timestamp_to=end_time))
        res = es_client.search(index=index, body=query, size=10000,scroll='2m')
        scroll_id = res['_scroll_id']
        while True:

            if len(res['hits']['hits']) > 0:
                items.extend(res['hits']['hits'])
            else:
                break

            res = es_client.scroll(scroll_id=scroll_id, scroll='2m')
            log.info("Number of items: " + str(len(items)))

        df = GenerateDataset(index)

        dfTicker = GenerateDataset.createDataStructure(df, items)
        dfTicker.drop_duplicates(subset=['tweet'],inplace=True,ignore_index=True)
        dfTicker['length'] = dfTicker['cashtags'].str.len()
        dfTicker = dfTicker.loc[:, ['timestamp', 'tweet','sentiment_score','cashtags','length']]
        dfTicker['timestamp'] = dfTicker['timestamp'].apply(lambda x: x.split('T')[0])
        dfTicker = dfTicker[(dfTicker['length']>=1) & dfTicker['length']<=4]

        log.info('Completion of raw ElasticSearch Dataframe and document taggging')
        log.info('Length of Dataframe, dfTicker: ' + str(len(dfTicker)))

    return dfTicker

def finalDataFrame(index):
    if index == 'news':
        df = (themedf(index,'',yesterdayDate, f'{currentDate}' +'T23:00:00').explode('tickers').dropna())
        df['adj_sentiment_score'] = df['sentiment_score']/df['length']
        groupby_df = (
        df.groupby(['published_datetime', 'tickers']).agg({'adj_sentiment_score': ['mean']}).reset_index())

        dfnew = pd.DataFrame()
        dfnew['date'] = groupby_df['published_datetime']
        dfnew['ticker'] = groupby_df['tickers']
        dfnew['average sentiment'] = ((groupby_df.adj_sentiment_score)['mean'])
        dfnew['key'] = dfnew['date'] + dfnew['ticker']

        log.info('Length of Dataframe, dfnew: ' + str(len(dfnew)))

    elif index == 'tweets':
        df = (themedf(index, '', yesterdayDate, f'{currentDate}' + 'T23:00:00').explode('cashtags').dropna())
        df['adj_sentiment_score'] = df['sentiment_score']/df['length']
        groupby_df = (df.groupby(['timestamp', 'cashtags']).agg({'sentiment_score': ['mean']}).reset_index())

        dfnew = pd.DataFrame()
        dfnew['date'] = groupby_df.timestamp
        dfnew['ticker'] = groupby_df.cashtags
        dfnew['average sentiment'] = ((groupby_df.sentiment_score)['mean'])
        dfnew['key'] = dfnew['date'] + dfnew['ticker']

        log.info('Length of Dataframe, dfnew: ' + str(len(dfnew)))

    return dfnew

def finalSave(df,index):
    dfdata = pd.read_csv(saveLocation + f'{currentDate}' + f'{index}' + '.csv')
    cleandf = dfdata.merge(df, how='left', on='key').loc[:, ['date_x', 'ticker_x', 'count', 'average sentiment']]
    cleandf.columns = ['date', 'ticker', 'count', 'average sentiment']

    if index == 'news':
        try:
            path = f'{finalSaveLocationNews}'+f'{currentDate}'+'.csv'
            cleandf.to_csv(path,index=False)

            log.info('Successfully sent to Final Data News Directory: ' + path)

        except Exception as e:
            log.debug('ERROR SENDING FILE: ' + path)

    elif index == 'tweets':
        try:
            path = f'{finalSaveLocationTweets}' + f'{currentDate}'+'.csv'
            cleandf.to_csv(path, index = False)

            log.info('Successfully sent to Final Data Tweets Directory: ' + path)

        except Exception as e:
            log.debug('ERROR SENDING FILE: ' + path)