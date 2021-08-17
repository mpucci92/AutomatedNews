import pandas as pd
from CONFIG import configFile

class GenerateDataset:
    """
    Class used to generate dataframe based on a particular ElasticSearch Query

    """

    def __init__(self, index):
        self.index = index.lower()

        self.tweetColumns = [
            'name',
            'tweet',
            'cashtags',
            'timestamp',
            'sentiment',
            'sentiment_score',
            'label'
        ]

        self.dictTweetStructures = {
            'timestamp': [],
            'name': [],
            'tweet': [],
            'sentiment': [],
            'sentiment_score': [],
            'label': [],
            'cashtags': []
        }

        self.newsColumns = [
            'title',
            'published_datetime',
            'tickers',
            'sentiment',
            'sentiment_score',
            'article_source'
        ]

        self.dictNewsStructures = {
            'title': [],
            'published_datetime': [],
            'tickers': [],
            'sentiment': [],
            'sentiment_score': [],
            'article_source': []
        }

    def createDataStructure(self, dataListDicts):

        if self.index == 'tweets':
            df = pd.DataFrame(columns=self.tweetColumns)

            for i in range(len(dataListDicts)):

                for feature in self.tweetColumns:
                    if feature in dataListDicts[i]['_source']:
                        self.dictTweetStructures[feature].append(dataListDicts[i]['_source'][feature])

                    else:
                        self.dictTweetStructures[feature].append(None)

            for col in self.tweetColumns:
                df[col] = self.dictTweetStructures[col]

        elif self.index == 'news':
            df = pd.DataFrame(columns=self.newsColumns)

            for i in range(len(dataListDicts)):

                for feature in self.newsColumns:
                    if feature in dataListDicts[i]['_source']:
                        self.dictNewsStructures[feature].append(dataListDicts[i]['_source'][feature])

                    else:
                        self.dictNewsStructures[feature].append(None)

            for col in self.newsColumns:
                df[col] = self.dictNewsStructures[col]

        return df
