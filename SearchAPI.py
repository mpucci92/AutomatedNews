# FINAL #

class APISearch:
    """
    API Used to perform general searches

    """

    def terms_filter(self, field, value):

        return {"terms": {field: value}}

    def range_filter(self, field, gte=None, lte=None):

        _filter = {}

        if lte:
            _filter.update({"lte": lte})

        if gte:
            _filter.update({"gte": gte})

        return {"range": {field: _filter}}

    def search_news(self, search_string="", sentiment=None, tickers=None, article_source=None, timestamp_from=None,
                    timestamp_to=None, sentiment_greater=None, sentiment_lesser=None, sentiment_field=None,
                    language=None, authors=None, categories=None, **kwargs):

        query = {
            "query": {
                "function_score": {
                    "query": {
                        "bool": {}
                    },
                    "field_value_factor": {
                        "field": "published_datetime",
                        "missing": 0,
                        "factor": 1
                    }
                }
            }
        }

        if search_string:
            query['query']['function_score']['query']['bool']['must'] = [
                {"match": {"search": search_string}}
            ]
        else:
            query['sort'] = {
                "published_datetime": {
                    "order": "desc"
                }
            }

        filters = []
        if sentiment:
            filters.append(self.terms_filter("sentiment", sentiment))

        if tickers:
            filters.append(self.terms_filter("tickers", tickers))

        if language:
            filters.append(self.terms_filter("language", language))

        if authors:
            filters.append(self.terms_filter("article_source", authors))

        if categories:
            filters.append(self.terms_filter("categories", categories))

        if article_source:
            filters.append(self.terms_filter("article_source", article_source))

        if timestamp_from or timestamp_to:
            filters.append(self.range_filter("published_datetime", timestamp_from, timestamp_to))

        if sentiment_greater or sentiment_lesser:
            filters.append(self.range_filter(sentiment_field, sentiment_greater, sentiment_lesser))

        query['query']['function_score']['query']['bool']['filter'] = filters

        return query

    def search_tweets(self, search_string="", sentiment=None, tickers=None, article_source=None, timestamp_from=None,
                      timestamp_to=None, sentiment_greater=None, sentiment_lesser=None, sentiment_field=None,
                      language=None,
                      authors=None, hashtags=None, replies_count=None, retweets_count=None, likes_count=None, **kwargs):

        query = {
            "query": {
                "function_score": {
                    "query": {
                        "bool": {}
                    },
                    "field_value_factor": {
                        "field": "timestamp",
                        "missing": 0,
                        "factor": 1
                    }
                }
            }
        }

        if search_string:
            query['query']['function_score']['query']['bool']['must'] = [
                {"match": {"tweet": search_string}}
            ]
        else:
            query['sort'] = {
                "timestamp": {
                    "order": "desc"
                }
            }

        filters = []

        if sentiment:
            filters.append(self.terms_filter("sentiment", sentiment))

        if tickers:
            filters.append(self.terms_filter("cashtags", tickers))

        if language:
            filters.append(self.terms_filter("language", language))

        if authors:
            filters.append(self.terms_filter("name", authors))

        if hashtags:
            filters.append(self.terms_filter("hashtags", hashtags))

        if timestamp_from or timestamp_to:
            filters.append(self.range_filter("timestamp", timestamp_from, timestamp_to))

        if sentiment_greater or sentiment_lesser:
            filters.append(self.range_filter(sentiment_field, sentiment_greater, sentiment_lesser))

        if replies_count:
            filters.append(self.range_filter("replies_count", replies_count))

        if retweets_count:
            filters.append(self.range_filter("retweets_count", retweets_count))

        if likes_count:
            filters.append(self.range_filter("likes_count", likes_count))

        query['query']['function_score']['query']['bool']['filter'] = filters

        return query


class NewsVolumeQuery:

    def tweetVolumeQuery(self,tickers, start_date, end_date, sort_field, sort_order):
        query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"cashtags": f"{tickers}"}},
                        {"range": {"timestamp": {"gte": f"{start_date}", "lte": f"{end_date}"}}}
                    ]
                }
            },
            "sort": [{f"{sort_field}": {"order": f"{sort_order}"}}]
        }

        return query

    def newsVolumeQuery(self,tickers, start_date, end_date, sort_field, sort_order):
        query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"tickers": f"{tickers}"}},
                        {"range": {"published_datetime": {"gte": f"{start_date}", "lte": f"{end_date}"}}}
                    ]
                }
            },
            "sort": [{f"{sort_field}": {"order": f"{sort_order}"}}]
        }

        return query

    def newsAggQuery(self,index,tickers,start_date, end_date):

        if index == 'news':

            query = {
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"tickers": f"{tickers}"}},
                            {"range": {"published_datetime": {"gte": f"{start_date}", "lte": f"{end_date}"}}}
                        ]
                    }
                },
                "aggs": {
                    "ticker_counts": {
                        "terms": {
                            "field": "tickers",
                            "size": 1
                        }
                    }
                }
            }

        elif index == 'tweets':
            query = {
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"cashtags": f"{tickers}"}},
                            {"range": {"timestamp": {"gte": f"{start_date}", "lte": f"{end_date}"}}}
                        ]
                    }
                },
                "aggs": {
                    "ticker_counts": {
                        "terms": {
                            "field": "cashtags",
                            "size": 1
                        }
                    }
                }
            }

        return query

    def allTickersQuery(self,index, start_date, end_date):

        if index == 'news':

            query = {
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {"published_datetime": {"gte": f"{start_date}", "lte": f"{end_date}"}}}
                        ]
                    }
                },
                "aggs": {
                    "ticker_counts": {
                        "terms": {
                            "field": "tickers",
                            "size": 10000
                        }
                    }
                }
            }

        elif index == 'tweets':
            query = {
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {"timestamp": {"gte": f"{start_date}", "lte": f"{end_date}"}}}
                        ]
                    }
                },
                "aggs": {
                    "ticker_counts": {
                        "terms": {
                            "field": "cashtags",
                            "size": 10000
                        }
                    }
                }
            }

        return query
