from NewsTweetsVolume import *
from getData import *


if __name__ == '__main__':
    # Step 1 - Generate News Volume per period - Set this to 1 to grab over the last day

    dfTweets = newsVolumeDailyTS('tweets',1, f'{currentDate}'+'T23:59:59')
    saveData('tweets',dfTweets)

    dfNews = newsVolumeDailyTS('news', 1, f'{currentDate}' + 'T23:59:59')
    saveData('news', dfNews)

    # Step 2 - create final dataframe and save to 'Final Data' folder

    df = finalDataFrame('news')
    finalSave(df, 'news')

    df = finalDataFrame('tweets')
    finalSave(df, 'tweets')




