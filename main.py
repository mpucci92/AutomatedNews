from NewsTweetsVolume import *
from getData import *
from logPostProcessor import *


if __name__ == '__main__':
    """
    Change the value of '1' to a different value to change lookback period to obtain data
    """

    
    indexes = ['news','tweets']

    for index in indexes:
        dfVolumeDaily = newsVolumeDailyTS(index, 1, f'{currentDate}' + 'T23:59:59')
        saveData(index,dfVolumeDaily)

        df = finalDataFrame(index)
        finalSave(df, index)

    logPostProcess()




