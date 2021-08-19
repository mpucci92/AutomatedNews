import logging
import os
from datetime import date

filename = os.path.basename(__file__)
path = os.path.abspath(filename)
directory = os.path.dirname(path)
saveLocationLogs = directory + "\\Logs\\"

currentDate = date.today()
currentDate = currentDate.strftime("%Y-%m-%dT%H:%M:%S")
currentDate = currentDate.split('T')[0]

def datalogger():

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(saveLocationLogs + f'{currentDate}' + '.log')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info('---START OF THE FILE---')
    logger.info('---Data Collection Process has Begun!---')

    return logger
