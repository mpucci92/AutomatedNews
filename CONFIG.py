# FINAL #

import json

def configFile():
    with open(r"E:\Data\newsCONFIG.json", 'r') as json_file:
        data = json.load(json_file)

    return data

