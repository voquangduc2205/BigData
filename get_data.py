import time
import datetime
import json
import requests
import numpy as np
import pandas as pd
import csv

#define get data API config
getCurrentWeatherUrl = "https://api.weather.com/v2/pws/observations/current"
getCurrentApiHeader = {}
getCurrentApiHeader["apiKey"] = "e1f10a1e78da46f5b10a1e78da96f525"
getCurrentApiHeader['units'] = 'e'
getCurrentApiHeader['format'] = 'json' 

def readStation():
    df = pd.read_csv('city.csv', skipinitialspace=True)
    return df['pwsId']

def getAddres():
    df = pd.read_csv('city.csv', skipinitialspace=True)
    return df['address']
    
def getCurrentWeatherInfo(strationId):
    getCurrentApiHeader['stationId'] = strationId
    res = requests.get(getCurrentWeatherUrl, getCurrentApiHeader)
    if res.status_code != 200:
        if res.status_code == 204:
            return 'No content weather'
        return strationId
    return res.json()

def getAllCurrentWeatherInfo():
    allIds = readStation()
    allAddress = getAddres()
    json_file = open('data.json','a')
    for i in range(len(allIds)):
        try:
            data = getCurrentWeatherInfo(allIds[i])
            if data != 'No content weather':
                print(i)
                data['city'] = allAddress[i]
                json.dump(data,json_file , 
                            indent=4,  
                            separators=(',',': '))        
        except:
            continue
while True:
    getAllCurrentWeatherInfo()
    print('Done')