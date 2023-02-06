import requests
import json
import numpy as np
import pandas as pd
import csv

getCurrentWeatherUrl = "https://api.weather.com/v2/pws/observations/current"
getCurrentApiHeader = {}
getCurrentApiHeader["apiKey"] = "e1f10a1e78da46f5b10a1e78da96f525"
getCurrentApiHeader['units'] = 'e'
getCurrentApiHeader['format'] = 'json'

def readStation():
    df = pd.read_csv('city.csv', skipinitialspace=True)
    return df['pwsId']

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
    for i in allIds:
        print(getCurrentWeatherInfo(i))
        
while True:
    getAllCurrentWeatherInfo()

