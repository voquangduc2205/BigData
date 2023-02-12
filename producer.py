from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic  

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

#define kafka config
bootstrap_servers = ['localhost:9092']

topic_list = []
topic_list.append(NewTopic(name="weather_data", num_partitions=5, replication_factor=1))
topic_list.append(NewTopic(name="temperature", num_partitions=5, replication_factor=1))
topic_list.append(NewTopic(name="humidity", num_partitions=5, replication_factor=1))
topic_list.append(NewTopic(name="wind", num_partitions=5, replication_factor=1))

#define a producer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

try:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
    value_serializer = json_serializer # function callable
    )
except Exception:
    print("Unable to create producer!")

try:
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')
    print("Connect to Kafka successfully!")
except Exception:
    print("Unable to connect Kafka!")
    
try:
    admin_client.create_topics(topic_list)
    print("Create topic successfully!")
except Exception:
    print("Unable to create topic!")

# define job to get data
def readStation():
    df = pd.read_csv('city.csv', skipinitialspace=True)
    return df['pwsId']

def getCurrentWeatherInfo(strationId):
    getCurrentApiHeader['stationId'] = strationId
    try:
        res = requests.get(getCurrentWeatherUrl, getCurrentApiHeader)
        if res.status_code != 200:
            if res.status_code == 204:
                return 'No content weather'
            return strationId
    except:
        print("Error when fetching data!")
    return res.json()

def write_json(new_data, filename='weather_data.json'):
    with open(filename,'r+') as file:
          # First we load existing data into a dict.
        file_data = json.load(file)
        # Join new_data with file_data inside emp_details
        file_data["emp_details"].append(new_data)
        # Sets file's current position at offset.
        file.seek(0)
        # convert back to json.
        json.dump(file_data, file, indent = 4)
 
    # python object to be appended

def readJson():
    input_file = open ('data.json')
    json_array = json.load(input_file)
    return json_array


def getAllCurrentWeatherInfo():
    cnt = 0
    jsonData = readJson()
    for item in jsonData:
        cnt +=1
        print(cnt)
        # producer.send('weather_data', item)
        producer.send('temperature', item)
        # producer.send('humidity', item)
        # producer.send('wind', item)
        time.sleep(1)
    # for i in allIds:
    #     now = datetime.datetime.now()
    #     raw_data = getCurrentWeatherInfo(i)
    #     print("Time get data:", datetime.datetime.now() - now)
    #     now = datetime.datetime.now()
    #     # print(raw_data)
    #     # try:
    #     #     if raw_data['observations'] is not None:
    #     #         cnt+= 1
    #     #         print('Number of records: ', cnt)
    #     #         producer.send('weather_data', raw_data)
    #     # except Exception as e:
    #     #     print(e)   
    #     #     print("Unable to send data to Kafka!")
    #     cnt += 1
    #     producer.send('weather_data', raw_data)
    #     print('Number of records: ', cnt)
    #     print("Time write data", datetime.datetime.now() - now)
   
allIds = readStation()  

    
getAllCurrentWeatherInfo()
print('====================================')
print("Done!")
print('====================================')
