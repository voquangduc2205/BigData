import time
import json
from kafka import KafkaProducer
import requests

kafka_bootstrap_servers = ['127.0.0.1:9092']
kafka_topic_name = 'sample_topic1'

producer = KafkaProducer(bootsrap_servers=kafka_bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

json_message = None
city_name = None
temperature = None
humidity = None
openweathermap_api_endpoint = None
appid = None

def get_weather_detail(openweathermap_api_endpoint):
    api_response = requests.get(openweathermap_api_endpoint)
    json_data = api_response.json()
    print(json_data)

    return json_data


