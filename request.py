import requests
import json

def getAPI(url, params=None):
    response = requests.get(url=url, params=params)
    return response.json()

# Send request with field1, field2 packaged in url
print(getAPI(url="https://api.thingspeak.com/update?api_key=T7H40F0X82VGW7L5&field1=20&field2=33"))

#Send request with field1, field2 packaged in body by json
params = {
    "field1": 20,
    "field2": 33
}
print(getAPI(url="https://api.thingspeak.com/update?api_key=T7H40F0X82VGW7L5", params=json.dumps(params)))

#Get temperature and humidity from response data
response = getAPI(url="https://api.thingspeak.com/channels/1529099/feeds.json?results=2")
result = {
    "temperature": response['feeds'][0]['field1'],
    "humidity": response['feeds'][0]['field2']
}
print(result)