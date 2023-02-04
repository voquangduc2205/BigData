import requests
import json
city_name_array = [];
city_name_array = [line.rstrip() for line in open('city.txt')]
# print(city_name_array);

city_name = 'Tokyo'
search_endpoint = 'https://api.weather.com/v3/location/search?apiKey=e1f10a1e78da46f5b10a1e78da96f525&language=en-US&query=Toky&locationType=city%2Cairport%2CpostCode%2Cpws&format=json'
res = requests.get(search_endpoint)
# print(res.text)
jsonRes = json.loads(res.text)
city_search_result = jsonRes['location']['city']
index_expected_result = []
for i in range(0, len(city_search_result)):
    if city_search_result[i] == city_name:
        index_expected_result.append(i)

city_data = []

for i in index_expected_result:
    data = {}
    station = {}
    
    station["address"] = jsonRes["location"]["address"][i]
    station["locId"] = jsonRes["location"]["locId"][i]
    station["city"] = jsonRes["location"]["city"][i]
    station["country"] = jsonRes["location"]["country"][i]
    station["countryCode"] = jsonRes["location"]["countryCode"][i]
    station["latitude"] = jsonRes["location"]["latitude"][i]
    station["longitude"] = jsonRes["location"]["longitude"][i]
    
    data["station"] = station
    city_data.append(data)

print(city_data)

# print(jsonRes)