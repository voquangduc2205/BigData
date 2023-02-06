exit()
city_name_array = []
city_name_array = [line.rstrip() for line in open('city',encoding='utf-8')]
# print(city_name_array);

file_city = open('city.csv', 'w',encoding='utf-8',newline="")
csv_writer = csv.writer(file_city)
log_timeout = open('timeout.log', 'a')
getLocatioinApiUrl = 'https://api.weather.com/v3/location/search'
getLocatioinApiHeader = {}
getLocatioinApiHeader['apiKey'] = 'e1f10a1e78da46f5b10a1e78da96f525'
getLocatioinApiHeader['format'] = 'json'
getLocatioinApiHeader['language'] = 'en-US'

stations_data = []

fileHeader = ['pwsId','longitude','latitude','countryCode','country','locId','address','city']
csv_writer.writerow(fileHeader)
for city_name in city_name_array:
    city_data = []
    getLocatioinApiHeader['query'] = city_name
    try: 
        response = requests.get(getLocatioinApiUrl, getLocatioinApiHeader)
    except:
        log_timeout.write(city_name)
        log_timeout.write("\n")
        continue
    city_search_result =[]
    if response.status_code != 200 :
        continue
    res = response.json()    
    city_search_result = res['location']['city']
    index_expected_result = []
    for i in range(0, len(city_search_result)):
        if city_search_result[i] == city_name:
            index_expected_result.append(i)
    for i in index_expected_result:
        station = {}
        station["pwsId"] = res["location"]["pwsId"][i] 
        station["longitude"] = res["location"]["longitude"][i]
        station["latitude"] = res["location"]["latitude"][i]
        station["countryCode"] = res["location"]["countryCode"][i]
        station["country"] = res["location"]["country"][i]
        station["locId"] = res["location"]["locId"][i]
        station["address"] = res["location"]["address"][i]
        station["city"] = res["location"]["city"][i]
        stations_data.append(station)
        if station["pwsId"] != None:
            csv_writer.writerow(station.values())
        print(station["pwsId"])
    stations_data.append(city_data)
file_city.close()
log_timeout.close()
exit()








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
    station = {}
    
    station["address"] = jsonRes["location"]["address"][i]
    station["locId"] = jsonRes["location"]["locId"][i]
    station["city"] = jsonRes["location"]["city"][i]
    station["country"] = jsonRes["location"]["country"][i]
    station["countryCode"] = jsonRes["location"]["countryCode"][i]
    station["latitude"] = jsonRes["location"]["latitude"][i]
    station["longitude"] = jsonRes["location"]["longitude"][i]
    station["pwsId"] = jsonRes["location"]["pwsId"][i]
    city_data.append(station)


getCurrentWeatherUrl = "https://api.weather.com/v2/pws/observations/current"
getCurrentApiHeader = {}
getCurrentApiHeader["apiKey"] = "e1f10a1e78da46f5b10a1e78da96f525"
getCurrentApiHeader['units'] = 'e'
getCurrentApiHeader['format'] = 'json'

for station in city_data:
    getCurrentApiHeader['stationId'] = station['pwsId']
    res = requests.get(getCurrentWeatherUrl, getCurrentApiHeader)
    print(res.text)
# print(jsonRes)