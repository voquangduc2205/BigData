from kafka import KafkaProducer
import SpotifyGetData
import pickle
import time
import json


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


spotipyGetData = SpotifyGetData.MySpotify()
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=json_serializer)
topic_artist = 'artists'
artists = spotipyGetData.artist_genre()

i = 0
for artist in artists:
    i = i+1
    print(i)
    print(artist)
    print('-----------------------------')
    producer.send(topic_artist, artist)
    time.sleep(1)
