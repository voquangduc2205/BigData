import pymongo
import datetime

from pymongo import MongoClient
client = MongoClient()

mongo_url = 'mongodb+srv://ducvq:rocketdata@fake-database.iw3ot2b.mongodb.net/'

client = MongoClient(mongo_url)

db = client['test']

station_collection = db['station']

post = {"author": "Mike",
        "text": "My first blog post!",
        "tags": ["mongodb", "python", "pymongo"],
        "date": datetime.datetime.utcnow()}

print(db.list_collection_names())
station_collection.insert_one(post)