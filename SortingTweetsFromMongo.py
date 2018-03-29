import logging
from pymongo import MongoClient
import pymongo
import json
import urllib.parse
from bson import Binary, Code
from bson.json_util import dumps

handler = logging.FileHandler('sortMongo.log')
handler.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('myLogger')
logger.addHandler(handler)

credPath = '/Users/martikvm/PycharmProjects/DoubleSearch/credentialsMongo.txt'
credFile = open(credPath, 'r')
loginMA = urllib.parse.quote_plus(credFile.readline().strip())
passwordMA = urllib.parse.quote_plus(credFile.readline()).strip()
client = MongoClient("mongodb+srv://" +
                     loginMA + ":" +
                     passwordMA +
                     "@doublesearchintwitter-m3qge.mongodb.net")
db = client.twitter_database
tweets = db.tweets
logger.warning('Подключено к MongoDB.')

print(client.database_names())
tweets.create_index([("text", pymongo.ASCENDING)])
# pipeline = [
#     {"$group": {"_id": "$text", "count": {"$sum": 1}, "authors": {"$addToSet": "$user"}}},
#    {"$match": {"count": {"$gt": 1}}}
#]
# doublesTweets = tweets.aggregate(pipeline, {'allowDiskUse': 'true'})
line = ""
with open("/Users/martikvm/PycharmProjects/DoubleSearch/doubleTweetsFromMongo.txt", "w") as output:
    for doc in tweets.find().sort("text", 1):
        if line != doc["text"]:
            line = doc["text"]
            print(doc)
            output.write(dumps(doc))
            output.write("\n")
