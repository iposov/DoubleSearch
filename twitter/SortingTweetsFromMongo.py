import logging
from pymongo import MongoClient
import pymongo
import urllib.parse
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
print(client.database_names())
logger.warning('Подключено к MongoDB.')

tweets.create_index([("text", pymongo.ASCENDING)])
line = ""
countDoubles = 0
with open("/Users/martikvm/PycharmProjects/DoubleSearch/singleTweetsFromMongo.txt", "w") as output:
    with open("/Users/martikvm/PycharmProjects/DoubleSearch/doubleTweetsFromMongo.txt", "w") as doublesOutput:
        for doc in tweets.find().sort("text", 1):
            if line != doc["text"]:
                if countDoubles > 0:
                    doublesOutput.write(dumps(line, ensure_ascii=False))
                    doublesOutput.write("\n" + str(countDoubles + 1) + "\n\n")
                    countDoubles = 0
                line = doc["text"]
                print(doc)
                output.write(dumps(doc, ensure_ascii=False))
                output.write("\n")
            else:
                countDoubles += 1
