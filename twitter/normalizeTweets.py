from pymongo import MongoClient
import urllib.parse
import re

credPath = 'credentialsMongo.txt'
credFile = open(credPath, 'r')
loginMA = urllib.parse.quote_plus(credFile.readline().strip())
passwordMA = urllib.parse.quote_plus(credFile.readline()).strip()
client = MongoClient("mongodb+srv://" +
                     loginMA + ":" +
                     passwordMA +
                     "@doublesearchintwitter-m3qge.mongodb.net")
db = client.twitter_database
tweets = db.tweets

allTweets = tweets.find().sort("text", 1)
keepAllTweets = []
print(allTweets.count())
for doc in allTweets:
    text = doc['text'].replace("\n", " ")
    text = re.sub("[^А-Яа-яёЁ]+", "", text)
    keepAllTweets.append(text.lower())
keepAllTweets.sort()
print(len(keepAllTweets))
with open('normalizedTweets.txt', 'w') as file:
    line = ""
    countDoubles = 0
    for tweet in keepAllTweets:
        if line != tweet:
            if countDoubles > 0:
                file.write(tweet + '\n' + str(countDoubles + 1) + '\n\n')
                countDoubles = 0
            line = tweet
        else:
            countDoubles += 1
