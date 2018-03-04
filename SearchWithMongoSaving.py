from twython import TwythonStreamer
import datetime
import logging
from pymongo import MongoClient

APP_KEY = '32z3PM3ONqnCK6GKNZG5nn2a0'
APP_SECRET = 'LGEKF2o9YV8ppfUTxPe525VudHHJR7UkynSd8wbycDV69dhtGf'
OAUTH_TOKEN = '577060724-UmUdd7novSznvbZtke5KEEeEVpywxNGTBJM8dGma'
OAUTH_TOKEN_SECRET = 'i9Eq8kux7GGvntjphv1ZHchdhwchAci7J4wmrylYlfFHh'

handler = logging.FileHandler('pyMongo.log')
handler.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('myLogger')
logger.addHandler(handler)

client = MongoClient("mongodb+srv://vera_martina:Siol-3917@doublesearchintwitter-m3qge.mongodb.net/tweets")
db = client.twitter_database
tweets = db.tweets
tweets.delete_many({})

class MyStreamer(TwythonStreamer):

    def on_success(self, data):
        if 'text' in data and data['lang'] == 'ru':
            try:
                data['retweeted_status']
                logger.warning('Найден ретвит')
            except:
                tweet = {'text': data['text'],
                         'id': data['id'],
                         'created_at': data['created_at'],
                         'quote_status': data['is_quote_status'],
                         'quote_count': data['quote_count'],
                         'retweet_count': data['retweet_count'],
                         'reply_count': data['reply_count'],
                         'lang': data['lang'],
                         'user': {
                             'user_id': data['user']['id'],
                             'user_name': data['user']['name'],
                             'followers_count': data['user']['followers_count'],
                             'friends_count': data['user']['friends_count']
                            }
                         }
                tweets.insert_one(tweet)
                logger.warning(tweets.count())

    def on_error(self, status_code, data):
        print(status_code)
        print(data)
        logger.error(status_code)
        logger.error(data)
        self.disconnect()


stream = MyStreamer(APP_KEY, APP_SECRET,
                    OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

words = []
for line in open('/Users/martikvm/PycharmProjects/DoubleSearch/popular_words.txt'):
    words.append(line)
start = datetime.datetime.now()
while True:
    try:
        stream.statuses.filter(lang='ru', track=words)
    except Exception as exc:
        print(exc)
        logger.error(exc)
        if (datetime.datetime.now() - start).seconds < 60:
            break
