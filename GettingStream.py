from twython import TwythonStreamer
import datetime, logging

APP_KEY = '32z3PM3ONqnCK6GKNZG5nn2a0'
APP_SECRET = 'LGEKF2o9YV8ppfUTxPe525VudHHJR7UkynSd8wbycDV69dhtGf'
OAUTH_TOKEN = '577060724-UmUdd7novSznvbZtke5KEEeEVpywxNGTBJM8dGma'
OAUTH_TOKEN_SECRET = 'i9Eq8kux7GGvntjphv1ZHchdhwchAci7J4wmrylYlfFHh'

handler = logging.FileHandler('retweets.log')
handler.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('myLogger')
logger.addHandler(handler)

class MyStreamer(TwythonStreamer):
    counter = 0

    def on_success(self, data):
        if 'text' in data and data['lang'] == 'ru':
            try:
                data['retweeted_status']
 #               logger.warning('Найден ретвит')
            except:
                print(data['text'] + '\n' +
                    str(data['id']),
                      file=open('/Users/martikvm/PycharmProjects/DoubleSearch/testing.txt', 'a'))
                self.counter += 1
#                if self.counter % 100 == 0:
#                    logger.warning('Найдено твитов:       ' + str(self.counter))

    def on_error(self, status_code, data):
        print(status_code)
        print(data)
#        logger.error(status_code)
 #       logger.error(data)
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
#        if datetime.datetime.now().day == 22 and datetime.datetime.now().hour == 0:
#            logger.warning('Сбор твитов окончен.')
#            break
    except Exception as exc:
        print(exc)
#        logger.error(exc)
        if (datetime.datetime.now() - start).seconds < 60:
            break
