from twython import TwythonStreamer
import datetime


APP_KEY = '32z3PM3ONqnCK6GKNZG5nn2a0'
APP_SECRET = 'LGEKF2o9YV8ppfUTxPe525VudHHJR7UkynSd8wbycDV69dhtGf'
OAUTH_TOKEN = '577060724-UmUdd7novSznvbZtke5KEEeEVpywxNGTBJM8dGma'
OAUTH_TOKEN_SECRET = 'i9Eq8kux7GGvntjphv1ZHchdhwchAci7J4wmrylYlfFHh'

class MyStreamer(TwythonStreamer):
    counter = 0
    def on_success(self, data):
            if 'text' in data and data['lang'] == 'ru':
                print(data['text'] + '\n', file=open('/Users/martikvm/PycharmProjects/DoubleSearch/result.txt', 'a'))
                self.counter += 1
            if self.counter % 100 == 0:
                print(self.counter)

    def on_error(self, status_code, data):
        print(status_code)
        print(data)
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
        if (datetime.datetime.now() - start).seconds < 60:
            break

