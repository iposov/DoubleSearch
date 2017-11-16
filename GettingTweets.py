from twython import Twython

APP_KEY = '32z3PM3ONqnCK6GKNZG5nn2a0'
APP_SECRET = 'LGEKF2o9YV8ppfUTxPe525VudHHJR7UkynSd8wbycDV69dhtGf'
OAUTH_TOKEN = '577060724-UmUdd7novSznvbZtke5KEEeEVpywxNGTBJM8dGma'
OAUTH_TOKEN_SECRET = 'i9Eq8kux7GGvntjphv1ZHchdhwchAci7J4wmrylYlfFHh'

twitter = Twython(
    APP_KEY, APP_SECRET,
    OAUTH_TOKEN, OAUTH_TOKEN_SECRET
)

# print(twitter.search(q='python', result_type='popular'))

results = twitter.cursor(twitter.search, q='python')
for result in results:
    print(result['lang'])
    print(result['text'])
