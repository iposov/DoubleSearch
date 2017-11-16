import logging

path = '/Users/martikvm/PycharmProjects/DoubleSearch/result.txt'

file = open(path, 'r')

handler = logging.FileHandler('hello.log')
handler.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('myLogger')
logger.addHandler(handler)

try:
    tweets = file.read().split('\n\n')
    print(len(tweets))
    tweets.sort()
    count = 0
    for tweet in tweets:
        print(tweet + '\n', file=open('/Users/martikvm/PycharmProjects/DoubleSearch/resultSorted.txt', 'a'))
        count += 1
        if count % 1000 == 0:
            logger.warning('Отсортировано..   ' + str(count))
    print('Сортировка окончена')
except Exception as e:
    logger.warning(e)

tweets = open('/Users/martikvm/PycharmProjects/DoubleSearch/resultSorted.txt', 'r').read().split('\n\n')
lastTweet = tweets[0]
quantity = 1
print(len(tweets))
for n in range(1, len(tweets)):
    curTweet = tweets[n]
    if lastTweet == curTweet and quantity == 1:
        print(curTweet + '\n', file = open('/Users/martikvm/PycharmProjects/DoubleSearch/doubleTweets.txt', 'a'))
        quantity += 1
    elif lastTweet == curTweet and quantity > 1:
        quantity += 1
    elif quantity > 1:
        print(str(quantity) + '\n\n', file = open('/Users/martikvm/PycharmProjects/DoubleSearch/doubleTweets.txt', 'a'))
        quantity = 1
        lastTweet = curTweet
    else:
        lastTweet = curTweet
        quantity = 1
if quantity > 1:
    print(str(quantity), file=open('/Users/martikvm/PycharmProjects/DoubleSearch/doubleTweets.txt', 'a'))
