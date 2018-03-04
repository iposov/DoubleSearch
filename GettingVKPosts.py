from vkstreaming import Streaming
import vkstreaming

response = vkstreaming.getServerUrl('3af6043f3af6043f3af6043f783aa93cb333af63af6043f6301786bc51f61b908819df3')
api = Streaming(response["endpoint"], response["key"])

api.del_all_rules()
for line in open('/Users/martikvm/PycharmProjects/DoubleSearch/popular_words_100_1.txt'):
    api.add_rules(line.lower(), line)
# api.add_rules('Сегодня', 'сегодня')

rules = api.get_rules()
for rule in rules:
    print(("{tag:15}:{value}").format(**rule))

@api.stream
def my_func(event):
    print("[{}]: {}".format(event['author']['id'], event['text']))

api.start()
