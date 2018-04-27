import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import collections

dictOfDoubles = {}
with open("/Users/martikvm/PycharmProjects/DoubleSearch/twitter/doubleTweetsFromMongo.txt", "r") as doublesFile:
    for line in doublesFile:
        lineStripped = line.strip()
        if lineStripped.isdigit():
            if lineStripped in dictOfDoubles:
                dictOfDoubles[lineStripped] += 1
            else:
                dictOfDoubles[lineStripped] = 1
print(list(dictOfDoubles.keys()))
print(list(dictOfDoubles.values()))

keysArray = dictOfDoubles.keys()
values = []
numKeys = []
for key in keysArray:
    numKeys.append(int(key))
numKeys = sorted(numKeys)
for key in numKeys:
    values.append(dictOfDoubles.get(str(key)))
dictOfDoubles.clear()
print(numKeys)
print(values)
for t in range(0, len(numKeys)):
    dictOfDoubles[numKeys[t]] = values[t]

x = np.arange(len(dictOfDoubles))
vals = list(dictOfDoubles.values())
plt.bar(x, vals)
plt.xticks(x, list(dictOfDoubles.keys()))
plt.show()
