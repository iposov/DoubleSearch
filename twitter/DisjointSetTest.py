from twitter.DisjointSet import DisjointSet

vertices = ['a', 'b', 'c', 'd', 'e', 'h', 'i']
parent = {}

for v in vertices:
    parent[v] = v

ds = DisjointSet(10)

ds.union(1, 2)
ds.union(5, 6)
print(ds.find(1))
print(ds.find(2))
print(ds.find(5))
print(ds.find(6))

ds.union(2, 5)
print('-----------')
print(ds.find(1))
print(ds.find(2))
print(ds.find(5))
print(ds.find(6))

def colour_tweets(rows_with_ids, edges):
    new_colour = 1
    for row in rows_with_ids:
        flag = True
        for id in row["ids"]:
            to_change = set()
            if flag:
                new_colour = edges[id]
                flag = False
            to_change.add(edges[id])

            edges[id] = new_colour
            for i in range(0, len(edges)):
                if edges[i] in to_change:
                    edges[i] = new_colour


def colour_tweets2(rows_with_ids, edges):
    for row in rows_with_ids:
        ids = row["ids"]
        for x in ids[1:]:
            edges.union(ids[0], x)

edges = list(range(20))
print(edges)
test_df = [{"ids": [1, 2, 3]}, {"ids": [5, 6, 7, 8]}, {"ids": [1, 2, 19]}, {"ids": [0, 2, 15]}, {"ids": [8, 3]}]
colour_tweets(test_df, edges)
print(edges)

edges = DisjointSet(20)
print(edges.colors_list())
test_df = [{"ids": [1, 2, 3]}, {"ids": [5, 6, 7, 8]}, {"ids": [1, 2, 19]}, {"ids": [0, 2, 15]}, {"ids": [8, 3]}]
colour_tweets2(test_df, edges)
print(edges.colors_list())

n = 2000000
edges = DisjointSet(n)
print(edges.colors_list()[:100])
test_df = []
for i in range(0, n - 4, 5):
    test_df.append({"ids": [i, i + 1, i + 2]})
print('----')
colour_tweets2(test_df, edges)
print(edges.colors_list()[:100])

#n = 20000
#edges = list(range(n))
#print(edges[:100])
#test_df = []
#for i in range(0, n - 4, 5):
#    test_df.append({"ids": [i, i + 1, i + 2]})
#print('----')
#colour_tweets(test_df, edges)
#print(edges[:100])