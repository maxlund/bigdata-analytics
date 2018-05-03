def myprint(x):
    print(x)

with open("min_temps.txt") as f:
    data = f.readlines()
    data = sorted(data, key=lambda x: x[11:15], reverse=True)
    [myprint(x) for x in data[:10]]
    print("------------------------")
    [myprint(x) for x in data[-10:]]
    #[myprint(x) for x in data[]]
    print(len(data))
