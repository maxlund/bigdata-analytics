with open("/nfshome/hadoop_examples/shared_data/temperatures-big.csv") as f:
    data = [x.strip('\n') for x in f.readlines()]

data = [x.split(';') for x in data]
years = dict()

for entry in data:
    key = entry[1][0:4]
    value = float(entry[3])
    if key in years:
        if years[key] < value:
            years[key] = value
    else:
        years[key] = value

with open("temps_no_spark.txt", "wb") as f:
    for k, i in years.items():
        f.write(k + ": " i + "\n")
