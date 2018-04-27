import csv

with open("/nfshome/hadoop_examples/shared_data/temperatures-big.csv", "rb") as f:
    data = csv.reader(f, delimiter=";")

    years = dict()

    for entry in data:
        key = entry[1][0:4]
        value = float(entry[3])
        if key in years:
            if years[key] < value:
                years[key] = value
            else:
                years[key] = value

# need to save?
# with open("maxtemps_no_spark.txt", "wb") as f:
#     for k, i in years.items():
#         f.write(k + ": " i + "\n")
