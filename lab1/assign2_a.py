from pyspark import SparkContext

sc = SparkContext(appName = "assign2a")

# Assignment 2 a)
# temps_file lines are in format:
# Station number;Date;Time;Air temperature;Quality
# 102170;2013-11-01;06:00:00;6.8;G
temps_file = sc.textFile("data/temperature-readings.csv")
# split each line
lines = temps_file.map(lambda line: line.split(";"))
# filter out invalid years
lines = lines.filter(lambda x: int(x[1][0:4]) >= 1950 and int(x[1][0:4]) <= 2014)
# create key-value pairs of the ((year, month), temp)
month_temps = lines.map(lambda x: (x[1][0:7], float(x[3])))
# filter by temp over 10
temp_over = month_temps.filter(lambda x: x[1] > 10)
temp_over_per_month = temp_over.map(lambda x: (x[0], 1))
# count totals
counts = temp_over_per_month.reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("year_month_counts_a")
