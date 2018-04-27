from pyspark import SparkContext

sc = SparkContext(appName = "lab1")

# Assignment 1
# a)
# temps_file lines are in format:
# Station number;Date;Time;Air temperature;Quality
# 102170;2013-11-01;06:00:00;6.8;G
temps_file = sc.textFile("data/temperature-readings.csv")
# split each line
lines = temps_file.map(lambda line: line.split(";"))
# create key-value pairs of the year and the temperature incl. station number
#lines = lines.filter(lambda x: len(x[1]) > 4) # remove entries with missing yea
year_temps = lines.map(lambda x: (x[1][0:4], (float(x[3]), x[0])))
# filter out only years in the inteval 1950-2014
year_temps = year_temps.filter(lambda x: int(x[0]) >= 1950 and int(x[0]) <= 2014)
# reduce by year and sort on highest temperature
max_temps = year_temps.reduceByKey(lambda a, b: a if a[0] >= b[0] else b)
max_temps_sorted = max_temps.sortByKey(ascending=False, keyfunc=lambda k: k[1][0])
max_temps.saveAsTextFile("max_temps")
max_temps_sorted.saveAsTextFile("max_temps_sorted")
