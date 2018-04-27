from pyspark import SparkContext


sc = SparkContext(appName = "assign2")

# Assignment 3

# temps_file lines are in format:
# Station number;Date;Time;Air temperature;Quality
# 102170;2013-11-01;06:00:00;6.8;G
temps_file = sc.textFile("data/temperature-readings.csv")
# split each line
lines = temps_file.map(lambda line: line.split(";"))
# filter out invalid years
lines = lines.filter(lambda x: int(x[1][0:4]) >= 1960 and int(x[1][0:4]) <= 2014)
# create key-value pairs with key: (year, month, st_number) value: avg. temperature
temps = lines.map(lambda x: ((x[1][0:7], x[0]), float(x[3])))

sum_count = temps.combineByKey(lambda value: (value, 1),
                               lambda x, value: (x[0] + value, x[1] + 1),
                               lambda x, y: (x[0] + y[0], x[1] + y[1]))

average_temps = sum_count.map(lambda (key, (total_sum, count)): (key, total_sum / count))

average_temps.collectAsMap()
average_temps.saveAsTextFile("average_year_month_temps")

