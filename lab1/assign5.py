from pyspark import SparkContext


sc = SparkContext(appName = "assign5")

# Assignment 5

# temps_file lines are in format:
# Station number;Date;Time;Air temperature;Quality
# 102170;2013-11-01;06:00:00;6.8;G

# precip_file lines are in format:
# Station number, Date, Time, Precipitation, Quality
# 103100;1995-08-01;00:00:00;0.0;Y

precip_file = sc.textFile("data/precipitation-readings.csv")
station_file = sc.textFile("data/stations-Ostergotland.csv")
# split each line
precip_lines = precip_file.map(lambda line: line.split(";"))
station_lines = station_file.map(lambda line: line.split(";"))
# filter out correct years in precip rdd
precip_lines= precip_lines.filter(lambda line: int(line[1][0:4]) >= 1993 and int(line[1][0:4]) <= 2016)
# get only the Ostergotland station numbers
station_numbers = station_lines.map(lambda line: line[0])
# create a broadcast variable of these strings
station_numbers = sc.broadcast(station_numbers.collect())
# create key-value pairs of ((station_number, year+month), precipitation)
precip = precip_lines.map(lambda line: ((line[0], line[1][0:7]), float(line[3])))
#sum monthly precipitation for each station
sum_monthly = precip.reduceByKey(lambda x,y: x + y)
# filter out ostergotland stations in precipitation rdd
sum_filtered = sum_monthly.filter(lambda x: x[0][0] in station_numbers.value)
#map values to (value,1) to be able to count and remove station from key
sum_filtered = sum_filtered.map(lambda x: (x[0][1], (x[1], 1)))
#sum up all the precips and count the occurences to be able to average
final_sum = sum_filtered.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
#average the values by dividing by count
final_average = final_sum.map(lambda x: (x[0], x[1][0] / x[1][1]))
#output
final_average.saveAsTextFile("ostergotland_avg_precip")
