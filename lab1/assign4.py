from pyspark import SparkContext


sc = SparkContext(appName = "assign4")

# Assignment 4

# temps_file lines are in format:
# Station number;Date;Time;Air temperature;Quality
# 102170;2013-11-01;06:00:00;6.8;G

# precip_file lines are in format:
# Station number, Date, Time, Precipitation, Quality
# 103100;1995-08-01;00:00:00;0.0;Y

temps_file = sc.textFile("data/temperature-readings.csv")
precip_file = sc.textFile("data/precipitation-readings.csv")
# split each line
temps_lines = temps_file.map(lambda line: line.split(";"))
precip_lines = precip_file.map(lambda line: line.split(";"))
# create key-value pairs of ((station_number, date), temperature)
temps = temps_lines.map(lambda line: (line[0], float(line[3])))
# create key-value pairs of ((station_number, date), precipitation)
precip = precip_lines.map(lambda line: ((line[0], line[1]), float(line[3])))
# sum up the total precipitation for a given station number at a given day
precip_day_sum = precip.reduceByKey(lambda a, b: a + b)
# map back to key-values of (station_number, total_precipitation)
precip = precip_day_sum.map(lambda x: (x[0][0], x[1]))
# filter out invalid temperatures and precipitations
temps = temps.filter(lambda x: x[1] >= 25 and x[1] <= 30)
precip = precip.filter(lambda x: x[1] >= 100 and x[1] <= 200)
# get max temperature and precipitation for each station number
temps_max = temps.reduceByKey(lambda a, b: a if a >= b else b)
precip_max = precip.reduceByKey(lambda a, b: a if a >= b else b)
# join the two RDDs to get key-values (station_number, (max_temp, max_precip))
combined_vals = temps_max.join(precip_max)
combined_vals.saveAsTextFile("station_number_max_temp_precip")
