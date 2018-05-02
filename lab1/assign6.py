from pyspark import SparkContext


sc = SparkContext(appName = "assign6")

# Assignment 6

# temps_file lines are in format:
# Station number;Date;Time;Air tempserature;Quality
# 102170;2013-11-01;06:00:00;6.8;G

temps_file = sc.textFile("data/temperature-readings.csv")
station_file = sc.textFile("data/stations-Ostergotland.csv")
# split each line
temps_lines = temps_file.map(lambda line: line.split(";"))
station_lines = station_file.map(lambda line: line.split(";"))
# get only the Ostergotland station numbers
station_numbers = station_lines.map(lambda line: line[0])
# create a broadcast variable of these strings
station_numbers = sc.broadcast(station_numbers.collect())
# filter out only the ostergotland stations
temps_lines = temps_lines.filter(lambda x: x[0] in station_numbers.value)
# filter out years 1950-2014 and create key-value pairs of (year+month, temps)
temps_1950_2014 = temps_lines.filter(lambda line: int(line[1][0:4]) >= 1950 and  int(line[1][0:4]) <= 2014)
temps_1950_2014 = temps_1950_2014.map(lambda line: (line[1][0:7], float(line[3])))
# filter out years 1950-1980 and create key-value pairs of (month, temps)
temps_1950_1980 = temps_lines.filter(lambda line: int(line[1][0:4]) >= 1950 and  int(line[1][0:4]) <= 1980)
temps_1950_1980 = temps_1950_1980.map(lambda line: (line[1][5:7], float(line[3])))
# get the average tempsitation by station number for every month of every year in 1950-2014
sum_count_1950_2014 = temps_1950_2014.combineByKey(lambda value: (value, 1),
                                                    lambda x, value: (x[0] + value, x[1] + 1),
                                                    lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_temps_1950_2014 = sum_count_1950_2014.map(lambda (key, (total_sum, count)): (key, total_sum / count))
# get the average monthly tempsitation by station number over all years 1950-1980
sum_count_1950_1980 = temps_1950_1980.combineByKey(lambda value: (value, 1),
                                                    lambda x, value: (x[0] + value, x[1] + 1),
                                                    lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_temps_1950_1980 = sum_count_1950_1980.map(lambda (key, (total_sum, count)): (key, total_sum / count))
# create a dictionary/hasmap broadcast variable of the total average over 1950-1980
average_var = sc.broadcast(average_temps_1950_1980.collectAsMap())
# take the difference from the average_var values and the values in each (year+month, avg) key-value pair
temps_differences = average_temps_1950_2014.map(lambda x: (x[0], x[1] - average_var.value[x[0][-2:]]))

temps_differences.saveAsTextFile("ostergotland_avg_temp_diffs")
