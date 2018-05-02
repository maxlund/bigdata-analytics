from pyspark import SparkContext


sc = SparkContext(appName = "assign6")

# Assignment 6

# tempss_file lines are in format:
# Station number;Date;Time;Air tempserature;Quality
# 102170;2013-11-01;06:00:00;6.8;G

# precip_file lines are in format:
# Station number, Date, Time, Precipitation, Quality
# 103100;1995-08-01;00:00:00;0.0;Y

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

# print('temp lines i station numbs:', temp_lines.collect()[:10])

# filter out years 1950-2014 and create key-value pairs of (year+month, temps)
temps_1950_2014 = temps_lines.filter(lambda line: int(line[1][0:4]) >= 1950 and  int(line[1][0:4]) <= 2014)
temps_1950_2014 = temps_1950_2014.map(lambda line: (line[1][0:7], float(line[3])))

# bla = temps_1950_2014.collect()
# print("temps 1950-2014", bla[:10])

# filter out years 1950-1980 and create key-value pairs of (month, temps)
temps_1950_1980 = temps_lines.filter(lambda line: int(line[1][0:4]) >= 1950 and  int(line[1][0:4]) <= 1980)
temps_1950_1980 = temps_1950_1980.map(lambda line: (line[1][5:7], float(line[3])))


# bla2 = temps_1950_1980.collect()
# print("temps 1950-1980", bla2[:10])

# get the average tempsitation by station number for every month of every year in 1950-2014
sum_count_1950_2014 = temps_1950_2014.combineByKey(lambda value: (value, 1),
                                                    lambda x, value: (x[0] + value, x[1] + 1),
                                                    lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_temps_1950_2014 = sum_count_1950_2014.map(lambda (key, (total_sum, count)): (key, total_sum / count))

# avg1 = average_temps_1950_2014.collect()
# print("avg 1950-2014", avg1[:10])

# get the average monthly tempsitation by station number over all years 1950-1980
sum_count_1950_1980 = temps_1950_1980.combineByKey(lambda value: (value, 1),
                                                    lambda x, value: (x[0] + value, x[1] + 1),
                                                    lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_temps_1950_1980 = sum_count_1950_1980.map(lambda (key, (total_sum, count)): (key, total_sum / count))


# avg2 = average_temps_1950_1980.collect()
# print("avg 1950-1980", avg2[:10])

# create a dictionary/hasmap broadcast variable of the total average over 1950-1980
average_var = sc.broadcast(average_temps_1950_1980.collectAsMap())

print(average_var.value)
# take the difference from the average_var values and the values in each (year+month, avg) key-value pair
temps_differences = average_temps_1950_2014.map(lambda x: (x[0], x[1] - average_var.value[x[0][-2:]]))

# print('temps_diffs', temps_differences.collect()[:10])

temps_differences.saveAsTextFile("ostergotland_avg_temp_diffs")
