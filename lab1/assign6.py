from pyspark import SparkContext


sc = SparkContext(appName = "assign6")

# Assignment 6

# temps_file lines are in format:
# Station number;Date;Time;Air temperature;Quality
# 102170;2013-11-01;06:00:00;6.8;G

# precip_file lines are in format:
# Station number, Date, Time, Precipitation, Quality
# 103100;1995-08-01;00:00:00;0.0;Y

# ---------> READ THIS
### TODO : WOPS! It's supposed to be TEMPERATURE NOT PRECIPITATION!!! ####

precip_file = sc.textFile("data/precipitation-readings.csv")
station_file = sc.textFile("data/stations-Ostergotland.csv")
# split each line
precip_lines = precip_file.map(lambda line: line.split(";"))
station_lines = station_file.map(lambda line: line.split(";"))

# get only the Ostergotland station numbers
station_numbers = station_lines.map(lambda line: line[0])

# create a broadcast variable of these strings
station_numbers = sc.broadcast(station_numbers.collect())

# filter out only the ostergotland stations
precip_lines = precip_lines.filter(lamdba x: x[0][0] in station_numbers.value)

# filter out years 1950-2014 and create key-value pairs of (year+month, precip)
precip_1950_2014 = precip_lines.filter(lambda line: int(line[1][0:4]) <= 1950 and  int(line[1][0:4]) >= 2014)
precip_1950_2014 = precip_1950_2014.map(lambda line: (line[1][0:7], float(line[3])))

# filter out years 1950-1980 and create key-value pairs of (month, precip)
precip_1950_1980 = precip_lines.filter(lambda line: int(line[1][0:4]) <= 1950 and  int(line[1][0:4]) >= 1980)
precip_1950_1980 = precip_1950_1980.map(lambda line: (line[1][5:7], float(line[3])))

# get the average precipitation by station number for every month of every year in 1950-2014
sum_count_1950_2014 = precip_1950_2014.combineByKey(lambda value: (value, 1),
                                                    lambda x, value: (x[0] + value, x[1] + 1),
                                                    lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_precip_1950_2014 = sum_count_1950_2014.map(lambda (key, (total_sum, count)): (key, total_sum / count))

# get the average monthly precipitation by station number over all years 1950-1980
sum_count_1950_1980 = precip_1950_1980.combineByKey(lambda value: (value, 1),
                                                    lambda x, value: (x[0] + value, x[1] + 1),
                                                    lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_precip_1950_1980 = sum_count_1950_1980.map(lambda (key, (total_sum, count)): (key, total_sum / count))

# broadcast the total average over 1950-1980
average_var = sc.broadcast(sorted(average_precip_1950_1980.collect()))

# TODO : since broadcast var is sorted, we can index the list by slicing the month and getting the correct one
precip_differences = average_precip_1950_2014.map(lambda x: 

average_precip.saveAsTextFile("ostergotland_avg_precip")
