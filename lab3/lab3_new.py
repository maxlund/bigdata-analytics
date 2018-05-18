from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp 
from datetime import datetime, timedelta
from pyspark import SparkContext

sc = SparkContext(appName="lab_kernel") 

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2]) 
    # haversine formula
    dlon = lon2 - lon1 
    dlat = lat2 - lat1

    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2 
    c = 2 * asin(sqrt(a))

    km = 6367 * c 
    
    return km

def gaussian(distance, h_value):
    return exp(-(distance**2 / h_value))

def date_distance(a, b):
    tot_days = datetime.strptime(a, "%Y-%m-%d") - datetime.strptime(b, "%Y-%m-%d")
    return abs(tot_days.days) % 365

def time_distance(a, b):
    td = datetime.strptime(a, '%H:%M:%S') - datetime.strptime(b, '%H:%M:%S')
    hours = td.seconds / (60*60)
    return min(hours, 24 - hours)

# if we reason that around 200km is a good threshold for how close two stations 
# should be, we tried different h-values to find an appropriate one
h_distance = 10000

# looking at some temperature diagrams, we reason that around 15 days might be a 
# good interval. also, since we have taken the distance threshold to be 200km, 
# the temperature difference of our day-of-year distance should be somewhat similar.
h_date = 45

# estimating a good time of day threshold is difficult because of the uneven
# hours of light in sweden depending on time of the year. we choose/guess that
# 4 hours might be reasonable
h_time = 4

# our targets to forecast are the following:
latitude = 58.41
longitude = 15.618
date = "2013-07-04"

# temps file lines are in format:
# Station number;Date;Time;Air temperature;Quality
# 102170;2013-11-01;06:00:00;6.8;G

# stations file lines are in format:
# Station number;Station name;Measurement height;Latitude;Longitude;Readings from;Readings to;Elevation
# 102170;ostmark-asarna;2.0;60.2788;12.8538;2013-11-01 00:00:00;2016-09-30 23:59:59;135.0

stations_file = sc.textFile("data/stations.csv")
temps_file = sc.textFile("data/temperature-readings.csv").sample(False, 0.1)
temps_lines = temps_file.map(lambda line: line.split(";"))
station_lines = stations_file.map(lambda line: line.split(";"))

# create key-value pairs of ((stnumber, date, timeofday), temperature)
temps = temps_lines.map(lambda line: ((line[0], line[1], line[2]), float(line[3])))
# filter out dates later than our target date
temps = temps.filter(
    lambda x: 
    (datetime.strptime(date, "%Y-%m-%d") - datetime.strptime(x[0][1], "%Y-%m-%d")).days > 0
)

# create a dict to broadcast of the stations long/lat values
stations = station_lines.map(lambda line: (line[0], (float(line[3]), float(line[4]))))
stations_dict = sc.broadcast(stations.collectAsMap())

print("*** filter done ***")

# calculate the distance (km) kernel values
distances_kernel_values = temps.map(
    lambda x:
    (
        # key of the rdd is (stnumber, date, timeofday)
        (x[0][0], x[0][1], x[0][2]), 
        # value of the rdd is (temperature, kv for distance)
        (x[1], gaussian(
            haversine(
                longitude,
                latitude,
                stations_dict.value[x[0][0]][1],
                stations_dict.value[x[0][0]][0]),
            h_distance))
    )
)

print("*** PRINTING: distance_kernel_values: ***")
print(distances_kernel_values.take(10))

# calculate date distance kernel values
dates_distances_kernel_values = distances_kernel_values.map(
    lambda x:
    (
        # key of the rdd is (stnumber, date, timeofday)
        (x[0][0], x[0][1], x[0][2]),
        # value of the rdd is (temperature, kv for distance, kv for date)
        (x[1][0], x[1][1], gaussian(date_distance(date, x[0][1]), h_date))
    )
)

print("*** PRINTING: dates_distance_kernel_values: ***")
print(dates_distances_kernel_values.take(10))

# persist this RDD for later calculations
dates_distances_kernel_values.cache()

# our time-of-days of interest
times = ["04:00:00","06:00:00","08:00:00","10:00:00","12:00:00","14:00:00",
         "16:00:00","18:00:00","20:00:00","22:00:00","00:00:00"]

all_kernels_sum = list()
i = 0
# calculate the time-of-day kernel values and sum with the other kernels
for time in times:
    all_kernels_sum.append(dates_distances_kernel_values.map(
    lambda x:
        (
            # key of the rdd is (stnumber, date, timeofday)
            # we remap the key to a constant value that is the same for all key-value pairs
            "key",
            # value of the rdd is (temperature, sum(kv for distance, kv for date, kv for time))
            (x[1][0], x[1][1] + x[1][2] + gaussian(time_distance(time, x[0][2]), h_time))
        )
    )
    )

    print("*** PRINTING: all_kernels_sum: ***")
    print(all_kernels_sum[i].take(10))
    i += 1
      

temperature_predictions = {time: 0 for time in times}

for kernel, time in zip(all_kernels_sum, times):
    # since all keys are the same, the reduceByKey will produce the value 
    # (sum(kernel_value * temperature), sum(kernel_value))
    kernel = kernel.reduceByKey(lambda a, b: (a[0] * a[1] + b[0] * b[1], a[1] + b[1]))
    print("*** PRINTING: kernel (after reduceByKey: ***")
    print(kernel.take(10))
    # we can now calculate the final prediction as the weighted average
    pred = kernel.map(lambda x: (x[0], x[1][0] / x[1][1])).collectAsMap()
    print("*** PRINTING: pred ***")
    print(pred.take(10))
    temperature_predictions[time] = pred["key"]

print(temperature_predictions)
print("*** kernel values done ***")
