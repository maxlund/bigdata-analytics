from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

# Assignment 4
# a)
# temps_file lines are in format:
# Station number;Date;Time;Air temperature;Quality
# 102170;2013-11-01;06:00:00;6.8;G

temps_file = sc.textFile("data/temperature-readings.csv")
precip_file = sc.textFile("data/precipitation-readings.csv")
# split each line
temps_lines = temps_file.map(lambda line: line.split(";"))
temps_readings = temps_lines.map(lambda l: 
                        Row(
                            station=l[0], 
                            date=l[1], 
                            year=l[1].split("-")[0], 
                            time=l[2], 
                            temp=float(l[3]), 
                            quality=l[4]
                        ))

precip_lines = precip_file.map(lambda line: line.split(";"))
precip_readings = precip_lines.map(lambda l: 
                        Row(
                            station=l[0], 
                            date=l[1], 
                            year=l[1].split("-")[0], 
                            time=l[2], 
                            precip=float(l[3]),
                            quality=l[4]
                        ))

schema_temps_readings = sqlContext.createDataFrame(temps_readings)
schema_temps_readings.registerTempTable("temps_readings")

schema_precip_readings = sqlContext.createDataFrame(precip_readings)
schema_precip_readings.registerTempTable("precip_readings")

# example query
# max1950 = sqlContext.sql("SELECT max(value) as value FROM temps_readings WHERE year=1950")

# example table using as df
# schemaTempReadingsMin = schemaTempReadings.groupBy('year', 'month', 'day', 'station').agg(F.min('value').alias('dailymin')).orderBy(['year', 'month', 'day', 'station'], ascending=[0,0,0,1])

stations = sqlContext.sql(
   """SELECT station as stnumber, max(temp) as max_temp, max(precip) as max_precip FROM
   (SELECT station, temp, precip
   FROM temps_readings
   INNER JOIN precip_readings ON temps_readings.station = precip_readings.station) D
   GROUP BY stnumber
   HAVING max_temp >= 25 AND max_temp <= 30 AND max_precip >= 100 AND max_precip <= 200""")

stations.rdd.saveAsTextFile("4_stations")

#[myprint(line) for line in max_temps.rdd.collect()]
#[myprint(line) for line in min_temps.rdd.collect()]
