from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

def myprint(x):
    print(x)

# Assignment 1
# a)
# temps_file lines are in format:
# Station number;Date;Time;Air temperature;Quality
# 102170;2013-11-01;06:00:00;6.8;G

temps_file = sc.textFile("data/temperature-readings.csv")
# split each line
lines = temps_file.map(lambda line: line.split(";"))
temps_readings = lines.map(lambda l: 
                        Row(
                            station=l[0],
                            year=l[1].split("-")[0],
                            month=l[1].split("-")[1],
                            time=l[2], 
                            value=float(l[3]), 
                            quality=l[4]
                        ))
schema_temps_readings = sqlContext.createDataFrame(temps_readings)
schema_temps_readings.registerTempTable("temps_readings")


# example query
# max1950 = sqlContext.sql("SELECT max(value) as value FROM temps_readings WHERE year=1950")

# example table using as df
# schemaTempReadingsMin = schemaTempReadings.groupBy('year', 'month', 'day', 'station').agg(F.min('value').alias('dailymin')).orderBy(['year', 'month', 'day', 'station'], ascending=[0,0,0,1])

count_temps = sqlContext.sql(
   """
   SELECT year, month, count(value) as temps_counts
    FROM temps_readings 
    WHERE year >= 1950 AND year <= 2014 AND value > 10 
    GROUP BY year, month
    ORDER BY temps_counts DESC
   """)

count_temps_stations = sqlContext.sql(
    """
    SELECT year, month, COUNT(station)
    FROM
    (SELECT DISTINCT year, month, station
    FROM temps_readings 
    WHERE year >= 1950 AND year <= 2014 AND value > 10) dt
    GROUP BY year, month
    """
)

count_temps.rdd.saveAsTextFile("2_count")
count_temps_stations.rdd.saveAsTextFile("2_count_stations")
