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
oster_file = sc.textFile("data/stations-Ostergotland.csv")

# split each line
temps_lines = temps_file.map(lambda line: line.split(";"))
temps_readings = temps_lines.map(lambda l: 
                        Row(
                            station=l[0], 
                            date=l[1], 
                            year=l[1].split("-")[0], 
                            month=l[1].split("-")[1],
                            time=l[2], 
                            temp=float(l[3]), 
                            quality=l[4]
                        ))

oster_lines = oster_file.map(lambda line: line.split(";"))
oster_readings = oster_lines.map(lambda l: 
                        Row(
                            station=l[0]
                        ))

schema_temps_readings = sqlContext.createDataFrame(temps_readings)
schema_temps_readings.registerTempTable("temps_readings")

schema_oster_readings = sqlContext.createDataFrame(oster_readings)
schema_oster_readings.registerTempTable("oster_readings")

# example query
# max1950 = sqlContext.sql("SELECT max(value) as value FROM temps_readings WHERE year=1950")

# example table using as df
# schemaTempReadingsMin = schemaTempReadings.groupBy('year', 'month', 'day', 'station').agg(F.min('value').alias('dailymin')).orderBy(['year', 'month', 'day', 'station'], ascending=[0,0,0,1])

diff = sqlContext.sql(
    """
    SELECT t1.year, t1.month, (t1.avg_temp - t2.avg_monthly_temp) AS temp_difference
    FROM 
    (SELECT t.year, t.month, avg(t.temp) as avg_temp
    FROM temps_readings t 
    INNER JOIN oster_readings o1
    ON t.station = o1.station
    WHERE t.year >= 1950 AND t.year <= 2014
    GROUP BY t.year, t.month) t1 INNER JOIN
    (SELECT l.month, avg(l.temp_avg) as avg_monthly_temp
    FROM oster_readings o2
    INNER JOIN
    (SELECT station, year, month, avg(temp) AS temp_avg FROM temps_readings
    WHERE year >= 1950 AND year <= 1980
    GROUP BY station, year, month) l
    ON o2.station = l.station
    GROUP BY l.month) t2
    ON t1.month = t2.month
    """
)

diff.rdd.saveAsTextFile("6_diff")

