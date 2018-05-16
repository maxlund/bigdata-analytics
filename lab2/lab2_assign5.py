from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

# Assignment 5
# a)
# temps_file lines are in format:
# Station number;Date;Time;Air temperature;Quality
# 102170;2013-11-01;06:00:00;6.8;G

precip_file = sc.textFile("data/precipitation-readings.csv")
oster_file = sc.textFile("data/stations-Ostergotland.csv")
# split each line

precip_lines = precip_file.map(lambda line: line.split(";"))
precip_readings = precip_lines.map(lambda l: 
                        Row(
                            station=l[0], 
                            date=l[1], 
                            year=l[1].split("-")[0], 
                            year_month=l[1].split("-")[0:2],
                            time=l[2], 
                            precip=float(l[3]),
                            quality=l[4]
                        ))

oster_lines = oster_file.map(lambda line: line.split(";"))
oster_readings = oster_lines.map(lambda l: 
                        Row(
                            station=l[0]
                        ))

schema_oster_readings = sqlContext.createDataFrame(oster_readings)
schema_oster_readings.registerTempTable("oster_readings")

schema_precip_readings = sqlContext.createDataFrame(precip_readings)
schema_precip_readings.registerTempTable("precip_readings")

stations = sqlContext.sql(
    """
    SELECT p.year_month, avg(p.precip_sum) as avg_monthly_precipitation
    FROM oster_readings o
    INNER JOIN
    (SELECT station, year_month, sum(precip) AS precip_sum FROM precip_readings
    WHERE year >= 1993 AND year <= 2016
    GROUP BY station, year_month) p
    ON o.station = p.station
    GROUP BY p.year_month
    ORDER BY p.year_month
    """
)

stations.rdd.saveAsTextFile("5_precip")
