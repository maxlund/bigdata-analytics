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

monthly_station_avg = sqlContext.sql(
    """
    SELECT t.station, t.year, t.month, AVG(t.temp) AS station_avg
    FROM temps_readings t
    INNER JOIN oster_readings o
    ON t.station = o.station
    WHERE t.year >= 1950 AND t.year <= 2014
    GROUP BY t.station, t.year, t.month
    """
)

monthly_station_avg.registerTempTable("monthly_station_avg")

short_term_avg = sqlContext.sql(
    """
    SELECT year, month, AVG(station_avg) AS year_month_avg
    FROM monthly_station_avg
    GROUP BY year, month
    """
)

short_term_avg.registerTempTable("short_term_avg")

long_term_avg = sqlContext.sql(
    """
    SELECT month, AVG(year_month_avg) AS month_avg
    FROM short_term_avg
    WHERE year >= 1950 AND year <= 1980
    GROUP BY month
    """
)

long_term_avg.registerTempTable("long_term_avg")

temp_differences = sqlContext.sql(
    """
    SELECT s.year, s.month, (s.year_month_avg - l.month_avg) AS temp_difference
    FROM short_term_avg s
    INNER JOIN long_term_avg l
    ON l.month = s.month
    """
)

temp_differences.rdd.saveAsTextFile("6_diff")

