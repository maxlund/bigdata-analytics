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

precip_filter = sqlContext.sql(
    """
    SELECT station, MAX(daily_precip) AS max_precip
    FROM
    (SELECT station, SUM(precip) AS daily_precip FROM precip_readings
    GROUP BY station, date) dt
    GROUP BY station
    HAVING MAX(daily_precip) >= 100 AND MAX(daily_precip) <= 200
    """
)

precip_filter.registerTempTable("precip_filter")

temps_filter = sqlContext.sql(
    """
    SELECT station, MAX(temp) AS max_temp
    FROM temps_readings
    GROUP BY station
    HAVING MAX(temp) >= 25 AND MAX(temp) <= 30
    """
)

temps_filter.registerTempTable("temps_filter")

joined_result = sqlContext.sql(
    """
    SELECT t.station, t.max_temp, p.max_precip FROM
    temps_filter t
    INNER JOIN precip_filter p
    ON t.station = p.station
    """    
)

joined_result.rdd.saveAsTextFile("4_stations")

