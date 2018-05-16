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
                            date=l[1], 
                            year=l[1].split("-")[0], 
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

max_temps = sqlContext.sql(
   """
   SELECT DISTINCT t2.year, t2.station, t2.value
   FROM temps_readings t2
   INNER JOIN
   (SELECT year, max(value) AS max_value
   FROM temps_readings
   WHERE year >= 1950 AND year <= 2014
   GROUP BY year) t1
   ON t1.year = t2.year AND t1.max_value = t2.value
   ORDER BY t2.value DESC
   """
)


min_temps = sqlContext.sql(
   """
   SELECT DISTINCT t2.year, t2.station, t2.value
   FROM temps_readings t2
   INNER JOIN
   (SELECT year, min(value) AS min_value
   FROM temps_readings
   WHERE year >= 1950 AND year <= 2014
   GROUP BY year) t1
   ON t1.year = t2.year AND t1.min_value = t2.value
   ORDER BY t2.value DESC
   """
)

max_temps.rdd.saveAsTextFile("1_max_temps")
min_temps.rdd.saveAsTextFile("1_min_temps")

#[myprint(line) for line in max_temps.rdd.collect()]
#[myprint(line) for line in min_temps.rdd.collect()]
