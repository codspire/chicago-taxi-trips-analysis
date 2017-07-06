'''
Created on Jun 15, 2017

@author: rnagar
Dataset: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
https://stackoverflow.com/questions/37257111/reading-parquet-files-from-multiple-directories-in-pyspark
https://aws.amazon.com/blogs/big-data/submitting-user-applications-with-spark-submit/
'''
from datetime import date
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import dayofmonth
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import month
from pyspark.sql.functions import udf
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import year
from pyspark.sql.types import *
from pyspark.sql.types import DoubleType
from pyspark.sql.types import TimestampType

from IPython.core.display import display
from dateutil.parser import parse

dataFile = "s3a://public-datasets-playground-062117/chicago-taxi-data/uncompressed-files/taxi_trips.csv"
dataFile = "s3a://public-datasets-playground-062117/chicago-taxi-data/uncompressed-files/taxi_trips_part_1.csv"
outFilePath = "s3a://public-datasets-playground-062117/chicago-taxi-data/parquet/chi-taxi-trips-"
# dataFile = "hdfs:///tmp/taxi_trips.csv"
# outFilePath = "hdfs:///user/zeppelin/chicago-taxi-data/chi-taxi-trips-"
mergedFilePath = "s3a://public-datasets-playground-062117/chicago-taxi-data/parquet/chi-taxi-trips-all"
mergedFilePath = "/mnt/data/parquet/chi-taxi-trips-all"


def initSpark():
    global spark
    spark = SparkSession.builder.appName("ChicagoTaxi062117").getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set('spark.hadoop.parquet.enable.summary-metadata', 'false')
    return spark

'''
Cleanse money column (convert to  float by removing '$' from the data) 
''' 
def normalizeMoney(num):
    if num is not None:
        return float(num.replace('$', ''))
    else:
        return 0.0

'''
Create Date object from Date in String type
'''
def stringToDateTime(num):
    if num is not None:
        return parse(num)
    else:
        return None
    

'''
'''
def createTargetDF():
    
    taxiTripsDF = spark.read.format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("mode", "DROPMALFORMED") \
    .option("nullValue", "") \
    .option("treatEmptyValuesAsNulls", "true") \
    .load(dataFile)
   
   # 1: Remove spaces from column names 
    taxiTripsDF = taxiTripsDF.withColumnRenamed("Trip ID", "TripID") \
    .withColumnRenamed("Taxi ID", "TaxiID").withColumnRenamed("Trip Start Timestamp", "TripStartTS") \
    .withColumnRenamed("Trip End Timestamp", "TripEndTS").withColumnRenamed("Trip Seconds", "TripSeconds") \
    .withColumnRenamed("Trip Miles", "TripMiles").withColumnRenamed("Pickup Census Tract", "PickupCensusTract") \
    .withColumnRenamed("Dropoff Census Tract", "DropoffCensusTract").withColumnRenamed("Pickup Community Area", "PickupCommunityArea") \
    .withColumnRenamed("Dropoff Community Area", "DropoffCommunityArea").withColumnRenamed("Trip Total", "TripTotal") \
    .withColumnRenamed("Payment Type", "PaymentType").withColumnRenamed("Pickup Centroid Latitude", "PickupCentroidLatitude") \
    .withColumnRenamed("Pickup Centroid Longitude", "PickupCentroidLongitude") \
    .withColumnRenamed("Pickup Centroid Location", "PickupCentroidLocation") \
    .withColumnRenamed("Dropoff Centroid Latitude", "DropoffCentroidLatitude") \
    .withColumnRenamed("Dropoff Centroid Longitude", "DropoffCentroidLongitude") \
    .withColumnRenamed("Dropoff Centroid  Location", "DropoffCentroidLocation") \
    .withColumnRenamed("Community Areas", "CommunityAreas")
    
    dateTimeUdf = udf(stringToDateTime, TimestampType())
    moneyUdf = udf(normalizeMoney, DoubleType())
    
    # 2: Update data types of money and date time columns
    taxiTripsDF = taxiTripsDF.withColumn("Tips", moneyUdf(taxiTripsDF.Tips)).withColumn("Fare", moneyUdf(taxiTripsDF.Fare)) \
    .withColumn("Tolls", moneyUdf(taxiTripsDF.Tolls)).withColumn("Extras", moneyUdf(taxiTripsDF.Extras)) \
    .withColumn("TripTotal", moneyUdf(taxiTripsDF.TripTotal)).withColumn("TripStartTS", dateTimeUdf(taxiTripsDF.TripStartTS)) \
    .withColumn("TripEndTS", dateTimeUdf(taxiTripsDF.TripEndTS))
    
    # 3: Add date columns from timestamp
    taxiTripsDF = taxiTripsDF.withColumn('TripStartDT', taxiTripsDF['TripStartTS'].cast('date'))
    taxiTripsDF = taxiTripsDF.withColumn('TripEndDT', taxiTripsDF['TripEndTS'].cast('date'))

    # Print to check the data values
    taxiTripsDF.show(1, False)
    
    # 4: Extract Day, Month and Year from timestamp column
    taxiTripsDF = taxiTripsDF.withColumn("TripStartDay", dayofmonth(taxiTripsDF["TripStartDT"])) \
    .withColumn("TripStartMonth", month(taxiTripsDF["TripStartDT"])).withColumn("TripStartYear", year(taxiTripsDF["TripStartDT"]))
    
    # Print to check the data values
    taxiTripsDF.show(1, False)
    
    display(taxiTripsDF.dtypes)
    
    taxiTripsCachedDF = taxiTripsDF.cache()

    return taxiTripsCachedDF

# display(taxiTripsDF.agg({"TripStartDT": "max"}).collect())
# display(taxiTripsDF.agg({"TripStartDT": "min"}).collect())

def writeParquet(df):    
    for year in range(2012, 2017):
    #     annualTaxiTripsDF = df.select("*").filter((taxiTripsDF["TripStartDT"] >= date(year, 1, 1)) & (df["TripStartDT"] <= date(year, 12, 31)))
    #     annualDF = df.filter((df["TripStartDT"] >= date(year, 1, 1)) & (df["TripStartDT"] <= date(year, 12, 31)))
        annualDF = df.filter(df["TripStartDT"].between(date(year, 1, 1), date(year, 12, 31)))
        
    # using repartition(partition count) -- preferred
        annualDF.repartition(12).write \
        .option("mergeSchema", "false") \
        .option("spark.sql.parquet.mergeSchema", "false") \
        .option("filterPushdown", "true") \
        .option("spark.sql.parquet.filterPushdown", "true") \
        .option("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .option("nullValue", "") \
        .option("treatEmptyValuesAsNulls", "true") \
        .mode("append") \
        .parquet(outFilePath + str(year) + "-parquet")

    # using repartition(partition column) -- will create sub folders like TripStartMonth=1, TripStartMonth=2
    #    annualDF.write \
    #    .partitionBy("TripStartMonth") \
    #    .option("mergeSchema", "false") \
    #    .option("spark.sql.parquet.mergeSchema", "false") \
    #    .option("filterPushdown", "true") \
    #    .option("spark.sql.parquet.filterPushdown", "true") \
    #    .option("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    #    .mode("overwrite") \
    #    .parquet(outFilePath + str(year) + "-parquet")
        
        
def readParquet():
    path = "s3a://public-datasets-playground-062117/chicago-taxi-data/parquet/chi-taxi-trips-*-parquet"
    
    taxiTripsParquetDF = spark.read \
            .option("mergeSchema", "false") \
            .option("spark.sql.parquet.mergeSchema", "false") \
            .option("filterPushdown", "true") \
            .option("spark.sql.parquet.filterPushdown", "true") \
            .option("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .parquet(path)
    
    taxiTripsParquetDF.printSchema()
    print(taxiTripsParquetDF.count())
    return taxiTripsParquetDF

def mergeParquet(df):
    
    df.repartition(12).write \
    .option("mergeSchema", "false") \
    .option("spark.sql.parquet.mergeSchema", "false") \
    .option("filterPushdown", "true") \
    .option("spark.sql.parquet.filterPushdown", "true") \
    .option("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .option("nullValue", "") \
    .option("treatEmptyValuesAsNulls", "true") \
    .mode("append") \
    .parquet(mergedFilePath)        
        
def readAfterMerge():
    
    taxiTripsParquetDF = spark.read \
            .option("mergeSchema", "false") \
            .option("spark.sql.parquet.mergeSchema", "false") \
            .option("filterPushdown", "true") \
            .option("spark.sql.parquet.filterPushdown", "true") \
            .option("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .parquet(mergedFilePath)
    
    taxiTripsParquetDF.printSchema()
    print(taxiTripsParquetDF.count())

initSpark()
# print(spark)
df = createTargetDF()
writeParquet(df)
df2 = readParquet()
mergeParquet(df2)
readAfterMerge()