## Overview
Analysis of City Of Chicago Taxi Trip Dataset Using AWS EMR, Spark, PySpark, Zeppelin and Airbnb's Superset

## City of Chicago Taxi Trips Dataset
https://data.cityofchicago.org/Transportation/Taxi-Trips-Dashboard/spcw-brbq
https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew

![](screenshots/superset_dashboard.PNG)

## Step 1: Data Shaping/Munging
- Analyze Raw Data
- Perform Basic Transformations
	- Rename Columns
	- Data Type Changes
	- Add New Columns
	- Filter Rows
	- Filter Columns

[chi-taxi-data-csv-aws-parquet.py](chi-taxi-data-csv-aws-parquet.py) performs the basic data munging and saves the Spark DataFrame into Parquet format. Below is the Spark DataFrame schema after all transformations.

```bash
root
 |-- TripID: string (nullable = true)
 |-- TaxiID: string (nullable = true)
 |-- TripStartTS: timestamp (nullable = true)
 |-- TripEndTS: timestamp (nullable = true)
 |-- TripSeconds: integer (nullable = true)
 |-- TripMiles: double (nullable = true)
 |-- PickupCensusTract: long (nullable = true)
 |-- DropoffCensusTract: long (nullable = true)
 |-- PickupCommunityArea: integer (nullable = true)
 |-- DropoffCommunityArea: integer (nullable = true)
 |-- Fare: double (nullable = true)
 |-- Tips: double (nullable = true)
 |-- Tolls: double (nullable = true)
 |-- Extras: double (nullable = true)
 |-- TripTotal: double (nullable = true)
 |-- PaymentType: string (nullable = true)
 |-- Company: string (nullable = true)
 |-- PickupCentroidLatitude: double (nullable = true)
 |-- PickupCentroidLongitude: double (nullable = true)
 |-- PickupCentroidLocation: string (nullable = true)
 |-- DropoffCentroidLatitude: double (nullable = true)
 |-- DropoffCentroidLongitude: double (nullable = true)
 |-- DropoffCentroidLocation: string (nullable = true)
 |-- CommunityAreas: integer (nullable = true)
 |-- TripStartDT: date (nullable = true)
 |-- TripEndDT: date (nullable = true)
 |-- TripStartDay: integer (nullable = true)
 |-- TripStartMonth: integer (nullable = true)
 |-- TripStartYear: integer (nullable = true)
```

## Step 2: Convert To Parquet
Create Parquet Format for improved performance and resource optimization. Raw csv file size is ~40GB (105 Million Rows) so its better to split it into small files.

- Writing to Parquet format is quite cpu intensive, so make sure to use the right EC2 instance. My standalone Spark instance on `c4.8xlarge (36 vCPU, 60GiB)` took around 4hrs to create the parquet file of this ~40GB csv. The size of parquet file was ~7GB.
- See instructions on [how to setup Standalone Spark/PySpark on EC2]( https://gist.github.com/codspire/ee4a46ec054f962d9ef028b27fcb2635)
- Below is the `spark-submit` command to submit the job for parquet file conversion
```bash
spark-submit  --master local[*]  --verbose --name ConvertTextToParquet --driver-memory 20g  --executor-memory 10g --num-executors 4 --conf spark.local.dir=/mnt/spark-tmp/ /mnt/data/chi-taxi-data-csv-aws-parquet.py
```

- `local[*]` enables utilization all available cpu cores
- `spark.local.dir` specified directory that Spark will use for writing temp files. Default is the system tmp folder which may run out of space so its better to overwrite the path using this conf
- Though you can directly read/write file on S3 (s3a://), I preferred creating the file locally and then using `aws s3 sync` to copy over to S3

## Step 3: Basic Data Analysis Using PL/SQL (Hive, Spark)
### Lauch EMR Cluster
- Launch an EMR cluster with Spark, Zeppelin, Ganglia, Hive, Presto, Hue (latest version is emr-5.6.0 at the time of writing)
- Enter below Spark configuration on the lauch wizard to ensure we are using KryoSerializer and Python v3.4

```bash
[{"classification":"spark","properties":{"maximizeResourceAllocation":"true"}},{"classification":"spark-defaults","properties":{"spark.serializer":"org.apache.spark.serializer.KryoSerializer"}},{"configurations":[{"classification":"export","properties":{"PYSPARK_PYTHON":"python34"}}],"classification":"spark-env","properties":{}}]
```

### Perform Basic Analysis Using Zeppelin
Example [Zeppelin Notebook](note.json)

[See screenshots here](zeppelin.md)

## Step 4: Create External Hive Table Using Parquet Format
```bash
$ hive
```
```sql
CREATE EXTERNAL TABLE taxi_trips (
    TripID STRING,
    TaxiID STRING,
    TripStartTS TIMESTAMP,
    TripEndTS TIMESTAMP,
    TripSeconds INT,
    TripMiles DOUBLE,
    PickupCensusTract BIGINT,
    DropoffCensusTract BIGINT,
    PickupCommunityArea INT,
    DropoffCommunityArea INT,
    Fare DOUBLE,
    Tips DOUBLE,
    Tolls DOUBLE,
    Extras DOUBLE,
    TripTotal DOUBLE,
    PaymentType STRING,
    Company STRING,
    PickupCentroidLatitude DOUBLE,
    PickupCentroidLongitude DOUBLE,
    PickupCentroidLocation STRING,
    DropoffCentroidLatitude DOUBLE,
    DropoffCentroidLongitude DOUBLE,
    DropoffCentroidLocation STRING,
    CommunityAreas INT,
    TripStartDT DATE,
    TripEndDT DATE,
    TripStartDay INT,
    TripStartMonth INT,
    TripStartYear INT)
stored as parquet
location 's3a://public-datasets-playground-062117/chicago-taxi-data/parquet/chi-taxi-trips-all/';
```
```sql
msck repair table taxi_trips;           /* recover partitions */
```

### Validate Table in Presto

```sql
$ presto-cli

presto> use hive.default;

presto:default> select hour(TripStartTS) as hour, avg(TripSeconds)/60 as tripInMins, count(1) as count from taxi_trips group by hour(TripStartTS) order by count desc;
 hour |     tripInMins     |  count
------+--------------------+---------
   19 | 12.585234551989377 | 7104377
   18 | 14.042581427486926 | 6745440
   20 | 11.444035802385878 | 6548069
   17 | 14.843894753815928 | 5956555
   21 | 10.946652785995106 | 5767988
   22 | 10.683772844717275 | 5493497
   16 | 15.134574050879708 | 5427821
   13 | 12.688132510566179 | 5167909
   15 | 14.585873584152186 | 5158962
   14 | 13.393816767167303 | 5122017
   12 |  12.33043721606718 | 5024320
   23 | 10.323169864874446 | 5002130
    9 | 12.845914319166274 | 4745949
   10 | 12.103471554779185 | 4649520
   11 | 12.004151887297978 | 4628539
    0 | 10.075400069414952 | 4168664
    8 | 13.478928875400731 | 3835823
    1 |  9.664002302442155 | 3503534
    2 |  9.202288828088022 | 2809336
    7 | 14.479037699934851 | 2360458
    3 |   9.40261201083416 | 2102572
    4 |  9.994485353629853 | 1401128
    6 | 14.478796399359839 | 1343408
    5 | 12.403897722965414 | 1103011
(24 rows)

Query 20170706_192558_00075_cmzkg, FINISHED, 2 nodes
Splits: 296 total, 296 done (100.00%)
0:07 [105M rows, 2.04GB] [15.4M rows/s, 306MB/s]
```
Presto Console can be accessed at
```bash
http://[EMR Master DNS Name]:8889/
```

## Step 5: Setup Superset for creating Dashboard
[Getting Started With Superset: Airbnb’s data exploration platform](https://gist.github.com/codspire/41dd399912fdafbefcd2f2eb76022363)

### Create Datasource in Superset to connect to Presto

Use the connection string in below format
```bash
presto://[EMR Master DNS Name]:8889/hive/default
```
- Go to Sources -> Create Table that matches the table in Presto
- Go to Slices -> Creare the Slice and add it to a Dashboard

[Here are some sample visualization of Taxi Trips](presto-superset.md)

