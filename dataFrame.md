### csv -> dataFrame (v1.6)
1. var rdd = textFile("")
2. rdd.split(",")
3. var df = rdd.toDF( scheme 정의 | 스키마 무시하고 사용할때 캐스팅해서 사용)

### csv -> dataFrame (v 2.3)
1. spark.read.csv(.csv)
2. var df = rdd.toDF( scheme 정의 | 스키마 무시하고 사용할때 캐스팅해서 사용)


###  device
```
val devDF = spark.read.json("/FileStore/tables/devices.json")
devDF.take(1)
devDF.printSchema()
devDF.show(5)
devDF.take(10).foreach(println)

var makeModelDF = devDF.select("make","model")
makeModelDF.show()

var roninDF = devDF.select("devnum","make","model").where("make == 'Ronin'")
roninDF.take(1)
```

###  account
```

var accountDF = sqlContext.read.table("accounts") //1.6
var accountDF = spark.read.table("accounts")
accountDF.take(1)
accountDF.where("zipcode = 94913")
         .write
         .option("header","true")
         .csv("/loudacre/accounts_zip94913")
```


### StructType
```
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val fields = List(StructField("devnum", LongType),
    StructField("release_dt", TimestampType),
    StructField("make", StringType),
    StructField("model", StringType),
    StructField("dev_type", StringType))

val struct = StructType(fields)


var devDF = spark.read.schema(struct).json("/FileStore/tables/devices.json")
devDF.take(1)
devDF.printSchema()
```



###
```
var accountsDF = spark.read.table("accounts")
var fnCol = accountsDF("first_name")
var lucyCol = (fnCol === "Lucy")

//조건만들기
accountsDF.select($"first_name",$"last_name", lucyCol).take(3)
accountsDF.where(lucyCol).show(5)

//groupby
accountsDF.groupBy("first_name").count().show(5)

```
### Join Account Data with Cellular Towers by Zip Code
 schema
```
parquet-tools schema ~/training_materials/data/base_stations.parquet

message spark_schema {
  optional int32 id;
  optional binary zip (UTF8);
  optional binary city (UTF8);
  optional binary state (UTF8);
  optional double lat;
  optional double lon;
}
```
head
```
parquet-tools head ~/training_materials/data/base_stations.parquet
id = 1
zip = 86502
city = Chambers
state = AZ
lat = 35.2375
lon = -109.523

id = 2
zip = 86514
city = Teec Nos Pos
state = AZ
lat = 36.7797
lon = -109.359

id = 3
zip = 85602
city = Benson
state = AZ
lat = 31.9883
lon = -110.2941

id = 4
zip = 86011
city = Flagstaff
state = AZ
lat = 35.6308
lon = -112.0524

id = 5
zip = 86016
city = Gray Mountain
state = AZ
lat = 35.6308
lon = -112.0524

```
#### parquet -> dataframe
```
var baseDF = spark.read.parquet("/FileStore/tables/loudacre/base_stations.parquet")
baseDF.take(1)

var accountsDF = spark.read.table("accounts2")
accountsDF.printSchema()
accountsDF.select("acct_num","zipcode")
.join(baseDF,$"zip"===$"zipcode")
.show()


var accountdeviceDF = spark.read
                           .option("inferSchema","true")
                           .option("header","true")
                           .csv("/FileStore/tables/loudacre/part_00000_f3b62dad_1054_4b2e_81fd_26e54c2ae76a-baf18.csv")
                           
var activeAccountDF = accountsDF.where(accountsDF("acct_close_dt") === null)
accountsDF.join(accountdeviceDF, accountdeviceDF("account_id") === accountsDF("acct_num")).show()


var deviceDF = spark.read.json("/FileStore/tables/devices.json")
deviceDF.groupBy("devnum").count().withColumnRenamed('count', 'active_num').collect()
```


##  Working With RDDs
```
hdfs dfs -put $DEVDATA/frostroad.txt /loudacre/ 
hdfs dfs -ls /loudacre/
```

```
var frostroadRdd = sc.textFile("/FileStore/tables/loudacre/frostroad.txt")
frostroadRdd.count()

var lines = frostroadRdd.collect()
for(line <- lines) println(line)

```


### Transform Data in an RDD 
```
hdfs dfs -put $DEVDATA/makes*.txt /loudacre/ 
hdfs dfs -ls /loudacre/ 
```

```
var makes1Rdd = sc.textFile("/FileStore/tables/loudacre/makes1.txt")
for(make <- makes1Rdd.collect()) println(make)

var makes2Rdd = sc.textFile("/FileStore/tables/loudacre/makes2.txt")
for(make <- makes2Rdd.collect()) println(make)

var allmakesRdd = makes1Rdd.union(makes2Rdd)
for(make <- allmakesRdd.collect()) println(make)

var uniqueMakesRdd =allmakesRdd.distinct
for(make <- uniqueMakesRdd.collect()) println(make)
```

###  Transforming Data Using RDDs 

```
var weblogsRdd = sc.textFile("/FileStore/tables/loudacre/weblogs/")
weblogsRdd.map(line => line.length).take(5)
var jpglogsRdd = weblogsRdd.filter(line => line.contains(".jpg"))
var lines = jpglogsRdd.take(5)
lines.foreach(println)         

```
