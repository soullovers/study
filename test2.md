# sqoop import
```
sqoop eval \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--query "describe accounts"



sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--target-dir /loudacre/problem1/solution/ \
--table accounts \
--columns "acct_num,first_name,last_name,acct_create_dt"


hdfs dfs -ls /loudacre/problem1/solution/
hdfs dfs -cat /loudacre/problem1/solution/part-m-00000
```


## sqoop export (accounts)
```
mysql> create table accounts_new like accounts;

sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--target-dir /loudacre/problem2/ \
--table accounts \
--fields-terminated-by "\t"


sqoop export \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accounts_new \
--input-fields-terminated-by "\t" \
--export-dir "/loudacre/problem2/" 
```

### sqoop
```
--as-avrodatafile                                        
--as-parquetfile                                  
--as-sequencefile
--as-textfile
--compression-codec
--delete-target-dir

```


## sqoop export
```
sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--target-dir /loudacre/problem2/ \
--table accounts \
--fields-terminated-by "\t"


[training@localhost ~]$ hdfs dfs -put ~/training_materials/data/base_stations.tsv /loudacre/
[training@localhost ~]$ hdfs dfs -ls /loudacre/


sqoop export \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table basestations_new \
--input-fields-terminated-by "\t" \
--export-dir "/loudacre/base_stations/" 
```


## Problem 3. hive table -> hdfs (parquet)
```
var deviceDF = spark.read.table("devices_json")
deviceDF.show(5)

deviceDF.where($"make" === "MeeToo").show(5)

```

#### save
```
deviceDF.write
        .mode("overwrite")
        .format("parquet")
        .option("header","true")
        .option("compression","gzip")
        .save("/FileStore/tables/problem3/solution")
        
// avro
deviceDF.write
        .format("com.databricks.spark.avro")
        .mode("overwrite")
        .option("header","true")
        .option("compression","uncompressed")
        .save("/FileStore/tables/problem3/solution")
```

## Problem 4. hdfs-> new file format (snappy) ->hdfs
```
var accountsDF = spark.read.csv("/FileStore/tables/accounts/accounts.txt")
accountsDF.show(5)
```

#### etl
```
var cleanAccountDF = accountsDF.select($"_c0".alias("id"), 
                  $"_c3".alias("fname"),
                  $"_c4".alias("lname"),
                  $"_c5".alias("street"),
                  $"_c6".alias("city"),
                  $"_c7".alias("state"),
                  $"_c8".alias("zip")
                 )
cleanAccountDF.show(5)

```

#### save
```
cleanAccountDF.write
              .option("compression","snappy")
              .parquet("/FileStore/tables/problem4/solution")

```


## Problem 5 Calculate how many customers live in each city of the country. 
#### read
```
var accountsDF = spark.read.csv("/FileStore/tables/accounts/accounts.txt")
accountsDF.show(5)
```

#### etl
```
var cleanAccountDF = accountsDF.select($"_c0".alias("id"), 
                  $"_c3".alias("fname"),
                  $"_c4".alias("lname"),
                  $"_c5".alias("street"),
                  $"_c6".alias("city"),
                  $"_c7".alias("state"),
                  $"_c8".alias("zip")
                 )
cleanAccountDF.show(5)

var groupByCityDF = cleanAccountDF.groupBy("city","state")
                                  .count()
                                  .select($"city", $"state", $"count".alias("total_number"))
groupByCityDF.show(5)
groupByCityDF.printSchema()

```

#### save
```
groupByCityDF.coalesce(1)
             .write
             .mode("overwrite")
             .format("csv")
             .option("delimiter","\t")
             .option("header","false")
             .save("/FileStore/tables/problem5/solution/")
```

## Problem 6
#### test filte
```
var accountsDF = spark.read.csv("/FileStore/tables/accounts/accounts.txt")
accountsDF.show(5)
```

#### etl
```
var cleanAccountDF = accountsDF.select($"_c0".alias("id"), 
                  $"_c3".alias("first_name"),
                  $"_c4".alias("last_name"),
                  $"_c5".alias("street"),
                  $"_c6".alias("city"),
                  $"_c7".alias("state"),
                  $"_c8".alias("zip")
                 )
cleanAccountDF.show(5)
cleanAccountDF.write
              .mode("overwrite")
              .parquet("/FileStore/tables/problem6/data")
```
#### read
```
var accountsDF = spark.read.format("parquet")
                      .load("/FileStore/tables/problem6/data/")
accountsDF.printSchema()
accountsDF.show()
```

#### etl
```
accountsDF.createOrReplaceTempView("account")

val sqlDF = spark.sql("SELECT first_name, last_name, concat(substring(first_name,0,1), last_name) as alias FROM account")
sqlDF.show()
```

#### save
```
sqlDF.write
    .format("parquet")
    .option("compression", "snappy")
    .save("/FileStore/tables/problem6/solution/")

```

## Problem 7
#### read
```
var customerDF = spark.read
                      .format("csv")
                      .option("header","true")
                      .load("/FileStore/tables/problem7/customer.csv")
customerDF.show(5)
customerDF.printSchema()

var billingDF = spark.read
                     .format("csv")
                     .option("header","true")
                     .load("/FileStore/tables/problem7/billing.csv")
billingDF.show(5)
billingDF.printSchema()
```

#### etl
```
customerDF.createOrReplaceTempView("customer")
billingDF.createOrReplaceTempView("billing")

val sqlDF = spark.sql("""
    select concat_ws(' ',fname,lname) full_name, cast(sum(float(amount)) as decimal(9,2)) as amount
    from customer c join billing b on b.custid = c.id 
    group by full_name
""")
sqlDF.show(5)
```

#### save
```
sqlDF.coalesce(1)
     .write
     .format("csv")
     .mode("overwrite")
     .option("header","false")
     .option("sep","\t")
     .save("/FileStore/tables/problem7/solution/")

```

## Problem 8 
#### read
```
var employeesDF = spark.read
                       .format("csv")
                       .option("header","true")
                       .option("sep",",")
                       .load("/FileStore/tables/problem8/employees.csv")
employeesDF.show(5)
employeesDF.printSchema()
```

#### etl
```
var sqlDF = employeesDF.createOrReplaceTempView("employees")

var resultDF = spark.sql("""
    select concat_ws(' ', finst_name, last_name) as full_name, date_format(to_date(birthday,'dd/MM/yy'),'MM/dd') as anniversary from employees
    order by anniversary
""")
```

#### save
```
resultDF.coalesce(1)
        .write
        .mode("overwrite")
        .format("csv")
        .option("sep","\t")
        .option("header","false")
        .save("/FileStore/tables/problem8/solution/")
```

## Problem 9
#### read
```
var sensorDF = spark.read
                    .format("csv")
                    .option("header", "true")
                    .option("compression","uncompressed")
                    .load("/FileStore/tables/problem9/sensor.csv")

sensorDF.show(5)
sensorDF.printSchema()
```

#### etl
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

var resultDF = sensorDF
      .select(sensorDF("Phone_Model"), sensorDF("Temperature").cast(FloatType).alias("temp"))
      .groupBy("Phone_Model")
      .avg("temp")
resultDF.show(5)
```

#### sql
```
var sqlDF = sensorDF.createOrReplaceTempView("sensor")

var resultDF = spark.sql("""
    select Phone_Model, cast(avg(float(Temperature)) as decimal(14,12)) from sensor
    group by Phone_Model
""")
resultDF.show()
```

#### save
```
resultDF.coalesce(1)
        .write
        .mode("overwrite")
        .format("csv")
        .option("header","false")
        .option("compression","uncompressed")
        .option("sep",",")
        .save("/FileStore/tables/problem9/solution")
```


### compression
- uncompressed
