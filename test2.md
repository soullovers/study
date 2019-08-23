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

deviceDF.where($"make" === "MeeToo").show(5)
deviceDF.write
.mode("overwrite")
.option("header","true")
.option("compression","gzip")
.parquet("/FileStore/tables/problem3/solution")
```

## Problem 4. hdfs-> new file format (snappy) ->hdfs
```
var accountsDF = spark.read.csv("/FileStore/tables/accounts/accounts.txt")
accountsDF.show(5)

var cleanAccountDF = accountsDF.select($"_c0".alias("id"), 
                  $"_c3".alias("fname"),
                  $"_c4".alias("lname"),
                  $"_c5".alias("street"),
                  $"_c6".alias("city"),
                  $"_c7".alias("state"),
                  $"_c8".alias("zip")
                 )
cleanAccountDF.show(5)
cleanAccountDF.write
              .option("compression","snappy")
              .parquet("/FileStore/tables/problem4/solution")

```


## Problem 5 Calculate how many customers live in each city of the country. 
```
var accountsDF = spark.read.csv("/FileStore/tables/accounts/accounts.txt")
accountsDF.show(5)

var cleanAccountDF = accountsDF.select($"_c0".alias("id"), 
                  $"_c3".alias("fname"),
                  $"_c4".alias("lname"),
                  $"_c5".alias("street"),
                  $"_c6".alias("city"),
                  $"_c7".alias("state"),
                  $"_c8".alias("zip")
                 )
cleanAccountDF.show(5)

var groupByCityDF = cleanAccountDF.selectExpr("city", "state", "cast(count as string) as total_number")
groupByCityDF.show(5)
groupByCityDF.printSchema()


groupByCityDF.write
.mode("overwrite")
.option("delimiter","\n")
.csv("/FileStore/tables/problem5/solution/")
```

## Problem 6
#### test filte
```
var accountsDF = spark.read.csv("/FileStore/tables/accounts/accounts.txt")
accountsDF.show(5)

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

```
var accountsDF = spark.read.format("parquet")
                      .load("/FileStore/tables/problem6/data/")
accountsDF.printSchema()
accountsDF.show()

accountsDF.createOrReplaceTempView("account")

val sqlDF = spark.sql("SELECT first_name, last_name, concat(substring(first_name,0,1), last_name) as alias FROM account")
sqlDF.show()

sqlDF.write
.format("parquet")
.option("compression", "snappy")
.save("/FileStore/tables/problem6/solution/")

```

## Problem 7
```

```
