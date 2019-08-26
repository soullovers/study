### 1
```
sqoop eval \
--connect jdbc:mysql://gateway/problem1 \
--username cloudera
--password cloudera
--query "describe sensor"

sqoop import \
--connect jdbc:mysql://gateway/problem1 \
--username cloudera
--password cloudera
--table sensor
--target-dir /user/cert/problem1/solution/
--delete-target-dir
--fields=terminated-by ","
--columns "time,sensor,customer"
--m1
```

### 2
```
sqoop export \
--connect jdbc:mysql://gateway/problem2 \
--username cloudera
--password cloudera
--table solution
--export-dir /user/cert/problem2/data/customer/
--input-fields-terminated-by "\t"
```

### 3
```
var billingDF = spark.read.table("problem3.billing")
billingDF.createOrReplaceTempView("billing")

val sqlDF = spark.sql("""
  SELECT id,charge,code, tstamp FROM billing where charge > 10.00 
""")
sqlDF.show()


sqlDF.coalesce(1)
.write
.format("parquet")
.option("sep","\t")
.option("compression","gzip")
.save("/user/cert/problem3/solution/")

spark.read.parquet("/user/cert/problem3/solution/00.gz.parquet").printSchema()

```


### 4
```
spark-shell2 --driver-memory 2g --executor-memory 2g
```

```
var customerDF = spark.read
.json("/user/cert/problem4/data/customer")
customerDF.show(5)
customerDF.printSchema()


customerDF.write
.format("com.databricks.spark.avro")
.mode("overwrite")
.option("compression","snappy")
.save("/user/cert/problem4/solution/")
```

#### 확인
```
hdfs dfs -ls /user/cert/problem4/solution/
```


#### 5
```
var employeeDF = spark.read
.format("csv")
.option("header","false")
.load("/user/cert/problem5/data/employee/")
employeeDF.printSchema()
employeeDF.show()


employeeDF.createOrReplaceTempView("employee")

val sqlDF = spark.sql("""
  SELECT city, state,count(*) as total_number FROM employee 
  group by city, state 
""")
sqlDF.show()


sqlDF.coalesce(1)
.write
.option("sep",",")
.format("csv"
.save("/user/cert/problem5/solution/")



hdfs dfs -ls /user/cert/problem5/solution/
hdfs dfs -cat /user/cert/problem5/solution/part-00001.csv | head -5
```




### 6
```
var customerDF = spark.read
.format("com.databricks.spark.avro")
.load("/user/cert/problem6/data/customer/")
customerDF.printSchema()
customerDF.show()


customerDF.createOrReplaceTempView("customer")

val sqlDF = spark.sql("""
  SELECT concat_ws(' ', first, last) FROM customer 
  where state='TX'
""")
sqlDF.show()


sqlDF.write
.format("textfile")
.save("/user/cert/problem6/solution/")


hdfs dfs -ls /user/cert/problem6/solution/
hdfs dfs -cat /user/cert/problem6/solution/part-0001 |head -5

```

### 7
```
var billingDF = spark.read
.format("textfile")
.option("sep","\t")
.load("/user/cert/problem7/data/billing/")
billingDF.printSchema()
billingDF.show(5)

var customerDF = spark.read
.format("textfile")
.option("sep","\t")
.load("/user/cert/problem7/data/customer/")

billingDF.createOrReplaceTempView("billing")
customerDF.createOrReplaceTempView("customer")

var sqlDF = spark.sql("""
  select concat_ws(' ', fname, lname) as name, cast(sum(float(amount)) as decimal(9,2)) amt
  from customer c 
  join billing b on b.custid = c.id
  group by id, name
""")

sqlDF.write
.format("csv")
.option("sep"."\t")
.option("header","false")
.save("/user/cert/problem7/solution/")


hdfs dfs -ls /user/cert/problem7/solution/
hdfs dfs -cat /user/cert/problem7/solution/part-0001 |head -5

```


### 8
```
var customerDF = spark.read
.format("parquet")
.load("/user/cert/problem8/data/customer/")
customerDF.show(5)
customerDF.printSchema()

customerDF.createOrReplaceTempView("customer")
var sqlDF = spark.sql("""
  select last_name, first_name from customer
  order by last_name
""")


sqlDF.write
.format("parquet")
.option("compression","uncompressed")
.save("/user/cert/problem8/solution/")
```

### 9
```
var employeeDF = spark.read
.format("textfile")
.option("sep","\t")
.load("/user/cert/problem9/data/employee/")

var resultDF = employeeDF.select($"id",$"first_name",$"last_name",$"adress",$"city","$state",$"zip")


resultDF.format("csv")
.option("sep","|")
.save("/user/cert/problem9/solution/")


```
































