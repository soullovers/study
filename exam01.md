```
var d3 = spark.read.table("billing")
d3.printSchema()
d3.show()

d3.createOrReplaceTempView("billing3")
var r3 = spark.sql("""
  select * from billing3 where charge > 5
""")
r3.printSchema()
r3.show()


r3.coalesce(1).write.mode("overwrite").format("parquet").option("compression", "gzip").save("/FileStore/tables/cca2/problem3/solution")


spark.read.parquet("/FileStore/tables/cca2/problem3/solution").printSchema()
spark.read.parquet("/FileStore/tables/cca2/problem3/solution").show()

```

```
var d4 = spark.read.format("json").load("/FileStore/tables/data/devices.json")
d4.printSchema()
d4.show()



d4.write.mode("overwrite").format("com.databricks.spark.avro").option("compression","snappy").save("/FileStore/tables/cca2/problem4/solution")

// COMMAND ----------

spark.read.format("com.databricks.spark.avro").load("/FileStore/tables/cca2/problem4/solution").printSchema()
spark.read.format("com.databricks.spark.avro").load("/FileStore/tables/cca2/problem4/solution").show()

```

```
// COMMAND ----------

var d5 = spark.read.format("csv").option("sep",",").load("/FileStore/tables/data/accounts")
d5.printSchema()
d5.show()

// COMMAND ----------

d5.createOrReplaceTempView("employee5")
var r5 = spark.sql("""
  select _c6, _c7, count(1) from employee5 group by _c6, _c7
""")
r5.printSchema()
r5.show()


// COMMAND ----------

r5.coalesce(1).write.mode("overwrite").format("csv").option("header", false).option("sep",",").save("/FileStore/tables/cca2/problem5/solution")

// COMMAND ----------

spark.read.format("csv").load("/FileStore/tables/cca2/problem5/solution").printSchema()
spark.read.format("csv").load("/FileStore/tables/cca2/problem5/solution").show()


```

```
// COMMAND ----------

var d6 = spark.read.format("csv").load("FileStore/tables/data/accounts")
d6.printSchema()
d6.show()

// COMMAND ----------

d6.createOrReplaceTempView("customer6")
var r6 = spark.sql("""
  select concat_ws(" " , _c3, _c4) as full_name from customer6 where _c7 = 'AZ'
""")
r6.printSchema()
r6.show()

// COMMAND ----------

r6.coalesce(1).write.mode("overwrite").format("csv").save("/FileStore/tables/cca2/problem6/solution")

// COMMAND ----------

spark.read.format("csv").load("/FileStore/tables/cca2/problem6/solution").printSchema()
spark.read.format("csv").load("/FileStore/tables/cca2/problem6/solution").show()

```

```
// COMMAND ----------

var d71 = spark.read.format("csv").option("sep","\t").load("/FileStore/tables/data/customer.txt")
d71.printSchema()
d71.show()

// COMMAND ----------

var d72 = spark.read.format("csv").option("sep","\t").load("/FileStore/tables/data/bill.txt")
d72.printSchema()
d72.show()

// COMMAND ----------

d71.createOrReplaceTempView("customer2")
d72.createOrReplaceTempView("billing2")

var r7 = spark.sql("""
  select concat_ws(" ",c._c1, c._c2) as full_name, b._c2 from customer2 c join billing2 b on c._c0=b._c1 
""")
r7.printSchema()
r7.show()

// COMMAND ----------

r7.coalesce(1).write.mode("overwrite").format("csv").option("sep","\t").save("/FileStore/tables/cca2/problem7/solution")

// COMMAND ----------

spark.read.format("csv").option("sep","\t").load("/FileStore/tables/cca2/problem7/solution").printSchema()
spark.read.format("csv").option("sep","\t").load("/FileStore/tables/cca2/problem7/solution").show()

```

```
// COMMAND ----------

var d8 = spark.read.format("parquet").load("/FileStore/tables/data/accounts.parquet")
d8.printSchema()
d8.show()

// COMMAND ----------

d8.createOrReplaceTempView("customer8")
var r8 = spark.sql("""
  select last_name, first_name from customer8 order by last_name
""")
r8.printSchema()
r8.show()


// COMMAND ----------

r8.coalesce(1).write.mode("overwrite").format("parquet").option("compression", "uncompressed").save("/FileStore/tables/cca2/problem8/solution")

// COMMAND ----------

spark.read.format("parquet").load("/FileStore/tables/cca2/problem8/solution").printSchema()
spark.read.format("parquet").load("/FileStore/tables/cca2/problem8/solution").show()

```

```
// COMMAND ----------

var d9 = spark.read.format("csv").load("/FileStore/tables/data/accounts")
d9.printSchema()
d9.show()

// COMMAND ----------

d9.createOrReplaceTempView("accounts9")
var r9 = spark.sql("""
  select _c0 as id, _c3 as fname,_c4 as lname,_c5 as addr,_c6 as city,_c7 as state,_c8 as zip from accounts9 
""")
r9.printSchema()
r9.show()


// COMMAND ----------

r9.coalesce(1).write.mode("overwrite").format("csv").option("sep","|").save("/FileStore/tables/cca2/problem9/solution")

// COMMAND ----------

spark.read.format("csv").option("sep","|").load("/FileStore/tables/cca2/problem9/solution").printSchema()
spark.read.format("csv").option("sep","|").load("/FileStore/tables/cca2/problem9/solution").show()

// COMMAND ----------

```
