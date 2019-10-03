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



r3.coalesce(1).write.mode("overwrite").format("parquet").option("compression","gzip").save("/FileStore/tables/cca4/problem3/solution/")



spark.read.parquet("/FileStore/tables/cca4/problem3/solution/").printSchema()
spark.read.parquet("/FileStore/tables/cca4/problem3/solution/").show()

```

```

var d4 = spark.read.format("csv").load("/FileStore/tables/data/accounts")
d4.printSchema()
d4.show(5)



d4.show(5)



d4.createOrReplaceTempView("customer4")

var r4 = spark.sql("""
  select int(_c0) as id,  _c3 as fname,  _c4 as lname,  _c5 as street,  _c6 as city,  _c7 as state,  int(_c8) as zip from customer4
""")
r4.printSchema()
r4.show()



r4.coalesce(1).write.format("parquet").option("compression","snappy").save("/FileStore/tables/cca4/problem4/solution/")



spark.read.parquet("/FileStore/tables/cca4/problem4/solution/").printSchema()
spark.read.parquet("/FileStore/tables/cca4/problem4/solution/").show()

```

```

var d5 = spark.read.format("csv").load("/FileStore/tables/data/accounts")
d5.printSchema()
d5.show()



d5.createOrReplaceTempView("customer5")
var r5 = spark.sql("""
  select _c6 as city, _c7 as state, count(1) from customer5 group by city, state
""")
r5.printSchema()
r5.show()




r5.coalesce(1).write.format("csv").option("sep","\t").option("header",false).save("/FileStore/tables/cca4/problem5/solution/")



spark.read.option("sep","\t").csv("/FileStore/tables/cca4/problem5/solution/").printSchema()
spark.read.option("sep","\t").csv("/FileStore/tables/cca4/problem5/solution/").show()

```

```

var d6 = spark.read.format("parquet").load("/FileStore/tables/data/accounts.parquet")
d6.printSchema()
d6.show()



d6.createOrReplaceTempView("employee6")
var r6 = spark.sql("""
  select acct_num, first_name, last_name, concat(substr(first_name,0,1),last_name) as alias from employee6
""")
r6.printSchema()
r6.show()



r6.coalesce(1).write.format("parquet").option("compression","snappy").save("/FileStore/tables/cca4/problem6/solution/")



spark.read.parquet("/FileStore/tables/cca4/problem6/solution/").printSchema()
spark.read.parquet("/FileStore/tables/cca4/problem6/solution/").show()

```

```

var d71 = spark.read.format("csv").option("sep","\t").load("/FileStore/tables/data/customer.txt")
d71.printSchema()
d71.show()



var d72 = spark.read.format("csv").option("sep","\t").load("/FileStore/tables/data/bill.txt")
d72.printSchema()
d72.show()



d71.createOrReplaceTempView("customer7")
d72.createOrReplaceTempView("billing7")
var r7 = spark.sql("""
  select concat_ws(" ",c._c1,c._c2) as full_name,b._c2 from customer7 c join billing7 b on c._c0 = b._c1 
""")
r7.printSchema()
r7.show()
r7.count()



var r72 = spark.sql("""
  select concat_ws(" ",c._c1,c._c2) as full_name,sum(float(b._c2)) from customer7 c join billing7 b on c._c0 = b._c1 group by c._c0,full_name
""")
r72.printSchema()
r72.show()
r72.count()



r7.coalesce(1).write.format("csv").option("sep","\t").save("/FileStore/tables/cca4/problem7/solution/")



spark.read.option("sep","\t").csv("/FileStore/tables/cca4/problem7/solution/").printSchema()
spark.read.option("sep","\t").csv("/FileStore/tables/cca4/problem7/solution/").show()

```

```

var d8 = spark.read.format("csv").load("/FileStore/tables/data/accounts")
d8.printSchema()
d8.show()



d8.createOrReplaceTempView("customer8")
var r8 = spark.sql("""
  select   concat_ws(" ", _c3, _c4) as full_name,  substr(_c10, 6, 5) as anni from customer8 order by anni
""")
r8.printSchema()
r8.show()



r8.coalesce(1).write.format("csv").option("sep","\t").save("/FileStore/tables/cca4/problem8/solution/")



spark.read.option("sep","\t").csv("/FileStore/tables/cca4/problem8/solution/").printSchema()
spark.read.option("sep","\t").csv("/FileStore/tables/cca4/problem8/solution/").show()


```

```

var d9 = spark.read.format("csv").option("sep",",").load("/FileStore/tables/data/devicestatus.csv")
d9.printSchema()
d9.show()



d9.createOrReplaceTempView("sensor")
var r9 = spark.sql("""
  select _c1 as model, avg(float(_c7)) as temp  from sensor where _c7 between 40 and 45 group by _c1
""")
r9.printSchema()
r9.show()



var r92 = spark.sql("""
  select _c1 as model, cast(avg(float(_c7)) as decimal(19,12)) as temp  from sensor where _c7 between 40 and 45 group by _c1
""")
r92.printSchema()
r92.show()


```

```

var r10 = spark.sql("""
  select _c1 as model, count(1) as temp  from sensor where _c7 between 40 and 45 group by _c1
""")
r10.printSchema()
r10.show(30)
r10.count()



var r11 = spark.sql("""
  select _c1 as model, _c7 as temp  from sensor where _c7 between 40 and 45
""")
r11.show()



r11.createOrReplaceTempView("sensor2")
var r12 = spark.sql("""
  select model, count(*) from sensor2 group by model
""")
r12.show(30)



r11.write.saveAsTable("sensor2")



spark.sql("""
  select * from sensor2
""").show()


```

