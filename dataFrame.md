### csv -> dataFrame (v1.6)
1. var rdd = textFile("")
2. rdd.split(",")
3. var df = rdd.toDF( scheme 정의 | 스키마 무시하고 사용할때 캐스팅해서 사용)

### csv -> dataFrame (v 2.3)
1. spark.read.csv(.csv)
2. var df = rdd.toDF( scheme 정의 | 스키마 무시하고 사용할때 캐스팅해서 사용)


# device
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

# account
```
var accountDF = spark.read.table("accounts")
accountDF.take(1)
accountDF.where("zipcode = 94913").option("header","true").write()
```
