### csv -> dataFrame (v1.6)
1. var rdd = textFile("")
2. rdd.split(",")
3. var df = rdd.toDF( scheme 정의 | 스키마 무시하고 사용할때 캐스팅해서 사용)

### csv -> dataFrame (v 2.3)
1. spark.read.csv(.csv)
2. var df = rdd.toDF( scheme 정의 | 스키마 무시하고 사용할때 캐스팅해서 사용)
