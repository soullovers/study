## 파일-> RDD생성
```
var myrdd = sc.textFile("file:/home/training/training_materials/data/frostroad.txt")

myrdd.count()

myrdd.collect()
```




## 파일 확인
```
head -n 20 ~/training_materials/data/weblogs/2013-09-15.log
head -n 1 ~/training_materials/data/weblogs/2013-09-15.log
```

## 파일 -> hdfs
```
$ hdfs dfs -mkdir /loudacre 
$ hdfs dfs -put ~/training_materials/data/weblogs/ /loudacre/weblogs
```

## exam
```
scala> val logfiles="/loudacre/weblogs/*"
val logrdd = sc.textFile(logfiles) 
var jpglogsRdd=logrdd.filter(line => line.contains(".jpg"))
jpglogsRdd.take(10) 
jpglogsRdd.count()

```
## number of JPG
```
sc.textFile(logfiles).filter(line => line.cotains(".jpg")).count()
```
## 라인별 글자수
```
logrdd.map(line => line.length).take(5)

```


## iplist저장
```
logrdd.map(line => line.split(' ')).take(5)
logrdd.map(line => line.split(' ')(0)).take(5)
var ipsRdd=logrdd.map(line => line.split(' ')(0))
ipsRdd.take(10).foreach(println) 
ipsRdd.saveAsTextFile("/loudacre/iplist")
```
확인
```
hdfs dfs -ls /loudacre/iplist
```

###  ipaddress/userid
```
var userids = logrdd.map(line => line.split(' ')(0) +"/"+line.split(' ')(2))
```






