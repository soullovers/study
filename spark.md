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
var userids = logrdd.map(line => line.split(' ')(0) + "/" +line.split(' ')(2))
userids.take(5).foreach(println)

```


# data load
```
 hdfs dfs -put $DEVDATA/activations /loudacre/
 hdfs dfs -ls /loudacre/activations
 ```
## xml -> data

### Stub
```
// Stub code to copy into Spark Shell

import scala.xml._

// Given a string containing XML, parse the string, and 
// return an iterator of activation XML records (Nodes) contained in the string

def getActivations(xmlstring: String): Iterator[Node] = {
    val nodes = XML.loadString(xmlstring) \\ "activation"
    nodes.toIterator
}

// Given an activation record (XML Node), return the model name
def getModel(activation: Node): String = {
   (activation \ "model").text
}

// Given an activation record (XML Node), return the account number
def getAccount(activation: Node): String = {
   (activation \ "account-number").text

}

```

###  save to hdfs
```
var xmlfiles = "/loudacre/activations/*"
var xmlrdd = sc.wholeTextFiles(xmlfiles)
var result = xmlrdd.flatMap(pair => getActivations(pair._2)).flatMap(activation => getAccount(activation) + ":" + getModel(activation))
result.saveAsTextFile("/loudacre/account-models")

```

### 확인
```
hdfs dfs -ls /loudacre/account-models
hdfs dfs -cat /loudacre/account-models/part-00000
```




## bonus
```
 hdfs dfs -put ~/training_materials/data/devicestatus.txt /loudacre/
 var devicerdd = sc.textFile("/loudacre/devicestatus.txt")
 devicerdd.map(status => status.split(status.cha19))
```

## solution
```
// Upload data files to HDFS before running solution

// Example data:
//2014-03-15:10:10:33,Ronin S2,1a7eca8d-60c9-4d25-8609-d6cfd1ac80a1,0,24,82,72,enabled,enabled,enabled,41,62,36.49259162,-121.003629078
//2014-03-15:10:10:33/Titanic 2300/d86dbb9d-ff3c-40c6-8685-01f1fac45d9f/59/83/9/3/28/0/enabled/disabled/enabled/34.3456792864/-117.768326105

// Load the data file
val devstatus = sc.textFile("/loudacre/devicestatus.txt")

// Filter out lines with < 20 characters, use the 20th character as the delimiter, parse the line, and filter out bad lines
val cleanstatus = devstatus.
    filter(line => line.length>20).
    map(line => line.split(line.charAt(19))).
    filter(values => values.length == 14)
    
// Create a new RDD containing date, manufacturer, device ID, latitude and longitude
val devicedata = cleanstatus.
    map(values => (values(0), values(1).split(' ')(0), values(2), values(12), values(13)))

// Save to a CSV file as a comma-delimited string (trim parenthesis from tuple toString)
devicedata.
    map(values => values.toString).
    map(s => s.substring(1,s.length-1)).
    saveAsTextFile("/loudacre/devicestatus_etl")

```


## pair
```
pair => pair.swap
pair => pair._1
```
