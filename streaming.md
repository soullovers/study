
##  Write an Apache Spark Streaming Application 
### stubs
```
package stubs

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object StreamingLogs {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: solution.StreamingLogs <hostname> <port>")
      System.exit(1)
    } 
 
    // get hostname and port of data source from application arguments
    val hostname = args(0)
    val port = args(1).toInt
    
    // Create a new SparkContext
    val sc = new SparkContext()

    // Set log level to ERROR to avoid distracting extra output
    sc.setLogLevel("ERROR")

    // Create and configure a new Streaming Context 
    // with a 1 second batch duration
    val ssc = new StreamingContext(sc,Seconds(1))

    // Create a DStream of log data from the server and port specified   
    val logs = ssc.socketTextStream(hostname,port)

    // Filter the DStream to only include lines containing the string “KBDOC”
    val kbreqs = logs.filter(line => line.contains("KBDOC"))

    // Test application by printing out the first 5 lines received in each batch 
    kbreqs.print(5)

    // Save the filtered logs to text files
    kbreqs.saveAsTextFiles("/loudacre/streamlog/kblogs")

    // Print out the count of each batch RDD in the stream
    kbreqs.foreachRDD(rdd => println("Number of KB requests: " + rdd.count()))

    // Start the streaming context and then wait for application to terminate
    ssc.start()
    ssc.awaitTermination()

  }
}
```



### 실행
```
cd $DEVSH/exercises/spark-streaming/streaminglogs_project
mvn package 
spark-submit --master 'local[2]' --class stubs.StreamingLogs target/streamlog-1.0.jar localhost 1234
```


### 확인
```
hdfs dfs -ls /loudacre/streamlog

```



## Process Multiple Batches with Apache Spark Streaming
### stubs
```
package stubs

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object StreamingLogsMB {
  
  // Given an array of new counts, add up the counts 
  // and then add them to the old counts and return the new total
  def updateCount = (newCounts: Seq[Int], state: Option[Int]) => {
     val newCount = newCounts.foldLeft(0)(_ + _)
     val previousCount = state.getOrElse(0)
     Some(newCount + previousCount) 
  }
  
  
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: solution.StreamingLogsMB <hostname> <port>")
      System.exit(1)
    } 
 
    // get hostname and port of data source from application arguments
    val hostname = args(0)
    val port = args(1).toInt

    // Create a Spark Context
    val sc = new SparkContext()

    // Set log level to ERROR to avoid distracting extra output
    sc.setLogLevel("ERROR")

    // Configure the Streaming Context with a 1 second batch duration
    val ssc = new StreamingContext(sc,Seconds(1))

    // Create a DStream of log data from the server and port specified   
    val logs = ssc.socketTextStream(hostname,port)

    // Every two seconds, display the total number of requests over the 
    // last 5 seconds
    logs.countByWindow(Seconds(5),Seconds(2)).print()

    // Bonus: Display the top 5 users every second

    // Enable checkpointing (required for all window and state operations)
    ssc.checkpoint("logcheckpt")
    
    // Count requests by user ID for every batch
    val userreqs = logs.
       map(line => (line.split(" ")(2),1)).
       reduceByKey((x,y) => x+y)

    // Update total user requests
    val totalUserreqs = userreqs.updateStateByKey(updateCount)

    // Sort each state RDD by hit count in descending order    
    val topUserreqs=totalUserreqs.
       map(pair => pair.swap).
       transform(rdd => rdd.sortByKey(false)).
       map(pair => pair.swap)

    topUserreqs.print()
    


    ssc.start()
    ssc.awaitTermination()
  }
}

```

### 실행

```
$ cd \  $DEVSH/exercises/spark-streaming-multi/streaminglogsMB_project 
$ spark-submit --master 'local[2]' --class stubs.StreamingLogsMB target/streamlogmb-1.0.jar localhost 1234 
```
