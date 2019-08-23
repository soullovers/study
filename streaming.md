
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
spark-submit --master 'local[2]' --class stubs.StreamingLogs target/streamlog-1.0.jar localhost 1234
```
