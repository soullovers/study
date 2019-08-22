## Query DataFrames Using Column Expressions


#### 3. CreateanewDataFramecalledaccountsDF basedontheHiveaccounts table.
```
var accountsDF = spark.read.table("accounts")
```


### 4. Try a simple query with select, using both column reference syntaxes
```
accountsDF.select($"first_name",$"last_name")
accountsDF.select(accountsDF("first_name"),accountsDF("last_name"))
```

### 5. To explore column expressions, create a column object to work with, based on the first_name column in the accountsDF DataFrame.
```
var fnCol = accountsDF("first_name")
```

### 7. New Column objects are created when you perform operations on existing columns. Create a new Column object based on a column expression that identifies users whose first name is Lucy using the equality operator on the f n C o l object you created above.
```
var isLucy = (fnCol === "Lucy")
```

### 8. Use the lucyCol column expressionina select statement.Because lucyCol is based on a boolean expression, the column values will be true or false depending on the value of the f i r s t _ n a m e column. Confirm that users named Lucy are identified with the value true.
```
accountsDF.select($"first_name",$"last_name",isLucy).show()
```

### 9. The where operation requires a boolean-based column expression. Use the l u c y C o l column expression in a where transformation and view the data in the resulting DataFrame. Confirm that only users named Lucy are in the data.
```
accountsDF.where(isLucy).show()
```

### 10. Column expressions do not need to be assigned to a variable. Try the same query without using the l u c y C o l variable.
```
accountsDF.where($"first_name"==="Lucy").show(5)
accountsDF.where(fnCol==="Lucy").show(5)
```


### 11. Column expressions are not limited to where operations like those above. 
They can be used in any transformation for which a simple column could be used, such as a select.
Try selecting the city and state columns,and the first three characters of the phone_number column 
(in the U.S., the first three digits of a phone number are known as the area code). 
Use the substr operator on the phone_number column to extract the area code.
```
accountsDF.select($"city",$"state",accountsDF("phone_number").substr(1,3)).show(5)
```
### 12. Notice that in the last step, the values returned by the query were correct, but the column name was substring(phone_number, 1, 3),which is longand hard to work with. 
 Repeat the same query, using the alias operator to rename that column as area_code.
```
accountsDF.select($"city",$"state",$"phone_number".substr(1,3).alias("area_code")).show(5)
```

### 13. Perform a query that results in a DataFrame with just first_name and last_name columns, and only includes users whose first and last names both begin with the same two letters. 
(For example, the user Robert Roget would be included, because both his first and last names begin with “Ro”.)
```
accountsDF.where($"first_name".startsWith("Ro") && $"last_name".startsWith("Ro")).show(5)
```

## accountsDF struct
```
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val fields = List(StructField("acct_num", LongType),
    StructField("acct_create_dt", TimestampType),
    StructField("acct_close_dt", TimestampType),
    StructField("first_name", StringType),
    StructField("last_name", StringType),
    StructField("address", StringType),
    StructField("city", StringType),                  
    StructField("state", StringType),
    StructField("zipcode", StringType),
    StructField("phone_number", StringType),
    StructField("created", TimestampType),                  
    StructField("modified", TimestampType))

val struct = StructType(fields)

var accountsDF = spark.read.schema(struct).csv("/FileStore/tables/loudacre/accounts.txt")
accountsDF.printSchema()
accountsDF.show(5)
```

## Group and Count Data by Name

### 14. Query the accountsDF DataFrame using groupBy with count to find out the total number people sharing each last name. 
 (Note that the count aggregation transformation returns a DataFrame, unlike the count DataFrame action, which returns a single value to the driver.)
```
accountsDF.groupBy("first_name").count().show(5)
```

###v15. You can also group by multiple columns. Query accountsDF again, this time counting the number of people who share the same last and first name.
```
accountsDF.groupBy("first_name","last_name").count().show(5)
```

## Join Account Data with Cellular Towers by Zip Code

### 16. In this section, you will join the accounts data that you have been using with data about cell tower base station locations, which is in the base_stations.parquet HDFS loudacre directory . Start by reviewing the schema and a few records of the data. Use the parquet-tools command in a separate terminal window (not the one running the Spark shell).
 ```
$ parquet-tools schema hdfs://master-1/loudacre/base_stations.parquet 
$ parquet-tools head hdfs://master-1/loudacre/base_stations.parquet
```
### 18. Some account holders live in zip codes that have a base station. 
 Join baseDF and accountsDF to find those users, and for each, include their account ID, first name, last name, and the ID and location data for the base station in their zip code.
```
accountsDF.join(baseDF, accountsDF("zipcode") === baseDF("zip")).
          select("acct_num", "first_name", "last_name", "id", "lon","zip").show(5)
```

## Count Active Devices
### 19. The accountdevice CSV data files contain data lists all the devices used by all the accounts. 
 Each row in the data set includes a row ID, an account ID, a device ID for the type of device, the date the device was activated for the account, and the specific device’s ID for that account.
 The CSV data files are in the $DEVDATA/accountdevice directory . 
Review the data in the data set, then upload the directory and its contents to the HDFS directory /loudacre/accountdevice.
```
hdfs dfs -put $DEVDATA/accountdevice/ /loudacre/accountdevice
hdfs dfs -ls /loudacre/accountdevice/
```
### 20. Createa DataFrame based on the accountdevice datafiles.
```
var accountDeviceDF = spark.read
                            .option("inferSchema","true")
                            .option("header","true")
                            .csv("/FileStore/tables/loudacre/part_00000_f3b62dad_1054_4b2e_81fd_26e54c2ae76a-baf18.csv")

accountDeviceDF.show(5)
```

### 21. Use the account device data and the DataFrames you created previously in this exercise to find the total number of each device model across all active accounts 
 (that is, accounts that have not been closed). 
 The new DataFrame should be sorted from most to least common model. 
 Save the data as Parquet files in a directory called /loudacre/top_devices with the following columns:device_id, make, model, active_num
 ```
accountsDF.printSchema()
var activeAccountDF = accountsDF.where(accountsDF("acct_close_dt").isNull)
                          
activeAccountDF.show(5)

```
