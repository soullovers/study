# sqoop import
```
sqoop eval \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--query "describe accounts"



sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--target-dir /loudacre/problem1/solution/ \
--table accounts \
--columns "acct_num,first_name,last_name,acct_create_dt"


hdfs dfs -ls /loudacre/problem1/solution/
hdfs dfs -cat /loudacre/problem1/solution/part-m-00000
```


## sqoop export (accounts)
```
mysql> create table accounts_new like accounts;

sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--target-dir /loudacre/problem2/ \
--table accounts \
--fields-terminated-by "\t"


sqoop export \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accounts_new \
--input-fields-terminated-by "\t" \
--export-dir "/loudacre/problem2/" 
```

## sqoop export
```
sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--target-dir /loudacre/problem2/ \
--table accounts \
--fields-terminated-by "\t"


[training@localhost ~]$ hdfs dfs -put ~/training_materials/data/base_stations.tsv /loudacre/
[training@localhost ~]$ hdfs dfs -ls /loudacre/


sqoop export \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table basestations_new \
--input-fields-terminated-by "\t" \
--export-dir "/loudacre/base_stations/" 
```


## hive table -> hdfs
```
var deviceDF = spark.read.table("devices_json")
deviceDF.show(5)

deviceDF.where($"make" === "MeeToo").show(5)
```
