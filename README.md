# mysql 접속
```
mysql -u trining -p
show databases;

```

```
sqoop eval \
  --connect jdbc:mysql://localhost/loudacre \
  --username training --password training \
  --decribe

```
# sqoop import
```
sqoop import --table accounts \
  --connect jdbc:mysql://localhost/loudacre \
  --username training --password training \
  --columns "acct_num,first_name,last_name,state" \
  --where "state='CA'" \
  --target-dir /loudacre/accounts \
  --delete-target-dir \
  --fields-terminated-by "\t" 
```

## codec
```
sqoop import --table accounts \
  --connect jdbc:mysql://localhost/loudacre \
  --username training --password training \
  --columns "acct_num,first_name,last_name,state" \
  --where "state='CA'" \
  --target-dir /loudacre/accounts \
  --delete-target-dir \
  --fields-terminated-by "\t" \
  --compression-codec org.apache.hadoop.io.compress.SnappyCodec
```

## 데이터확인
```
sqoop eval \
  --connect jdbc:mysql://localhost/loudacre \
  --username training --password training \
  --query "describe accounts"
```

## hdfs확인
```
hdfs dfs -rm -R $target-dir
hdfs dfs -ls /loudacre/accounts
hdfs dfs -cat /loudacre/accounts/
```

## 테이블 생성
```
mysql> create table new_account like accounts
```

## export

```
sqoop export \
  --connect jdbc:mysql://localhost/loudacre \
  --username training --password training \
  --export-dir /loudacre/accounts \
  --fields-terminated-by "\t" \
  --table new_account
```
