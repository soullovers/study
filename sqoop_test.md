```
sqoop eval \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--query "describe accounts"

```
```

sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accounts \
--fields-terminated-by ',' \
--columns "acct_num,first_name,last_name" \
--target-dir /loudacre/test3/problem1 \
--delete-target-dir
```

```
sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accounts \
--fields-terminated-by ',' \
--target-dir /loudacre/test3/problem1 \
--delete-target-dir
```

```
sqoop export \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accounts_new \
--export-dir /loudacre/test3/problem1 \
--input-fields-terminated-by ',' 
```


```
sqoop eval \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--query "select * from accounts_new limit 5"
```
