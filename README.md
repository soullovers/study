# mysql 접속
```
mysql -u trining -p
show databases;

```


# sqoop import
```
sqoop import --table accounts \
  --connect jdbc:mysql://localhost/loudacre \
  --username training --password taining \
  --columns "acct_num,first_name,last_name,state" \
  --where "state='CA'"
  --target-dir /loudacre/accounts
```

