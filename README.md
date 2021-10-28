# fifio 

apache flink source sink io (support apache flink 1.14.0)

## jdbc source sink 

support mysql、oracle、pg、sqlserver...
```sql
CREATE CATALOG hello WITH(
    'type' = 'myjdbc',
    'base-url' = 'jdbc:mysql://127.0.0.1:3306'
    'default-database' = 'hello',
    'username' = 'root', 
    'password' = 'root'
);
```

## odps source sink
```sql
CREATE CATALOG hello WITH(
    'type' = 'odps',
    'base-url' = 'https://odps.service.aliyun.com'
    'default-database' = 'hello',
    'username' = 'access-id', 
    'password' = 'access-key'
);
```

## elasticsearch source sink
```sql
CREATE CATALOG hello WITH(
    'type' = 'elasticsearch',
    'base-url' = 'http://127.0.0.1:9200',
    'username' = 'elastic', 
    'password' = 'elastic'
);
```

