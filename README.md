# fifio
apache flink source sink io

## jdbc source sink 
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
    'base-url' = 'http://127.0.0.1:9090',
    'username' = 'access-id', 
    'password' = 'access-key'
);
```

