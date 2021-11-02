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
CREATE CATALOG odps WITH(
    'type' = 'odps',
    'base-url' = 'http://service.cn.maxcompute.aliyun.com/api'
    'default-database' = 'hello',
    'username' = 'access-id', 
    'password' = 'access-key'
);

CREATE TABLE MyUserTable (
  id BIGINT,
  name STRING,
  age INT,
  status BOOLEAN,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'odps',
   'url' = 'http://service.cn.maxcompute.aliyun.com/api',
   'project' = 'users',
   'access-id' = 'access-id',
   'access-key' = 'access-key',
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

## kafka source sink
> only support json format
```sql 
CREATE CATALOG hello WITH(
    'type' = 'kafka',
    'base-url' = '127.0.0.1:9092',
    'properties.group.id' = 'testGroup', 
    'scan.startup.mode = 'earliest-offset'
);
```