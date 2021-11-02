package com.github.apengda.fifio.kafka;

import com.github.apengda.fifio.elasticsearch.DbMetaDialect;
import com.github.apengda.fifio.elasticsearch.frame.DbInfo;
import com.github.apengda.fifio.elasticsearch.frame.TableInfo;
import com.github.apengda.fifio.kafka.util.KafkaUtil;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaDialect implements DbMetaDialect {
    public static final String PUBLIC = "public";
    public static final String IDENTIFIER = "kafka";
    public static final Map<String, TableInfo> tableInfoMap = new ConcurrentHashMap<>();

    @Override
    public String dialectName() {
        return IDENTIFIER;
    }

    @Override
    public String test(DbInfo dbInfo) throws Exception {
        try (Admin admin = KafkaUtil.admin(dbInfo)) {
            return admin.describeCluster(new DescribeClusterOptions().timeoutMs(2000)).clusterId().get();
        }
    }

    @Override
    public List<String> listDatabases(DbInfo dbInfo) throws Exception {
        return Arrays.asList(PUBLIC);
    }

    @Override
    public List<String> listTables(DbInfo dbInfo, String databaseName, String tablePattern) throws Exception {
        return KafkaUtil.topicList(dbInfo);
    }

    @Override
    public List<String> listViews(DbInfo dbInfo, String databaseName) throws Exception {
        return Collections.emptyList();
    }

    @Override
    public TableInfo tableInfo(final DbInfo dbInfo, String databaseName, final String tableName) throws Exception {
        TableInfo tableInfo = tableInfoMap.get(tableName);
        if (tableInfo != null) {
            return tableInfo;
        }
        synchronized (KafkaDialect.class) {
            List<String> list = KafkaUtil.recordListForMeta(dbInfo, tableName);
            tableInfo = KafkaUtil.guessTableInfo(tableName, list);
            if (tableInfo != null) {
                tableInfoMap.put(tableName, tableInfo);
            }
        }
        return tableInfo;
    }

    @Override
    public String toFlinkType(TableInfo.TableColumn tableColumn) {
        return tableColumn.typeName;
    }
}
