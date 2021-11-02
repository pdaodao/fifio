package com.github.apengda.fifio.odps.util;

import com.aliyun.odps.*;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.type.TypeInfo;
import com.github.apengda.fifio.elasticsearch.frame.DbInfo;
import org.apache.commons.collections.map.CaseInsensitiveMap;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OdpsUtil {

    public static Odps initOdps(DbInfo dbInfo) {
        return initOdps(dbInfo.getUrl(), dbInfo.getDbName(), dbInfo.getUsername(), dbInfo.getPassword());
    }

    public static Odps initOdps(final String url, String project, String accessId, String accessKey) {
        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.getRestClient().setConnectTimeout(10);
        odps.getRestClient().setReadTimeout(60);
        odps.getRestClient().setRetryTimes(2);
        odps.setDefaultProject(project);
        odps.setEndpoint(url);
        return odps;
    }

    public static String checkAndConvertPartitionSpecFormat(String partition) {
        if (partition == null) {
            return partition;
        } else {
            String odpsPartition = partition.replace("/", ",").replace(" ", "");
            try {
                return new PartitionSpec(odpsPartition).toString().replace("'", "");
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid partition format: \"" + partition + "\"");
            }
        }
    }

    public static Table getTable(Odps odps, String projectName, String tableName) {
        return odps.tables().get(projectName, tableName);
    }

    public static List<Column> columnsInfo(final TableSchema tableSchema,
                                           final List<String> columns) {
        // 字段信息
        final Map<String, TypeInfo> columnTypeMap = new CaseInsensitiveMap();
        tableSchema.getColumns().stream().forEach(t -> {
            columnTypeMap.put(t.getName(), t.getTypeInfo());
        });
        return columns.stream().map(t -> {
            return new Column(t, columnTypeMap.get(t));
        }).collect(Collectors.toList());
    }


    public static boolean isPartitioned(Table table) {
        if (table.isVirtualView()) {
            return false;
        }
        return table.getSchema().getPartitionColumns().size() > 0;
    }

    /**
     * 数据表分片
     *
     * @param table
     * @return
     */
    public static List<String> getPartitionsString(Table table) {
        return table.getPartitions().stream().map(t -> t.getPartitionSpec().toString()).collect(Collectors.toList());
    }

}
