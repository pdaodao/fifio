package com.github.apengda.fifio.elasticsearch.dialect;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.apengda.fifio.base.JsonUtil;
import com.github.apengda.fifio.elasticsearch.DbMetaDialect;
import com.github.apengda.fifio.elasticsearch.frame.DbInfo;
import com.github.apengda.fifio.elasticsearch.frame.TableInfo;
import com.github.apengda.fifio.elasticsearch.util.EsUtil;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;

import java.util.*;
import java.util.stream.Collectors;


public class ElasticsearchDialect implements DbMetaDialect {
    public static final String PUBLIC = "public";
    public static final String IDENTIFIER = "elasticsearch";

    @Override
    public String dialectName() {
        return IDENTIFIER;
    }

    @Override
    public String test(DbInfo dbInfo) throws Exception {
        try (RestHighLevelClient client = EsUtil.createClient(dbInfo)) {
            MainResponse mainResponse = client.info(RequestOptions.DEFAULT);
            return mainResponse.getClusterName() + "(" + mainResponse.getVersion() + ")";
        }
    }

    @Override
    public List<String> listDatabases(DbInfo dbInfo) throws Exception {
        return Arrays.asList(PUBLIC);
    }

    @Override
    public List<String> listTables(DbInfo dbInfo, String databaseName, String tablePattern) throws Exception {
        try (RestHighLevelClient client = EsUtil.createClient(dbInfo)) {
            String res = EsUtil.get(client.getLowLevelClient(), "/_cat/indices", null, null);
            List<Map<String, Object>> rows = JsonUtil.MAPPER.readValue(res, new TypeReference<List<Map<String, Object>>>() {
            });
            return rows.stream().map(t -> (String) t.get("index")).collect(Collectors.toList());
        }
    }

    @Override
    public List<String> listViews(DbInfo dbInfo, String databaseName) throws Exception {
        return Collections.emptyList();
    }

    @Override
    public TableInfo tableInfo(DbInfo dbInfo, String databaseName, String tableName) throws Exception {
        final TableInfo tableInfo = new TableInfo(databaseName, tableName);
        try (RestHighLevelClient client = EsUtil.createClient(dbInfo)) {
            String res = EsUtil.get(client.getLowLevelClient(), String.format("/%s/_mapping", tableName), null, null);
            Map<String, Object> map = JsonUtil.MAPPER.readValue(res, Map.class);
            if (map != null) {
                map = (Map<String, Object>) map.values().iterator().next();
            }
            if (map != null && map.containsKey("mappings")) {
                Map<String, Object> mappings = (Map<String, Object>) map.get("mappings");
                if (mappings != null && mappings.containsKey("properties")) {
                    Map<String, Object> props = (Map<String, Object>) mappings.get("properties");
                    for (Map.Entry<String, Object> entry : props.entrySet()) {
                        String name = entry.getKey();
                        Map<String, Object> m = (Map<String, Object>) entry.getValue();
                        String type = Objects.toString(m.get("type"));
                        tableInfo.addColumn(new TableInfo.TableColumn(name, type));
                    }
                }
            }
        }
        return tableInfo;
    }

    @Override
    public String toFlinkType(TableInfo.TableColumn tableColumn) {
        final String typeName = tableColumn.typeName.toLowerCase();
        if (typeName.contains("text")
                || typeName.contains("keyword")
                || typeName.contains("document") || typeName.contains("string")) {
            return "STRING";
        }
        if (typeName.contains("bool")) {
            return "BOOLEAN";
        }
        if (typeName.contains("float")) {
            return "FLOAT";
        }
        if (typeName.contains("double")) {
            return "DOUBLE";
        }
        if (typeName.contains("smallint")) {
            return "SMALLINT";
        }
        if (typeName.contains("int")) {
            return "INT";
        }
        if (typeName.contains("long")) {
            return "BIGINT";
        }
        if (typeName.contains("date")) {
            return "DATE";
        }
        if (typeName.contains("time")) {
            return "TIME";
        }
        throw new UnsupportedOperationException(
                String.format("Doesn't support Elasticsearch type '%s' yet", typeName));
    }

}
