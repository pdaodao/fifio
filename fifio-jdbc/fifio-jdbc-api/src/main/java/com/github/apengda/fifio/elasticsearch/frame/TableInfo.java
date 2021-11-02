package com.github.apengda.fifio.elasticsearch.frame;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class TableInfo {
    private final String databaseName;
    private final String tableName;
    private List<TableColumn> columnList = new ArrayList<>();

    private String pkName;
    private List<String> pkFields;

    public TableInfo(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public TableInfo addColumn(TableColumn column) {
        if (column != null) {
            columnList.add(column);
        }
        return this;
    }

    public String getPkName() {
        return pkName;
    }

    public TableInfo setPkName(String pkName) {
        this.pkName = pkName;
        return this;
    }

    public List<String> getPkFields() {
        return pkFields;
    }

    public TableInfo setPkFields(List<String> pkFields) {
        this.pkFields = pkFields;
        return this;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<TableColumn> getColumnList() {
        return columnList;
    }

    public static class TableColumn {
        public final String name;
        public final String typeName;
        public int position = 0;
        public Boolean autoIncrement = false;
        public String remark;
        public Integer size = null;
        public Integer scale;
        public Boolean nullable = true;

        public TableColumn(String name, String typeName) {
            this.name = name;
            this.typeName = typeName;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", TableColumn.class.getSimpleName() + "[", "]")
                    .add("name='" + name + "'")
                    .add("typeName='" + typeName + "'")
                    .toString();
        }
    }
}
