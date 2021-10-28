package com.github.apengda.fifio.jdbc.dialect;

import com.github.apengda.fifio.jdbc.DbMetaDialect;
import com.github.apengda.fifio.jdbc.frame.DbInfo;
import com.github.apengda.fifio.jdbc.frame.TableInfo;
import com.github.apengda.fifio.jdbc.util.DbUtil;

import java.sql.*;
import java.util.*;

public class CommonMetaDialect implements DbMetaDialect {

    @Override
    public String dialectName() {
        return "CommonJdbc";
    }

    @Override
    public String test(DbInfo dbInfo) throws Exception {
        try (Connection connection = getConnection(dbInfo, null)) {
            final DatabaseMetaData meta = connection.getMetaData();
            return meta.getDatabaseProductName() + "(" + meta.getDatabaseProductVersion() + ")";
        }
    }

    @Override
    public List<String> listDatabases(final DbInfo dbInfo) throws SQLException {
        try (Connection connection = getConnection(dbInfo, null)) {
            final List<String> dbs = new ArrayList<>();
            try (ResultSet rs = connection.getMetaData().getCatalogs()) {
                while (rs.next()) {
                    dbs.add(rs.getString(1));
                }
            }
            return dbs;
        }
    }

    @Override
    public List<String> listTables(final DbInfo dbInfo, String databaseName, String tablePattern) throws SQLException {
        return listTables(dbInfo, databaseName, tablePattern, DbUtil.TABLE);
    }

    @Override
    public List<String> listViews(final DbInfo dbInfo, String databaseName) throws SQLException {
        return listTables(dbInfo, databaseName, null, DbUtil.VIEW);
    }

    protected List<String> listTables(final DbInfo dbInfo, String databaseName, String tablePattern, String[] tableType) throws SQLException {
        try (Connection connection = getConnection(dbInfo, databaseName)) {
            final DatabaseMetaData metaData = connection.getMetaData();
            final List<String> tables = new ArrayList<>();
            String schemaPattern = null;
            String tableNamePattern = null;
            if (tablePattern != null) {
                schemaPattern = DbUtil.getTableSchema(tablePattern);
                tableNamePattern = DbUtil.getTableName(tablePattern);
            }
            try (ResultSet rs = metaData.getTables(databaseName, schemaPattern, tableNamePattern, tableType)) {
                while (rs.next()) {
                    String schema = rs.getString("TABLE_SCHEM");
                    String tableName = rs.getString("TABLE_NAME");
                    if (schema == null) {
                        tables.add(tableName);
                    } else {
                        tables.add(schema + "." + tableName);
                    }
                }
            }
            return tables;
        }
    }

    @Override
    public TableInfo tableInfo(final DbInfo dbInfo, final String databaseName, final String tableName) throws SQLException {
        try (Connection connection = getConnection(dbInfo, databaseName)) {
            final DatabaseMetaData meta = connection.getMetaData();
            final TableInfo tableInfo = new TableInfo(databaseName, tableName);

            try (ResultSet rs = meta.getColumns(databaseName, DbUtil.getTableSchema(tableName), DbUtil.getTableName(tableName), null)) {
                final Map<String, Boolean> rsFieldMap = new HashMap<>();
                final ResultSetMetaData rsMeta = rs.getMetaData();
                int count = rsMeta.getColumnCount();
                for (int i = 1; i <= count; i++) {
                    rsFieldMap.put(rsMeta.getColumnLabel(i), true);
                }
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String typeName = rs.getString("TYPE_NAME");
                    TableInfo.TableColumn columnInfo = new TableInfo.TableColumn(columnName, typeName);
                    tableInfo.addColumn(columnInfo);

                    columnInfo.position = rs.getInt("ORDINAL_POSITION");
                    columnInfo.remark = rs.getString("REMARKS");

                    if (rs.getString("COLUMN_SIZE") != null) {
                        columnInfo.size = rs.getInt("COLUMN_SIZE");
                    }
                    if (rs.getString("DECIMAL_DIGITS") != null) {
                        columnInfo.scale = rs.getInt("DECIMAL_DIGITS");
                    }
                    if (rsFieldMap.containsKey("IS_AUTOINCREMENT")) {
                        String autoincrement = rs.getString("IS_AUTOINCREMENT");
                        if ("YES".equalsIgnoreCase(autoincrement)) {
                            columnInfo.autoIncrement = true;
                        }
                    }
                    String nullable = rs.getString("IS_NULLABLE");
                    if ("NO".equalsIgnoreCase(nullable)) {
                        columnInfo.nullable = false;
                    }
                }
            }
            Collections.sort(tableInfo.getColumnList(), (o1, o2) -> Integer.compare(o1.position, o2.position));

            // pk
            try (ResultSet rs = meta.getPrimaryKeys(databaseName, DbUtil.getTableSchema(tableName),
                    DbUtil.getTableName(tableName))) {
                Map<Integer, String> keySeqColumnName = new HashMap<>();
                String pkName = null;
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    pkName = rs.getString("PK_NAME"); // all the PK_NAME should be the same
                    int keySeq = rs.getInt("KEY_SEQ");
                    keySeqColumnName.put(keySeq - 1, columnName); // KEY_SEQ is 1-based index
                }
                final List<String> pkFields =
                        Arrays.asList(new String[keySeqColumnName.size()]);
                keySeqColumnName.forEach(pkFields::set);
                tableInfo.setPkFields(pkFields);
                if (!pkFields.isEmpty()) {
                    pkName = pkName == null ? "pk_" + String.join("_", pkFields) : pkName;
                    tableInfo.setPkName(pkName);
                }
            }
            return tableInfo;
        }
    }

    protected Connection getConnection(DbInfo dbInfo, String databaseName) throws SQLException {
        final String url = DbUtil.buildUrl(dbInfo.getUrl(), databaseName);
        return DriverManager.getConnection(url, dbInfo.getUsername(), dbInfo.getPassword());
    }

    @Override
    public String toFlinkType(final TableInfo.TableColumn tableColumn) {
        if (tableColumn == null) {
            throw new IllegalArgumentException("convert to flink type column info is null!");
        }
        if (tableColumn.typeName == null) {
            throw new IllegalArgumentException(String.format("column '%s' type name is empty", tableColumn.name));
        }
        try {
            final String typeName = tableColumn.typeName.trim().toLowerCase();
            String type = doFromJdbcType(typeName, tableColumn.size, tableColumn.scale);
            if (!tableColumn.nullable) {
                type = type + " NOT NULL";
            }
            if (typeName.startsWith("_")) {
                return "ARRAY<" + type + ">";
            }
            return type;
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("column '%s' type  '%s'  convert to flink error", tableColumn.name, tableColumn.typeName, e));
        }
    }

    protected String doFromJdbcType(String typeName, Integer size, Integer scale) {
        if (typeName.contains("text") || typeName.contains("string")) {
            return "STRING";
        }
        if (typeName.contains("char")) {
            if (size == null) {
                return "STRING";
            }
            String type = "VARCHAR";
            if (typeName.equals("char")) {
                type = "CHAR";
            }
            if (size != null) {
                return type + "(" + size + ")";
            }
            return type;
        }
        if (typeName.contains("bool")) {
            return "BOOLEAN";
        }
        if (typeName.equalsIgnoreCase("bit")) {
            return "BOOLEAN";
        }
        if (typeName.contains("byte") || typeName.contains("binary") || typeName.contains("blob")) {
            return "BYTES";
        }
        if (typeName.contains("int") || typeName.contains("serial")) {
            if (typeName.contains("tinyint") && size != null && 1 == size) {
                return "BOOLEAN";
            }
            if (typeName.contains("int2") || typeName.equals("smallint") || typeName.contains("tinyint")) {
                return "SMALLINT";
            }
            if (typeName.equals("bigint unsigned")) {
                return "DECIMAL(20, 0)";
            }
            if (typeName.contains("big") || typeName.contains("int8")
                    || (typeName.contains("unsigned") && !typeName.contains("small"))) {
                return "BIGINT";
            }
            return "INT";
        }
        if (typeName.contains("float") || typeName.contains("real")) {
            if (typeName.contains("8")) {
                return "DOUBLE";
            }
            return "FLOAT";
        }

        if (typeName.contains("double")) {
            return "DOUBLE";
        }

        if (typeName.contains("numeric") || typeName.contains("decimal")) {
            if (size != null && scale != null && size > 0) {
                return String.format("DECIMAL(%d,%d)", size, scale);
            }
            return "DECIMAL(38,18)";
        }

        if (typeName.contains("timestamp")) {
            if (typeName.contains("timestamptz")) {
                return "TIMESTAMP_LTZ(6)";
            }
            return "TIMESTAMP";
        }
        if (typeName.contains("time")) {
            return size > 0 ? "TIME(" + scale + ")" : "TIME";
        }
        if (typeName.contains("date")) {
            return "DATE";
        }
        throw new UnsupportedOperationException(
                String.format("Doesn't support jdbc type '%s' yet", typeName));
    }
}
