package com.github.apengda.fifio.odps.dialect;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableFilter;
import com.github.apengda.fifio.elasticsearch.DbMetaDialect;
import com.github.apengda.fifio.elasticsearch.frame.DbInfo;
import com.github.apengda.fifio.elasticsearch.frame.TableInfo;
import com.github.apengda.fifio.elasticsearch.util.DbUtil;
import com.github.apengda.fifio.odps.util.OdpsUtil;
import com.google.common.collect.Lists;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.aliyun.odps.OdpsType.*;

public class OdpsDialect implements DbMetaDialect {

    private static boolean typeEquals(String typeName, OdpsType... types) {
        for (OdpsType type : types) {
            if (type.name().equals(typeName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String dialectName() {
        return "odps";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.aliyun.odps.jdbc.OdpsDriver");
    }

    @Override
    public String test(DbInfo dbInfo) throws Exception {
        Odps odps = OdpsUtil.initOdps(dbInfo);
        return odps.getLogViewHost();
    }

    @Override
    public List<String> listDatabases(DbInfo dbInfo) throws SQLException {
        return Lists.newArrayList(dbInfo.getDbName());
    }

    @Override
    public List<String> listTables(DbInfo dbInfo, String databaseName, String tablePattern) throws Exception {
        Odps odps = OdpsUtil.initOdps(dbInfo);
        final List<String> tables = new ArrayList<>();
        TableFilter tableFilter = null;
        if (tablePattern != null) {
            tableFilter = new TableFilter();
            tableFilter.setName(tablePattern);
        }
        odps.tables()
                .iterable(databaseName, tableFilter)
                .forEach(table -> tables.add(table.getName()));
        return tables;
    }

    @Override
    public List<String> listViews(DbInfo dbInfo, String databaseName) throws SQLException {
        return Collections.emptyList();
    }

    @Override
    public TableInfo tableInfo(DbInfo dbInfo, String databaseName, String tableName) throws SQLException {
        tableName = DbUtil.getTableName(tableName);
        final Odps odps = OdpsUtil.initOdps(dbInfo);
        final TableInfo tableInfo = new TableInfo(databaseName, tableName);
        Table table = odps.tables().get(databaseName, tableName);
        table.getSchema().getColumns().forEach(f -> {
            TableInfo.TableColumn tableColumn = new TableInfo.TableColumn(f.getName(), f.getTypeInfo().getOdpsType().name());
            tableInfo.addColumn(tableColumn);
        });
        return tableInfo;
    }

    @Override
    public String toFlinkType(TableInfo.TableColumn tableColumn) {
        final String type = tableColumn.typeName.toUpperCase();
        if (typeEquals(type, STRING, CHAR, VARCHAR)) {
            return "STRING";
        }
        if (typeEquals(type, BOOLEAN)) {
            return "BOOLEAN";
        }
        if (typeEquals(type, TINYINT)) {
            return "TINYINT";
        }
        if (typeEquals(type, SMALLINT)) {
            return "SMALLINT";
        }
        if (typeEquals(type, INT)) {
            return "INT";
        }
        if (typeEquals(type, BIGINT)) {
            return "BIGINT";
        }
        if (typeEquals(type, FLOAT)) {
            return "FLOAT";
        }
        if (typeEquals(type, DOUBLE)) {
            return "DOUBLE";
        }
        if (typeEquals(type, DATE)) {
            return "DATE";
        }
        if (typeEquals(type, TIMESTAMP)) {
            return "TIMESTAMP";
        }
        throw new IllegalArgumentException("unknown column type:" + tableColumn.typeName + " of column:" + tableColumn.name);
    }
}
