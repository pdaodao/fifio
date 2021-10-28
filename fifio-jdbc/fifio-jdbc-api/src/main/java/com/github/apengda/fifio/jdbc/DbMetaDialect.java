package com.github.apengda.fifio.jdbc;

import com.github.apengda.fifio.jdbc.frame.DbInfo;
import com.github.apengda.fifio.jdbc.frame.TableInfo;

import java.util.List;
import java.util.Optional;

public interface DbMetaDialect {

    String dialectName();

    default Integer fetchSize() {
        return null;
    }

    default Optional<String> defaultDriverName() {
        return Optional.empty();
    }

    default String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    String test(final DbInfo dbInfo) throws Exception;

    List<String> listDatabases(final DbInfo dbInfo) throws Exception;

    List<String> listTables(final DbInfo dbInfo, String databaseName, String tablePattern) throws Exception;

    List<String> listViews(final DbInfo dbInfo, String databaseName) throws Exception;

    TableInfo tableInfo(final DbInfo dbInfo, String databaseName, String tableName) throws Exception;

    String toFlinkType(TableInfo.TableColumn tableColumn);
}
