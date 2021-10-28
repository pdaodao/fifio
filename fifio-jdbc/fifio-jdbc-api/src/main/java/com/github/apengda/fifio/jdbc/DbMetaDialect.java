package com.github.apengda.fifio.jdbc;

import com.github.apengda.fifio.jdbc.frame.TableInfo;

import java.sql.Connection;
import java.sql.SQLException;
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

    List<String> listDatabases(Connection connection) throws SQLException;

    List<String> listTables(Connection connection, String databaseName, String tablePattern) throws SQLException;

    List<String> listViews(Connection connection, String databaseName) throws SQLException;

    TableInfo tableInfo(Connection connection, String databaseName, String tableName) throws SQLException;

    String toFlinkType(TableInfo.TableColumn tableColumn);
}
