package com.github.apengda.fifio.jdbc.catalog;

import com.github.apengda.fifio.jdbc.JdbcDialect;
import com.github.apengda.fifio.jdbc.JdbcDialects;
import com.github.apengda.fifio.jdbc.frame.TableInfo;
import org.apache.flink.connector.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.UnresolvedDataType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.*;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;


public class MyJdbcCatalog extends AbstractJdbcCatalog {
    private final JdbcDialect jdbcDialect;

    public MyJdbcCatalog(String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
        this.jdbcDialect = JdbcDialects.get(baseUrl).orElseThrow(() -> new IllegalArgumentException("unsupported database " + baseUrl));
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (Connection conn = getConnection(null)) {
            return jdbcDialect.listDatabases(conn);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    public Connection getConnection(String dbName) throws SQLException {
        String url = defaultUrl;
        if (dbName != null) {
            url = this.baseUrl + dbName;
        }
        return DriverManager.getConnection(url, username, pwd);
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        try (Connection conn = getConnection(databaseName)) {
            return jdbcDialect.listTables(conn, databaseName, null);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing table in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        try (Connection connection = getConnection(tablePath.getDatabaseName())) {
            final TableInfo tableInfo = jdbcDialect.tableInfo(connection, tablePath.getDatabaseName(), tablePath.getObjectName());
            final Schema.Builder schemaBuilder = Schema.newBuilder();
            if (tableInfo.getPkFields() != null && !tableInfo.getPkFields().isEmpty()) {
                schemaBuilder.primaryKeyNamed(tableInfo.getPkName(), tableInfo.getPkFields());
            }
            for (TableInfo.TableColumn column : tableInfo.getColumnList()) {
                final UnresolvedDataType type = DataTypes.of(jdbcDialect.toFlinkType(column));
                schemaBuilder.column(column.name, type);
            }
            final Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR.key(), IDENTIFIER);
            props.put(URL.key(), defaultUrl);
            props.put(TABLE_NAME.key(), tablePath.getObjectName());
            props.put(USERNAME.key(), username);
            props.put(PASSWORD.key(), pwd);
            if (jdbcDialect.fetchSize() != null) {
                props.put("scan.fetch-size", Integer.toString(jdbcDialect.fetchSize()));
            }
            return CatalogTable.of(schemaBuilder.build(), null, new ArrayList<>(), props);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath objectPath) throws CatalogException {
        final String databaseName = objectPath.getDatabaseName();
        try (Connection connection = getConnection(databaseName)) {
            List<String> ts = jdbcDialect.listTables(connection, databaseName, objectPath.getObjectName());
            return ts.size() > 0;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("check table exists of '%s' failed  in catalog %s", objectPath.getObjectName(), getName()), e);
        }
    }
}
