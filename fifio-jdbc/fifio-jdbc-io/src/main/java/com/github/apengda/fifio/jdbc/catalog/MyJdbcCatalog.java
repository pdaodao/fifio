package com.github.apengda.fifio.jdbc.catalog;

import com.github.apengda.fifio.jdbc.DbMetaDialect;
import com.github.apengda.fifio.jdbc.DbMetaDialectLoader;
import com.github.apengda.fifio.jdbc.frame.TableInfo;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalogUtils;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.*;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.util.Preconditions.checkArgument;


public class MyJdbcCatalog extends AbstractDbMetaCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(MyJdbcCatalog.class);
    protected final String username;
    protected final String pwd;
    protected final String baseUrl;
    protected final String defaultUrl;

    private final DbMetaDialect jdbcDialect;

    public MyJdbcCatalog(String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
        super(catalogName, defaultDatabase);

        checkArgument(!StringUtils.isNullOrWhitespaceOnly(username));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(pwd));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(baseUrl));

        JdbcCatalogUtils.validateJdbcUrl(baseUrl);

        this.username = username;
        this.pwd = pwd;
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        this.defaultUrl = this.baseUrl + defaultDatabase;
        this.jdbcDialect = DbMetaDialectLoader.load(baseUrl, null);
    }

    @Override
    public void open() throws CatalogException {
        // test connection, fail early if we cannot connect to database
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
        } catch (SQLException e) {
            throw new ValidationException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }

        LOG.info("Catalog {} established connection to {}", getName(), defaultUrl);
    }


    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new JdbcDynamicTableFactory());
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
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        try (Connection conn = getConnection(databaseName)) {
            return jdbcDialect.listViews(conn, databaseName);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing view in catalog %s", getName()), e);
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
