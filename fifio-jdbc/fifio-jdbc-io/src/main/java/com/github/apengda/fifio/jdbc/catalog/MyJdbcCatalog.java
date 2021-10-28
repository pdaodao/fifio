package com.github.apengda.fifio.jdbc.catalog;

import com.github.apengda.fifio.jdbc.frame.DbInfo;
import com.github.apengda.fifio.jdbc.util.DbUtil;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.*;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;


public class MyJdbcCatalog extends AbstractDbMetaCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(MyJdbcCatalog.class);

    public MyJdbcCatalog(String catalogName, String defaultDatabase, DbInfo dbInfo) {
        super(catalogName, defaultDatabase, dbInfo);
    }


    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new JdbcDynamicTableFactory());
    }

    protected Map<String, String> buildTableProps(ObjectPath tablePath) {
        final Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR.key(), IDENTIFIER);
        props.put(URL.key(), DbUtil.buildUrl(dbInfo.getUrl(), tablePath.getDatabaseName()));
        props.put(TABLE_NAME.key(), tablePath.getObjectName());
        props.put(USERNAME.key(), dbInfo.getUsername());
        props.put(PASSWORD.key(), dbInfo.getPassword());
        if (jdbcDialect.fetchSize() != null) {
            props.put("scan.fetch-size", Integer.toString(jdbcDialect.fetchSize()));
        }
        return props;
    }

}
