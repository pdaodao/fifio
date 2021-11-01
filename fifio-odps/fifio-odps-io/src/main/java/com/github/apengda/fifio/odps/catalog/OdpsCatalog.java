package com.github.apengda.fifio.odps.catalog;

import com.github.apengda.fifio.jdbc.catalog.AbstractDbMetaCatalog;
import com.github.apengda.fifio.jdbc.frame.DbInfo;
import com.github.apengda.fifio.odps.OdpsDynamicTableFactory;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.Factory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.github.apengda.fifio.odps.OdpsConstant.*;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

public class OdpsCatalog extends AbstractDbMetaCatalog {

    public OdpsCatalog(String catalogName, String defaultDatabase, DbInfo dbInfo) {
        super(catalogName, defaultDatabase, dbInfo);
    }

    @Override
    protected Map<String, String> buildTableProps(ObjectPath tablePath) {
        final Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR.key(), IDENTIFIER);
        props.put(URL.key(), dbInfo.getUrl());
        props.put(PROJECT.key(), tablePath.getDatabaseName());
        props.put(TABLE.key(), tablePath.getObjectName());
        props.put(ACCESSID.key(), dbInfo.getUsername());
        props.put(ACCESSKEY.key(), dbInfo.getPassword());
        return props;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new OdpsDynamicTableFactory());
    }
}
