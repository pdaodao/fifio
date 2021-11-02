package com.github.apengda.fifio.odps.catalog;

import com.github.apengda.fifio.elasticsearch.catalog.AbstractDbMetaCatalog;
import com.github.apengda.fifio.elasticsearch.frame.DbInfo;
import com.github.apengda.fifio.odps.OdpsDynamicTableFactory;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.Factory;

import java.util.Map;
import java.util.Optional;

import static com.github.apengda.fifio.odps.OdpsConstant.*;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

public class OdpsCatalog extends AbstractDbMetaCatalog {

    public OdpsCatalog(String catalogName, String defaultDatabase,
                       DbInfo dbInfo,
                       Map<String, String> options) {
        super(catalogName, defaultDatabase, dbInfo, options);
    }

    @Override
    protected Map<String, String> buildTableProps(ObjectPath tablePath, Map<String, String> props) {
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
