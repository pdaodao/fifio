package com.github.apengda.fifio.elasticsearch.catalog;

import com.github.apengda.fifio.elasticsearch.dialect.ElasticsearchDialect;
import com.github.apengda.fifio.elasticsearch.frame.DbInfo;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.Factory;

import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

public class ElasticsearchCatalog extends AbstractDbMetaCatalog {

    public ElasticsearchCatalog(String catalogName,
                                DbInfo dbInfo,
                                Map<String, String> options) {
        super(catalogName, ElasticsearchDialect.PUBLIC, dbInfo, options);
    }

    @Override
    protected Map<String, String> buildTableProps(ObjectPath tablePath, Map<String, String> props) {
        props.put(CONNECTOR.key(), ElasticsearchDialect.IDENTIFIER);
//        props.put(URL.key(), dbInfo.getUrl());
//        props.put(PROJECT.key(), tablePath.getDatabaseName());
//        props.put(TABLE.key(), tablePath.getObjectName());
//        props.put(ACCESSID.key(), dbInfo.getUsername());
//        props.put(ACCESSKEY.key(), dbInfo.getPassword());
        return props;
    }

    @Override
    public Optional<Factory> getFactory() {
//        return Optional.of(new OdpsDynamicTableFactory());
        return null;
    }
}
