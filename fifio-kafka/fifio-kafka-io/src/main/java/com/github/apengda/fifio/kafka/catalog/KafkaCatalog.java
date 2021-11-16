package com.github.apengda.fifio.kafka.catalog;

import com.github.apengda.fifio.jdbc.catalog.AbstractDbMetaCatalog;
import com.github.apengda.fifio.jdbc.frame.DbInfo;
import com.github.apengda.fifio.jdbc.util.DbUtil;
import com.github.apengda.fifio.kafka.KafkaDialect;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.Factory;

import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

public class KafkaCatalog extends AbstractDbMetaCatalog {

    public KafkaCatalog(String catalogName,
                        DbInfo dbInfo,
                        Map<String, String> options) {
        super(catalogName, KafkaDialect.PUBLIC, dbInfo, options);
    }

    @Override
    protected Map<String, String> buildTableProps(ObjectPath tablePath, Map<String, String> props) {
        props.put(CONNECTOR.key(), KafkaDialect.IDENTIFIER);
        props.put("topic", DbUtil.getTableName(tablePath.getObjectName()));
        props.put("properties.bootstrap.servers", dbInfo.getUrl());
        props.put("format", "json");
        return props;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new KafkaDynamicTableFactory());
    }
}