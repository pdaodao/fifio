package com.github.apengda.fifio.kafka;

import com.github.apengda.fifio.jdbc.DbMetaDialect;
import com.github.apengda.fifio.jdbc.DbMetaDialectFactory;

public class KafkaDialectFactory implements DbMetaDialectFactory {

    @Override
    public boolean accept(String url, String typeName) {
        if (typeName == null) {
            return false;
        }
        return KafkaDialect.IDENTIFIER.equalsIgnoreCase(typeName.trim());
    }

    @Override
    public DbMetaDialect create() {
        return new KafkaDialect();
    }
}
